/*
 Copyright 2016 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.notification;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraNotificationListener;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;
import com.gs.fw.common.mithra.transaction.UpdateOperation;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MithraProcessInfo;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockInputStream;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockOutputStream;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;



public class MithraNotificationEventManagerImpl implements MithraNotificationEventManager, MithraNotificationMessageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MithraNotificationEventManagerImpl.class);
    private static final String MESSAGE_PROTOCOL_VERSION = "P0-";

    //New Mithra application notifications
    private Map<RegistrationKey, RegistrationEntryList> mithraApplicationNotificationSubscriber = UnifiedMap.newMap();
    private Map mithraListToNotificationListenerMap = new IdentityHashMap();
    private Map mithraListToDatabaseIdentifierMap = new IdentityHashMap();

    private ConcurrentHashMap<RegistrationKey, MithraNotificationListener> mithraNotificationSubscriber = new ConcurrentHashMap<RegistrationKey, MithraNotificationListener>();
    private MithraMessagingAdapterFactory adapterFactory;
    private Map<String, MithraNotificationMessagingAdapter> subjectToAdapterMap = UnifiedMap.newMap();
    private Map<String, List<MithraNotificationEvent>> mithraNoTxNotificationEvents = UnifiedMap.newMap();
    private LinkedBlockingQueue channel;
    private ExecutorService queuedExecutor;
    private ScheduledExecutorService clockDaemon;
    private static final int PERIOD = 100;
    private LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(null, false);
    private LZ4BlockInputStream lz4BlockInputStream = new LZ4BlockInputStream(null);
    private volatile boolean shutdown;
    private Thread shutdownHook = null;

    public long getMithraVmId()
    {
        return MithraProcessInfo.getVmId();
    }

    public MithraNotificationEventManagerImpl(MithraMessagingAdapterFactory adapterFactory)
    {
        this(adapterFactory, true);
    }
    public MithraNotificationEventManagerImpl(MithraMessagingAdapterFactory adapterFactory, boolean useShutdownHook)
    {
        initializeNotificationHelperThreads();
        this.adapterFactory = adapterFactory;
        if (useShutdownHook)
        {
            setupShutdownHook();
        }
    }

    private void setupShutdownHook()
    {
        Thread hook = new Thread()
        {
            @Override
            public void run()
            {
                if (!shutdown)
                {
                    forceSendNow();
                    shutdown(true);
                }
            }
        };
        this.shutdownHook = hook;
        Runtime.getRuntime().addShutdownHook(hook);
    }

    private synchronized MithraNotificationMessagingAdapter getOrCreateAdapter(String subject)
    {
        MithraNotificationMessagingAdapter result = subjectToAdapterMap.get(subject);
        if (null == result)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Creating messaging adapter for subject: " + subject);
            }
            result = adapterFactory.createMessagingAdapter(encodeSubject(subject));
            result.setMessageProcessor(this);
            subjectToAdapterMap.put(subject, result);
        }
        return result;
    }

    private String encodeSubject(String subject)
    {
//        return subject;
        return MESSAGE_PROTOCOL_VERSION+subject;
    }

    private String decodeSubject(String subject)
    {
//        return subject;
        return subject.substring(MESSAGE_PROTOCOL_VERSION.length());
    }

    public void registerForApplicationNotification(String subject,
                                                   MithraApplicationNotificationListener listener, RelatedFinder finder, List mithraObjectList, Operation operation)
    {
        Runnable task = this.getApplicationNotificationSubscriptionRunnable(subject, finder, listener, mithraObjectList, operation);
        if (logger.isDebugEnabled())
        {
            logger.debug("***************** Adding List Notification Registration task to queue ***************************");
        }
        addTaskToQueue(task);
    }

    public void registerForApplicationClassLevelNotification(String subject,
            MithraApplicationClassLevelNotificationListener listener, RelatedFinder finder)
    {
        Runnable task = this.getApplicationClassLevelNotificationSubscriptionRunnable(subject, finder, listener);
        if (logger.isDebugEnabled())
        {
            logger.debug("***************** Adding Class-Level Notification Registration task to queue ***************************");
        }
        addTaskToQueue(task);
    }

    public void registerForNotification(String subject, MithraObjectPortal portal)
    {
        String finderClassName = portal.getFinder().getFinderClassName();
        RegistrationKey key = new RegistrationKey(subject, finderClassName);
        MithraNotificationListener existingListener = mithraNotificationSubscriber.get(key);
        if (existingListener == null)
        {
            MithraNotificationListener listener = portal.getCache().createNotificationListener(portal);
            if (mithraNotificationSubscriber.putIfAbsent(key, listener) == null)
            {
                Runnable task = this.getNotificationAdapterRunnable(subject);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Registered :" + listener.getFinderClassname() + " for subject: " + subject);
                    logger.debug("***************** Adding Mithra Notification Registration task to queue ***************************");
                }
                addTaskToQueue(task);
            }
        }
    }

    @Override
    public void initializeFrom(MithraNotificationEventManager old)
    {
        Set<RegistrationKey> existingRegistrations = UnifiedSet.newSet(old.getExistingRegistrations());
        old.shutdown();
        for(RegistrationKey key: existingRegistrations)
        {
            ReladomoClassMetaData reladomoClassMetaData = ReladomoClassMetaData.fromFinderClassName(key.getClassname());
            if (MithraManagerProvider.getMithraManager().getConfigManager().isClassConfigured(reladomoClassMetaData.getBusinessOrInterfaceClassName()))
            {
                registerForNotification(key.getSubject(), reladomoClassMetaData.getFinderInstance().getMithraObjectPortal());
            }
        }
    }

    @Override
    public Set<RegistrationKey> getExistingRegistrations()
    {
        return mithraNotificationSubscriber.keySet();
    }

    public Map getMithraListToNotificationListenerMap()
    {
        return this.mithraListToNotificationListenerMap;
    }

    public Map getMithraListToDatabaseIdentiferMap()
    {
        return this.mithraListToDatabaseIdentifierMap;
    }

    public void broadcastNotificationMessage(Map notificationEvents, long requestorVmId)
    {
        Runnable task = this.getSendMithraNotificationMessageRunnable(notificationEvents, requestorVmId);
        if (logger.isDebugEnabled())
        {
            logger.debug("***************** Adding BroadcastNotificationMessage task to queue ***************************");
        }
        addTaskToQueue(task);
    }

    private byte[] convertObjectToBytes(Object data)
            throws IOException
    {
        byte[] pileOfBytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(200);
        synchronized (this.lz4BlockOutputStream)
        {
            lz4BlockOutputStream.reset(bos);
            ObjectOutputStream oos = new ObjectOutputStream(lz4BlockOutputStream);
            oos.writeObject(data);
            oos.flush();
            lz4BlockOutputStream.finish();
            bos.flush();
            pileOfBytes = bos.toByteArray();
            bos.close();
        }
        return pileOfBytes;
    }

    private Object convertBytesToObject(byte[] input) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(input);
        Object result = null;
        synchronized (this.lz4BlockInputStream)
        {
            this.lz4BlockInputStream.reset(bis);
            ObjectInputStream ois = new ObjectInputStream(this.lz4BlockInputStream);
            result = ois.readObject();
            ois.close();
            this.lz4BlockInputStream.close();
            bis.close();
        }
        return result;
    }

    @Override
    public void processNotificationMessage(String subject, byte[] message)
    {
        subject = decodeSubject(subject);
        ExternalizableMithraNotificationMessage notificationMessage;
        try
        {
            notificationMessage = (ExternalizableMithraNotificationMessage) convertBytesToObject(message);

            long senderMithraVmId = notificationMessage.getMithraVmId();
            long senderRequestorVmId = notificationMessage.getRequestorVmId();

            if (logger.isDebugEnabled())
            {
                logger.debug("***************** Mithra: " + MithraProcessInfo.getVmId() + " received message sent by Mithra: " + senderMithraVmId + " on behalf of Mithra: " + senderRequestorVmId + "  with topic: " + subject);
            }

            if (MithraProcessInfo.getVmId() != senderMithraVmId && MithraProcessInfo.getVmId() != senderRequestorVmId)
            {
                Runnable task = this.getProcessIncomingMessagesRunnable(subject, notificationMessage.getNotificationEvents());
                if (logger.isDebugEnabled())
                {
                    logger.debug("***************** Mithra: " + MithraProcessInfo.getVmId() + " will process message received from Mithra: " + senderMithraVmId + " with topic: " + subject);
                }
                addTaskToQueue(task);
            }
        }
        catch (IOException e)
        {
            logger.error("Unable to deserialize Mithra notification message for subject " + subject, e);
        }
        catch (ClassNotFoundException e)
        {
            logger.error("Unable to deserialize Mithra notification message for subject " + subject, e);
        }
    }

    public void addMithraNotificationEvent(String databaseIdentifier, String classname, byte databaseOperation,
                                           MithraDataObject mithraDataObject, Object sourceAttribute)
    {
        MithraDataObject[] dataObjects = new MithraDataObject[1];
        dataObjects[0] = mithraDataObject;
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation, dataObjects, null, null, sourceAttribute);
    }

    // this is called from batch(I/U/D) database operations

    public void addMithraNotificationEvent(String databaseIdentifier, String classname, byte databaseOperation,
                                           List mithraObjects, Object sourceAttribute)
    {
        MithraDataObject[] dataObjects = new MithraDataObject[mithraObjects.size()];
        MithraDataObject dataObject;
        for (int i = 0; i < mithraObjects.size(); i++)
        {
            dataObject = ((MithraTransactionalObject) mithraObjects.get(i)).zGetTxDataForRead();
            dataObjects[i] = dataObject;
        }
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation, dataObjects, null, null, sourceAttribute);
    }

    public void addMithraNotificationEventForBatchUpdate(String databaseIdentifier, String classname, byte databaseOperation, List updateOperations, List updateWrappers, Object sourceAttribute)
    {
        MithraDataObject[] dataObjects = new MithraDataObject[updateOperations.size()];
        MithraDataObject dataObject;
        for (int i = 0; i < updateOperations.size(); i++)
        {
            dataObject = ((UpdateOperation) updateOperations.get(i)).getMithraObject().zGetTxDataForRead();
            dataObjects[i] = dataObject;
        }
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation, dataObjects, updateWrappers, null, sourceAttribute);
    }

    public void addMithraNotificationEventForMultiUpdate(String databaseIdentifier, String classname, byte databaseOperation, MultiUpdateOperation multiUpdateOperation, Object sourceAttribute)
    {
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation,
                multiUpdateOperation.getDataObjectsForNotification(), multiUpdateOperation.getUpdates(), null, sourceAttribute);
    }

    public void addMithraNotificationEventForMassDelete(String databaseIdentifier, String classname, byte databaseOperation, Operation operationForMassDelete)
    {
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation, null, null, operationForMassDelete, null);
    }

    //This is called from single insert, update or delete.

    public void addMithraNotificationEventForUpdate(String databaseIdentifier, String classname, byte databaseOperation,
                                                    MithraDataObject mithraDataObject, List updateWrappers,
                                                    Object sourceAttribute)
    {
        MithraDataObject[] dataObjects = new MithraDataObject[1];
        dataObjects[0] = mithraDataObject;
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation, dataObjects, updateWrappers, null, sourceAttribute);
    }

    public void addMithraNotificationEventForUpdate(String databaseIdentifier, String classname, byte databaseOperation,
                                                    MithraDataObject mithraDataObject, AttributeUpdateWrapper updateWrapper,
                                                    Object sourceAttribute)
    {
        MithraDataObject[] dataObjects = new MithraDataObject[1];
        dataObjects[0] = mithraDataObject;
        List updateWrappers = null;
        if (updateWrapper != null)
        {
            updateWrappers = ListFactory.create(updateWrapper);
        }
        createAndAddNotificationEvent(databaseIdentifier, classname, databaseOperation, dataObjects, updateWrappers, null, sourceAttribute);
    }

    private void createAndAddNotificationEvent(String databaseIdentifier, String classname, byte databaseOperation,
                                               MithraDataObject[] dataObjects, List updateWrappers, Operation deleteOperation, Object sourceAttribute)
    {
        MithraNotificationEvent event = this.createNotificationEvent(classname, databaseOperation, dataObjects,
                updateWrappers, deleteOperation, sourceAttribute);
        addNotificationEvent(databaseIdentifier, event);
    }

    private MithraNotificationEvent createNotificationEvent(String classname, byte databaseOperation, MithraDataObject[] dataObjects, List updateWrappers, Operation deleteOperation, Object sourceAttribute)
    {
        Attribute[] updatedAttributes = null;
        Attribute attribute;
        if (updateWrappers != null)
        {
            updatedAttributes = new Attribute[updateWrappers.size()];
            for (int i = 0; i < updateWrappers.size(); i++)
            {
                attribute = ((AttributeUpdateWrapper) updateWrappers.get(i)).getAttribute();
                updatedAttributes[i] = attribute;
            }
        }
        return new MithraNotificationEvent(classname, databaseOperation,
                dataObjects, updatedAttributes, deleteOperation, sourceAttribute);
    }

    private void addNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (null == tx)
        {
            this.addNoTxMithraNotificationEvent(databaseIdentifier, notificationEvent);
        }
        else
        {
            tx.addMithraNotificationEvent(databaseIdentifier, notificationEvent);
        }
    }

    private synchronized void addNoTxMithraNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent)
    {
        List<MithraNotificationEvent> list = mithraNoTxNotificationEvents.get(databaseIdentifier);

        if (list == null)
        {
            list = new ArrayList<MithraNotificationEvent>();
            mithraNoTxNotificationEvents.put(databaseIdentifier, list);
        }
        list.add(notificationEvent);
    }

    public void clearNotificationSubscribers()
    {
        this.mithraNotificationSubscriber.clear();
        this.mithraNoTxNotificationEvents.clear();
        this.mithraListToNotificationListenerMap.clear();
        this.mithraApplicationNotificationSubscriber.clear();
    }

    public void waitUntilCurrentNotificationTasksAreDone()
    {
        QueueMarker marker = new QueueMarker();
        this.addTaskToQueue(marker);
        marker.waitUntilDone();
    }

    public List getNotificationSubscribers()
    {
        Set<RegistrationKey> keys = mithraNotificationSubscriber.keySet();
        List<String> subscribers = new ArrayList<String>(keys.size());
        for (RegistrationKey key : keys)
        {
            subscribers.add(key.getClassname());
        }
        return subscribers;
    }

    private Runnable getApplicationNotificationSubscriptionRunnable(final String subject, final RelatedFinder finder,
                                                                    final MithraApplicationNotificationListener listener, final List list, final Operation operation)
    {
        return new Runnable()
        {
            public void run()
            {
                RegistrationEntryList registrationEntryList = getRegistrationEntryList(subject, finder);
                registrationEntryList.addRegistrationEntry(list, listener, operation);
            }
        };
    }

    private Runnable getApplicationClassLevelNotificationSubscriptionRunnable(final String subject, final RelatedFinder finder,
            final MithraApplicationClassLevelNotificationListener listener)
    {
        return new Runnable()
        {
            public void run()
            {
                RegistrationEntryList registrationEntryList = getRegistrationEntryList(subject, finder);
                registrationEntryList.addClassLevelRegistrationEntry(listener);
            }
        };
    }

    // Must only be called from the thread which is used for registration
    private RegistrationEntryList getRegistrationEntryList(String subject, RelatedFinder finder)
    {
        RegistrationEntryList registrationEntryList;
        RegistrationKey key = new RegistrationKey(subject, finder.getFinderClassName());
        if (logger.isDebugEnabled())
        {
            logger.debug("**************Adding application notification registration with key:  " + key.toString());
        }
        registrationEntryList = mithraApplicationNotificationSubscriber.get(key);
        if (registrationEntryList == null)
        {
            registrationEntryList = new RegistrationEntryList();
            mithraApplicationNotificationSubscriber.put(key, registrationEntryList);
        }
        return registrationEntryList;
    }

    private Runnable getNotificationAdapterRunnable(final String subject)
    {
        return new Runnable()
        {
            public void run()
            {
                getOrCreateAdapter(subject);
            }
        };
    }

    private Runnable getSendMithraNotificationMessageRunnable(final Map notificationEvents, final long requestorVmId)
    {
        return new Runnable()
        {
            public void run()
            {
                for (Iterator it = notificationEvents.keySet().iterator(); it.hasNext();)
                {
                    String databaseIdentifier = (String) it.next();
                    List events = (ArrayList) notificationEvents.get(databaseIdentifier);
                    ExternalizableMithraNotificationMessage notificationMessage = new ExternalizableMithraNotificationMessage(events);
                    notificationMessage.setMithraVmId(MithraProcessInfo.getVmId());
                    notificationMessage.setRequestorVmId(requestorVmId);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("***************** Mithra: " + MithraProcessInfo.getVmId() + " sending message with topic: " + databaseIdentifier);
                    }
                    try
                    {
                        getOrCreateAdapter(databaseIdentifier).broadcastMessage(convertObjectToBytes(notificationMessage));
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Unable to serialize Mithra notification message");
                    }
                }
            }
        };
    }

    public synchronized Map<String, List<MithraNotificationEvent>> getNotificationsToSend()
    {
        Map<String, List<MithraNotificationEvent>> result = mithraNoTxNotificationEvents;
        if (result.isEmpty())
        {
            result = Collections.EMPTY_MAP;
        }
        else
        {
            mithraNoTxNotificationEvents = UnifiedMap.newMap();
        }
        return result;
    }

    private Runnable getSendNoTxNotificationMessageBatchRunnable()
    {
        return new Runnable()
        {
            public void run()
            {
                Map<String, List<MithraNotificationEvent>> thingsToSend = getNotificationsToSend();
                if (!thingsToSend.isEmpty())
                {
                    for (Iterator<String> it = thingsToSend.keySet().iterator(); it.hasNext(); )
                    {
                        String databaseIdentifier = it.next();
                        List<MithraNotificationEvent> events = thingsToSend.get(databaseIdentifier);
                        ExternalizableMithraNotificationMessage notificationMessage = new ExternalizableMithraNotificationMessage(events);
                        notificationMessage.setMithraVmId(MithraProcessInfo.getVmId());
                        notificationMessage.setRequestorVmId(MithraProcessInfo.getVmId());
                        try
                        {
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("***************** Mithra: " + MithraProcessInfo.getVmId() + " sending message with topic: " + databaseIdentifier);
                            }
                            getOrCreateAdapter(databaseIdentifier).broadcastMessage(convertObjectToBytes(notificationMessage));
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Unable to serialize Mithra notification message");
                        }
                    }
                }
            }
        };
    }


    public void processNotificationEvents(String subject, List<MithraNotificationEvent> notificationEvents)
    {
        MithraNotificationEvent notificationEvent;
        for (int i = 0; i < notificationEvents.size(); i++)
        {
            notificationEvent = notificationEvents.get(i);
            MithraNotificationListener listener;
            RegistrationKey key = new RegistrationKey(subject, notificationEvent.getClassname());
            listener = mithraNotificationSubscriber.get(key);
            if (listener != null)
            {
                try
                {
                    if (MithraNotificationEvent.INSERT == notificationEvent.getDatabaseOperation())
                    {
                        listener.onInsert(notificationEvent.getDataObjects(), notificationEvent.getSourceAttribute());
                    }
                    else if (MithraNotificationEvent.UPDATE == notificationEvent.getDatabaseOperation())
                    {
                        listener.onUpdate(notificationEvent.getDataObjects(), notificationEvent.getUpdatedAttributes(), notificationEvent.getSourceAttribute());
                    }
                    else if (MithraNotificationEvent.DELETE == notificationEvent.getDatabaseOperation())
                    {
                        listener.onDelete(notificationEvent.getDataObjects());
                    }
                    else if (MithraNotificationEvent.MASS_DELETE == notificationEvent.getDatabaseOperation())
                    {
                        listener.onMassDelete(notificationEvent.getOperationForMassDelete());
                    }
                }
                catch (Throwable t)
                {
                    logger.error("Error in Mithra notification listener", t);
                }
            }
            else if (logger.isDebugEnabled())
            {
                logger.debug("There is no listener registered to process this incoming message for class " + notificationEvent.getClassname());
            }
            this.processApplicationNotification(key, notificationEvent);
        }
    }

    private void processApplicationNotification(RegistrationKey key, MithraNotificationEvent notificationEvent)
    {
        RegistrationEntryList entryList = mithraApplicationNotificationSubscriber.get(key);
        if (entryList != null)
        {
            entryList.processNotification(notificationEvent);
        }
    }

    private Runnable getProcessIncomingMessagesRunnable(final String subject, final List<MithraNotificationEvent> notificationEvents)
    {
        return new Runnable()
        {
            public void run()
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Started processing incoming notification message");
                }
                processNotificationEvents(subject, notificationEvents);
            }
        };
    }

    private void addTaskToQueue(Runnable task)
    {
        queuedExecutor.execute(task);
    }

    private void initializeNotificationHelperThreads()
    {
        channel = new LinkedBlockingQueue();
        queuedExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, channel,
                new ThreadFactory()
                {
                    public Thread newThread(Runnable r)
                    {
                        Thread t = new Thread(r);
                        t.setDaemon(true);
                        t.setPriority(Thread.NORM_PRIORITY);
                        t.setName("MithraNotificationThread");
                        return t;
                    }

                });
        clockDaemon = Executors.newScheduledThreadPool(1, new ThreadFactory()
        {
            public Thread newThread(Runnable r)
            {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setPriority(Thread.NORM_PRIORITY);
                t.setName("NoTxMithraNotificationThread");
                return t;
            }
        });
        clockDaemon.scheduleAtFixedRate(this.getSendNoTxNotificationMessageBatchRunnable(), 0, PERIOD, TimeUnit.MILLISECONDS);
    }

    public void shutdown()
    {
        this.shutdown(false);
    }

    protected void shutdown(boolean fromShutdownHook)
    {
        this.shutdown = true;
        if (queuedExecutor != null)
        {
            queuedExecutor.shutdownNow();
        }
        if (clockDaemon != null)
        {
            clockDaemon.shutdownNow();
        }

        for (MithraNotificationMessagingAdapter adapter : this.subjectToAdapterMap.values())
        {
            adapter.shutdown();
        }
        adapterFactory.shutdown();
        if (!fromShutdownHook && this.shutdownHook != null)
        {
            Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
        }
        this.shutdownHook = null;
    }

    public boolean isQueuedExecutorChannelEmpty()
    {
        return this.channel.isEmpty();
    }

    public void forceSendNow()
    {
        this.getSendNoTxNotificationMessageBatchRunnable().run();
        while (!channel.isEmpty())
        {
            ArrayList all = new ArrayList(channel.size());
            channel.drainTo(all);
            for (int i = 0; i < all.size(); i++)
            {
                Runnable r = (Runnable) all.get(i);
                r.run();
            }
        }
    }

    /**
     * Methods in this class are not synchronized because they are engineered to be called from one
     * thread.
     */
    private static class RegistrationEntryList
    {
        private List<MithraApplicationNotificationRegistrationEntry> registrationList = new ArrayList<MithraApplicationNotificationRegistrationEntry>();
        private final ReferenceQueue queue = new ReferenceQueue();

        public void addRegistrationEntry(List list, MithraApplicationNotificationListener listener, Operation operation)
        {
            MithraApplicationNotificationRegistrationEntry registrationEntry;

            if (operation != null)
            {
                registrationEntry = new OperationBasedNotificationRegistrationEntry(operation, list, listener, queue);
            }
            else
            {
                registrationEntry = new IndexBasedNotificationRegistrationEntry(list, listener, queue);
            }

            this.registrationList.add(registrationEntry);
            if (logger.isDebugEnabled())
            {
                logger.debug("*************** Added registration entry to registrationEntryList***************");
            }
        }

        public void addClassLevelRegistrationEntry(MithraApplicationClassLevelNotificationListener listener)
        {
            this.registrationList.add(new ClassLevelNotificationRegistrationEntry(listener));
            if (logger.isDebugEnabled())
            {
                logger.debug("*************** Added class-level registration entry to registrationEntryList***************");
            }
        }

        public void processNotification(MithraNotificationEvent notificationEvent)
        {
            this.expungeStaleEntries();
            for (int i = 0; i < this.registrationList.size(); i++)
            {
                MithraApplicationNotificationRegistrationEntry entry = this.registrationList.get(i);
                if (entry != null)
                {
                    try
                    {
                        entry.processNotification(notificationEvent);
                    }
                    catch (Throwable t)
                    {
                        logger.error("Error in application notification listener", t);
                    }
                }
            }
        }

        private void expungeStaleEntries()
        {
            Object r;
            while ((r = queue.poll()) != null)
            {
                this.registrationList.remove(r);
            }
        }
    }

    private static class QueueMarker implements Runnable
    {
        private volatile boolean ran = false;

        public void run()
        {
            synchronized (this)
            {
                ran = true;
                this.notifyAll();
            }
        }

        public synchronized void waitUntilDone()
        {
            while (!ran)
            {
                try
                {
                    this.wait();
                }
                catch (InterruptedException e)
                {
                    // just let it wait some more.
                }
            }
        }
    }

}
