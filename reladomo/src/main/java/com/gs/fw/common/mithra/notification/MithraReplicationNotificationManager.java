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
import com.gs.fw.common.mithra.MithraDatabaseObject;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.notification.replication.ReplicatedTransaction;
import com.gs.fw.common.mithra.notification.replication.ReplicationNotificationConnectionManager;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class MithraReplicationNotificationManager
{
    protected Logger logger = LoggerFactory.getLogger(MithraReplicationNotificationManager.class);
    private Map replicationNotificationInitValuesMap = new UnifiedMap();
    private List replicationNotificationPollingThreads = new ArrayList();
    private Map tableNameDatabaseObjectMap = new UnifiedMap();

    private int batchSize = 250;

    public MithraReplicationNotificationManager()
    {

    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public void clearReplicationNotificationMaps()
    {
         replicationNotificationInitValuesMap.clear();
         tableNameDatabaseObjectMap.clear();
         replicationNotificationPollingThreads.clear();
         ReplicationNotificationConnectionManager.getInstance().getConnectionManagerList().clear();
    }

    public void addDatabaseObject(MithraDatabaseObject databaseObject, String schemaName, boolean connectionManagerProvidesSchema, long replicationPollingInterval)
    {
        String tableName = databaseObject.getTableName();
        List databaseObjectsForSameTable = (List) tableNameDatabaseObjectMap.get(tableName);
        if(databaseObjectsForSameTable == null)
        {
            databaseObjectsForSameTable = new ArrayList();
            tableNameDatabaseObjectMap.put(tableName, databaseObjectsForSameTable);
        }
        databaseObjectsForSameTable.add(databaseObject);
        addReplicationThreadInitValues(databaseObject.getConnectionManager(), schemaName,connectionManagerProvidesSchema,replicationPollingInterval);
    }

    private void addReplicationThreadInitValues(Object connectionManager,  String schemaName, boolean getFromConnectionManager,  long interval)
    {
        ConnectionManagerInitValuesKey key = new ConnectionManagerInitValuesKey(connectionManager, schemaName, getFromConnectionManager);
        if(!replicationNotificationInitValuesMap.containsKey(key))
        {
            ReplicationThreadInitValues initValues = new ReplicationThreadInitValues(schemaName, connectionManager,  interval);
            replicationNotificationInitValuesMap.put(key, initValues);
            ReplicationNotificationConnectionManager replicationconnectionManager = ReplicationNotificationConnectionManager.getInstance();
            if(schemaName == null)
                replicationconnectionManager.addConnectionManager(connectionManager);
            else if(getFromConnectionManager)
                replicationconnectionManager.addConnectionManager(connectionManager,schemaName, true);
            else
                replicationconnectionManager.addConnectionManager(connectionManager, schemaName);
        }
    }

    public void initializeNotificationPollingThreads()
    {
        if(!replicationNotificationInitValuesMap.isEmpty())
        {
            int initialIndex = replicationNotificationPollingThreads.size();

            Iterator it = replicationNotificationInitValuesMap.keySet().iterator();
            while(it.hasNext())
            {
                ReplicationThreadInitValues initValues = (ReplicationThreadInitValues) replicationNotificationInitValuesMap.get(it.next());
                int sourceAttributeValue = initialIndex;
                initialIndex++;
                try
                {
                   Operation op = RunsMasterQueueFinder.eventId().eq(-1).and(RunsMasterQueueFinder.sourceId().eq(sourceAttributeValue));
                   RunsMasterQueueFinder.findOneBypassCache(op);
                   ScheduledExecutorService daemon = createPollingThread(sourceAttributeValue);
                   replicationNotificationPollingThreads.add(daemon);
                   this.startReplicationNotificationPollingThreads(daemon, initValues.getInterval(), sourceAttributeValue);

                }
                catch(Exception e)
                {
                    logger.error("Error during initialization of replication notification polling threads. The RUNS Master Queue Table (ap_UPD_QUEUE) was not found" );
                }
            }
            replicationNotificationInitValuesMap.clear();
        }
    }

    private ScheduledExecutorService createPollingThread(final int sourceAttributeValue)
    {
        return Executors.newScheduledThreadPool(1, new ThreadFactory()
        {
            public Thread newThread(Runnable r)
            {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("RUNS Polling Thread "+sourceAttributeValue);
                return t;
            }
        });
    }

    private void startReplicationNotificationPollingThreads(ScheduledExecutorService daemon, long interval, int sourceAttributeValue)
    {
        Operation op = RunsMasterQueueFinder.sourceId().eq(sourceAttributeValue);
                  op = op.and(RunsMasterQueueFinder.all());
        daemon.scheduleAtFixedRate(this.getReplicationNotificationRunnable(op,  sourceAttributeValue), 0, interval, TimeUnit.MILLISECONDS);
    }

    public void shutdownReplicationNotification()
    {
        for(int i = 0; i < replicationNotificationPollingThreads.size(); i++)
        {
            ScheduledExecutorService t = (ScheduledExecutorService) replicationNotificationPollingThreads.get(i);
            t.shutdown();
        }
    }

    protected Runnable getReplicationNotificationRunnable(final Operation op, final int sourceAttributeValue)
    {
        return new Runnable()
        {
            public void run()
            {
                try
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Thread "+Thread.currentThread().getName()+" is executing");
                    }
                    RunsMasterQueueList list = new RunsMasterQueueList(op);
                    list.setBypassCache(true);
                    list.setOrderBy(RunsMasterQueueFinder.eventId().ascendingOrderBy());
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Size: "+list.size());
                    }
                    if (list.size() < batchSize)
                    {
                        processReplicationEvents(list);
                    }
                    else
                    {
                        List replicationTransactions = createReplicationTransactions(list);
                        Collections.sort(replicationTransactions);

                        ArrayList toProcess = new ArrayList(100);
                        for(int i=0;i<replicationTransactions.size();i++)
                        {
                            ReplicatedTransaction replicatedTran = (ReplicatedTransaction) replicationTransactions.get(i);
                            toProcess.addAll(replicatedTran.getEvents());
                            if (toProcess.size() > batchSize && replicatedTran.isContiguous())
                            {
                                processReplicationEvents(toProcess);
                                toProcess.clear();
                            }
                        }
                        if (toProcess.size() > 0)
                        {
                            processReplicationEvents(toProcess);
                        }
                    }
                }
                catch(Throwable e)
                {
                    logger.error("Error in polling thread",e);
                }
            }

            private List createReplicationTransactions(List events)
            {
                ArrayList result = new ArrayList();
                for(int i=0;i<events.size();i++)
                {
                    boolean consumed = false;
                    RunsMasterQueue runsMasterQueue = (RunsMasterQueue) events.get(i);
                    for(int j=result.size() - 1; j >=0 && !consumed; j--)
                    {
                        ReplicatedTransaction rt = (ReplicatedTransaction) result.get(j);
                        consumed = rt.add(runsMasterQueue);
                    }
                    if (!consumed)
                    {
                        result.add(new ReplicatedTransaction(runsMasterQueue));
                    }
                }
                return result;
            }

            private void processReplicationEvents(List list)
            {
                Map replicationEventsByEntityMap = new UnifiedMap();
                int maxOverallEventId = 0;
                for(int i = 0; i < list.size(); i++)
                {
                    RunsMasterQueue object = (RunsMasterQueue) list.get(i);
                    if (maxOverallEventId < object.getEventId())
                    {
                        maxOverallEventId = object.getEventId();
                    }
                    String entity = object.getEntity();

                    List masterQueueListByEntity =  (List) replicationEventsByEntityMap.get(entity);
                    if(masterQueueListByEntity == null)
                    {
                        masterQueueListByEntity = new ArrayList();
                        replicationEventsByEntityMap.put(entity, masterQueueListByEntity);
                    }
                    masterQueueListByEntity.add(object);
                }
                if(!replicationEventsByEntityMap.isEmpty())
                {
                   processReplicationEntries(replicationEventsByEntityMap, maxOverallEventId);
                }
            }

            private void removeEntriesFromMasterQueue(int maxEventId, int sourceAttributeValue)
            {
                Operation op = RunsMasterQueueFinder.eventId().lessThanEquals(maxEventId);
                op = op.and(RunsMasterQueueFinder.sourceId().eq(sourceAttributeValue));

                RunsMasterQueueList list = new RunsMasterQueueList(op);
                list.deleteAll();
            }

            private void processReplicationEntries(Map replicationEventsByEntityMap, int maxOverallEventId)
            {
                Set byEntityKeys = replicationEventsByEntityMap.keySet();
                Iterator it =  byEntityKeys.iterator();
                ArrayList notificationsToSend = new ArrayList((int) (batchSize*1.5));
                while(it.hasNext())
                {
                    String entity = (String) it.next();
                    List masterQueueListByEntity = (List) replicationEventsByEntityMap.get(entity);

                    RunsMasterQueue obj = (RunsMasterQueue)masterQueueListByEntity.get(0);
                    int minEventId = obj.getEventId();
                    int maxEventId = obj.getEventId();

                    for(int i = 1; i < masterQueueListByEntity.size(); i++)
                    {
                        obj = (RunsMasterQueue)masterQueueListByEntity.get(i);
                        if (obj.getEventId() > maxEventId)
                        {
                            maxEventId = obj.getEventId();
                        }
                    }
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Entity "+entity+" min id: "+minEventId+" max id: "+maxEventId);
                    }

                    List list = (List )tableNameDatabaseObjectMap.get(entity);

                    if(list != null)
                    {
                        for(int d = 0; d < list.size(); d++)
                        {
                            MithraReplicatedDatabaseObject dbo = (MithraReplicatedDatabaseObject) list.get(d);
                            this.processReplicationNotificationEvents(dbo, minEventId, maxEventId, sourceAttributeValue, notificationsToSend);
                        }
                    }
                }
                String databaseIdentifier = ReplicationNotificationConnectionManager.getInstance().getDatabaseIdentifier(sourceAttributeValue);
                MithraManager mithraManager = MithraManagerProvider.getMithraManager();
                long requestorVmId = mithraManager.getNotificationEventManager().getMithraVmId();
                mithraManager.getNotificationEventManager().broadcastNotificationMessage(
                        Maps.fixedSize.of(databaseIdentifier, notificationsToSend), requestorVmId);

                deleteEntries(maxOverallEventId, byEntityKeys, replicationEventsByEntityMap, mithraManager);
            }

            private void deleteEntries(final int maxOverallEventId, final Set byEntityKeys, final Map replicationEventsByEntityMap,
                                       MithraManager mithraManager)
            {
                mithraManager.executeTransactionalCommand(new TransactionalCommand() {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        RunsMasterQueueFinder.setTransactionModeReadCacheUpdateCausesRefreshAndLock(tx);

                        Iterator it =  byEntityKeys.iterator();
                        while(it.hasNext())
                        {
                            String entity = (String) it.next();
                            List masterQueueListByEntity = (List) replicationEventsByEntityMap.get(entity);

                            RunsMasterQueue obj = (RunsMasterQueue)masterQueueListByEntity.get(0);
                            int minEventId = obj.getEventId();
                            int maxEventId = obj.getEventId();

                            for(int i = 1; i < masterQueueListByEntity.size(); i++)
                            {
                                obj = (RunsMasterQueue)masterQueueListByEntity.get(i);
                                if (obj.getEventId() > maxEventId)
                                {
                                    maxEventId = obj.getEventId();
                                }
                            }

                            List list = (List )tableNameDatabaseObjectMap.get(entity);

                            if(list != null)
                            {
                                MithraReplicatedDatabaseObject dbo = (MithraReplicatedDatabaseObject) list.get(0);
                                dbo.deleteReplicationNotificationData(minEventId, maxEventId);
                            }
                        }
                        removeEntriesFromMasterQueue(maxOverallEventId, sourceAttributeValue);
                        return null;
                    }
                });
            }

            private void processReplicationNotificationEvents(MithraReplicatedDatabaseObject dbo, int minEventId,
                int maxEventId, int sourceAttribute, List notificationsToSend)
            {
                String databaseIdentifier = ReplicationNotificationConnectionManager.getInstance().getDatabaseIdentifier(sourceAttribute);
                String notificationEventIdentifier = ((MithraDatabaseObject)dbo).getNotificationEventIdentifier();
                Map replicatedRowsByActionMap = dbo.findReplicatedData(minEventId, maxEventId);
                List insertedRows = (List) replicatedRowsByActionMap.get("I");
                List updatedRows = (List) replicatedRowsByActionMap.get("U");
                List deletedRows = (List) replicatedRowsByActionMap.get("D");
                if (deletedRows != null)
                {
                    processDeleteReplicationNotification(notificationEventIdentifier, deletedRows, databaseIdentifier, notificationsToSend);
                }
                if (insertedRows != null)
                {
                    processInsertReplicationNotification(notificationEventIdentifier, insertedRows, databaseIdentifier, notificationsToSend);
                }
                if (updatedRows != null)
                {
                    processUpdateReplicationNotification(notificationEventIdentifier, updatedRows, databaseIdentifier, notificationsToSend);
                }

            }
        };

    }

    protected void processInsertReplicationNotification(String notificationEventIdentifier, List insertedRows,
                                                      String databaseIdentifier, List notificationsToSend)
    {
        MithraNotificationEvent event = new MithraNotificationEvent(notificationEventIdentifier,
                MithraNotificationEvent.INSERT,
                (MithraDataObject[]) insertedRows.toArray(
                    new MithraDataObject[insertedRows.size()]), null, null, null);
        List notificationEvents = new ArrayList(1);
        notificationEvents.add(event);
        MithraManager.getInstance().getNotificationEventManager().processNotificationEvents(databaseIdentifier,
            notificationEvents);
        notificationsToSend.add(event);
    }

    protected void processUpdateReplicationNotification(String notificationEventIdentifier, List updatedRows,
                                                      String databaseIdentifier, List notificationsToSend)
    {
        MithraNotificationEvent event = new MithraNotificationEvent(notificationEventIdentifier,
                MithraNotificationEvent.UPDATE,
                (MithraDataObject[]) updatedRows.toArray(
                    new MithraDataObject[updatedRows.size()]), null, null, null);
        List notificationEvents = new ArrayList(1);
        notificationEvents.add(event);
        MithraManager.getInstance().getNotificationEventManager().processNotificationEvents(databaseIdentifier,
            notificationEvents);
        notificationsToSend.add(event);
    }

    protected void processDeleteReplicationNotification(String notificationEventIdentifier, List deletedRows,
                                                      String databaseIdentifier, List notificationsToSend)
    {
        MithraNotificationEvent event = new MithraNotificationEvent(notificationEventIdentifier,
                MithraNotificationEvent.DELETE,
                (MithraDataObject[]) deletedRows.toArray(
                    new MithraDataObject[deletedRows.size()]), null, null, null);
        List notificationEvents = new ArrayList(1);
        notificationEvents.add(event);
        MithraManager.getInstance().getNotificationEventManager().processNotificationEvents(databaseIdentifier,
            notificationEvents);
        notificationsToSend.add(event);
    }

    private static class ConnectionManagerInitValuesKey
    {
        Object connectionManager;
        String schemaName;
        boolean getFromConnectionManager;


        public ConnectionManagerInitValuesKey(Object connectionManagerName, String schemaName, boolean getFromConnectionManager)
        {
            this.schemaName = schemaName;
            this.connectionManager = connectionManagerName;
            this.getFromConnectionManager = getFromConnectionManager;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final ConnectionManagerInitValuesKey that = (ConnectionManagerInitValuesKey) o;

            if (getFromConnectionManager != that.getFromConnectionManager) return false;
            if (connectionManager != that.connectionManager) return false; // identity check
            return !(schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null);

        }

        public int hashCode()
        {
            int result;
            result = connectionManager.hashCode();
            result = 29 * result + (schemaName != null ? schemaName.hashCode() : 0);
            result = 29 * result + (getFromConnectionManager ? 1 : 0);
            return result;
        }

    }

    private static class ReplicationThreadInitValues
    {
        String schemaName;
        long interval;
        Object connectionManager;

        public ReplicationThreadInitValues(String schemaName, Object connectionManager, long interval)
        {
            this.schemaName = schemaName;
            this.connectionManager = connectionManager;
            this.interval = interval;
        }

        public String getSchemaName()
        {
            return schemaName;
        }

        public long getInterval()
        {
            return interval;
        }

        public Object getConnectionManager()
        {
            return connectionManager;
        }
    }
}
