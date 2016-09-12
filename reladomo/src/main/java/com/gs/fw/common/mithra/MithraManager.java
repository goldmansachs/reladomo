
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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.cache.offheap.FastUnsafeOffHeapDataStorage;
import com.gs.fw.common.mithra.cache.offheap.FastUnsafeOffHeapIntArrayStorage;
import com.gs.fw.common.mithra.cache.offheap.OffHeapFreeThread;
import com.gs.fw.common.mithra.mithraruntime.MithraRuntimeType;
import com.gs.fw.common.mithra.behavior.state.NoTransactionBehaviorChooser;
import com.gs.fw.common.mithra.behavior.state.TransactionalBehaviorChooser;
import com.gs.fw.common.mithra.notification.MithraNotificationEventManager;
import com.gs.fw.common.mithra.notification.MithraReplicationNotificationManager;
import com.gs.fw.common.mithra.notification.UninitializedNotificationEventManager;
import com.gs.fw.common.mithra.portal.MithraTransactionalPortal;
import com.gs.fw.common.mithra.remote.MithraRemoteTransactionProxy;
import com.gs.fw.common.mithra.remote.RemoteMithraObjectConfig;
import com.gs.fw.common.mithra.remote.ServerTransactionWorkerTask;
import com.gs.fw.common.mithra.superclassimpl.MithraDatedTransactionalObjectImpl;
import com.gs.fw.common.mithra.superclassimpl.MithraTransactionalObjectImpl;
import com.gs.fw.common.mithra.transaction.LocalTm;
import com.gs.fw.common.mithra.transaction.MithraNestedTransaction;
import com.gs.fw.common.mithra.transaction.MithraRootTransaction;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class MithraManager
{

    private static final Logger logger = LoggerFactory.getLogger(MithraManager.class);
    private static final long RETRY_TRANSACTION_TIME = 2000;
    private static final Random randomGenerator = new Random();

    private JtaProvider jtaProvider = new DefaultJtaProvider(new LocalTm());
    private ThreadLocal<MithraTransaction> threadTransaction = new ThreadLocal<MithraTransaction>();
    private int transactionTimeout = 60; // default 60 seconds

    private AtomicInteger databaseRetrieveCount = new AtomicInteger(0);
    private int remoteRetrieveCount = 0;

    private static final MithraManager instance = new MithraManager();

    private MithraNotificationEventManager notificationEventManager = new UninitializedNotificationEventManager ();
    private static final TransactionalBehaviorChooser NO_TRANSACTION_BEHAVIOR_CHOOSER = new NoTransactionBehaviorChooser();
    private TransactionStyle defaultTransactionStyle = new TransactionStyle(transactionTimeout);
    private boolean isRetryAfterTimeout = false;
    private MithraConfigurationManager configManager = new MithraConfigurationManager();
    private boolean captureTransactionLevelPerformanceData = false;

    public static MithraManager getInstance()
    {
        return instance;
    }

    static
    {
        try
        {
            // these are classes that use Atomic*Updater. There is some sort of bug in AWT thread where these can't initialize correctly.
            Class.forName(AbstractDatedCache.class.getName());
            Class.forName(ArrayBasedQueue.class.getName());
            Class.forName(ConcurrentDatedObjectIndex.class.getName());
            Class.forName(ConcurrentFullUniqueIndex.class.getName());
            Class.forName(ConcurrentOffHeapStringIndex.class.getName());
            Class.forName(ConcurrentOnHeapStringIndex.class.getName());
            Class.forName(ConcurrentTempPool.class.getName());
            Class.forName(ConcurrentWeakPool.class.getName());
            Class.forName(CooperativeCpuTaskFactory.class.getName());
            Class.forName(CpuBoundTask.class.getName());
            Class.forName(DatedTransactionalState.class.getName());
            Class.forName(FastUnsafeOffHeapDataStorage.class.getName());
            Class.forName(FastUnsafeOffHeapIntArrayStorage.class.getName());
            Class.forName(OffHeapFreeThread.class.getName());
            Class.forName(MinExchange.class.getName());
            Class.forName(MithraCompositeListQueue.class.getName());
            Class.forName(MithraDatedTransactionalObjectImpl.class.getName());
            Class.forName(MithraTransactionalObjectImpl.class.getName());
            Class.forName(MithraUnsafe.class.getName());
            Class.forName(NonLruQueryIndex.class.getName());
            Class.forName(PeekableQueue.class.getName());
            Class.forName(ServerTransactionWorkerTask.class.getName());
            Class.forName(SingleListBasedQueue.class.getName());
            Class.forName(TransactionalState.class.getName());
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not initialize concurrent classes", e);
        }
    }
    
    private MithraManager()
    {
        // singleton
    }

    public MithraConfigurationManager getConfigManager()
    {
        return this.configManager;
    }

    public void setConfigManager(MithraConfigurationManager configManager)
    {
        this.configManager = configManager;
    }

    public int getTransactionTimeout()
    {
        return this.transactionTimeout;
    }

    /**
     * sets the transaction timeout.
     * @param transactionTimeout
     */
    public void setTransactionTimeout(int transactionTimeout)
    {
        this.transactionTimeout = transactionTimeout;
        this.defaultTransactionStyle = new TransactionStyle(transactionTimeout);
    }

    public void setCaptureTransactionLevelPerformanceData(boolean captureTransactionLevelPerformanceData)
    {
        this.captureTransactionLevelPerformanceData = captureTransactionLevelPerformanceData;
    }

    public boolean isCaptureTransactionLevelPerformanceData()
    {
        return this.captureTransactionLevelPerformanceData;
    }

    public boolean canCaptureTransactionLevelPerformanceData()
    {
        return this.captureTransactionLevelPerformanceData && this.threadTransaction.get() != null;
    }

    public boolean isRetryAfterTimeout()
    {
        return this.isRetryAfterTimeout;
    }

    public void setRetryAfterTimeout(boolean retryAfterTimeout)
    {
        this.isRetryAfterTimeout = retryAfterTimeout;
        this.defaultTransactionStyle = new TransactionStyle(transactionTimeout, MithraTransaction.DEFAULT_TRANSACTION_RETRIES, retryAfterTimeout);
    }
    
    public int getDatabaseRetrieveCount()
    {
        return this.databaseRetrieveCount.get();
    }

    public void incrementDatabaseRetrieveCount()
    {
        this.databaseRetrieveCount.incrementAndGet();

        if(this.canCaptureTransactionLevelPerformanceData())
        {
            this.threadTransaction.get().incrementDatabaseRetrieveCount();
        }
    }

    public int getRemoteRetrieveCount()
    {
        return this.remoteRetrieveCount;
    }

    public void incrementRemoteRetrieveCount()
    {
        this.remoteRetrieveCount++;

        if(this.canCaptureTransactionLevelPerformanceData())
        {
            this.threadTransaction.get().incrementRemoteRetrieveCount();
        }
    }

    public void fullyInitialize()
    {
        this.configManager.fullyInitialize();
    }

    public TransactionManager getJtaTransactionManager()
    {
        return this.jtaProvider.getJtaTransactionManager();
    }

    /**
     * sets the Java Transaction API provider. This method must be called as part of
     * initialization.
     * @param jtaProvider
     */
    public void setJtaTransactionManagerProvider(JtaProvider jtaProvider)
    {
        this.jtaProvider = jtaProvider;
    }

    /**
     * starts a new <code> JTA Transaction</code> if there is no transaction
     * associated with the current thread. If there is a transaction on the thread
     * it returns a nested transaction. Note: nested transactions cannot be rolled
     * back to committed independently. Calling commit on a nested transaction does
     * nothing. Calling rollback on a nested transaction rollsback the entire transaction.
     */
    public MithraTransaction startOrContinueTransaction() throws MithraTransactionException
    {
        return this.startOrContinueTransaction(defaultTransactionStyle);
    }

    /**
     * Behaves like startOrContinueTransaction(), but with a custom transaction style
     * @param style defines the parameters for this transaction.
     * @return
     * @throws MithraTransactionException
     */
    public MithraTransaction startOrContinueTransaction(TransactionStyle style) throws MithraTransactionException
    {
        MithraTransaction result;
        MithraTransaction parent = this.getCurrentTransaction();
        if (parent != null)
        {
            result = new MithraNestedTransaction(parent);
        }
        else
        {
            Transaction jtaTx;
            try
            {
                if (this.getJtaTransactionManager().getStatus() != Status.STATUS_ACTIVE)
                {
                    this.getJtaTransactionManager().setTransactionTimeout(style.getTimeout());
                    this.getJtaTransactionManager().begin();
                }
                jtaTx = this.getJtaTransactionManager().getTransaction();
                result = this.createMithraRootTransaction(jtaTx, style.getTimeout()*1000);
            }
            catch (NotSupportedException e)
            {
                throw new MithraTransactionException("JTA exception", e);
            }
            catch (SystemException e)
            {
                throw new MithraTransactionException("JTA exception", e);
            }
            catch (RollbackException e)
            {
                throw new MithraTransactionException("JTA exception", e);
            }
        }
        this.setThreadTransaction(result);
        return result;
    }

    private void setThreadTransaction(MithraTransaction result)
    {
        this.threadTransaction.set(result);
    }

    public void removeTransaction(MithraTransaction tx)
    {
        if (this.threadTransaction.get() == tx)
        {
            this.setThreadTransaction(null);
        }
    }

    public void popTransaction(MithraTransaction tx)
    {
        if (this.threadTransaction.get() != tx)
        {
            getLogger().error("pop transaction was called with the wrong transaction", new Exception("for tracing"));
        }
        else
        {
            this.setThreadTransaction(tx.getParent());
        }
    }

    private boolean isStatusActive(int status)
    {
        return status == Status.STATUS_ACTIVE       || status == Status.STATUS_COMMITTING  ||
               status == Status.STATUS_PREPARED     || status == Status.STATUS_PREPARING   ||
               status == Status.STATUS_ROLLING_BACK || status == Status.STATUS_MARKED_ROLLBACK;
    }

    public TransactionalBehaviorChooser zGetTransactionalBehaviorChooser()
    {
        MithraTransaction tx = this.getCurrentTransaction();
        if (tx != null)
        {
            return tx;
        }
        return NO_TRANSACTION_BEHAVIOR_CHOOSER;
    }

    /**
     *
     * @return the current transaction. If there is no transaction, null is returned.
     * @throws MithraTransactionException
     */
    public MithraTransaction getCurrentTransaction() throws MithraTransactionException
    {
        MithraTransaction mithraThreadTx = this.threadTransaction.get();
        if (mithraThreadTx != null)
        {
            if (!isStatusActive(mithraThreadTx.getJtaTransactionStatus()))
            {
                this.removeTransaction(mithraThreadTx);
                throw new MithraTransactionException("transaction associated with the current thread is no longer active! Status is " +
                        mithraThreadTx.getJtaTransactionStatusDescription() +
                        " Transaction started " + (System.currentTimeMillis() - mithraThreadTx.getRealStartTime()) + " ms ago.", true);
            }
        }
        return mithraThreadTx;
    }

    public MithraTransaction zGetCurrentTransactionWithNoCheck() throws MithraTransactionException
    {
        return this.threadTransaction.get();
    }

    public void joinJtaTransaction(Transaction jtaTx)
    {
        boolean activeTransaction = false;
        try
        {
            if (jtaTx != null)
            {
                activeTransaction = this.isStatusActive(jtaTx.getStatus());
            }
        }
        catch (SystemException e)
        {
            throw new MithraTransactionException("Could not get transaction status", e);
        }
        if (activeTransaction) // there is a jtaTx, but we didn't know about it!
        {
            try
            {
                MithraTransaction mithraTx = createMithraRootTransaction(jtaTx, this.getTransactionTimeout()*1000);
                mithraTx.setStarted(false);
                this.setThreadTransaction(mithraTx);
            }
            catch (SystemException e)
            {
                throw new MithraTransactionException("Could not synchronize with outside transaction", e);
            }
            catch (RollbackException e)
            {
                throw new MithraTransactionException("Could not synchronize with outside transaction", e);
            }
        }
        else
        {
            throw new MithraTransactionException("JTA transaction must be active.");
        }
    }

    public void leaveJtaTransaction()
    {
        MithraTransaction mithraTx = this.threadTransaction.get();
        if (mithraTx == null)
        {
            throw new MithraTransactionException("no transaction exists!");
        }
        if (!(mithraTx instanceof MithraRootTransaction))
        {
            throw new MithraTransactionException("must leave JTA transaction from the same nesting level");
        }
        MithraRootTransaction rootTx = (MithraRootTransaction) mithraTx;
        rootTx.leaveJtaTransaction();
    }

    private MithraRootTransaction createMithraRootTransaction(Transaction jtaTx, int timeoutInMilliseconds) throws SystemException, RollbackException
    {
        MithraRootTransaction result = new MithraRootTransaction(jtaTx, timeoutInMilliseconds);
        MithraTransactionalPortal.initializeTransactionalQueryCache(result);
        jtaTx.registerSynchronization(result);
        return result;
    }

    /**
     *
     * @return true is there is an ongoing transaction
     * @throws MithraTransactionException
     */
    public boolean isInTransaction() throws MithraTransactionException
    {
        return this.getCurrentTransaction() != null;
    }

    public void cleanUpPrimaryKeyGenerators()
    {
        MithraPrimaryKeyGenerator.getInstance().clearPrimaryKeyGenerators();
    }

    public int getRandomInt(int max)
    {
        int result;
        synchronized (randomGenerator)
        {
            result = randomGenerator.nextInt(max);
        }
        return result;
    }

    /**
     * Use this method very carefully. It can easily lead to a deadlock.
     * executes the transactional command in a separate thread. Using a separate thread
     * creates a brand new transactional context and therefore this transaction will not join
     * the context of any outer transactions. For example, if the outer transaction rolls back, this command will not.
     * Calling this method will lead to deadlock if the outer transaction has locked any object that will be accessed
     * in this transaction. It can also lead to deadlock if the same table is accessed in the outer transaction as
     * this command. It can also lead to a deadlock if the connection pool is tied up in the outer transaction
     * and has nothing left for this command.
     * @param command an implementation of TransactionalCommand
     * @return whatever the transaction command's execute method returned.
     * @throws MithraBusinessException
     */
    public <R> R executeTransactionalCommandInSeparateThread(final TransactionalCommand<R> command)
            throws MithraBusinessException
    {
        return this.executeTransactionalCommandInSeparateThread(command, this.defaultTransactionStyle);
    }

    /**
     * Use this method very carefully. It can easily lead to a deadlock.
     * executes the transactional command in a separate thread with a custom number of retries. Using a separate thread
     * creates a brand new transactional context and therefore this transaction will not join
     * the context of any outer transactions. For example, if the outer transaction rolls back, this command will not.
     * Calling this method will lead to deadlock if the outer transaction has locked any object that will be accessed
     * in this transaction. It can also lead to deadlock if the same table is accessed in the outer transaction as
     * this command. It can also lead to a deadlock if the connection pool is tied up in the outer transaction
     * and has nothing left for this command.
     * @param command an implementation of TransactionalCommand
     * @param retries number of times to retry in case of retriable exceptions
     * @return whatever the transaction command's execute method returned.
     * @throws MithraBusinessException if something goes wrong. the transaction will be fully rolled back in this case.
     */
    public Object executeTransactionalCommandInSeparateThread(final TransactionalCommand command, final int retries)
            throws MithraBusinessException
    {
        return executeTransactionalCommandInSeparateThread(command, new TransactionStyle(this.transactionTimeout, retries));
    }

    /**
     * Use this method very carefully. It can easily lead to a deadlock.
     * executes the transactional command in a separate thread with a custom number of retries. Using a separate thread
     * creates a brand new transactional context and therefore this transaction will not join
     * the context of any outer transactions. For example, if the outer transaction rolls back, this command will not.
     * Calling this method will lead to deadlock if the outer transaction has locked any object that will be accessed
     * in this transaction. It can also lead to deadlock if the same table is accessed in the outer transaction as
     * this command. It can also lead to a deadlock if the connection pool is tied up in the outer transaction
     * and has nothing left for this command.
     * @param command an implementation of TransactionalCommand
     * @param txStyle the options for this transaction (retries, timeout, etc).
     * @return whatever the transaction command's execute method returned.
     * @throws MithraBusinessException if something goes wrong. the transaction will be fully rolled back in this case.
     */
    public <R> R executeTransactionalCommandInSeparateThread(final TransactionalCommand<R> command, final TransactionStyle txStyle)
            throws MithraBusinessException
    {
        final Object[] result = new Object[1];
        ExceptionHandlingTask runnable = new ExceptionHandlingTask()
        {
            @Override
            public void execute()
            {
                result[0] = executeTransactionalCommand(command, txStyle);
            }
        };
        ExceptionCatchingThread.executeTask(runnable);
        return (R) result[0];
    }

    /**
     * executes the given transactional command with the custom number of retries
     * @param command
     * @param retryCount number of times to retry if the exception is retriable (e.g. deadlock)
     * @throws MithraBusinessException
     */
    public <R> R executeTransactionalCommand(final TransactionalCommand<R> command, final int retryCount)
            throws MithraBusinessException
    {
        return this.executeTransactionalCommand(command, new TransactionStyle(this.transactionTimeout, retryCount));
    }

    /**
     * executes the given transactional command with the custom transaction style.
     * @param command
     * @param style
     * @throws MithraBusinessException
     */
    public <R> R executeTransactionalCommand(final TransactionalCommand<R> command, final TransactionStyle style)
            throws MithraBusinessException
    {
        String commandName = command.getClass().getName();
        MithraTransaction tx = this.getCurrentTransaction();
        if (tx != null)
        {
            try
            {
                return command.executeTransaction(tx);
            }
            catch(RuntimeException e)
            {
                throw e;
            }
            catch (Throwable throwable)
            {
                getLogger().error(commandName+" rolled back tx, will not retry.", throwable);
                tx.expectRollbackWithCause(throwable);
                throw new MithraBusinessException(commandName+" transaction failed", throwable);
            }
        }
        R result = null;
        int retryCount = style.getRetries() + 1;
        do
        {
            try
            {
                tx = this.startOrContinueTransaction(style);
                tx.setTransactionName("Transactional Command: "+commandName);
                result = command.executeTransaction(tx);

                tx.commit();
                retryCount = 0;
            }
            catch (Throwable throwable)
            {
                retryCount = MithraTransaction.handleTransactionException(tx, throwable, retryCount, style);
            }
        }
        while(retryCount > 0);
        return result;
    }

    public void sleepBeforeTransactionRetry()
    {
        try
        {
            Thread.sleep(RETRY_TRANSACTION_TIME/2+this.getRandomInt((int)(RETRY_TRANSACTION_TIME/2)));
        }
        catch (InterruptedException e)
        {
            getLogger().warn("Unexpected interruption", e);
        }
    }

    /**
     * executes the transactional command. If the command throws an exception, the transaction is rolled back.
     * If the exception is caused by a retriable condition (e.g. database deadlock), it is automatically
     * retried (up to 10 times).
     * @param command an implementation of TransactionalCommand
     * @return whatever the transaction command's execute method returned.
     * @throws MithraBusinessException
     */
    public <R> R executeTransactionalCommand(final TransactionalCommand<R> command)
            throws MithraBusinessException
    {
        return this.executeTransactionalCommand(command, this.defaultTransactionStyle);
    }

    public MithraRemoteTransactionProxy startRemoteTransactionProxy(Xid xid, int timeoutInMilliseconds)
    {
        MithraRemoteTransactionProxy result = new MithraRemoteTransactionProxy(xid, timeoutInMilliseconds);
        this.setThreadTransaction(result);
        return result;
    }

    public void setNotificationEventManager(MithraNotificationEventManager manager)
    {
        if (manager != null)
        {
            this.notificationEventManager = manager;
        }
    }

    public MithraNotificationEventManager getNotificationEventManager()
    {
        return this.notificationEventManager;
    }

    public void mustBeInTransaction(String error)
    {
        if (!this.isInTransaction())
        {
            throw new MithraBusinessException(error);
        }
    }
    private static Logger getLogger()
    {
        return logger;
    }    

    public long getCurrentProcessingTime()
    {
        long result;
        MithraTransaction tx = this.getCurrentTransaction();
        if (tx != null)
        {
            result = tx.getProcessingStartTime();
        }
        else
        {
            result = System.currentTimeMillis();
        }
        int lastDigit =(int) (result % 10);
        if (lastDigit == 9) result -= 3;
        else result -= (result % 3);
        return result;
    }

    /**
     * sets the value of minimum queries to keep per class. 32 is the default. This can be
     * overriden in a configuration file using &lt;MithraRuntime defaultMinQueriesToKeep="100"&gt;
     * or on a per object basis
     * &lt;MithraObjectConfiguration className="com.gs.fw.para.domain.desk.product.ProductScrpMap" cacheType="partial" minQueriesToKeep="100"/&gt;
     * @param defaultMinQueriesToKeep
     */
    public void setDefaultMinQueriesToKeep(int defaultMinQueriesToKeep)
    {
        this.configManager.setDefaultMinQueriesToKeep(defaultMinQueriesToKeep);
    }

    /**
     * sets the value of relationship cache per class. 10000 is the default. This can be
     * overriden in a configuration file using &lt;MithraRuntime defaultRelationshipCacheSize="100"&gt;
     * or on a per object basis
     * &lt;MithraObjectConfiguration className="com.gs.fw.para.domain.desk.product.ProductScrpMap" cacheType="partial" relationshipCacheSize="50000"/&gt;
     * @param defaultRelationshipCacheSize
     */
    public void setDefaultRelationshipCacheSize(int defaultRelationshipCacheSize)
    {
        this.configManager.setDefaultRelationshipCacheSize(defaultRelationshipCacheSize);
    }

    public void cleanUpRuntimeCacheControllers()
    {
        this.configManager.cleanUpRuntimeCacheControllers();
    }

    public void cleanUpRuntimeCacheControllers(Set<String> classesToCleanUp)
    {
        this.configManager.cleanUpRuntimeCacheControllers(classesToCleanUp);
    }

    /**
     * @deprecated use readConfiguration instead
     * @param mithraFileIs
     * @return
     * @throws MithraBusinessException
     */
    public List<MithraRuntimeConfig> initDatabaseObjects(InputStream mithraFileIs)
    throws MithraBusinessException
    {
        return this.configManager.initDatabaseObjects(mithraFileIs);
    }

    public List<MithraRuntimeConfig> initDatabaseObjects(MithraRuntimeType mithraRuntimeType)
    {
        return this.configManager.initDatabaseObjects(mithraRuntimeType);
    }

    /**
     * only used for MithraTestResource. Do not use. use readConfiguration instead.
     * @param mithraRuntimeType
     * @param hook
     */
    public void zLazyInitObjectsWithCallback(MithraRuntimeType mithraRuntimeType, MithraConfigurationManager.PostInitializeHook hook)
    {
        configManager.lazyInitObjectsWithCallback(mithraRuntimeType, hook);
    }

    public MithraObjectPortal initializePortal(String className)
    {
        return this.configManager.initializePortal(className);
    }

    /**
     * Parses the configuration file. Should only be called by MithraTestResource. Use readConfiguration to parse
     * and initialize the configuration.
     * @param mithraFileIs input stream containing the runtime configuration.
     * @return the parsed configuration
     */
    public MithraRuntimeType parseConfiguration(InputStream mithraFileIs)
    {
        return this.configManager.parseConfiguration(mithraFileIs);
    }

    public Set getThreeTierConfigSet()
    {
        return this.configManager.getThreeTierConfigSet();
    }

    /**
     * This method will load the cache of the object already initialized. A Collection is used
     * to keep track of the objects to load.
     * @param portals list of portals to load caches for
     * @param threads number of parallel threads to load
     * @throws MithraBusinessException if something goes wrong during the load
     */
    public void loadMithraCache(List<MithraObjectPortal> portals, int threads) throws MithraBusinessException
    {
        this.configManager.loadMithraCache(portals, threads);
    }

    public void readConfiguration(InputStream mithraFileIs) throws MithraBusinessException
    {
        this.configManager.readConfiguration(mithraFileIs);
    }

    public void initializeRuntime(MithraRuntimeType mithraRuntimeType)
    {
        this.configManager.initializeRuntime(mithraRuntimeType);
    }

    /**
     * Clears all query caches. Note: During a transaction, Mithra allocates a special query cache, which will NOT be
     * cleared by this method.
     */
    public void clearAllQueryCaches()
    {
        this.configManager.clearAllQueryCaches();
    }

    public Set<MithraRuntimeCacheController> getRuntimeCacheControllerSet()
    {
        return this.configManager.getRuntimeCacheControllerSet();
    }

    public MithraReplicationNotificationManager getReplicationNotificationManager()
    {
        return this.configManager.getReplicationNotificationManager();
    }

    public void zDealWithHungTx()
    {
        MithraTransaction mithraTx = this.threadTransaction.get();
        if (mithraTx != null)
        {
            this.threadTransaction.set(null);
        }
    }

    public Set<RemoteMithraObjectConfig> getCacheReplicableConfigSet()
    {
        return this.configManager.getCacheReplicableConfigSet();
    }
}
