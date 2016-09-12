
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

package com.gs.fw.common.mithra.transaction;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalState;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.behavior.PerPortalTemporalContainer;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.slf4j.Logger;

import javax.transaction.Status;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;


public abstract class MithraLocalTransaction extends MithraTransaction
{
    protected static final byte MITHRA_STATUS_STARTED = 1;
    protected static final byte MITHRA_STATUS_COMMITTING = 2;
    protected static final byte MITHRA_STATUS_ROLLING_BACK = 3;
    protected static final byte MITHRA_STATUS_COMMITTED = 4;
    protected static final byte MITHRA_STATUS_ROLLED_BACK = 5;
    protected static final byte MITHRA_STATUS_MARKED_ROLLBACK_ONLY = 6;

    protected static final ReentrantLock globalCoordinationLock = new ReentrantLock();

    protected final InternalList txObjects = new InternalList();
    protected final InternalList readLockedObjects = new InternalList();
    protected Set<Cache> txCaches = new UnifiedSet<Cache>();
    protected Set<MithraObjectPortal> customPortals = null;
    protected byte txStatus;
    protected int bulkInsertThreshold = 0;
    private TransactionalState[] readLockedTxStates = new TransactionalState[6];
    private DatedTransactionalState[] readLockedDatedTxStates = new DatedTransactionalState[6];
    protected final Map<String, List<MithraNotificationEvent>> notificationEvents = new UnifiedMap();

    private OperationMode operationMode = OperationMode.WRITE;
    private boolean prepared = false;
    private UnifiedMap<TransactionalState, TransactionalState> sharedTxStates;
    private UnifiedMap<DatedTransactionalState, DatedTransactionalState> sharedDatedTxStates;
    private UnifiedMap<MithraObjectPortal, PerPortalTemporalContainer> perPortalTemporalContainerMap;
    private boolean sharedTxStatesCleared;

    private int expectedExecuteReturn;
    private int expectedExecuteBatchReturn;

    /**
     * Indicates whether this transaction was created from within Mithra proper.
     * Transactions started using JTA directly will set this to false.
     */
    protected boolean startedTransaction;

    private MithraTransaction waitingForTransaction;
    private boolean isInOperationEvaluationMode;
    private String transactionName;
    private final long realStartTime;
    private long processingStartTime = Long.MIN_VALUE;
    private final Thread startingThread;
    private int timeoutInMilliseconds;
    private static final int MAX_WAIT_FOR_DEAD_TRANSACTION = 2000;
    private TransactionLocalMap transactionLocalMap = new TransactionLocalMap();
    private int databaseRetrieveCount = 0;
    private int remoteRetrieveCount = 0;
    private UnifiedMap<MithraObjectPortal, MithraPerformanceData> performanceDataByPortal;

    protected MithraLocalTransaction(int timeoutInMilliseconds)
    {
        this.timeoutInMilliseconds = timeoutInMilliseconds;
        this.realStartTime = System.currentTimeMillis();
        startingThread = Thread.currentThread();
    }

    private TransactionalState getReadLockedTxState(int persistenceState)
    {
        TransactionalState result = readLockedTxStates[persistenceState];
        if (readLockedTxStates[persistenceState] == null)
        {
            result = new TransactionalState(this, persistenceState);
            readLockedTxStates[persistenceState] = result;
        }
        return result;
    }

    private DatedTransactionalState getReadLockedDatedTxState(int persistenceState)
    {
        DatedTransactionalState result = readLockedDatedTxStates[persistenceState];
        if (readLockedDatedTxStates[persistenceState] == null)
        {
            result = new DatedTransactionalState(this, persistenceState, null, null, null, true);
            readLockedDatedTxStates[persistenceState] = result;
        }
        return result;
    }

    @Override
    public long getTimeoutInMilliseconds()
    {
        return timeoutInMilliseconds;
    }

    @Override
    public TransactionLocalMap getTransactionLocalMap()
    {
        return this.transactionLocalMap;
    }

    @Override
    public TransactionalState getReadLockedTransactionalState(TransactionalState transactionalState, int persistenceState)
    {
        if (transactionalState == null || transactionalState.hasNoTransactions())
        {
            return this.getReadLockedTxState(persistenceState);
        }
        TransactionalState result = null;
        boolean newResult = false;
        synchronized (this)
        {
            if (sharedTxStates == null)
            {
                sharedTxStates = new UnifiedMap();
            }
            else
            {
                result = sharedTxStates.get(transactionalState);
            }
            if (result == null)
            {
                newResult = true;
                result = new TransactionalState(transactionalState, this, persistenceState);
                sharedTxStates.put(transactionalState, result);
            }
        }
        if (newResult) result.addToAllExcept(this);
        return result;
    }

    @Override
    public DatedTransactionalState getReadLockedDatedTransactionalState(DatedTransactionalState transactionalState, int persistenceState)
    {
        if (transactionalState == null || transactionalState.hasNoTransactions())
        {
            return this.getReadLockedDatedTxState(persistenceState);
        }
        DatedTransactionalState result = null;
        boolean newResult = false;
        synchronized (this)
        {
            if (sharedDatedTxStates == null)
            {
                sharedDatedTxStates = new UnifiedMap();
            }
            else
            {
                result = sharedDatedTxStates.get(transactionalState);
            }
            if (result == null)
            {
                newResult = true;
                result = new DatedTransactionalState(transactionalState, this, persistenceState);
                sharedDatedTxStates.put(transactionalState, result);
            }
        }
        if (newResult) result.addToAllExcept(this);
        return result;
    }

    public MithraTransaction getMithraRootTransaction()
    {
        return this;
    }

    @Override
    public void addSharedTransactionalState(TransactionalState transactionalState)
    {
        synchronized (this)
        {
            if (sharedTxStatesCleared)
            {
                transactionalState.removeThreadTx(this);
                return;
            }
            if (sharedTxStates == null)
            {
                sharedTxStates = new UnifiedMap();
            }
            sharedTxStates.put(transactionalState, transactionalState);
        }
    }

    @Override
    public void addSharedDatedTransactionalState(DatedTransactionalState transactionalState)
    {
        synchronized (this)
        {
            if (sharedTxStatesCleared)
            {
                transactionalState.removeThreadTx(this);
                return;
            }
            if (sharedDatedTxStates == null)
            {
                sharedDatedTxStates = new UnifiedMap();
            }
            sharedDatedTxStates.put(transactionalState, transactionalState);
        }
    }

    @Override
    public void enrollReadLocked(MithraTransactionalObject mithraTransactionalObject)
    {
        this.readLockedObjects.add(mithraTransactionalObject);
    }

    @Override
    public boolean isInFuture(long time)
    {
        long txTime = processingStartTime;
        if (txTime == Long.MIN_VALUE)
        {
            txTime = realStartTime;
        }
        return time > txTime;
    }

    @Override
    public long getRealStartTime()
    {
        return realStartTime;
    }

    @Override
    public long getStartTime()
    {
        return getProcessingStartTime();
    }

    @Override
    public long getProcessingStartTime()
    {
        if (this.processingStartTime == Long.MIN_VALUE)
        {
            this.processingStartTime = System.currentTimeMillis();
        }
        return this.processingStartTime;
    }

    @Override
    public void setProcessingStartTime(long time)
    {
        this.processingStartTime = time;
    }

    @Override
    public void setRetryOnOptimisticLockFailure(boolean retryOnFailure)
    {
        // ignore this
    }

    @Override
    public boolean retryOnOptimisticLockFailure()
    {
        return false;
    }

    @Override
    protected boolean isNestedTransaction()
    {
        return false;
    }

    @Override
    public void setFlushNestedCommit(boolean value)
    {
        // Nothing to do, as this not a nested transaction.
    }

    protected void throwRetriableTransaction() throws MithraTransactionException
    {
        throw new MithraTransactionException("Deadlock detected.", this);
    }

    @Override
    public void setWaitingForTransaction(MithraTransaction waitingForTransaction)
    {
        this.waitingForTransaction = waitingForTransaction;
    }

    @Override
    public void zSetOperationEvaluationMode(boolean evalMode)
    {
        this.isInOperationEvaluationMode = evalMode;
    }

    @Override
    public boolean zIsInOperationEvaluationMode()
    {
        return this.isInOperationEvaluationMode;
    }

    @Override
    protected MithraTransaction getWaitingForTransaction()
    {
        return this.waitingForTransaction;
    }

    protected void finalCleanup(byte statusToSet)
    {
        if (customPortals != null)
        {
            Iterator it = customPortals.iterator();
            for (int i=0; i<this.customPortals.size(); i++)
            {
                MithraObjectPortal portal = (MithraObjectPortal) it.next();
                portal.clearTxParticipationMode(this);
            }
        }
        synchronized (this.txObjects)
        {
            this.synchronizedFinalCleanup();

            this.txObjects.clear();
            this.txStatus = statusToSet;
            MithraManagerProvider.getMithraManager().removeTransaction(this);
            this.setWaitingForTransaction(null);
            this.txObjects.notifyAll();
        }
        Iterator it = this.txCaches.iterator();
        for (int i=0; i<this.txCaches.size(); i++)
        {
            Cache cache = (Cache) it.next();
            cache.reloadDirty(this);
        }        
        this.txCaches.clear();
    }

    protected void synchronizedFinalCleanup()
    {
        // By default do nothing
    }

    protected void handleCacheCommit() throws MithraTransactionException
    {
        handleSharedReadTransactionalStates();
        synchronized (this.txObjects)
        {
            for (int i = 0; i < this.txObjects.size(); i++)
            {
                MithraTransactionalResource obj = (MithraTransactionalResource) this.txObjects.get(i);
                obj.zHandleCommit();
            }
            Iterator it = this.txCaches.iterator();
            for (int i=0; i<this.txCaches.size(); i++)
            {
                Cache cache = (Cache) it.next();
                cache.commit(this);
            }            
            this.synchronizedHandleCacheCommit();
            this.finalCleanup(MITHRA_STATUS_COMMITTED);
        }
        if (!this.notificationEvents.isEmpty())
        {
            MithraManagerProvider.getMithraManager().getNotificationEventManager().broadcastNotificationMessage(this.notificationEvents, getRequesterVmId());
        }
    }

    private void handleSharedReadTransactionalStates()
    {
        for(TransactionalState txState: readLockedTxStates)
        {
            if (txState != null) txState.clearCurrentTx();
        }
        for(DatedTransactionalState txState: readLockedDatedTxStates)
        {
            if (txState != null) txState.clearCurrentTx();
        }
        synchronized (this)
        {
            if (sharedTxStates != null)
            {
                sharedTxStates.forEachValue(new Procedure<TransactionalState>()
                {
                    public void value(TransactionalState txState)
                    {
                        txState.removeThreadTx(MithraLocalTransaction.this);
                    }
                } );
                this.sharedTxStates = null;
            }
            if (sharedDatedTxStates != null)
            {
                sharedDatedTxStates.forEachValue(new Procedure<DatedTransactionalState>()
                {
                    public void value(DatedTransactionalState txState)
                    {
                        txState.removeThreadTx(MithraLocalTransaction.this);
                    }
                } );
                this.sharedDatedTxStates = null;
            }
            this.sharedTxStatesCleared= true;
        }
    }

    protected void synchronizedHandleCacheCommit() throws MithraTransactionException
    {
        // By default do nothing
    }

    protected void handleCacheRollback() throws MithraTransactionException
    {
        handleSharedReadTransactionalStates();
        synchronized (this.txObjects)
        {
            for(int i = 0; i < this.txObjects.size();i++)
            {
                MithraTransactionalResource obj = (MithraTransactionalResource) this.txObjects.get(i);
                obj.zHandleRollback(this);
            }
            Iterator it = this.txCaches.iterator();
            for (int i=0; i<this.txCaches.size(); i++)
            {
                Cache cache = (Cache) it.next();
                cache.rollback(this);
            }
            this.synchronizedHandleCacheRollback();
            this.finalCleanup(MITHRA_STATUS_ROLLED_BACK);
        }
    }

    protected void synchronizedHandleCacheRollback() throws MithraTransactionException
    {
        // By default do nothing
    }

    protected void handleCachePrepare() throws MithraTransactionException
    {
        synchronized (this.txObjects)
        {
            Iterator it = this.txCaches.iterator();
            for (int i=0; i<this.txCaches.size(); i++)
            {
                Cache cache = (Cache) it.next();
                cache.prepareForCommit(this);
            }
            prepared = true;
        }
    }

    @Override
    public void setStarted(boolean val)
    {
        this.startedTransaction = val;
    }

    @Override
    public boolean startedTransaction()
    {
        return this.startedTransaction;
    }

    @Override
    public String getTransactionName()
    {
        return this.transactionName;
    }

    @Override
    public void setTransactionName(String transactionName)
    {
        this.transactionName = transactionName;
    }

    @Override
    public void enrollResource(MithraTransactionalResource resource)
    {
        synchronized (this.txObjects)
        {
            this.txObjects.add(resource);
            this.setWaitingForTransaction(null);
        }
    }

    @Override
    public void enrollCache(Cache cache)
    {
        synchronized (this.txObjects)
        {
            this.txCaches.add(cache);
            this.setWaitingForTransaction(null);
        }
    }

    @Override
    public void waitForTransactionToFinish() throws MithraTransactionException
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null)
        {
            waitUntilDone(System.currentTimeMillis(), (int) this.getTimeoutInMilliseconds());
        }
        else
        {
            this.waitForTransactionToFinish(threadTx);
        }
    }

    @Override
    public void waitForTransactionToFinish(MithraTransaction threadTx) throws MithraTransactionException
    {
        if(threadTx != null && threadTx.equals(this)) return;
        boolean lockAcquired = false;
        try
        {
            MithraLocalTransaction.globalCoordinationLock.lock();
            lockAcquired = true;
            synchronized (this.txObjects)
            {
                if (! (this.txStatus == MITHRA_STATUS_COMMITTED || this.txStatus == MITHRA_STATUS_ROLLED_BACK) && threadTx != null)
                {
                    if (this.getWaitingForTransaction() != null && this.isTransactionInWaitChain(threadTx))
                    {
                        this.throwRetriableTransaction();
                    }

                    threadTx.setWaitingForTransaction(this);
                    MithraLocalTransaction.globalCoordinationLock.unlock();
                    lockAcquired = false;
                    long now = System.currentTimeMillis();
                    int threadTxTimeToLive = (int) (threadTx.getTimeoutInMilliseconds() - now + threadTx.getRealStartTime());

                    waitUntilDone(now, threadTxTimeToLive);
                    threadTx.setWaitingForTransaction(null);
                }
            }
        }
        finally
        {
            if (lockAcquired)
            {
                MithraLocalTransaction.globalCoordinationLock.unlock();
            }
        }
    }

    private void waitUntilDone(long now, int threadTxTimeToLive)
    {
        synchronized (this.txObjects)
        {
            try
            {
                if (this.startingThread.isAlive())
                {
                    int thisTimeToLive = (int) (this.getTimeoutInMilliseconds() - now + this.getRealStartTime()) + MAX_WAIT_FOR_DEAD_TRANSACTION;
                    int timeToWait = Math.max(10, Math.min(threadTxTimeToLive, thisTimeToLive));
                    this.txObjects.wait(timeToWait);
                }
            }
            catch (InterruptedException e)
            {
                getLogger().error("unexpected exception", e);
            }

            checkForHungOrAbandonedTransaction();
        }
    }

    private void checkForHungOrAbandonedTransaction()
    {
        if (!(this.txStatus == MITHRA_STATUS_COMMITTED || this.txStatus == MITHRA_STATUS_ROLLED_BACK))
        {
            boolean isDead = !this.startingThread.isAlive() ||
                    (!prepared && this.timeoutInMilliseconds > 0 && this.txStatus != MITHRA_STATUS_COMMITTING &&
                            (System.currentTimeMillis() - this.realStartTime) > (this.timeoutInMilliseconds*11/10 + MAX_WAIT_FOR_DEAD_TRANSACTION));
            if (isDead)
            {
                markForRollbackOnly();
                // attempt to rollback the objects, but leave the caches and tx resources alone
                getLogger().error("Waiting for objects in hung or abandoned transaction! Attempting to rollback the bad transaction. Find and fix the cause" +
                        " of this dead transaction: Name: "+this.transactionName + " started: "+new Date(this.realStartTime).toString()+
                        " in thread "+this.startingThread.getName()+" which is "+
                        ( this.startingThread.isAlive() ? "alive with stack trace\n"+dumpThreads(): "dead"));
                handleCacheRollback();
                asyncRollback();
            }
            else
            {
                MithraTransactionException mithraTransactionException = new MithraTransactionException("waited too long for transaction to finish!");
                mithraTransactionException.setRetriable(true);
                throw mithraTransactionException;
            }
        }

    }

    protected String dumpThreads()
    {
        StringBuilder result = new StringBuilder(500);
        StackTraceElement[] elements = Thread.getAllStackTraces().get(this.startingThread);
        if (elements != null)
        {
            result.append("Thread: ").append(this.startingThread.getName()).append(" is ").append(this.startingThread.getState()).append(" at:" + "\n");
            for(StackTraceElement elm : elements)
            {
                result.append("    ").append(elm.toString()).append("\n");
            }
        }
        return result.toString();
    }

    protected void asyncRollback()
    {
        // subclass to override
    }

    protected void markForRollbackOnly()
    {
        this.txStatus = MITHRA_STATUS_MARKED_ROLLBACK_ONLY;
    }

    @Override
    public void waitUntilFinished(long timeout)
    {
        synchronized (this.txObjects)
        {
            if (! (this.txStatus == MITHRA_STATUS_COMMITTED || this.txStatus == MITHRA_STATUS_ROLLED_BACK))
            {
                int status = 0;
                try
                {
                    status = this.getJtaTransactionStatus();
                }
                catch (MithraTransactionException e)
                {
                    this.getLogger().error("could not get transaction status ", e);
                }

                if (status == Status.STATUS_NO_TRANSACTION || status == Status.STATUS_ROLLEDBACK)
                {
                    this.handleCacheRollback();
                    return;
                }

                try
                {
                    this.txObjects.wait(timeout);
                }
                catch (InterruptedException e)
                {
                    getLogger().error("unexpected exception", e);
                }
            }
        }
    }
    @Override
    public void setImmediateOperations(boolean val) throws MithraDatabaseException
    {
        // nothing to do
    }

    @Override
    public MithraTransaction getParent()
    {
        return null;
    }

    @Override
    public OperationMode getOperationMode()
    {
        return this.operationMode;
    }

    @Override
    public void setWriteOperationMode(OperationMode mode)
    {
        this.operationMode = mode;
    }

    @Override
    public void setTxParticipationMode(MithraObjectPortal portal, TxParticipationMode mode)
    {
        if (this.customPortals == null)
        {
            this.customPortals = new UnifiedSet<MithraObjectPortal>();
        }
        this.customPortals.add(portal);
        portal.setTxParticipationMode(mode, this);
    }

    protected abstract Logger getLogger();

    protected abstract long getRequesterVmId();

    /**
      * Cleanup code. Attempting to rollback all uncommitted transactions
      */
    @Override
    protected void finalize() throws Throwable
    {
        // If transaction is not committed we have to roll it back
        if (!(this.txStatus == MITHRA_STATUS_COMMITTED || this.txStatus == MITHRA_STATUS_ROLLED_BACK))
        {
            try
            {
                this.rollback();
            }
            catch (MithraTransactionException e)
            {
                getLogger().error("Could not rollback stale transaction", e);
            }
        }
        super.finalize();
    }

    @Override
    public int getBulkInsertThreshold()
    {
        return bulkInsertThreshold;
    }

    @Override
    public void setBulkInsertThreshold(int bulkInsertThreshold)
    {
        this.bulkInsertThreshold = bulkInsertThreshold;
    }

    @Override
    public PerPortalTemporalContainer getPerPortalTemporalContainer(MithraObjectPortal portal, AsOfAttribute processingAsOfAttribute)
    {
        PerPortalTemporalContainer result = null;
        if (this.perPortalTemporalContainerMap == null)
        {
            this.perPortalTemporalContainerMap = new UnifiedMap<MithraObjectPortal, PerPortalTemporalContainer>();
        }
        else
        {
            result = this.perPortalTemporalContainerMap.get(portal);
        }
        if (result == null)
        {
            result = new PerPortalTemporalContainer(this, portal, processingAsOfAttribute);
            this.perPortalTemporalContainerMap.put(portal, result);
        }
        return result;
    }

    public Thread getStartingThread()
    {
        return startingThread;
    }

    public int getExpectedExecuteReturn()
    {
        return expectedExecuteReturn;
    }

    public void setExpectedExecuteReturn(int expectedExecuteReturn)
    {
        this.expectedExecuteReturn = expectedExecuteReturn;
    }

    public int getExpectedExecuteBatchReturn()
    {
        return expectedExecuteBatchReturn;
    }

    public void setExpectedExecuteBatchReturn(int expectedExecuteBatchReturn)
    {
        this.expectedExecuteBatchReturn = expectedExecuteBatchReturn;
    }

    @Override
    protected void incrementDatabaseRetrieveCount()
    {
        this.databaseRetrieveCount++;
    }

    public int getDatabaseRetrieveCount()
    {
        return this.databaseRetrieveCount;
    }

    @Override
    protected void incrementRemoteRetrieveCount()
    {
        this.remoteRetrieveCount++;
    }

    public int getRemoteRetrieveCount()
    {
        return this.remoteRetrieveCount;
    }

    @Override
    public MithraPerformanceData getTransactionPerformanceDataFor(MithraObjectPortal mithraObjectPortal)
    {
        if(this.performanceDataByPortal == null)
        {
            this.performanceDataByPortal = new UnifiedMap<MithraObjectPortal, MithraPerformanceData>();
        }

        MithraPerformanceData performanceData = this.performanceDataByPortal.get(mithraObjectPortal);

        if(performanceData == null)
        {
            performanceData = new MithraPerformanceData(mithraObjectPortal);
            this.performanceDataByPortal.put(mithraObjectPortal, performanceData);
        }

        return performanceData;
    }

    @Override
    public Map<MithraObjectPortal, MithraPerformanceData> getTransactionPerformanceData()
    {
        return this.performanceDataByPortal;
    }
}
