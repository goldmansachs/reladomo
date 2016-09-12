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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.behavior.PerPortalTemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.util.MithraPerformanceData;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import java.util.Map;



public class MithraNestedTransaction extends MithraTransaction implements JtaBasedTransaction
{

    private MithraTransaction parent = null;
    private String transactionName = null;
    private boolean flushAtCommit = false;

    public MithraNestedTransaction(MithraTransaction parentTransaction) throws MithraTransactionException
    {
        this.parent = parentTransaction;
    }

    @Override
    public boolean isNestedTransaction()
    {
        return true;
    }

    @Override
    public void commit() throws MithraDatabaseException
    {
        try
        {
            // we do this so that the database can throw exceptions reasonably close to where the happened in code.
            if (flushAtCommit) this.executeBufferedOperations();
        }
        finally
        {
            MithraManagerProvider.getMithraManager().popTransaction(this);
        }
    }

    @Override
    public void rollback() throws MithraTransactionException
    {
        this.getMithraRootTransaction().setExpectRollback();
        MithraManagerProvider.getMithraManager().popTransaction(this);
    }

    @Override
    protected void rollbackWithCause(Throwable cause) throws MithraTransactionException
    {
        MithraRootTransaction mithraRootTransaction = this.getMithraRootTransaction();
        mithraRootTransaction.setExpectRollback();
        mithraRootTransaction.setRollbackCause(cause);
        MithraManagerProvider.getMithraManager().popTransaction(this);
    }

    private MithraRootTransaction getMithraRootTransaction()
    {
        MithraTransaction result = parent;
        while(result.getParent() != null) result = result.getParent();
        return (MithraRootTransaction) result;

    }

///////////////////////////////////////////////// The rest of the methods are delegated to the parent

    @Override
    public long getStartTime()
    {
        return this.getMithraRootTransaction().getStartTime();
    }

    @Override
    public long getRealStartTime()
    {
        return this.getMithraRootTransaction().getRealStartTime();
    }

    @Override
    public long getProcessingStartTime()
    {
        return this.getMithraRootTransaction().getProcessingStartTime();
    }

    @Override
    public void setProcessingStartTime(long time)
    {
        this.getMithraRootTransaction().setProcessingStartTime(time);
    }

    @Override
    public void setRetryOnOptimisticLockFailure(boolean retryOnFailure)
    {
        getMithraRootTransaction().setRetryOnOptimisticLockFailure(retryOnFailure);
    }

    @Override
    public boolean retryOnOptimisticLockFailure()
    {
        return getMithraRootTransaction().retryOnOptimisticLockFailure();
    }

    @Override
    public void setImmediateOperations(boolean val) throws MithraDatabaseException
    {
        getMithraRootTransaction().setImmediateOperations(val);
    }

    @Override
    public void setCautious() throws MithraDatabaseException
    {
        getMithraRootTransaction().setCautious();
    }

    @Override
    public boolean isCautious()
    {
        return getMithraRootTransaction().isCautious();
    }

    @Override
    public MithraTransaction getParent()
    {
        return this.parent;
    }

    public Transaction getJtaTransaction()
    {
        return getMithraRootTransaction().getJtaTransaction();
    }

    @Override
    public int getJtaTransactionStatus() throws MithraTransactionException
    {
        return getMithraRootTransaction().getJtaTransactionStatus();
    }

    @Override
    public void enrollObject(MithraTransactionalObject obj, Cache cache)
    {
        getMithraRootTransaction().enrollObject(obj, cache);
    }

    @Override
    public void enrollResource(MithraTransactionalResource resource)
    {
        getMithraRootTransaction().enrollResource(resource);
    }

    @Override
    public void enrollCache(Cache cache)
    {
        getMithraRootTransaction().enrollCache(cache);
    }

    @Override
    public void insert(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        getMithraRootTransaction().insert(obj);
    }

    @Override
    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraDatabaseException
    {
        getMithraRootTransaction().update(obj, attributeUpdateWrapper);
    }

    @Override
    public void delete(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        getMithraRootTransaction().delete(obj);
    }

    @Override
    public void deleteUsingOperation(Operation op) throws MithraDatabaseException
    {
        getMithraRootTransaction().deleteUsingOperation(op);
    }

    @Override
    public int deleteBatchUsingOperation(Operation op, int rowCount) throws MithraDatabaseException
    {
        return getMithraRootTransaction().deleteBatchUsingOperation(op, rowCount);
    }

    @Override
    public void purge(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        getMithraRootTransaction().purge(obj);
    }

    @Override
    public void purgeUsingOperation(Operation op) throws MithraDatabaseException
    {
        getMithraRootTransaction().purgeUsingOperation(op);
    }

    @Override
    public void executeBufferedOperations() throws MithraDatabaseException
    {
        getMithraRootTransaction().executeBufferedOperations();
    }

    @Override
    public void executeBufferedOperationsIfMoreThan(int i) throws MithraDatabaseException
    {
        getMithraRootTransaction().executeBufferedOperationsIfMoreThan(i);
    }

    @Override
    public void executeBufferedOperationsForOperation(Operation op, boolean bypassCache) throws MithraDatabaseException
    {
        getMithraRootTransaction().executeBufferedOperationsForOperation(op, bypassCache);
    }

    @Override
    public void executeBufferedOperationsForEnroll(MithraObjectPortal mithraObjectPortal)
    {
        getMithraRootTransaction().executeBufferedOperationsForEnroll(mithraObjectPortal);
    }

    @Override
    public void executeBufferedOperationsForPortal(MithraObjectPortal mithraObjectPortal)
    {
        getMithraRootTransaction().executeBufferedOperationsForPortal(mithraObjectPortal);
    }

    @Override
    public void setBulkInsertThreshold(int threshold)
    {
        getMithraRootTransaction().setBulkInsertThreshold(threshold);
    }

    @Override
    public int getBulkInsertThreshold()
    {
        return getMithraRootTransaction().getBulkInsertThreshold();
    }

    @Override
    public void addLogicalDeleteForPortal(MithraObjectPortal portal)
    {
        getMithraRootTransaction().addLogicalDeleteForPortal(portal);
    }

    @Override
    public void setStarted(boolean val)
    {
        // nothing do, a nested transaction is never the start of a transaction.
    }

    @Override
    public boolean startedTransaction()
    {
        return false;
    }

    @Override
    public void enlistResource(XAResource resource) throws SystemException, RollbackException
    {
        getMithraRootTransaction().enlistResource(resource);
    }

    @Override
    public void waitForTransactionToFinish() throws MithraTransactionException
    {
        getMithraRootTransaction().waitForTransactionToFinish();
    }

    @Override
    public void waitForTransactionToFinish(MithraTransaction threadTx) throws MithraTransactionException
    {
        getMithraRootTransaction().waitForTransactionToFinish(threadTx);
    }

    @Override
    public void waitUntilFinished(long timeout)
    {
        this.getMithraRootTransaction().waitUntilFinished(timeout);
    }

    @Override
    public MithraTransaction getWaitingForTransaction()
    {
        return getMithraRootTransaction().getWaitingForTransaction();
    }

    @Override
    public String getTransactionName()
    {
        return transactionName + " in "+getMithraRootTransaction().getTransactionName();
    }

    @Override
    public void setTransactionName(String transactionName)
    {
        this.transactionName = transactionName;
    }

    @Override
    public void setWaitingForTransaction(MithraTransaction waitingForTransaction)
    {
        getMithraRootTransaction().setWaitingForTransaction(waitingForTransaction);
    }

    public int hashCode()
    {
        return this.getMithraRootTransaction().hashCode();
    }

    public boolean equals(Object obj)
    {
        return this.getMithraRootTransaction().equals(obj);
    }

    @Override
    public void setFlushNestedCommit(boolean value)
    {
        this.flushAtCommit = value;
    }

//    public void addMithraNotificationMessage(MithraNotificationMessage message)
//    {
//        this.getMithraRootTransaction().addMithraNotificationMessage(message);
//    }

    @Override
    public void addMithraNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent)
    {
        this.getMithraRootTransaction().addMithraNotificationEvent(databaseIdentifier, notificationEvent);
    }

    @Override
    public void registerSynchronization(Synchronization synchronization)
    {
        this.getMithraRootTransaction().registerSynchronization(synchronization);
    }

    @Override
    public void registerLifeCycleListener(TransactionLifeCycleListener lifeCycleListener)
    {
        this.getMithraRootTransaction().registerLifeCycleListener(lifeCycleListener);
    }

    @Override
    public void zSetOperationEvaluationMode(boolean evalMode)
    {
        this.getMithraRootTransaction().zSetOperationEvaluationMode(evalMode);
    }

    @Override
    public boolean zIsInOperationEvaluationMode()
    {
        return this.getMithraRootTransaction().zIsInOperationEvaluationMode();
    }

    @Override
    public void setWriteOperationMode(OperationMode mode)
    {
        this.getMithraRootTransaction().setWriteOperationMode(mode);
    }

    @Override
    public OperationMode getOperationMode()
    {
        return getMithraRootTransaction().getOperationMode();
    }

    @Override
    public void setTxParticipationMode(MithraObjectPortal portal, TxParticipationMode mode)
    {
        this.getMithraRootTransaction().setTxParticipationMode(portal, mode);
    }

    @Override
    public long getTimeoutInMilliseconds()
    {
        return this.getMithraRootTransaction().getTimeoutInMilliseconds();
    }

    @Override
    public TransactionLocalMap getTransactionLocalMap()
    {
        return this.getMithraRootTransaction().getTransactionLocalMap();
    }

    @Override
    public void expectRollbackWithCause(Throwable throwable)
    {
        this.getMithraRootTransaction().expectRollbackWithCause(throwable);
    }

    @Override
    public TransactionalState getReadLockedTransactionalState(TransactionalState transactionalState, int persistenceState)
    {
        return this.getMithraRootTransaction().getReadLockedTransactionalState(transactionalState, persistenceState);
    }

    @Override
    public void addSharedTransactionalState(TransactionalState transactionalState)
    {
        this.getMithraRootTransaction().addSharedTransactionalState(transactionalState);
    }

    @Override
    public void addSharedDatedTransactionalState(DatedTransactionalState datedTransactionalState)
    {
        this.getMithraRootTransaction().addSharedDatedTransactionalState(datedTransactionalState);
    }

    @Override
    public DatedTransactionalState getReadLockedDatedTransactionalState(DatedTransactionalState prev, int persistenceState)
    {
        return this.getMithraRootTransaction().getReadLockedDatedTransactionalState(prev, persistenceState);
    }

    @Override
    public PerPortalTemporalContainer getPerPortalTemporalContainer(MithraObjectPortal portal, AsOfAttribute processingAsOfAttribute)
    {
        return this.getMithraRootTransaction().getPerPortalTemporalContainer(portal, processingAsOfAttribute);
    }

    @Override
    public void enrollReadLocked(MithraTransactionalObject object)
    {
        this.getMithraRootTransaction().enrollReadLocked(object);
    }

    @Override
    public boolean isInFuture(long time)
    {
        return this.getMithraRootTransaction().isInFuture(time); 
    }

    @Override
    public int getExpectedExecuteReturn()
    {
        return this.getMithraRootTransaction().getExpectedExecuteReturn();
    }

    @Override
    public int getExpectedExecuteBatchReturn()
    {
        return this.getMithraRootTransaction().getExpectedExecuteBatchReturn();
    }

    @Override
    public void setExpectedExecuteReturn(int expected)
    {
        this.getMithraRootTransaction().setExpectedExecuteReturn(expected);
    }

    @Override
    public void setExpectedExecuteBatchReturn(int expected)
    {
        this.getMithraRootTransaction().setExpectedExecuteBatchReturn(expected);
    }

    @Override
    protected void incrementDatabaseRetrieveCount()
    {
        this.getMithraRootTransaction().incrementDatabaseRetrieveCount();
    }

    @Override
    public int getDatabaseRetrieveCount()
    {
        return this.getMithraRootTransaction().getDatabaseRetrieveCount();
    }

    @Override
    protected void incrementRemoteRetrieveCount()
    {
        this.getMithraRootTransaction().incrementRemoteRetrieveCount();
    }

    @Override
    public int getRemoteRetrieveCount()
    {
        return this.getMithraRootTransaction().getRemoteRetrieveCount();
    }

    @Override
    public MithraPerformanceData getTransactionPerformanceDataFor(MithraObjectPortal mithraObjectPortal)
    {
        return this.getMithraRootTransaction().getTransactionPerformanceDataFor(mithraObjectPortal);
    }

    @Override
    public Map<MithraObjectPortal, MithraPerformanceData> getTransactionPerformanceData()
    {
        return getMithraRootTransaction().getTransactionPerformanceData();
    }
}
