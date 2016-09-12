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

import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.PerPortalTemporalContainer;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.behavior.state.TransactionalBehaviorChooser;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.transaction.MithraTransactionalResource;
import com.gs.fw.common.mithra.transaction.TransactionLocalMap;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;
import java.util.Map;



public abstract class MithraTransaction implements TransactionalBehaviorChooser
{


    public static final int DEFAULT_TRANSACTION_RETRIES = 10;

    private static final Logger logger = LoggerFactory.getLogger(MithraTransaction.class);
    private static IntObjectHashMap<String> jtaTransactionStatus;

    static
    {
        jtaTransactionStatus = IntObjectHashMap.newMap();
        jtaTransactionStatus.put(Status.STATUS_ACTIVE, "STATUS_ACTIVE");
        jtaTransactionStatus.put(Status.STATUS_COMMITTED, "STATUS_COMMITTED");
        jtaTransactionStatus.put(Status.STATUS_COMMITTING, "STATUS_COMMITTING");
        jtaTransactionStatus.put(Status.STATUS_MARKED_ROLLBACK, "STATUS_MARKED_ROLLBACK");
        jtaTransactionStatus.put(Status.STATUS_NO_TRANSACTION, "STATUS_NO_TRANSACTION");
        jtaTransactionStatus.put(Status.STATUS_PREPARED, "STATUS_PREPARED");
        jtaTransactionStatus.put(Status.STATUS_PREPARING, "STATUS_PREPARING");
        jtaTransactionStatus.put(Status.STATUS_ROLLEDBACK, "STATUS_ROLLEDBACK");
        jtaTransactionStatus.put(Status.STATUS_ROLLING_BACK, "STATUS_ROLLING_BACK");
        jtaTransactionStatus.put(Status.STATUS_UNKNOWN, "STATUS_UNKNOWN");
    }

    public static String getJtaTransactionStatusDescription(int status)
    {
        String result = (String)jtaTransactionStatus.get(status);
        if (result == null) return "Not a valid status "+status;
        return result;
    }

    public String getJtaTransactionStatusDescription() throws MithraTransactionException
    {
        return this.getJtaTransactionStatusDescription(this.getJtaTransactionStatus());
    }

    public TransactionalBehavior getTransactionalBehaviorForReadWithWaitIfNecessary(MithraTransactionalObject mto, TransactionalState transactionalState, int persistenceState)
    {
        return PersistenceState.getTransactionalBehaviorForTransactionForReadWithWaitIfNecessary(this, mto, transactionalState, persistenceState);
    }

    public TransactionalBehavior getTransactionalBehaviorForWriteWithWaitIfNecessary(MithraTransactionalObject mto, TransactionalState transactionalState, int persistenceState)
    {
        return PersistenceState.getTransactionalBehaviorForTransactionForWriteWithWaitIfNecessary(this, mto, transactionalState, persistenceState);
    }

    public DatedTransactionalBehavior getDatedTransactionalBehavior(MithraDatedTransactionalObject mto,
            DatedTransactionalState transactionalState, int persistenceState)
    {
        return DatedPersistenceState.getTransactionalBehaviorForTransaction(this, mto, transactionalState, persistenceState);
    }

    private static Logger getLogger()
    {
        return logger;
    }

    public static void handleNestedTransactionException(MithraTransaction tx, Throwable t)
    {
        String commandName = tx.getTransactionName();
        if (commandName == null) commandName = "";
        if (t instanceof RuntimeException)
        {
            throw (RuntimeException) t;
        }
        getLogger().error(commandName + " rolled back tx, will not retry.", t);
        tx.expectRollbackWithCause(t);
        throw new MithraBusinessException(commandName+" transaction failed", t);
    }

    public static int handleTransactionException(MithraTransaction tx, Throwable t, int retryCount)
    {
        return handleTransactionException(tx, t, retryCount, new TransactionStyle(MithraManagerProvider.getMithraManager().getTransactionTimeout()));
    }

    public static int handleTransactionException(MithraTransaction tx, Throwable t, int retryCount, TransactionStyle style)
    {
        String commandName = tx != null ? tx.getTransactionName() : "";
        if (commandName == null) commandName = "";
        if (t instanceof MithraBusinessException)
        {
            MithraBusinessException mithraException = (MithraBusinessException) t;
            boolean stillRetriable = false;
            if (tx != null)
            {
                retryCount--;
                stillRetriable = ((tx.startedTransaction() && mithraException.isRetriable()) ||
                        (style.isRetriableAfterTimeout() && mithraException.isTimedOut()));
                boolean retriable = stillRetriable && retryCount > 0;
                if (retriable)
                {
                    getLogger().warn(commandName+" rolling back tx; will retry "+retryCount +" more times. "+ mithraException.getMessage());
                    if (getLogger().isDebugEnabled())
                    {
                        getLogger().debug(commandName+" rolling back tx; will retry "+retryCount +" more times.", mithraException);
                    }
                }
                else
                {
                    getLogger().error("rolling back tx because of exception", mithraException);
                }
                tx.rollbackWithCause(mithraException);
                if (retriable)
                {
                    mithraException.waitBeforeRetrying();
                    return retryCount;
                }
            }
            if (stillRetriable)
            {
                getLogger().error(commandName+" rolled back tx. " + style.getRetries()+" retry attempts failed; will not retry.", mithraException);
            }
            else if (tx != null && !tx.startedTransaction())
            {
                getLogger().warn(commandName+" failed in inner transaction. "+ mithraException.getMessage());
                if (getLogger().isDebugEnabled())
                {
                    getLogger().debug(commandName+" failed in inner transaction. ", mithraException);
                }
            }
            else
            {
                getLogger().error(commandName+" rolled back tx. Exception is not retriable.", mithraException);
            }
            throw mithraException;
        }
        else if (t instanceof RuntimeException)
        {
            RuntimeException re = (RuntimeException) t;
            getLogger().error(commandName+" rolled back tx, will not retry.", re);
            if (tx != null)
            {
                tx.rollbackWithCause(re);
            }
            throw re;
        }
        getLogger().error(commandName+" rolled back tx, will not retry.", t);
        if (tx != null)
        {
            tx.rollbackWithCause(t);
        }
        throw new MithraBusinessException(commandName+" transaction failed", t);
    }

    public abstract void setRetryOnOptimisticLockFailure(boolean retryOnFailure);

    public abstract boolean retryOnOptimisticLockFailure();

    protected abstract boolean isNestedTransaction();

    /**
     * @deprecated Use getProcessingStartTime() instead.
     * @return see getProcessingStartTime()
     */
    public abstract long getStartTime();

    public abstract long getRealStartTime();

    /**
     * @return the first time this method is called for this transaction. It is effectively the 
     * time the transaction needed this value, usually for setting processing dates (IN_Z).
     */
    public abstract long getProcessingStartTime();

    public abstract void setProcessingStartTime(long time);

    public abstract void setImmediateOperations(boolean val) throws MithraDatabaseException;

    public abstract void setCautious() throws MithraDatabaseException;

    public abstract boolean isCautious();

    public abstract void commit() throws MithraDatabaseException;

    public abstract void rollback()  throws MithraTransactionException;

    protected abstract void rollbackWithCause(Throwable cause)  throws MithraTransactionException;

    public abstract int getJtaTransactionStatus() throws MithraTransactionException;

    public abstract void enrollObject(MithraTransactionalObject obj, Cache cache);

    public abstract void enrollResource(MithraTransactionalResource resource);

    public abstract void enrollCache(Cache cache);

    public abstract void insert(MithraTransactionalObject obj) throws MithraDatabaseException;

    public abstract void update(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraDatabaseException;

    public abstract void delete(MithraTransactionalObject obj) throws MithraDatabaseException;

    public abstract void deleteUsingOperation(Operation op) throws MithraDatabaseException;

    public abstract int deleteBatchUsingOperation(Operation op, int batchSize) throws MithraDatabaseException;

    public abstract void purge(MithraTransactionalObject obj) throws MithraDatabaseException;

    public abstract void purgeUsingOperation(Operation op) throws MithraDatabaseException;

    public abstract void executeBufferedOperations() throws MithraDatabaseException;

    public abstract void executeBufferedOperationsIfMoreThan(int i) throws MithraDatabaseException;

    public abstract void executeBufferedOperationsForOperation(Operation op, boolean bypassCache) throws MithraDatabaseException;

    public abstract void addLogicalDeleteForPortal(MithraObjectPortal portal);

    public abstract void setStarted(boolean val);

    public abstract boolean startedTransaction();

    public abstract void enlistResource(XAResource resource) throws SystemException, RollbackException;

    public abstract void waitForTransactionToFinish(MithraTransaction threadTx) throws MithraTransactionException;

    public abstract void waitForTransactionToFinish() throws MithraTransactionException;

    public abstract void waitUntilFinished(long timeout);

    protected boolean isTransactionInWaitChain(MithraTransaction tx)
    {
        final MithraTransaction watingForTx = this.getWaitingForTransaction();
        return watingForTx == tx || (watingForTx != null && watingForTx.isTransactionInWaitChain(tx));
    }

    protected abstract MithraTransaction getWaitingForTransaction();

    public abstract String getTransactionName();

    public abstract void setTransactionName(String transactionName);

    public abstract MithraTransaction getParent();

    public abstract void setWaitingForTransaction(MithraTransaction waitingForTransaction);

    public abstract void setFlushNestedCommit(boolean value);

    public abstract void registerSynchronization(Synchronization synchronization);

    public abstract void registerLifeCycleListener(TransactionLifeCycleListener lifeCycleListener);

    public abstract void zSetOperationEvaluationMode(boolean evalMode);

    public abstract boolean zIsInOperationEvaluationMode();

    public abstract void addMithraNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent);

    public abstract OperationMode getOperationMode();

    public abstract void setWriteOperationMode(OperationMode mode);

    public boolean isWriteOperationMode()
    {
        return this.getOperationMode() != OperationMode.READ; //everything else, including transactional reads are considered "write" for backward compatibility
    }

    public abstract void setTxParticipationMode(MithraObjectPortal portal, TxParticipationMode mode);

    public abstract void executeBufferedOperationsForEnroll(MithraObjectPortal mithraObjectPortal);

    public abstract void executeBufferedOperationsForPortal(MithraObjectPortal mithraObjectPortal);

    /**
     * sets the threshold above which bulk inserts are used in a transaction. By default bulk inserts are not used.
     * @param threshold a threshold of zero will disable bulk inserts
     */
    public abstract void setBulkInsertThreshold(int threshold);

    public abstract int getBulkInsertThreshold();

    public abstract long getTimeoutInMilliseconds();

    public abstract TransactionLocalMap getTransactionLocalMap();

    public abstract void expectRollbackWithCause(Throwable throwable);

    public abstract TransactionalState getReadLockedTransactionalState(TransactionalState transactionalState, int persistenceState);

    public abstract void addSharedTransactionalState(TransactionalState transactionalState);

    public abstract void enrollReadLocked(MithraTransactionalObject mithraTransactionalObject);

    public abstract boolean isInFuture(long time);

    public abstract void addSharedDatedTransactionalState(DatedTransactionalState datedTransactionalState);

    public abstract DatedTransactionalState getReadLockedDatedTransactionalState(DatedTransactionalState prev, int persistenceState);

    public abstract PerPortalTemporalContainer getPerPortalTemporalContainer(MithraObjectPortal portal, AsOfAttribute processingAsOfAttribute);

    protected abstract void incrementDatabaseRetrieveCount();

    public abstract int getDatabaseRetrieveCount();

    protected abstract void incrementRemoteRetrieveCount();

    public abstract int getRemoteRetrieveCount();

    public abstract MithraPerformanceData getTransactionPerformanceDataFor(MithraObjectPortal mithraObjectPortal);

    public abstract Map<MithraObjectPortal, MithraPerformanceData> getTransactionPerformanceData();

    public enum OperationMode
    {
        READ, TRANSACTIONAL_READ, WRITE, TEMP_WRITE_FOR_READ, TEMP_WRITE_FOR_WRITE
    }

    public abstract int getExpectedExecuteReturn();

    public abstract int getExpectedExecuteBatchReturn();

    public abstract void setExpectedExecuteReturn(int expected);

    public abstract void setExpectedExecuteBatchReturn(int expected);
}
