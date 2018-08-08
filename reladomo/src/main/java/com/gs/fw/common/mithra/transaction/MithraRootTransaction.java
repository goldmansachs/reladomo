
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
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.list.AbstractTransactionalOperationBasedList;
import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.notification.MithraNotificationEvent;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.SmallSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;
import javax.transaction.xa.XAResource;
import java.util.*;


public class MithraRootTransaction extends MithraLocalTransaction implements Synchronization, JtaBasedTransaction
{

    private static final Logger logger = LoggerFactory.getLogger(MithraRootTransaction.class.getName());

    private static final Extractor[] EXTRACTORS = new Extractor[] { new TxOperationsForIndependentClass.PortalExtractor() };
    private static final DoUntilProcedure independentCommit = new IndependentPortalCommitter();
    private static final DoUntilProcedure independentRollback = new IndependentPortalRollback();
    private static final DoUntilProcedure executeOperationsProc = new ExecuteOperations();
    private static final DoUntilProcedure handleFailedOperationsProc = new HandleFailedOperationsProc();

    private TxOperations dependentOperations = new TxOperations();
    private FullUniqueIndex independentOperations = new FullUniqueIndex(EXTRACTORS, 7);
    private Set<XAResource> enlistedResources = new HashSet<XAResource>(4);
    private int realPendingOperations;
    private SmallSet portalSet;

    private Transaction tx;
    private InternalList synchronizations;
    private InternalList lifeCycleListeners;
    private boolean expectRollback;

    private boolean isExecuting;
    private boolean immediateOperations;
    private boolean cautious;
    private boolean leftJtaTransaction = false;
    private boolean asyncRollback = false;

    private boolean retryOnOptimisticLockFailure = false;
    private Throwable rollbackCause;
    private static final List IDENTITY_EXTRACTOR_LIST = Arrays.asList(new Extractor[] { IdentityExtractor.getInstance() } );

    public MithraRootTransaction(Transaction tx, int timeoutInMilliseconds) throws MithraTransactionException
    {
        super(timeoutInMilliseconds);
        this.tx = tx;
        this.getLogger().debug("Starting transaction");
        this.setStarted(true);
    }

    @Override
    public void setRetryOnOptimisticLockFailure(boolean retryOnFailure)
    {
        this.retryOnOptimisticLockFailure = retryOnFailure;
    }

    @Override
    public boolean retryOnOptimisticLockFailure()
    {
        return this.retryOnOptimisticLockFailure;
    }

    @Override
    public void setImmediateOperations(boolean val) throws MithraDatabaseException
    {
        this.executeBufferedOperations();
        this.immediateOperations = val;
    }

    @Override
    public void setCautious() throws MithraDatabaseException
    {
        this.executeBufferedOperations();
        this.cautious = true;
    }

    @Override
    public boolean isCautious()
    {
        return this.cautious;
    }

    public Transaction getJtaTransaction()
    {
        return this.tx;
    }

    protected void setExpectRollback()
    {
        this.expectRollback = true;
    }

    public void afterCompletion(int status)
    {
        if (asyncRollback) return;
        int goodStatus = status;
        if (status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING)
        {
            goodStatus = Status.STATUS_COMMITTED;
            if (this.txStatus != MITHRA_STATUS_COMMITTING && this.startedTransaction)
            {
                logger.error("Unexpected commit in transaction " + this.getTransactionName(), new Exception("Not an Exception, just for tracing"));
            }
            this.handleCacheCommit();
        }
        else if (status == Status.STATUS_ROLLEDBACK || status == Status.STATUS_ROLLING_BACK)
        {
            goodStatus = Status.STATUS_ROLLEDBACK;
            if (!this.expectRollback && this.startedTransaction)
            {
                logger.error("Unexpected rollback of " + this.txObjects.size() + " objects in transaction " + " " + this.getTransactionName(), 
                                new Exception("Not an Exception, just for tracing"));
                int sizeToReport = this.txObjects.size();
                if (sizeToReport > 3)
                {
                    sizeToReport = 3;
                }
                for (int i = 0; i < sizeToReport; i++)
                {
                    logger.error("Object in rolled back tx: " + this.txObjects.get(i).getClass().getName());
                }
            }
            this.handleCacheRollback();
        }
        else
        {
            logger.error("Unhandled transaction status: " + status);
        }
        if (this.synchronizations != null)
        {
            for (int i = 0; i < synchronizations.size(); i++)
            {
                Synchronization s = (Synchronization) synchronizations.get(i);
                s.afterCompletion(goodStatus);
            }
        }
    }

    @Override
    protected void synchronizedHandleCacheCommit() throws MithraTransactionException
    {
        this.dependentOperations.synchronizedHandleCacheCommit();
        this.independentOperations.forAll(independentCommit);
    }

    @Override
    protected void synchronizedHandleCacheRollback() throws MithraTransactionException
    {
        this.dependentOperations.synchronizedHandleCacheRollback();
        this.independentOperations.forAll(independentRollback);
    }

    @Override
    public int getJtaTransactionStatus() throws MithraTransactionException
    {
        // javax.transaction.Status.STATUS_ACTIVE = 0
        // javax.transaction.Status.STATUS_COMMITTED = 3
        // javax.transaction.Status.STATUS_COMMITTING = 8
        // javax.transaction.Status.STATUS_MARKED_ROLLBACK = 1
        // javax.transaction.Status.STATUS_NO_TRANSACTION = 6
        // javax.transaction.Status.STATUS_PREPARED = 2
        // javax.transaction.Status.STATUS_PREPARING = 7
        // javax.transaction.Status.STATUS_ROLLEDBACK = 4
        // javax.transaction.Status.STATUS_ROLLING_BACK = 9
        // javax.transaction.Status.STATUS_UNKNOWN = 5
        try
        {
            return this.tx.getStatus();
        }
        catch(SystemException e)
        {
            throw new MithraTransactionException("could not get transaction status", e);
        }
    }

    private TxOperationsForIndependentClass getOrCreateIndependentOps(MithraObjectPortal portal)
    {
        TxOperationsForIndependentClass ops = (TxOperationsForIndependentClass) this.independentOperations.get(portal, IDENTITY_EXTRACTOR_LIST);
        if (ops == null)
        {
            ops = new TxOperationsForIndependentClass(portal);
            this.independentOperations.put(ops);
        }
        return ops;
    }

    protected void addUpdate(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraTransactionException
    {
        MithraObjectPortal mithraObjectPortal = obj.zGetPortal();
        boolean newOps;
        if (mithraObjectPortal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(mithraObjectPortal);
            newOps = ops.addUpdate(obj, attributeUpdateWrapper);
        }
        else
        {
            newOps = this.dependentOperations.addUpdate(obj, attributeUpdateWrapper);
        }
        if (newOps)
        {
            realPendingOperations++;
        }
    }

    protected void handleFailedBufferedOperation()
    {
        dependentOperations.handledFailedBufferedOperation();
        independentOperations.forAll(handleFailedOperationsProc);
        this.realPendingOperations = 0;
        this.markForRollbackOnly();
    }

    protected void addInsert(MithraTransactionalObject obj, MithraObjectPortal portal, int depthCheck) throws MithraTransactionException
    {
        boolean success = true;
        realPendingOperations++;
        try
        {
            if (portal.isIndependent())
            {
                TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
                ops.addInsert(obj, portal, depthCheck);
                if(obj.zGetTxDataForRead().zHasIdentity())
                {
                    success = false;
                    ops.executeBufferedOperations();
                    success = true;
                }
            }
            else
            {
                this.dependentOperations.addInsert(obj, portal, depthCheck);
                if(obj.zGetTxDataForRead().zHasIdentity())
                {
                    success = false;
                    this.dependentOperations.executeBufferedOperations();
                    success = true;
                }
            }
        }
        finally
        {
            if (!success)
            {
                handleFailedBufferedOperation();
            }
        }
    }

    protected void addDelete(MithraTransactionalObject obj, MithraObjectPortal portal) throws MithraTransactionException
    {
        realPendingOperations++;
        if (portal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
            ops.addDelete(obj, portal);
        }
        else
        {
            this.dependentOperations.addDelete(obj, portal);
        }
    }

    protected void addPurge(MithraTransactionalObject obj, MithraObjectPortal portal) throws MithraTransactionException
    {
        realPendingOperations++;
        if (portal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
            ops.addPurge(obj, portal);
        }
        else
        {
            this.dependentOperations.addPurge(obj, portal);
        }
    }

    @Override
    public void registerSynchronization(Synchronization synchronization)
    {
        if (this.synchronizations == null)
        {
            this.synchronizations = new InternalList(2);
        }
        this.synchronizations.add(synchronization);
    }

    @Override
    public void registerLifeCycleListener(TransactionLifeCycleListener lifeCycleListener)
    {
        if (this.lifeCycleListeners == null)
        {
            this.lifeCycleListeners = new InternalList(2);
        }
        this.lifeCycleListeners.add(lifeCycleListener);
    }

    @Override
    public void enrollObject(MithraTransactionalObject obj, Cache cache)
    {
        synchronized (this.txObjects)
        {
            this.txObjects.add(obj);
            this.txCaches.add(cache);
            this.setWaitingForTransaction(null);
        }
        this.checkForActiveTransaction();
    }

    protected void checkForActiveTransaction()
    {
        this.checkForActiveTransaction(false);
    }

    protected void checkForActiveTransaction(boolean doneIsOk)
    {
        if (this.leftJtaTransaction)
        {
            throw new MithraTransactionException("no operations allowed after leaving JTA transaction context");
        }
        try
        {
            int status = this.tx.getStatus();
            if (!doneIsOk && (status == Status.STATUS_ROLLING_BACK || status == Status.STATUS_MARKED_ROLLBACK))
            {
                boolean isTimedOut = false;
                long now = System.currentTimeMillis();
                String initialMessage = "Unexpected rollback. ";
                if (this.rollbackCause != null)
                {
                    initialMessage = "Expected forced rollback. See cause below. ";
                }
                int timeout = (int)(this.getTimeoutInMilliseconds()/1000);
                double timeInTx = ((double)(now - this.getRealStartTime()))/1000.0;
                if (timeInTx >= timeout)
                {
                    isTimedOut = true;
                    initialMessage = "Rollback due to timeout. This transaction took "+timeInTx+" s. The transaction timeout is set to "+timeout+" s. ";
                }
                MithraTransactionException mithraTransactionException = 
                                            new MithraTransactionException(initialMessage + "Transaction is marked for rollback. No more operations are allowed");
                mithraTransactionException.setTimedOut(isTimedOut);
                this.throwExceptionWithCause(mithraTransactionException);
            }
            if (status == Status.STATUS_ROLLEDBACK && this.txStatus != MITHRA_STATUS_ROLLED_BACK)
            {
                this.handleCacheRollback();
            }
        }
        catch (SystemException e)
        {
            throw new MithraTransactionException("Could not determine transaction status", e);
        }
        if (!doneIsOk && (this.txStatus == MITHRA_STATUS_COMMITTED || this.txStatus == MITHRA_STATUS_ROLLED_BACK))
        {
            throw new MithraTransactionException("Transaction is no longer active. No more operations allowed");
        }
    }

    @Override
    public void enrollResource(MithraTransactionalResource resource)
    {
        super.enrollResource(resource);
        this.checkForActiveTransaction();
    }

    @Override
    public void enrollCache(Cache cache)
    {
        super.enrollCache(cache);
        this.checkForActiveTransaction();
    }

    @Override
    public void insert(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        MithraObjectPortal portal = obj.zGetPortal();
        MithraObjectPortal[] superClassPortals = portal.getSuperClassPortals();
        int depthCheck = 0;
        if (superClassPortals != null)
        {
            depthCheck = superClassPortals.length - 1;
            for(int i=0;i<superClassPortals.length;i++)
            {
                this.addInsert(obj, superClassPortals[i], depthCheck);
            }
        }
        this.addInsert(obj, portal, depthCheck);

        if (this.immediateOperations || isCautious())
        {
            this.executeBufferedOperations();
        }
    }

    @Override
    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper attributeUpdateWrapper) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        this.addUpdate(obj, attributeUpdateWrapper);

        if (this.immediateOperations || isCautious())
        {
            this.executeBufferedOperations();
        }
    }

    @Override
    public void delete(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        MithraObjectPortal portal = obj.zGetPortal();
        this.addDelete(obj, portal);
        MithraObjectPortal[] superClassPortals = portal.getSuperClassPortals();
        if (superClassPortals != null)
        {
            for(int i=superClassPortals.length - 1;i>=0;i--)
            {
                this.addDelete(obj, superClassPortals[i]);
            }
        }

        if (this.immediateOperations || isCautious())
        {
            this.executeBufferedOperations();
        }
    }

    @Override
    public void deleteUsingOperation(Operation op) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        MithraObjectPortal portal = op.getResultObjectPortal();
        if (portal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
            ops.deleteUsingOperation(op);
        }
        else
        {
            this.dependentOperations.deleteUsingOperation(op);
        }
        if (this.immediateOperations || this.isCautious())
        {
            executeBufferedOperations();
        }
    }

    @Override
    public int deleteBatchUsingOperation(Operation op, int batchSize) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        MithraObjectPortal portal = op.getResultObjectPortal();
        if (portal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
            return ops.deleteBatchUsingOperation(op, batchSize);
        }
        else
        {
            return this.dependentOperations.deleteBatchUsingOperation(op, batchSize);
        }
    }

    @Override
    public void purge(MithraTransactionalObject obj) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        MithraObjectPortal portal = obj.zGetPortal();
        this.addPurge(obj, portal);
        MithraObjectPortal[] superClassPortals = portal.getSuperClassPortals();
        if (superClassPortals != null)
        {
            for(int i=superClassPortals.length - 1;i>=0;i--)
            {
                this.addPurge(obj, superClassPortals[i]);
            }
        }

        if (this.immediateOperations || isCautious())
        {
            this.executeBufferedOperations();
        }
    }

    @Override
    public void purgeUsingOperation(Operation op) throws MithraDatabaseException
    {
        this.checkForActiveTransaction();
        MithraObjectPortal portal = op.getResultObjectPortal();
        if (portal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
            ops.purgeUsingOperation(op);
        }
        else
        {
            this.dependentOperations.purgeUsingOperation(op);
        }
        if (this.immediateOperations || this.isCautious())
        {
            executeBufferedOperations();
        }
    }

    @Override
    public void addLogicalDeleteForPortal(MithraObjectPortal portal)
    {
        this.checkForActiveTransaction();
        if (portal.isIndependent())
        {
            TxOperationsForIndependentClass ops = getOrCreateIndependentOps(portal);
            ops.addLogicalDeleteForPortal(portal);
        }
        else
        {
            this.dependentOperations.addLogicalDeleteForPortal(portal);
        }
    }

    @Override
    public void executeBufferedOperationsForPortal(MithraObjectPortal mithraObjectPortal)
    {
        boolean success = true;
        try
        {
            if (mithraObjectPortal.isIndependent())
            {
                TxOperationsForIndependentClass ops = (TxOperationsForIndependentClass)
                        this.independentOperations.get(mithraObjectPortal, IDENTITY_EXTRACTOR_LIST);
                if (ops != null)
                {
                    success = false;
                    this.dependentOperations.executeBufferedOperations();
                    ops.executeBufferedOperations();
                    success = true;
                }
            }
            else
            {
                if (this.dependentOperations.hasPendingOperation(mithraObjectPortal))
                {
                    success = false;
                    this.dependentOperations.executeBufferedOperations();
                    success = true;
                }
            }
        }
        finally
        {
            if (!success)
            {
                handleFailedBufferedOperation();
            }
        }
    }

    @Override
    public void expectRollbackWithCause(Throwable throwable)
    {
        this.setExpectRollback();
        this.setRollbackCause(throwable);
    }

    @Override
    public void executeBufferedOperationsForEnroll(MithraObjectPortal mithraObjectPortal)
    {
        boolean success = true;
        try
        {
            if (mithraObjectPortal.isIndependent())
            {
                TxOperationsForIndependentClass ops = (TxOperationsForIndependentClass)
                        this.independentOperations.get(mithraObjectPortal, IDENTITY_EXTRACTOR_LIST);
                if (ops != null && ops.hasDeleteOperation())
                {
                    success = false;
                    this.dependentOperations.executeBufferedOperations();
                    ops.executeBufferedOperations();
                    success = true;
                }
            }
            else
            {
                success = false;
                this.dependentOperations.executeBufferedOperations(mithraObjectPortal, false);
                success = true;
            }
        }
        finally
        {
            if (!success)
            {
                handleFailedBufferedOperation();
            }
        }
    }

    protected SmallSet getPortalSet()
    {
        if (this.portalSet == null)
        {
            this.portalSet = new SmallSet(4);
        }
        this.portalSet.clear();
        return portalSet;
    }

    @Override
    public void executeBufferedOperationsForOperation(Operation op, boolean bypassCache) throws MithraDatabaseException
    {
        SmallSet dependentPortals = this.getPortalSet();
        op.addDependentPortalsToSet(dependentPortals);

        int size = dependentPortals.size();
        boolean lookForDelete = !bypassCache && size == 1 && op.usesImmutableUniqueIndex();
        boolean dependentsDone = false;
        boolean success = true;
        try
        {
            for(int i=0;i< size;i++)
            {
                MithraObjectPortal portal = (MithraObjectPortal) dependentPortals.get(i);
                if (portal.isIndependent())
                {
                    TxOperationsForIndependentClass ops = (TxOperationsForIndependentClass)
                            this.independentOperations.get(portal, IDENTITY_EXTRACTOR_LIST);
                    if (ops != null && ops.hasPendingOperation() && (!lookForDelete || ops.hasDeleteOperation()))
                    {
                        success = false;
                        this.dependentOperations.executeBufferedOperations();
                        dependentsDone = true;
                        ops.executeBufferedOperations();
                        success = true;
                    }
                }
                else if (!dependentsDone)
                {
                    success = false;
                    dependentsDone = this.dependentOperations.executeBufferedOperations(portal, lookForDelete);
                    success = true;
                }
            }
        }
        finally
        {
            if (!success)
            {
                handleFailedBufferedOperation();
            }
        }
    }

    @Override
    public void executeBufferedOperations() throws MithraDatabaseException
    {
        if (!this.isExecuting)
        {
            this.isExecuting = true;
            this.realPendingOperations = 0;
            boolean success = false;
            try
            {
                this.dependentOperations.executeBufferedOperations();
                this.independentOperations.forAll(executeOperationsProc);
                success = true;
            }
            finally
            {
                this.isExecuting = false;
                if (!success)
                {
                    handleFailedBufferedOperation();
                }
            }
        }
    }

    @Override
    public void executeBufferedOperationsIfMoreThan(int i) throws MithraDatabaseException
    {
        if (realPendingOperations > i)
        {
            this.executeBufferedOperations();
        }
    }

    public void beforeCompletion()
    {
        if (asyncRollback) return;
        this.executeBufferedOperations();
        if (this.synchronizations != null)
        {
            for(int i=0; i < this.synchronizations.size(); i++)
            {
                Synchronization s = (Synchronization) synchronizations.get(i);
                s.beforeCompletion();
            }
        }
        this.delistAll(XAResource.TMSUCCESS);
    }

    @Override
    public void enlistResource(XAResource resource) throws SystemException, RollbackException
    {
        if (this.enlistedResources.contains(resource)) return;
        if (this.getJtaTransaction().enlistResource(resource))
        {
            this.enlistedResources.add(resource);
        }
        else
        {
            throw new SystemException("could not enroll resource "+resource.getClass().getName());
        }
    }

    private void delistAll(int flag) throws MithraTransactionException
    {
        try
        {
            for (Iterator iterator = this.enlistedResources.iterator(); iterator.hasNext();)
            {
                XAResource xaResource = (XAResource) iterator.next();
                this.getJtaTransaction().delistResource(xaResource, flag);
                iterator.remove();
            }
        }
        catch (SystemException e)
        {
            throw new MithraTransactionException("could not delist resources", e);
        }
    }

    @Override
    protected void markForRollbackOnly()
    {
        super.markForRollbackOnly();
        try
        {
            Transaction jtaTx = this.getJtaTransaction();
            if (jtaTx.getStatus() == Status.STATUS_ACTIVE)
            {
                jtaTx.setRollbackOnly();
            }
        }
        catch (SystemException e)
        {
            logger.error("Could not mark transaction for rollback only", e);
        }
    }

    @Override
    protected void asyncRollback()
    {
        this.asyncRollback = true;
        super.asyncRollback();
        // this is a desperate attempt at rolling back, so handle as much of the errors as possible
        for (Iterator iterator = this.enlistedResources.iterator(); iterator.hasNext();)
        {
            XAResource xaResource = (XAResource) iterator.next();
            try
            {
                this.getJtaTransaction().delistResource(xaResource, XAResource.TMFAIL);
            }
            catch (Exception e)
            {
                this.getLogger().error("could not delist resource "+xaResource, e);
            }
            iterator.remove();
        }
        try
        {
            this.getJtaTransaction().rollback();
        }
        catch (SystemException e)
        {
            this.getLogger().error("could not fully rollback transaction", e);
        }
    }

    @Override
    public void commit() throws MithraDatabaseException
    {
        if (this.txStatus == MITHRA_STATUS_ROLLED_BACK || this.txStatus == MITHRA_STATUS_MARKED_ROLLBACK_ONLY)
        {
            throw new MithraTransactionException("cannot commit rolledback transaction");
        }
        if (this.expectRollback)
        {
            MithraTransactionException mithraTransactionException = 
                new MithraTransactionException("cannot commit a transaction that expects a rollback (most probably a nested transaction failed");
            this.throwExceptionWithCause(mithraTransactionException);
        }

        this.txStatus = MITHRA_STATUS_COMMITTING;
        this.executeBufferedOperations();

        notifyBeforeCommitLifeCycleListeners();

        this.delistAll(XAResource.TMSUCCESS);

        try
        {
            if (this.startedTransaction)
            {
                this.handleCachePrepare();

                // since we're registered as a synchronization, our afterCompletion method will be called
                // this throws too many exceptions to list
                logger.debug("Committing transaction");
                MithraManagerProvider.getMithraManager().getJtaTransactionManager().commit();
            }
            else
            {
                MithraManagerProvider.getMithraManager().removeTransaction(this);
            }
        }
        catch (Exception e)
        {
            long now = System.currentTimeMillis();
            String initialMessage = "Could not commit transaction.";
            boolean isTimedOut = false;
            int timeout = (int)(this.getTimeoutInMilliseconds()/1000);
            double timeInTx = ((double)(now - this.getRealStartTime()))/1000.0;
            if (timeInTx >= timeout)
            {
                isTimedOut = true;
                initialMessage = "Could not commit transaction. This transaction took "+timeInTx+" s. The transaction timeout is set to "+timeout+" s. ";
            }
            MithraTransactionException excp = new MithraTransactionException(initialMessage, e);
            excp.setTimedOut(isTimedOut);
            throw excp;
        }
    }

    private void notifyBeforeCommitLifeCycleListeners()
    {
        if (this.lifeCycleListeners != null)
        {
            for(int i=0;i<lifeCycleListeners.size();i++)
            {
                TransactionLifeCycleListener listener = (TransactionLifeCycleListener) lifeCycleListeners.get(i);
                listener.beforeCommit();
            }
        }
    }

    private void notifyBeforeRollbackLifeCycleListeners()
    {
        if (this.lifeCycleListeners != null)
        {
            for(int i=0;i<lifeCycleListeners.size();i++)
            {
                TransactionLifeCycleListener listener = (TransactionLifeCycleListener) lifeCycleListeners.get(i);
                try
                {
                    listener.beforeRollback();
                }
                catch (Throwable e)
                {
                    logger.error("Life cycle listerner throw exception. Will continue will rollback.", e);
                }
            }
        }
    }

    private void throwExceptionWithCause(MithraTransactionException mithraTransactionException)
    {
        if (this.rollbackCause != null)
        {
            mithraTransactionException.initCause(this.rollbackCause);
            if (this.rollbackCause instanceof MithraBusinessException)
            {
                mithraTransactionException.setRetriable(((MithraBusinessException)this.rollbackCause).isRetriable());
            }
        }
        throw mithraTransactionException;
    }

    @Override
    protected void rollbackWithCause(Throwable cause) throws MithraTransactionException
    {
        this.logger.error("Rolling back because of exception", cause);
        this.rollback();
    }

    @Override
    protected void handleCacheRollback() throws MithraTransactionException
    {
        super.handleCacheRollback();
    }

    @Override
    public void rollback()  throws MithraTransactionException
    {
        if (this.txStatus == MITHRA_STATUS_COMMITTED)
        {
            throw new MithraTransactionException("cannot rollback committed transaction.");
        }

        this.expectRollback = true;
        this.notificationEvents.clear();
        this.realPendingOperations = 0;
        notifyBeforeRollbackLifeCycleListeners();

        try
        {
            this.delistAll(XAResource.TMFAIL);
            // this throws too many exceptions to list
            int status = tx.getStatus();
            if (status != Status.STATUS_NO_TRANSACTION && status != Status.STATUS_ROLLEDBACK)
            {
                this.txStatus = MITHRA_STATUS_ROLLING_BACK;
                if (this.startedTransaction)
                {
                    logger.debug("Rolling back transaction");
                    this.tx.rollback();
                }
            }
            else if (status == Status.STATUS_ROLLEDBACK)
            {
                if (this.txStatus != MITHRA_STATUS_ROLLED_BACK)
                {
                    this.handleCacheRollback();
                }
            }
        }
        catch (Exception e)
        {
            if (this.txStatus != MITHRA_STATUS_ROLLED_BACK)
            {
                this.handleCacheRollback();
            }
            throw new MithraTransactionException("Could not rollback transaction", e);
        }
        finally
        {
            MithraManagerProvider.getMithraManager().removeTransaction(this);
        }
        this.independentOperations.clear();
        this.dependentOperations.clear();
    }

    @Override
    public void addMithraNotificationEvent(String databaseIdentifier, MithraNotificationEvent notificationEvent)
    {
        List<MithraNotificationEvent> notificationEventList = this.notificationEvents.get(databaseIdentifier);
        if (notificationEventList == null)
        {
            notificationEventList = new ArrayList<MithraNotificationEvent>();
            this.notificationEvents.put(databaseIdentifier, notificationEventList);
        }
        notificationEventList.add(notificationEvent);
    }

    public int hashCode()
    {
        return this.tx.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj instanceof JtaBasedTransaction)
        {
            JtaBasedTransaction other = (JtaBasedTransaction) obj;
            return this.tx.equals(other.getJtaTransaction());
        }
        return false;
    }

    @Override
    protected void finalize() throws Throwable
    {
        if (this.realPendingOperations > 0)
        {
            logger.error("A stale transaction had uncommitted work!");
        }
        super.finalize();
    }

    public void leaveJtaTransaction()
    {
        this.executeBufferedOperations();
        this.delistAll(XAResource.TMSUCCESS);
        this.leftJtaTransaction = true;
    }

    public void setRollbackCause(Throwable cause) throws MithraTransactionException
    {
        this.rollbackCause = cause;
        try
        {
            this.getJtaTransaction().setRollbackOnly();
        }
        catch (SystemException e)
        {
            throw new MithraTransactionException("could not set rollback only", e);
        }
    }

    private static class IndependentPortalCommitter implements DoUntilProcedure
    {
        public boolean execute(Object object)
        {
            ((TxOperationsForIndependentClass)object).synchronizedHandleCacheCommit();
            return false;
        }
    }

    private static class IndependentPortalRollback implements DoUntilProcedure
    {
        public boolean execute(Object object)
        {
            ((TxOperationsForIndependentClass)object).synchronizedHandleCacheRollback();
            return false;
        }
    }

    private static class ExecuteOperations implements DoUntilProcedure
    {
        public boolean execute(Object object)
        {
            ((TxOperationsForIndependentClass)object).executeBufferedOperations();
            return false;
        }
    }

    private static class HandleFailedOperationsProc implements DoUntilProcedure
    {
        public boolean execute(Object object)
        {
            ((TxOperationsForIndependentClass)object).handleFailedOperations();
            return false;
        }
    }

    @Override
    protected Logger getLogger()
    {
        return logger;
    }

    @Override
    protected long getRequesterVmId()
    {
        return MithraManager.getInstance().getNotificationEventManager().getMithraVmId();
    }
}
