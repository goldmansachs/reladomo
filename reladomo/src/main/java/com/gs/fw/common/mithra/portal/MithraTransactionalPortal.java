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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.behavior.txparticipation.FullTransactionalParticipationMode;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheWithOptimisticLockingTxParticipationMode;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.UpdateCountHolderImpl;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.QueryCache;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.transaction.MithraObjectPersister;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.ListFactory;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;



public class MithraTransactionalPortal extends MithraAbstractObjectPortal
{

    private static TransactionLocal transactionalQueryCache = new TransactionLocal();
    private TransactionLocal txPatricipationMode = new TransactionLocal();
    private TxParticipationMode defaultTxParticipationMode = FullTransactionalParticipationMode.getInstance();

    public MithraTransactionalPortal(MithraObjectDeserializer databaseObject, Cache cache,
        RelatedFinder finder, int relationshipCacheSize, int minQueriesToKeep,
            RelatedFinder[] superClassFinders, RelatedFinder[] subClassFinders, String uniqueAlias, int hierarchyDepth,
            MithraObjectPersister mithraObjectPersister)
    {
        super(databaseObject, cache, finder, relationshipCacheSize, minQueriesToKeep, hierarchyDepth, new UpdateCountHolderImpl(),
                mithraObjectPersister, (MithraTuplePersister) mithraObjectPersister, true);
        this.setSuperClassFinders(superClassFinders);
        this.setSubClassFinders(subClassFinders);
        this.setUniqueAlias(uniqueAlias);
    }

    public static void initializeTransactionalQueryCache(MithraTransaction tx)
    {
        transactionalQueryCache.set(tx, new QueryCache(1000, 50));
    }

    public List findForMassDeleteInMemory(Operation op, MithraTransaction tx)
    {
        if (this.getCache().size() == 0)
        {
            return ListFactory.EMPTY_LIST;
        }
        AnalyzedOperation analyzedOperation = new AnalyzedOperation(op);
        QueryCache queryCache = this.getQueryCache(tx);
        List result = null;
        CachedQuery cachedQuery = queryCache.findByEquality(analyzedOperation.getOriginalOperation());
        if (cachedQuery == null && analyzedOperation.isAnalyzedOperationDifferent())
        {
            cachedQuery = queryCache.findByEquality(analyzedOperation.getAnalyzedOperation());
        }
        if (cachedQuery != null)
        {
            result = cachedQuery.getResult();
        }
        if (result == null)
        {
            boolean partiallyCached = this.isPartiallyCached();
            UnifiedSet dependentPortals = new UnifiedSet(3);
            op.addDependentPortalsToSet(dependentPortals);
            if (!partiallyCached)
            {
                Iterator it = dependentPortals.iterator();
                while (it.hasNext() && !partiallyCached)
                {
                    MithraObjectPortal depPortal = (MithraObjectPortal) it.next();
                    partiallyCached = depPortal.isPartiallyCached();
                }
            }
            try
            {
                if (tx != null)
                {
                    tx.zSetOperationEvaluationMode(true);
                }
                if (!partiallyCached)
                {
                    result = op.applyOperationToFullCache();
                }
                else if (dependentPortals.size() == 1 && !op.isJoinedWith(this))
                {
                    // even though we're partally cached, the operation can search the cache, because there are no mapped ops
                    result = op.applyOperationToFullCache();
                }
            }
            finally
            {
                if (tx != null)
                {
                    tx.zSetOperationEvaluationMode(false);
                }
            }
        }
        return result;
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        tx.executeBufferedOperationsForPortal(this);

        List result = this.findForMassDeleteInMemory(op, tx);
        if (result == null)
        {
            tx.executeBufferedOperations();
            result = this.getMithraObjectPersister().findForMassDelete(op, forceImplicitJoin);
        }
        for(int i=0;i<result.size();i++)
        {
            MithraTransactionalObject mto = (MithraTransactionalObject) result.get(i);
            mto.zPrepareForDelete();
            mto.zSetTxPersistenceState(PersistenceState.DELETED);
        }
        this.getCache().removeAll(result);
        this.getPerClassUpdateCountHolder().incrementUpdateCount();
        this.getMithraObjectPersister().prepareForMassDelete(op, forceImplicitJoin);
    }

    @Override
    public void prepareForMassPurge(List toBePurged)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();

        for (int i = 0; i < toBePurged.size(); i++)
        {
            MithraTransactionalObject mto = (MithraTransactionalObject) toBePurged.get(i);
            prepareForDeleteAndEnroll(tx, mto);
        }
        this.getCache().removeAll(toBePurged);
        this.getPerClassUpdateCountHolder().incrementUpdateCount();
        this.getMithraObjectPersister().prepareForMassPurge(toBePurged);
    }

    private void prepareForDeleteAndEnroll(MithraTransaction tx, MithraTransactionalObject mto)
    {
        mto.zPrepareForDelete();
        MithraDataObject dataObject = mto.zGetTxDataForWrite();
        mto.zSetTxPersistenceState(PersistenceState.DELETED);
        TemporalContainer container = this.getCache().getOrCreateContainer(dataObject);
        container.voidData(dataObject);

        InTransactionDatedTransactionalObject otherDatedObject = new InTransactionDatedTransactionalObject(dataObject.zGetMithraObjectPortal(),
                                                                                                           dataObject, null, InTransactionDatedTransactionalObject.DELETED_STATE);
        tx.enrollObject(otherDatedObject, otherDatedObject.zGetCache());
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        tx.executeBufferedOperationsForPortal(this);

        TxParticipationMode originalTxParticipationMode = this.getTxParticipationMode(tx);
        this.setTxParticipationMode(ReadCacheWithOptimisticLockingTxParticipationMode.getInstance(), tx);
        List result = this.findForMassDeleteInMemory(op, tx);
        this.setTxParticipationMode(originalTxParticipationMode, tx);
        if (result == null)
        {
            tx.executeBufferedOperations();
            result = this.getMithraObjectPersister().findForMassDelete(op, forceImplicitJoin);
        }
        for (int i = 0; i < result.size(); i++)
        {
            MithraTransactionalObject mto = (MithraTransactionalObject) result.get(i);
            prepareForDeleteAndEnroll(tx, mto);
        }
        this.getCache().removeAll(result);
        this.getPerClassUpdateCountHolder().incrementUpdateCount();
        this.getMithraObjectPersister().prepareForMassPurge(op, forceImplicitJoin);
    }

    public void setDefaultTxParticipationMode(TxParticipationMode mode)
    {
        this.defaultTxParticipationMode = mode;
    }

    public QueryCache getQueryCache()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        return getQueryCache(tx);
    }

    private QueryCache getQueryCache(MithraTransaction tx)
    {
        QueryCache result = (QueryCache) transactionalQueryCache.get(tx);
        if (result == null) result = super.getQueryCache();
        return result;
    }

    public List zFindInMemoryWithoutAnalysis(Operation op, boolean isQueryCachable)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        CachedQuery result = null;
        if (isQueryCachable)
        {
            result = this.getQueryCache(tx).findByEquality(op);
        }
        List resultList;
        if (result == null || result.wasDefaulted())
        {
            if (tx == null)
            {
                resultList = resolveOperationOnCache(op);
            }
            else
            {
                resultList = applyOperationAndCheck(op, tx);
            }
        }
        else
        {
            resultList = result.getResult();
        }
        return resultList;
    }

    // during a transaction, we pretend that we don't have a full cache, to make sure
    // the transaction is getting 100% correct data
    private List applyOperationAndCheck(Operation op, MithraTransaction tx)
    {
        boolean oldEvaluationMode = tx.zIsInOperationEvaluationMode();
        List resultList = null;
        try
        {
            tx.zSetOperationEvaluationMode(true);
            if (this.isPureHome() || (!this.isOperationPartiallyCached(op) && !this.txParticipationRequired(tx, op)))
            {
                resultList = this.resolveOperationOnCache(op);
            }
            else
            {
                resultList = op.applyOperationToPartialCache();
                if (resultList == null && !this.isOperationPartiallyCached(op) && this.getTxParticipationMode(tx).mustParticipateInTxOnRead())
                {
                    // attempt to find what we have in cache to trigger waiting for potential other transactions
                    List untrustworthyResult = op.applyOperationToFullCache();
                    checkTransactionParticipationAndWaitForOtherTransactions(untrustworthyResult, tx);
                }
                resultList = checkTransactionParticipationAndWaitForOtherTransactions(resultList, tx);
            }
        }
        finally
        {
            tx.zSetOperationEvaluationMode(oldEvaluationMode);
        }
        if (this.isPureHome())
        {
            checkTransactionParticipationForPureObject(resultList, tx);
        }
        return resultList;
    }

    private boolean txParticipationRequired(MithraTransaction tx, Operation op)
    {
        boolean participate = this.getTxParticipationMode(tx).mustParticipateInTxOnRead();
        if (!participate)
        {
            UnifiedSet dependentPortals = new UnifiedSet(3);
            op.addDependentPortalsToSet(dependentPortals);
            if (dependentPortals.size() > 1)
            {
                Iterator it = dependentPortals.iterator();
                while (it.hasNext() && !participate)
                {
                    MithraObjectPortal depPortal = (MithraObjectPortal) it.next();
                    participate = depPortal.getTxParticipationMode(tx).mustParticipateInTxOnRead();
                }
            }
        }
        return participate;
    }

    private void checkTransactionParticipationForPureObject(List list, MithraTransaction tx)
    {
        if (list == null) return;
        if (this.getTxParticipationMode(tx).mustParticipateInTxOnRead())
        {
            for(int i=0;i<list.size();i++)
            {
                MithraTransactionalObject mto = (MithraTransactionalObject) list.get(i);
                mto.zLockForTransaction();
            }
        }
    }

    @Override
    protected CachedQuery findInCache(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            return this.findInCacheForNoTransaction(op, analyzedOperation, orderby, forRelationship);
        }
        else
        {
            return this.findInCacheForTransaction(analyzedOperation, orderby, tx, forRelationship, op);
        }
    }

    protected CachedQuery findInCacheForTransaction(AnalyzedOperation analyzedOperation, OrderBy orderby,
            MithraTransaction tx, boolean forRelationship, Operation op)
    {
        CachedQuery emptyNewCachedQuery = new CachedQuery(op, orderby); // must create before executing the query to avoid a race condition against concurrent updates
        List resultList = applyOperationAndCheck(analyzedOperation != null ? analyzedOperation.getAnalyzedOperation() : op, tx);
        return this.createAndCacheQuery(resultList, orderby, analyzedOperation, forRelationship, emptyNewCachedQuery);
    }

    /* returns null if the members of the list are not all participating in the transaction */
    private List checkTransactionParticipationAndWaitForOtherTransactions(List list, MithraTransaction tx)
    {
        if (list == null) return null;
        List result = list;
        if (this.getTxParticipationMode(tx).mustParticipateInTxOnRead())
        {
            for(int i=0;i<list.size();i++)
            {
                MithraTransactionalObject mto = (MithraTransactionalObject) list.get(i);
                if (!mto.zIsParticipatingInTransaction(tx))
                {
                    result = null;
                    mto.zWaitForExclusiveWriteTx(tx);
                }
            }
        }
        return result;
    }

    public TxParticipationMode getTxParticipationMode(MithraTransaction tx)
    {
        TxParticipationMode currentMode = (TxParticipationMode) this.txPatricipationMode.get(tx);
        if (currentMode == null)
        {
            return defaultTxParticipationMode;
        }
        return currentMode;
    }

    public TxParticipationMode getTxParticipationMode()
    {
        return this.getTxParticipationMode(MithraManagerProvider.getMithraManager().getCurrentTransaction());
    }

    public void clearTxParticipationMode(MithraTransaction tx)
    {
        this.txPatricipationMode.set(tx, null);
    }

    public void setTxParticipationMode(TxParticipationMode mode, MithraTransaction tx)
    {
        this.txPatricipationMode.set(tx, mode);
        this.getMithraObjectPersister().setTxParticipationMode(mode, tx);
    }

    @Override
    public Object getAsOneFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            return super.getAsOneFromCache(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        else
        {
            boolean oldEvaluationMode = tx.zIsInOperationEvaluationMode();
            try
            {
                tx.zSetOperationEvaluationMode(true);
                MithraTransactionalObject result = (MithraTransactionalObject)
                        this.getCache().getAsOne(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);

                result = checkObjectForTransactionParticipation(result, tx);
                return result;
            }
            finally
            {
                tx.zSetOperationEvaluationMode(oldEvaluationMode);
            }
        }
    }

    @Override
    public Object getAsOneByIndexFromCache(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1, int indexRef)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            return super.getAsOneByIndexFromCache(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1, indexRef);
        }
        else
        {
            boolean oldEvaluationMode = tx.zIsInOperationEvaluationMode();
            try
            {
                tx.zSetOperationEvaluationMode(true);
                MithraTransactionalObject result = (MithraTransactionalObject)
                        this.getCache().getAsOneByIndex(indexRef, srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);

                result = checkObjectForTransactionParticipation(result, tx);
                return result;
            }
            finally
            {
                tx.zSetOperationEvaluationMode(oldEvaluationMode);
            }
        }
    }

    private MithraTransactionalObject checkObjectForTransactionParticipation(MithraTransactionalObject result, MithraTransaction tx)
    {
        if (result == null) return null;
        if (this.getTxParticipationMode(tx).mustParticipateInTxOnRead())
        {
            if (!result.zIsParticipatingInTransaction(tx))
            {
                result.zWaitForExclusiveWriteTx(tx);
                result = null;
            }
        }
        return result;
    }
}
