
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

package com.gs.fw.common.mithra.list;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.block.procedure.primitive.ObjectIntProcedure;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.Index;
import com.gs.fw.common.mithra.extractor.EmbeddedValueExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.finder.Navigation;
import com.gs.fw.finder.OrderBy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public abstract class DelegatingList<E> implements MithraList<E>
{
    private static final int REFRESH_BULK_INSERT_THRESHOLD = 1000;
    protected static final int REFRESH_PARAMETER_THRESHOLD = REFRESH_BULK_INSERT_THRESHOLD;
    protected static final int OR_CLAUSE_THRESHOLD = 200;

    private MithraDelegatedList<E> delegated;
    private Operation operation;

    private transient volatile Object fastListOrCachedQuery;
    private volatile ListOptions listOptions;
    private boolean bypassCache = false;
    private boolean forRelationship = false;

    protected DelegatingList()
    {
    }

    public DelegatingList(Operation operation)
    {
        this.operation = operation;
    }

    public DelegatingList(com.gs.fw.finder.Operation operation)
    {
        this((Operation) operation);
    }

    protected Object zGetFastListOrCachedQuery()
    {
        return fastListOrCachedQuery;
    }

    protected void zSetFastListOrCachedQuery(Object fastListOrCachedQuery)
    {
        this.fastListOrCachedQuery = fastListOrCachedQuery;
    }

    protected void zSetCurrentTransaction(Object currentTransactionOrRemovedItems)
    {
        getOrCreateListOptions().currentTransaction = currentTransactionOrRemovedItems;
    }

    private ListOptions getOrCreateListOptions()
    {
        if (this.listOptions == null)
        {
            this.listOptions = new ListOptions();
        }
        return this.listOptions;
    }

    protected Object zGetCurrentTransaction()
    {
        if (this.listOptions == null) return null;
        return this.listOptions.currentTransaction;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public void setNumberOfParallelThreads(int numberOfThreads)
    {
        this.delegated = this.delegated.setNumberOfParallelThreads(this, numberOfThreads);
    }

    public int getNumberOfParallelThreads()
    {
        return this.delegated.getNumberOfParallelThreads();
    }

    public com.gs.fw.common.mithra.finder.orderby.OrderBy getOrderBy()
    {
        if (this.listOptions == null) return null;
        return (com.gs.fw.common.mithra.finder.orderby.OrderBy) this.listOptions.orderBy;
    }

    public void setOrderBy(OrderBy orderBy)
    {
        getOrCreateListOptions().orderBy = orderBy;
        this.getDelegated().sortWith(this, (com.gs.fw.common.mithra.finder.orderby.OrderBy) orderBy);
    }

    protected DeepFetchNode getDeepFetchedRelationships()
    {
        if (this.listOptions == null) return null;
        return this.listOptions.deepFetchRoot;
    }

    @Override
    public DeepFetchTree getDeepFetchTree()
    {
        if (this.listOptions == null) return null;
        return this.listOptions.deepFetchRoot;
    }

    public int hashCode()
    {
        return delegated.hashCode(this);
    }

    public int size()
    {
        return delegated.size(this);
    }

    public void clear()
    {
        delegated.clear(this);
    }

    public boolean isEmpty()
    {
        return this.size() == 0;
    }

    public boolean notEmpty()
    {
        return !isEmpty();
    }

    public Object[] toArray()
    {
        return delegated.toArray(this);
    }

    public E get(int index)
    {
        return delegated.get(this, index);
    }

    public E remove(int index)
    {
        return delegated.remove(this, index);
    }

    public void add(int index, E element)
    {
        delegated.add(this, index, element);
    }

    public int indexOf(Object o)
    {
        return delegated.indexOf(this, o);
    }

    public int lastIndexOf(Object o)
    {
        return delegated.lastIndexOf(this, o);
    }

    public boolean add(E o)
    {
        return delegated.add(this, o);
    }

    public boolean contains(Object o)
    {
        return delegated.contains(this, o);
    }

    public boolean equals(Object o)
    {
        return delegated.equals(this, o);
    }

    public boolean remove(Object o)
    {
        return delegated.remove(this, o);
    }

    public boolean addAll(int index, Collection<? extends E> c)
    {
        return delegated.addAll(this, index, c);
    }

    public boolean addAll(Collection<? extends E> c)
    {
        return delegated.addAll(this, c);
    }

    public boolean containsAll(Collection<?> c)
    {
        return delegated.containsAll(this, c);
    }

    public boolean removeAll(Collection<?> c)
    {
        return delegated.removeAll(this, c);
    }

    public boolean retainAll(Collection<?> c)
    {
        return delegated.retainAll(this, c);
    }

    public void forEachWithCursor(DoWhileProcedure closure)
    {
        if (this.listOptions != null && this.listOptions.deepFetchRoot != null)
        {
            throw new MithraBusinessException("The list cursor iteration doesn't support the deep-fetch feature.");
        }

        delegated.forEachWithCursor(this, closure);
    }

    public void forEachWithCursor(DoWhileProcedure closure, Operation postLoadFilter)
    {
        forEachWithCursor(closure, new OperationBasedFilter(postLoadFilter));
    }

    public void forEachWithCursor(DoWhileProcedure closure, Filter postLoadFilter)
    {
        if (this.listOptions != null && this.listOptions.deepFetchRoot != null)
        {
            throw new MithraBusinessException("The list cursor iteration doesn't support the deep-fetch feature.");
        }

        delegated.forEachWithCursor(this, closure, postLoadFilter);
    }

    public Iterator<E> iterator()
    {
        return delegated.iterator(this);
    }

    public List<E> subList(int fromIndex, int toIndex)
    {
        return delegated.subList(this, fromIndex, toIndex);
    }

    public ListIterator<E> listIterator()
    {
        return delegated.listIterator(this);
    }

    public ListIterator<E> listIterator(int index)
    {
        return delegated.listIterator(this, index);
    }

    public E set(int index, E element)
    {
        return delegated.set(this, index, element);
    }

    public <T> T[] toArray(T[] a)
    {
        return delegated.toArray(this, a);
    }

    protected MithraDelegatedList getDelegated()
    {
        return delegated;
    }

    protected void setDelegated(MithraDelegatedList<E> delegated)
    {
        this.delegated = delegated;
    }

    public void deepFetch(Navigation navigation)
    {
        DeepRelationshipAttribute deepRelationshipAttribute = (DeepRelationshipAttribute) navigation;
        getOrCreateListOptions();
        if (this.listOptions.deepFetchRoot == null)
        {
            this.listOptions.deepFetchRoot = new DeepFetchNode(null, (AbstractRelatedFinder) this.getMithraObjectPortal().getFinder());
        }
        boolean added = this.listOptions.deepFetchRoot.add(deepRelationshipAttribute);
        if (added)
        {
            this.getDelegated().incrementalDeepFetch(this);
        }
    }

    public void zSetForRelationship()
    {
        this.forRelationship = true;
    }

    protected boolean zIsForRelationship()
    {
        return this.forRelationship;
    }

    public void zSetRemoveHandler(DependentRelationshipRemoveHandler removeHandler)
    {
        this.delegated = ((MithraDelegatedTransactionalList) this.getDelegated()).zSetRemoveHandler(this, removeHandler);
    }

    public void zSetAddHandler(DependentRelationshipAddHandler addHandler)
    {
        this.delegated = ((MithraDelegatedTransactionalList) this.getDelegated()).zSetAddHandler(this, addHandler);
    }

    public void addOrderBy(OrderBy orderBy)
    {
        getOrCreateListOptions();
        if (this.listOptions.orderBy == null)
        {
            this.listOptions.orderBy = orderBy;
        }
        else
        {
            this.listOptions.orderBy = this.listOptions.orderBy.and(orderBy);
        }
        this.getDelegated().sortWith(this, (com.gs.fw.common.mithra.finder.orderby.OrderBy) orderBy);
    }

    public void setMaxObjectsToRetrieve(int count)
    {
        this.delegated = this.getDelegated().setMaxObjectsToRetrieve(this, count);
    }

    public boolean reachedMaxObjectsToRetrieve()
    {
        return this.getDelegated().reachedMaxObjectsToRetrieve(this);
    }

    public boolean isOperationBased()
    {
        return this.getDelegated().isOperationBased();
    }

    public void forceResolve()
    {
        this.getDelegated().forceResolve(this);
    }

    public void forceRefresh()
    {
        this.getDelegated().forceRefresh(this);
    }

    public boolean isStale()
    {
        return this.getDelegated().isStale(this);
    }

    public void setBypassCache(boolean bypassCache)
    {
        this.bypassCache = bypassCache;
    }

    public boolean isBypassCache()
    {
        return bypassCache;
    }

    public int count()
    {
        this.delegated = this.delegated.prepareForCount();
        return this.getDelegated().count(this);
    }

    public void insertAll()
    {
        this.generateAndSetPrimaryKeys();
        ((MithraDelegatedTransactionalList) this.getDelegated()).insertAll(this);
    }

    public void bulkInsertAll()
    {
        this.generateAndSetPrimaryKeys();
        ((MithraDelegatedTransactionalList) this.getDelegated()).bulkInsertAll(this);
    }

    public void cascadeInsertAll()
    {
        this.generateAndSetPrimaryKeys();
        ((MithraDelegatedTransactionalList) this.getDelegated()).cascadeInsertAll(this);
    }

    public void zCascadeCopyThenInsertAll()
    {
        this.generateAndSetPrimaryKeys();
        for (int i = 0; i < this.size(); i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) this.get(i);
            obj.zCascadeCopyThenInsert();
        }
    }

    public Map<RelatedFinder, StatisticCounter> zCascadeAddNavigatedRelationshipsStats(RelatedFinder finder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        for (int i = 0; i < this.size(); i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) this.get(i);
            obj.zAddNavigatedRelationshipsStats(finder, navigationStats);
        }
        return navigationStats;
    }

    public void cascadeInsertAllUntil(Timestamp exclusiveUntil)
    {
        this.generateAndSetPrimaryKeys();
        ((MithraDelegatedTransactionalList) this.getDelegated()).cascadeInsertAllUntil(this, exclusiveUntil);
    }

    public void cascadeDeleteAll()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).cascadeDeleteAll(this);
    }

    public void deleteAll()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).deleteAll(this);
    }

    public void deleteAllInBatches(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new MithraBusinessException(batchSize + " is an invalid batchSize, batchSize must be > 0");
        }
        ((MithraDelegatedTransactionalList) this.getDelegated()).deleteAllInBatches(this, batchSize);
    }

    protected void terminateAll()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).terminateAll(this);
    }

    protected void purgeAll()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).purgeAll(this);
    }

    public void purgeAllInBatches(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new MithraBusinessException(batchSize + " is an invalid batchSize, batchSize must be > 0");
        }
        ((MithraDelegatedTransactionalList) this.getDelegated()).purgeAllInBatches(this, batchSize);
    }

    public void cascadeTerminateAll()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).cascadeTerminateAll(this);
    }

    public void cascadeTerminateAllUntil(Timestamp exclusiveUntil)
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).cascadeTerminateAllUntil(this, exclusiveUntil);
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved(this);
    }

    public void zCopyDetachedValuesDeleteIfRemovedOnly()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).zCopyDetachedValuesDeleteIfRemovedOnly(this);
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved(this);
    }

    public void zCascadeUpdateInPlaceBeforeTerminate()
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).cascadeUpdateInPlaceBeforeTerminate(this);
    }

    public void copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(Timestamp exclusiveUntil)
    {
        ((MithraDelegatedTransactionalList) this.getDelegated()).copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(this, exclusiveUntil);
    }

    public void clearResolvedReferences()
    {
        this.getDelegated().clearResolvedReferences(this);
    }

    public boolean isModifiedSinceDetachment()
    {
        return this.getDelegated().isModifiedSinceDetachment(this);
    }

    public void restrictRetrievalTo(com.gs.fw.finder.Attribute attribute)
    {
    }

    protected void forceRefreshForSimpleList()
    {
        if (this.size() == 0)
        {
            return;
        }
        RelatedFinder finder = this.getMithraObjectPortal().getFinder();
        Attribute[] pkAttributes = finder.getPrimaryKeyAttributes();
        Attribute sourceAttribute = finder.getSourceAttribute();
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean oldEvaluationMode = tx != null && tx.zIsInOperationEvaluationMode();
        if (asOfAttributes != null)
        {
            List<? extends List> segregatedByDate;
            try
            {
                if (tx != null) tx.zSetOperationEvaluationMode(true);
                segregatedByDate = segregateByAsOfAttribute(asOfAttributes);
            }
            finally
            {
                if (tx != null) tx.zSetOperationEvaluationMode(oldEvaluationMode);
            }
            for (int i = 0; i < segregatedByDate.size(); i++)
            {
                List list = segregatedByDate.get(i);
                Operation dateOp;
                try
                {
                    if (tx != null) tx.zSetOperationEvaluationMode(true);
                    dateOp = asOfAttributes[0].nonPrimitiveEq(asOfAttributes[0].valueOf(list.get(0)));
                    if (asOfAttributes.length == 2)
                    {
                        dateOp = dateOp.and(asOfAttributes[1].nonPrimitiveEq(asOfAttributes[1].valueOf(list.get(0))));
                    }
                }
                finally
                {
                    if (tx != null)
                    {
                        tx.zSetOperationEvaluationMode(oldEvaluationMode);
                    }
                }
                if (pkAttributes.length == 1 || (pkAttributes.length == 2 && sourceAttribute != null))
                {
                    forceRefreshWithOnePk(list, pkAttributes, dateOp, tx, oldEvaluationMode);
                }
                else
                {
                    forceRefreshWithMultiplePk(list, pkAttributes, dateOp, tx, oldEvaluationMode);
                }
            }
        }
        else
        {
            if (pkAttributes.length == 1 || (pkAttributes.length == 2 && sourceAttribute != null))
            {
                forceRefreshWithOnePk(this, pkAttributes, null, tx, oldEvaluationMode);
            }
            else
            {
                forceRefreshWithMultiplePk(this, pkAttributes, null, tx, oldEvaluationMode);
            }
        }
    }

    private List<? extends List> segregateBySourceAttribute(List listToSegregate)
    {
        Attribute sourceAttribute = this.getMithraObjectPortal().getFinder().getSourceAttribute();
        if (sourceAttribute == null)
        {
            return ListFactory.create(listToSegregate);
        }
        //todo: optimize this for non-repeated source attribute
        MultiHashMap map = new MultiHashMap();
        for (int i = 0; i < listToSegregate.size(); i++)
        {
            Object o = listToSegregate.get(i);
            map.put(sourceAttribute.valueOf(o), o);
        }

        if (map.size() > 1)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create(listToSegregate);
        }
    }

    private List<? extends List> segregateByAsOfAttribute(AsOfAttribute[] asOfAttributes)
    {
        ExtractorBasedHashStrategy hashStrategy = ExtractorBasedHashStrategy.create(asOfAttributes);
        //todo: optimize this for non-repeated asOfAttributes
        MultiHashMap map = new MultiHashMap();
        for (int i = 0; i < this.size(); i++)
        {
            Object o = this.get(i);
            map.put(new KeyWithHashStrategy(o, hashStrategy), o);
        }

        if (map.size() > 1)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.<List>create(this);
        }
    }

    private void forceRefreshWithMultiplePk(List listToRefresh, Attribute[] pkAttributes, Operation extraOp, MithraTransaction tx, boolean oldEvaluationMode)
    {
        List<? extends List> lists;
        try
        {
            if (tx != null) tx.zSetOperationEvaluationMode(true);
            lists = segregateBySourceAttribute(listToRefresh);
        }
        finally
        {
            if (tx != null) tx.zSetOperationEvaluationMode(oldEvaluationMode);
        }
        MithraObjectPortal portal = this.getMithraObjectPortal();
        Attribute sourceAttribute = portal.getFinder().getSourceAttribute();
        for (int i = 0; i < lists.size(); i++)
        {
            TupleTempContext tempContext = new TupleTempContext(pkAttributes, true);
            tempContext.enableRetryHook();
            try
            {
                List segregatedList = lists.get(i);
                Object source;
                try
                {
                    if (tx != null) tx.zSetOperationEvaluationMode(true);
                    source = sourceAttribute != null ? sourceAttribute.valueOf(segregatedList.get(0)) : null;
                    tempContext.insert(segregatedList, portal, REFRESH_BULK_INSERT_THRESHOLD, false);
                }
                finally
                {
                    if (tx != null) tx.zSetOperationEvaluationMode(oldEvaluationMode);
                }
                Operation op = tempContext.exists(source);
                if (extraOp != null) op = op.and(extraOp);
                portal.findAsCachedQuery(op, null, true, false, 0, false);
            }
            finally
            {
                tempContext.destroy();
            }
        }
    }

    private void forceRefreshWithOnePk(List listToRefresh, Attribute[] pkAttributes, Operation extraOp, MithraTransaction tx, boolean oldEvaluationMode)
    {
        //todo: segregate by source attribute first
        int batches = listToRefresh.size() / REFRESH_PARAMETER_THRESHOLD;
        if (listToRefresh.size() % REFRESH_PARAMETER_THRESHOLD > 0) batches++;
        for (int b = 0; b < batches; b++)
        {
            Operation op;
            try
            {
                if (tx != null) tx.zSetOperationEvaluationMode(true);
                int end = Math.min(this.size(), (b + 1) * REFRESH_PARAMETER_THRESHOLD);
                op = pkAttributes[0].in(listToRefresh.subList(b * REFRESH_PARAMETER_THRESHOLD, end), pkAttributes[0]);
                if (pkAttributes.length == 2)
                {
                    op = op.and(pkAttributes[1].in(listToRefresh.subList(b * REFRESH_PARAMETER_THRESHOLD, end), pkAttributes[1]));
                }
                if (extraOp != null) op = op.and(extraOp);
            }
            finally
            {
                if (tx != null) tx.zSetOperationEvaluationMode(oldEvaluationMode);
            }
            this.getMithraObjectPortal().findAsCachedQuery(op, null, true, false, 0, false);
        }
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        throw new RuntimeException("subclass must override");
    }

    protected List intersection(DelegatingList other)
    {
        RelatedFinder finder = this.getMithraObjectPortal().getFinder();
        if (other.isOperationBased() && this.isOperationBased())
        {
            return finder.findMany(new AndOperation(this.getOperation(), other.getOperation()));
        }
        else
        {
            List result = finder.constructEmptyList();
            result.addAll(this);
            result.retainAll(other);
            return result;
        }

    }

    protected void generateAndSetPrimaryKeys()
    {
        // subclass to override when needed
    }

    protected void zGenerateAndSetPrimaryKeysWithSourceAttribute(SequenceAttribute attribute,
                                                                 boolean sequenceHasSourceAttribute, String sequenceName, String factoryName)
    {
        com.gs.fw.common.mithra.attribute.Attribute sourceAttribute = this.getMithraObjectPortal().getFinder().getSourceAttribute();
        Object firstNotSet = null;
        boolean isMultiSource = false;
        for (int i = 0; i < this.getDelegated().size(this); i++)
        {
            Object obj = this.getDelegated().get(this, i);
            if (!attribute.isSequenceSet(obj))
            {
                if (firstNotSet == null)
                {
                    firstNotSet = obj;
                }
                else if (!sourceAttribute.valueEquals(firstNotSet, obj))
                {
                    isMultiSource = true;
                    break;
                }
            }
        }
        if (firstNotSet == null) return; // everything is set already
        if (isMultiSource)
        {
            Map<Object, List> sourceAttributeMap = new HashMap<Object, List>();
            SimulatedSequencePrimaryKeyGenerator primaryKeyGenerator = null;
            //First check how many Mithra objects need to set this primary key attribute and keep a list for per sourceAttribute
            for (int i = 0; i < this.getDelegated().size(this); i++)
            {
                Object obj = this.getDelegated().get(this, i);
                if (!attribute.isSequenceSet(obj))
                {
                    Object source = sourceAttribute.valueOf(obj);
                    List subList = sourceAttributeMap.get(source);
                    if (subList == null)
                    {
                        subList = new FastList();
                        sourceAttributeMap.put(source, subList);
                    }
                    subList.add(obj);
                }
            }

            Iterator sourceAttributeMapIterator = sourceAttributeMap.keySet().iterator();
            while (sourceAttributeMapIterator.hasNext())
            {
                Object source = sourceAttributeMapIterator.next();
                primaryKeyGenerator = MithraPrimaryKeyGenerator.getInstance().getSimulatedSequencePrimaryKeyGeneratorForNoSourceAttribute(sequenceName, factoryName, source);
                List list = sourceAttributeMap.get(source);
                List<BulkSequence> bulkSequenceList = primaryKeyGenerator.getNextIdsInBulk(source, list.size());
                assignBulkIds(list, attribute, bulkSequenceList);
            }
        }
        else
        {
            zGenerateAndSetPrimaryKeysForSingleSource(attribute, sequenceHasSourceAttribute, sequenceName, factoryName, sourceAttribute.valueOf(firstNotSet));
        }
    }

    protected void zGenerateAndSetPrimaryKeysForSingleSource(SequenceAttribute attribute,
                                                             boolean sequenceHasSourceAttribute, String sequenceName, String factoryName, Object source)
    {
        this.getMithraObjectPortal(); // initialize the portal if it's not initialized
        int count = 0;
        for (int i = 0; i < this.getDelegated().size(this); i++)
        {
            if (!attribute.isSequenceSet(this.getDelegated().get(this, i)))
            {
                count++;
            }
        }
        if (count > 0)
        {
            SimulatedSequencePrimaryKeyGenerator primaryKeyGenerator;
            if (sequenceHasSourceAttribute)
            {
                primaryKeyGenerator =
                        MithraPrimaryKeyGenerator.getInstance().getSimulatedSequencePrimaryKeyGenerator(sequenceName, factoryName, source);
            }
            else
            {
                primaryKeyGenerator =
                        MithraPrimaryKeyGenerator.getInstance().getSimulatedSequencePrimaryKeyGeneratorForNoSourceAttribute(sequenceName, factoryName, source);
            }
            List<BulkSequence> bulkSequenceList = primaryKeyGenerator.getNextIdsInBulk(source, count);
            assignBulkIds(this, attribute, bulkSequenceList);
        }

    }

    private void assignBulkIds(List list, SequenceAttribute attribute, List<BulkSequence> bulkSequenceList)
    {
        if (attribute instanceof IntegerAttribute)
        {
            assignBulkIntegerIds(list, attribute, (IntegerAttribute) attribute, bulkSequenceList);
        }
        else
        {
            assignBulkLongIds(list, attribute, (LongAttribute) attribute, bulkSequenceList);
        }
    }

    private void assignBulkIntegerIds(List list, SequenceAttribute attribute, IntegerAttribute integerAttribute, List<BulkSequence> bulkSequenceList)
    {
        int seqPosition = 0;
        BulkSequence seq = bulkSequenceList.get(seqPosition);
        for (int i = 0; i < list.size(); i++)
        {
            Object obj = list.get(i);
            if (!attribute.isSequenceSet(obj))
            {
                if (!seq.hasMore())
                {
                    seqPosition++;
                    seq = bulkSequenceList.get(seqPosition);
                }
                integerAttribute.setIntValue(obj, (int) seq.getNext());
            }
        }
    }

    private void assignBulkLongIds(List list, SequenceAttribute attribute, LongAttribute longAttribute, List<BulkSequence> bulkSequenceList)
    {
        int seqPosition = 0;
        BulkSequence seq = bulkSequenceList.get(seqPosition);
        for (int i = 0; i < list.size(); i++)
        {
            Object obj = list.get(i);
            if (!attribute.isSequenceSet(obj))
            {
                if (!seq.hasMore())
                {
                    seqPosition++;
                    seq = bulkSequenceList.get(seqPosition);
                }
                longAttribute.setLongValue(obj, seq.getNext());
            }
        }
    }

    public void registerForNotification(MithraApplicationNotificationListener listener)
    {
        this.delegated = this.getDelegated().registerForNotification(this, listener);
    }

    protected void zSetAttributeNull(com.gs.fw.common.mithra.attribute.Attribute attr)
    {
        ((MithraDelegatedTransactionalList) delegated).setAttributeNull(this, attr);
    }

    protected void zSetBoolean(BooleanAttribute attr, boolean newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setBoolean(this, attr, newValue);
    }

    protected void zSetByte(ByteAttribute attr, byte newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setByte(this, attr, newValue);
    }

    protected void zSetByteArray(ByteArrayAttribute attr, byte[] newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setByteArray(this, attr, newValue);
    }

    protected void zSetChar(CharAttribute attr, char newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setChar(this, attr, newValue);
    }

    protected void zSetDate(DateAttribute attr, Date newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setDate(this, attr, newValue);
    }

    protected void zSetTime(TimeAttribute attr, Time newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setTime(this, attr, newValue);
    }

    protected void zSetDouble(DoubleAttribute attr, double newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setDouble(this, attr, newValue);
    }

    protected void zSetEvoValue(EmbeddedValueExtractor attribute, Object evo)
    {
        ((MithraDelegatedTransactionalList) delegated).setEvoValue(this, attribute, evo);
    }

    protected void zSetFloat(FloatAttribute attr, float newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setFloat(this, attr, newValue);
    }

    protected void zSetInteger(IntegerAttribute attr, int newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setInteger(this, attr, newValue);
    }

    protected void zSetLong(LongAttribute attr, long newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setLong(this, attr, newValue);
    }

    protected void zSetShort(ShortAttribute attr, short newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setShort(this, attr, newValue);
    }

    protected void zSetString(StringAttribute attr, String newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setString(this, attr, newValue);
    }

    protected void zSetTimestamp(TimestampAttribute attr, Timestamp newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setTimestamp(this, attr, newValue);
    }

    protected void zSetBigDecimal(BigDecimalAttribute attr, BigDecimal newValue)
    {
        ((MithraDelegatedTransactionalList) delegated).setBigDecimal(this, attr, newValue);
    }

    protected void zCascadeDeleteRelationships()
    {
    }

    protected void zCopyNonPersistentInto(DelegatingList target)
    {
        MithraDelegatedList<E> delegate = this.getDelegated().getNonPersistentDelegate();
        target.setDelegated(delegate);
        delegate.init(target, this.size());
        for (int i = 0; i < this.size(); i++)
        {
            MithraTransactionalObject item = (MithraTransactionalObject) this.get(i);
            target.add(item.getNonPersistentCopy());
        }
    }

    protected void zDetachInto(DelegatingList target)
    {
        DetachedList detached = DetachedList.DEFAULT;
        target.setDelegated(detached);
        detached.init(target, this.size());
        detached.setOperation(target, this.getOperation());
        for (int i = 0; i < this.size(); i++)
        {
            MithraTransactionalObject item = (MithraTransactionalObject) this.get(i);
            detached.add(target, item.getDetachedCopy());
        }
    }

    protected void zMakeDetached(Operation op, Object previousDetachedList)
    {
        if (previousDetachedList != null)
        {
            AdhocDetachedList detached = (AdhocDetachedList)
                    ((DelegatingList) previousDetachedList).fastListOrCachedQuery;
            List dontRemove = null;
            for (int i = 0; i < this.size(); i++)
            {
                MithraTransactionalObject item = (MithraTransactionalObject) this.get(i);
                if (item.zIsDetached() && detached.contains(item))
                {
                    if (dontRemove == null)
                    {
                        dontRemove = new FastList(this.size());
                    }
                    dontRemove.add(item);
                }
            }
            detached.markAllAsRemovedExcept(dontRemove);
            detached.addAll(this);
            this.setDelegated(DetachedList.DEFAULT);
            this.fastListOrCachedQuery = detached;
        }
        else
        {
            AdhocDetachedList detached = new AdhocDetachedList(this, this.size());
            MithraList many = this.getMithraObjectPortal().getFinder().findMany(op);
            for (int i = 0; i < many.size(); i++)
            {
                detached.addRemovedItem(((MithraTransactionalObject) many.get(i)).getDetachedCopy());
            }
            detached.setOperation(op);
            detached.addAll(this);
            this.setDelegated(DetachedList.DEFAULT);
            this.fastListOrCachedQuery = detached;
        }
//        DetachedList detached = DetachedList.DEFAULT;
//        Object[] current = this.toArray();
//        this.setDelegated(detached);
//        detached.init(this, current.length);
//        for(Object o: current)
//        {
//            this.add((E) o);
//        }
//        if (false)//previousDetachedList != null)
//        {
//            DelegatingList previous = (DelegatingList) previousDetachedList;
//            for (int i = 0; i < previous.size(); i++)
//            {
//                MithraTransactionalObject item = (MithraTransactionalObject) previous.get(i);
//                if (item.zIsDetached() && !this.contains(item))
//                {
//                    detached.addRemovedItem(this, item);
//                }
//            }
//        }
//        else
//        {
//            MithraList many = this.getMithraObjectPortal().getFinder().findMany(op);
//            for (int i = 0; i < many.size(); i++)
//            {
//                detached.addRemovedItem(this, ((MithraTransactionalObject) many.get(i)).getDetachedCopy());
//            }
//            detached.setOperation(this, op);
//        }
    }

    public MithraList zCloneForRelationship()
    {
        return this.delegated.zCloneForRelationship(this);
    }

    protected void zSetOperation(Operation op)
    {
        if (op.equals(this.operation))
        {
            this.operation = op;
        }
        else
        {
            throw new RuntimeException("cannot change operation");
        }
    }

    public void zSetTxDetachedDeleted()
    {
        for (int i = 0; i < this.size(); i++)
        {
            ((MithraTransactionalObject) this.get(i)).zSetTxDetachedDeleted();
        }
    }

    public void zSetNonTxDetachedDeleted()
    {
        for (int i = 0; i < this.size(); i++)
        {
            ((MithraTransactionalObject) this.get(i)).zSetNonTxDetachedDeleted();
        }
    }

    public void zMarkMoved(E item)
    {
        this.delegated.zMarkMoved(this, item);
    }

    public void setForceImplicitJoin(boolean forceImplicitJoin)
    {
        this.delegated = this.delegated.setForceImplicitJoin(this, forceImplicitJoin);
    }

    public MithraList getNonPersistentGenericCopy()
    {
        throw new RuntimeException("not implemented");
    }

    private void writeObject(ObjectOutputStream out)
        throws IOException
    {
        out.defaultWriteObject();
        this.delegated.writeObject(this, out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        this.delegated.readObject(this, in);
    }

    public Index zGetNotificationIndex()
    {
        return this.delegated.getInternalIndex(this);
    }

    private static class ListOptions implements Serializable
    {
        private transient volatile Object currentTransaction;
        private volatile DeepFetchNode deepFetchRoot;
        private OrderBy orderBy;
    }
}
