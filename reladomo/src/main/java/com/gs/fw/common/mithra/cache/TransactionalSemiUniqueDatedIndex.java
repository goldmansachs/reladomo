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

package com.gs.fw.common.mithra.cache;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDataStorage;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.DoProcedure;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



public abstract class TransactionalSemiUniqueDatedIndex implements TransactionalIndex, SemiUniqueDatedIndex
{

    private ExtractorBasedHashStrategy pkHashStrategy;
    private Extractor[] semiUniqueExtractors;
    private Extractor[] pkExtractors;
    private AsOfAttribute[] asOfAttributes;
    private ExtractorBasedHashStrategy hashStrategy;
    private SemiUniqueDatedIndex mainIndex;
    private TransactionLocal perTransactionStorage = new TransactionLocal();
    private final FastList preparedIndices = new FastList();
    private final boolean offHeap;

    public TransactionalSemiUniqueDatedIndex(String indexName, Extractor[] extractors, AsOfAttribute[] asOfAttributes,
            long timeToLive, long relationshipTimeToLive)
    {
        this(indexName, extractors, asOfAttributes, timeToLive, relationshipTimeToLive, null);
    }

    public TransactionalSemiUniqueDatedIndex(String indexName, Extractor[] extractors, AsOfAttribute[] asOfAttributes,
            long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage)
    {
        this.semiUniqueExtractors = extractors;
        this.asOfAttributes = asOfAttributes;
        this.hashStrategy = ExtractorBasedHashStrategy.create(this.semiUniqueExtractors);
        TimestampAttribute[] datedAttributes = new TimestampAttribute[this.asOfAttributes.length];
        for(int i=0;i<this.asOfAttributes.length;i++)
        {
            datedAttributes[i] = this.asOfAttributes[i].getFromAttribute();
        }
        this.pkExtractors = new Extractor[this.semiUniqueExtractors.length + datedAttributes.length];
        System.arraycopy(this.semiUniqueExtractors, 0, this.pkExtractors, 0, this.semiUniqueExtractors.length);
        System.arraycopy(datedAttributes, 0, this.pkExtractors, this.semiUniqueExtractors.length, datedAttributes.length);
        this.pkHashStrategy = ExtractorBasedHashStrategy.create(this.pkExtractors);

        this.mainIndex = this.createMainIndex(indexName, extractors, asOfAttributes,
                this.pkExtractors, timeToLive, relationshipTimeToLive, dataStorage);
        offHeap = dataStorage != null;
    }

    protected abstract SemiUniqueDatedIndex createMainIndex(String indexName, Extractor[] extractors,
            AsOfAttribute[] asOfAttributes, Extractor[] pkExtractors, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage);

    protected FullSemiUniqueDatedIndex getPerThreadAddedIndex(TransactionLocalStorage txStorage)
    {
        return txStorage == null ? null : txStorage.added;
    }

    @Override
    public List<Object> collectMilestoningOverlaps()
    {
        return this.getMainIndex().collectMilestoningOverlaps();
    }

    protected FullUniqueIndex getPerThreadDeletedIndex(TransactionLocalStorage txStorage)
    {
        return txStorage == null ? null : txStorage.deleted;
    }

    private Object checkDeletedIndex(Object result, TransactionLocalStorage txStorage)
    {
        if (result != null)
        {
            FullUniqueIndex perThreadDeletedIndex = this.getPerThreadDeletedIndex(txStorage);
            if (perThreadDeletedIndex != null)
            {
                if (result instanceof List)
                {
                    List list = (List) result;
                    List resultList = new FastList(list.size());
                    for (int i=0; i<list.size(); i++)
                    {
                        Object data = list.get(i);
                        if (perThreadDeletedIndex.getFromData(data) == null)
                        {
                            resultList.add(data);
                        }
                    }
                    result = resultList;
                }
                else
                {
                    Object data = result;
                    if (perThreadDeletedIndex.getFromData(data) != null)
                    {
                        result = null;
                    }
                }
            }
        }
        return result;
    }

    public Object removeOldEntry(Object data, Timestamp[] asOfDates)
    {
        return this.mainIndex.removeOldEntry(data, asOfDates);
    }

    protected SemiUniqueDatedIndex getMainIndex()
    {
        return mainIndex;
    }

    public Object getSemiUniqueFromData(Object data, Timestamp[] asOfDates)
    {
        return this.mainIndex.getSemiUniqueFromData(data, asOfDates);
    }

    public List getFromDataForAllDatesAsList(Object data)
    {
        return this.mainIndex.getFromDataForAllDatesAsList(data);
    }

    public boolean addSemiUniqueToContainer(Object data, TemporalContainer container)
    {
        return this.mainIndex.addSemiUniqueToContainer(data, container);
    }

    public Object getSemiUniqueAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getSemiUniqueAsOne(srcObject, srcData, relationshipHashStrategy, nonDatedHash, asOfDate0, asOfDate1);
        }
        if (result == null)
        {
            result = this.mainIndex.getSemiUniqueAsOne(srcObject, srcData, relationshipHashStrategy, nonDatedHash, asOfDate0, asOfDate1);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object getSemiUniqueAsOneWithDates(Object valueHolder, Extractor[] extractors, Timestamp[] dates, int nonDatedHash)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getSemiUniqueAsOneWithDates(valueHolder, extractors, dates, nonDatedHash);
        }
        if (result == null)
        {
            result = this.mainIndex.getSemiUniqueAsOneWithDates(valueHolder, extractors, dates, nonDatedHash);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public synchronized List removeOldEntryForRange(Object data)
    {
        return this.mainIndex.removeOldEntryForRange(data);
    }

    @Override
    public boolean removeAllIgnoringDate(Object data, DoProcedure procedure)
    {
        return this.mainIndex.removeAllIgnoringDate(data, procedure);
    }

    @Override
    public boolean prepareForReindex(Object businessObject, MithraTransaction tx)
    {
        boolean done = false;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullSemiUniqueDatedIndex threadIndex = txStorage == null ? null : txStorage.added;
        if (tx.isCautious())
        {
            if (threadIndex != null && threadIndex.remove(businessObject) != null)
            {
                FullUniqueIndex deletedIndex = txStorage.deleted;
                if (deletedIndex == null)
                {
                    deletedIndex = this.createPerThreadDeletedIndex(tx, txStorage);
                }
                deletedIndex.put(businessObject);
            }
            done = true;
        }
        else
        {
            if (threadIndex != null &&
                    threadIndex.remove(businessObject) != null)
            {
                done = true;
            }
        }
        return done;
    }

    @Override
    public void finishForReindex(Object businessObject, MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullSemiUniqueDatedIndex threadIndex = txStorage.added;
        threadIndex.put(businessObject, this.mainIndex.getNonDatedPkHashStrategy().computeHashCode(businessObject));
    }

    @Override
    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullSemiUniqueDatedIndex threadIndex = txStorage == null ? null : txStorage.added;
        if (threadIndex == null ||
                threadIndex.remove(businessObject) == null)
        {
            if (threadIndex == null)
            {
                txStorage = createPerThreadAddedIndex(tx, txStorage);
            }
            FullUniqueIndex deletedIndex = txStorage.deleted;
            if (deletedIndex == null)
            {
                deletedIndex = this.createPerThreadDeletedIndex(tx, txStorage);
            }
            deletedIndex.put(businessObject);
        }
    }

    public Object put(Object businessObject, int nonDatedPkHashCode)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
            FullSemiUniqueDatedIndex perThreadIndex = this.getPerThreadAddedIndex(txStorage);
            if (perThreadIndex == null)
            {
                txStorage = this.createPerThreadAddedIndex(tx, txStorage);
                perThreadIndex = txStorage.added;
            }
            if (offHeap)
            {
                nonDatedPkHashCode = perThreadIndex.getNonDatedPkHashStrategy().computeHashCode(businessObject);
            }
            return perThreadIndex.put(businessObject, nonDatedPkHashCode);
        }
        else
        {
            return this.mainIndex.put(businessObject, nonDatedPkHashCode);
        }
    }

    public Object putSemiUnique(Object key)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
            FullSemiUniqueDatedIndex perThreadIndex = this.getPerThreadAddedIndex(txStorage);
            if (perThreadIndex == null)
            {
                txStorage = this.createPerThreadAddedIndex(tx, txStorage);
                perThreadIndex = txStorage.added;
            }
            return perThreadIndex.putSemiUnique(key);
        }
        else
        {
            return this.mainIndex.putSemiUnique(key);
        }
    }

    public List removeAll(Filter filter)
    {
        return this.mainIndex.removeAll(filter);
    }

    public boolean evictCollectedReferences()
    {
        return this.mainIndex.evictCollectedReferences();
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return this.mainIndex.needToEvictCollectedReferences();
    }

    public CommonExtractorBasedHashingStrategy getNonDatedPkHashStrategy()
    {
        return this.mainIndex.getNonDatedPkHashStrategy();
    }

    public void forAllInParallel(ParallelProcedure procedure)
    {
        this.mainIndex.forAllInParallel(procedure);
    }

    public void ensureExtraCapacity(int extraCapacity)
    {
        this.mainIndex.ensureExtraCapacity(extraCapacity);
    }

    public Object get(Object dataHolder, List extractors) // for multi attribute indicies
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(dataHolder, extractors);
        }
        if (result == null)
        {
            result = this.mainIndex.get(dataHolder, extractors);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(Object dataHolder, Extractor[] extractors) // for multi attribute indicies
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(dataHolder, extractors);
        }
        if (result == null)
        {
            result = this.mainIndex.get(dataHolder, extractors);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public boolean containsInSemiUnique(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        if(this.getPerThreadDeletedIndex(txStorage) != null)
        {
            Object candidate = this.getFromSemiUnique(keyHolder, extractors);
            return candidate != null && (filter == null || filter.matches(candidate, keyHolder));
        }

        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        return perThreadAdded != null && perThreadAdded.containsInSemiUnique(keyHolder, extractors, filter)
                || this.mainIndex.containsInSemiUnique(keyHolder, extractors, filter);

    }


    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        if(this.getPerThreadDeletedIndex(txStorage) != null)
        {
            Object candidate = this.get(keyHolder, extractors);
            return candidate != null && (filter == null || filter.matches(candidate, keyHolder));
        }

        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        return perThreadAdded != null && perThreadAdded.contains(keyHolder, extractors, filter)
                || this.mainIndex.contains(keyHolder, extractors, filter);

    }

    public Extractor[] getExtractors()
    {
        return this.mainIndex.getExtractors();
    }

    public Object getFromPreparedUsingData(Object data)
    {
        Object result = null;
        synchronized(this.preparedIndices)
        {
            for (int i = 0; i < preparedIndices.size() && result == null; i++)
            {
                FullUniqueIndex index = (FullUniqueIndex) preparedIndices.get(i);
                result = index.getFromData(data);
            }
        }
        return result;
    }

    public Object remove(Object data)
    {
        Object result = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
            FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
            if (perThreadAdded != null)
            {
                result = perThreadAdded.remove(data);
            }
            if (result == null)
            {
                FullUniqueIndex perThreadDeleted = this.getPerThreadDeletedIndex(txStorage);
                if (perThreadDeleted == null)
                {
                    perThreadDeleted = createPerThreadDeletedIndex(tx, txStorage);
                }
                perThreadDeleted.put(data);
            }
        }
        else
        {
            result = this.mainIndex.remove(data);
        }
        return result;
    }

    public void clear()
    {
        this.mainIndex.clear();
    }

    private FullUniqueIndex createPerThreadDeletedIndex(MithraTransaction tx, TransactionLocalStorage txStorage)
    {
        FullUniqueIndex perThreadDeleted = new FullUniqueIndex(this.pkHashStrategy);
        if (txStorage == null)
        {
            txStorage = new TransactionLocalStorage();
            this.perTransactionStorage.set(tx, txStorage);
        }
        txStorage.deleted = perThreadDeleted;
        return perThreadDeleted;
    }

    private TransactionLocalStorage createPerThreadAddedIndex(MithraTransaction tx, TransactionLocalStorage txStorage)
    {
        FullSemiUniqueDatedIndex threadIndex = new FullSemiUniqueDatedIndex(this.hashStrategy,
                this.semiUniqueExtractors, this.asOfAttributes, this.pkExtractors, this.pkHashStrategy);
        if (txStorage == null)
        {
            txStorage = new TransactionLocalStorage();
            this.perTransactionStorage.set(tx, txStorage);
        }
        txStorage.added = threadIndex;
        return txStorage;
    }

    public Object putIgnoringTransaction(Object object, Object newData, boolean weak)
    {
        return this.mainIndex.put(newData, this.mainIndex.getNonDatedPkHashStrategy().computeHashCode(newData));
    }

    public Object preparePut(Object object)
    {
        throw new RuntimeException("not implemented");
    }

    public void commitPreparedForIndex(Object index)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Object removeIgnoringTransaction(Object object)
    {
        return this.mainIndex.remove(object);
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullSemiUniqueDatedIndex perThreadAddedIndex = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAddedIndex != null)
        {
            synchronized(this.preparedIndices)
            {
                this.preparedIndices.add(perThreadAddedIndex);
            }
        }
    }

    private void clearTransactionalState(MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullSemiUniqueDatedIndex addedIndex = this.getPerThreadAddedIndex(txStorage);
        if (addedIndex != null)
        {
            synchronized (this.preparedIndices)
            {
                this.preparedIndices.remove(addedIndex);
            }
        }
    }

    public void commit(MithraTransaction tx)
    {
        this.clearTransactionalState(tx);
    }

    public void rollback(MithraTransaction tx)
    {
        this.clearTransactionalState(tx);
    }

    public int getSemiUniqueSize()
    {
        return this.mainIndex.getSemiUniqueSize();
    }

    public Object removeUsingUnderlying(Object underlyingObject)
    {
        return this.mainIndex.remove(underlyingObject);
    }

    public Object getFromData(Object data, int nonDatedHashCode)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getFromData(data, nonDatedHashCode);
        }
        if (result == null)
        {
            result = this.mainIndex.getFromData(data, nonDatedHashCode);
        }
        return result;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        SemiUniqueDatedIndex threadAddedIndex = this.getPerThreadAddedIndex(txStorage);
        boolean done = false;
        if (threadAddedIndex != null)
        {
            done = threadAddedIndex.forAll(procedure);
        }
        if (!done)
        {
            FullUniqueIndex deletedIndex = this.getPerThreadDeletedIndex(txStorage);
            if (deletedIndex != null)
            {
                procedure = new IgnoreDeletedProcedure(procedure, deletedIndex);
            }
            done = this.mainIndex.forAll(procedure);
        }
        return done;
    }

    public PrimaryKeyIndex copy()
    {
        return this.mainIndex.copy();
    }

    public int size()
    {
        return this.mainIndex.size();
    }

    // new to differentiate
    public Object getFromSemiUnique(Object dataHolder, List extractors)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getFromSemiUnique(dataHolder, extractors);
        }
        if (result != null)
        {
            int startIndex = this.semiUniqueExtractors.length;
            int length = extractors.size() - startIndex;
            boolean matchMoreThanOne = false;
            for(int i=0;i<length;i++)
            {
                AsOfExtractor extractor = (AsOfExtractor) extractors.get(startIndex+i);
                matchMoreThanOne = matchMoreThanOne || extractor.matchesMoreThanOne();
            }
            if (matchMoreThanOne)
            {
                Object mainIndexResult = this.mainIndex.getFromSemiUnique(dataHolder, extractors);
                result = checkDeleted(result, txStorage, mainIndexResult);
            }
        }
        else
        {
            result = this.mainIndex.getFromSemiUnique(dataHolder, extractors);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object getFromSemiUnique(Object dataHolder, Extractor[] extractors)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullSemiUniqueDatedIndex perThreadAdded = this.getPerThreadAddedIndex(txStorage);
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getFromSemiUnique(dataHolder, extractors);
        }
        if (result != null)
        {
            int startIndex = this.semiUniqueExtractors.length;
            int length = extractors.length - startIndex;
            boolean matchMoreThanOne = false;
            for(int i=0;i<length;i++)
            {
                AsOfExtractor extractor = (AsOfExtractor) extractors[startIndex+i];
                matchMoreThanOne = matchMoreThanOne || extractor.matchesMoreThanOne();
            }
            if (matchMoreThanOne)
            {
                Object mainIndexResult = this.mainIndex.getFromSemiUnique(dataHolder, extractors);
                result = checkDeleted(result, txStorage, mainIndexResult);
            }
        }
        else
        {
            result = this.mainIndex.getFromSemiUnique(dataHolder, extractors);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    private Object checkDeleted(Object result, TransactionLocalStorage txStorage, Object mainIndexResult)
    {
        mainIndexResult = this.checkDeletedIndex(mainIndexResult, txStorage);
        if (mainIndexResult != null)
        {
            if (result instanceof List)
            {
                if (mainIndexResult instanceof List)
                {
                    result = combineWithDuplicateCheckListList((List) result, (List) mainIndexResult);
                }
                else
                {
                    result = combineWithDuplicateCheckListObject((List) result, mainIndexResult);
                }
            }
            else
            {
                if (mainIndexResult instanceof List)
                {
                    result = combineWithDuplicateCheckObjectList(result, (List) mainIndexResult);
                }
                else
                {
                    if (!this.pkHashStrategy.equals(result, mainIndexResult))
                    {
                        List temp = new FastList(2);
                        temp.add(result);
                        temp.add(mainIndexResult);
                        result = temp;
                    }
                }
            }
        }
        return result;
    }

    private Object combineWithDuplicateCheckObjectList(Object result, List list)
    {
        for(int i=0;i<list.size();i++)
        {
            if (this.pkHashStrategy.equals(list.get(i), result))
            {
                list.set(i, result);
                return list;
            }
        }
        list.add(result);
        return list;
    }

    private Object combineWithDuplicateCheckListObject(List threadList, Object mainObject)
    {
        if (notInList(threadList, threadList.size(), mainObject))
        {
            threadList.add(mainObject);
        }
        return threadList;
    }

    private Object combineWithDuplicateCheckListList(List threadList, List mainList)
    {
        int threadListSize = threadList.size();
        for(int i=0;i<mainList.size();i++)
        {
            Object o = mainList.get(i);
            if (notInList(threadList, threadListSize, o))
            {
                threadList.add(o);
            }

        }
        return threadList;
    }

    private boolean notInList(List threadList, int maxSize, Object o)
    {
        for(int i=0;i<maxSize;i++)
        {
            Object listObject = threadList.get(i);
            if (this.pkHashStrategy.equals(listObject, o))
            {
                return false;
            }
        }
        return true;
    }

    public Extractor[] getNonDatedExtractors()
    {
        return this.semiUniqueExtractors;
    }

    protected static class IgnoreDeletedProcedure implements DoUntilProcedure
    {
        private DoUntilProcedure procedure;
        private FullUniqueIndex deletedIndex;

        public IgnoreDeletedProcedure(DoUntilProcedure procedure, FullUniqueIndex deletedIndex)
        {
            this.procedure = procedure;
            this.deletedIndex = deletedIndex;
        }

        public boolean execute(Object o)
        {
            if (!deletedIndex.contains(o))
            {
                return procedure.execute(o);
            }
            return false;
        }
    }

    private static class TransactionLocalStorage
    {
        private FullSemiUniqueDatedIndex added;
        private FullUniqueIndex deleted;
    }

    @Override
    public void destroy()
    {
        this.mainIndex.destroy();
        this.mainIndex = null;
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {
        this.mainIndex.reportSpaceUsage(logger, className);
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return this.mainIndex.getOffHeapAllocatedIndexSize();
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return this.mainIndex.getOffHeapUsedIndexSize();
    }
}
