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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter2;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



public class TransactionalNonUniqueIndex implements IterableNonUniqueIndex, TransactionalIndex
{

    private ExtractorBasedHashStrategy hashStrategy;
    private ExtractorBasedHashStrategy pkHashStrategy;
    private Extractor[] pkExtractors;
    private Extractor[] indexExtractors;
    private NonUniqueIndex mainIndex;
    private static final TransactionalUnderlyingObjectGetter transactionalUnderlyingObjectGetter = new TransactionalUnderlyingObjectGetter();
    private static final NonTransactionalUnderlyingObjectGetter nonTransactionalUnderlyingObjectGetter = new NonTransactionalUnderlyingObjectGetter();
    private TransactionLocal perTransactionStorage = new TransactionLocal();

    public TransactionalNonUniqueIndex(String indexName, Extractor[] pkExtractors, Extractor[] indexExtractors)
    {
        this.pkExtractors = pkExtractors;
        this.indexExtractors = indexExtractors;
        this.pkHashStrategy = ExtractorBasedHashStrategy.create(pkExtractors);
        this.hashStrategy = ExtractorBasedHashStrategy.create(indexExtractors);
        this.mainIndex = new NonUniqueIndex(indexName, pkExtractors, indexExtractors, pkHashStrategy, hashStrategy);
        this.mainIndex.setUnderlyingObjectGetter(nonTransactionalUnderlyingObjectGetter);
    }

    @Override
    public boolean isInitialized()
    {
        return true;
    }

    @Override
    public Index getInitialized(IterableIndex iterableIndex)
    {
        return this;
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
    public void ensureExtraCapacity(int size)
    {
        this.mainIndex.ensureExtraCapacity(size);
    }

    protected List removeDeletedFromMainResult(Object fromMain, TransactionLocalStorage txStorage)
    {
        if (fromMain == null)
        {
            return null;
        }
        List result = null;
        FullUniqueIndex deletedPerThread = getDeletedFromLocalStorage(txStorage);
        if (deletedPerThread == null)
        {
            if (fromMain instanceof FullUniqueIndex)
            {
                result = ((FullUniqueIndex)fromMain).getAll();
            }
            else
            {
                result = new FastList(1);
                result.add(fromMain);
            }
        }
        else
        {
            if (fromMain instanceof FullUniqueIndex)
            {
                result = new FastList();
                ((FullUniqueIndex)fromMain).forAll(new AddNonDeleted(result, deletedPerThread));
            }
            else
            {
                if (deletedPerThread.getFromData(this.nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(fromMain)) == null)
                {
                    result = new FastList(1);
                    result.add(fromMain);
                }
            }
        }
        return result;
    }

    private FullUniqueIndex getDeletedFromLocalStorage(TransactionLocalStorage txStorage)
    {
        return txStorage == null ? null : txStorage.deleted;
    }

    protected Object combineResults(Object fromPerThread, Object fromMain, TransactionLocalStorage txStorage)
    {
        List result = removeDeletedFromMainResult(fromMain, txStorage);
        if (fromPerThread != null)
        {
            if (result == null)
            {
                result = new FastList();
            }
            if (fromPerThread instanceof FullUniqueIndex)
            {
                ((FullUniqueIndex) fromPerThread).forAll(new AddAll(result));
            }
            else
            {
                result.add(fromPerThread);
            }
        }
        return result;
    }

    // for int indicies:
    public Object get(int indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(char indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(Object indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(byte[] indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(long indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(double indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(float indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(boolean indexValue)
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(indexValue);
        }
        Object mainIndexResult = this.mainIndex.get(indexValue);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(Object dataHolder, List extractors) // for multi attribute indicies
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(dataHolder, extractors);
        }
        Object mainIndexResult = this.mainIndex.get(dataHolder, extractors);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public Object get(Object dataHolder, Extractor[] extractors) // for multi attribute indicies
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.get(dataHolder, extractors);
        }
        Object mainIndexResult = this.mainIndex.get(dataHolder, extractors);
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());

        if (this.getDeletedFromLocalStorage(txStorage) != null)
        {
            Object candidate = this.get(keyHolder, extractors);
            return candidate != null && (filter == null || filter.matches(candidate, keyHolder));
        }

        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        return (perThreadAdded != null && perThreadAdded.contains(keyHolder, extractors, filter))
               || this.mainIndex.contains(keyHolder, extractors, filter);
    }

    private NonUniqueIndex getAddedFromLocalStorage(TransactionLocalStorage txStorage)
    {
        return txStorage == null ? null : txStorage.added;
    }

    @Override
    public void findAndExecute(Object dataHolder, Extractor[] extractors, DoUntilProcedure procedure)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        Index perThreadDeleted = getDeletedFromLocalStorage(txStorage);
        if (perThreadAdded == null && perThreadDeleted == null)
        {
            this.mainIndex.findAndExecute(dataHolder, extractors, procedure);
        }
        else
        {
            Object result = this.get(dataHolder, extractors);
            if (result instanceof List)
            {
                List list = (List) result;
                boolean done = false;
                for(int i=0;i<list.size();i++)
                {
                    done = procedure.execute(list.get(i));
                }
            }
            else if (result instanceof FullUniqueIndex)
            {
                ((FullUniqueIndex)result).forAll(procedure);
            }
            else if (result != null)
            {
                procedure.execute(result);
            }
        }
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDates0, Timestamp asOfDate1)
    {
        throw new RuntimeException("should not get here");
    }

    public Object getNulls()
    {
        Object perThreadResult = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = getAddedFromLocalStorage(txStorage);
        if (perThreadAdded != null)
        {
            perThreadResult = perThreadAdded.getNulls();
        }
        Object mainIndexResult = this.mainIndex.getNulls();
        return this.combineResults(perThreadResult, mainIndexResult, txStorage);
    }

    public boolean isUnique()
    {
        return false;
    }

    public int getAverageReturnSize()
    {
        return this.mainIndex.getAverageReturnSize();
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        return this.mainIndex.getMaxReturnSize(multiplier);
    }

    public Extractor[] getExtractors()
    {
        return this.indexExtractors;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        throw new RuntimeException("not supported");
    }

    public Object put(Object businessObject)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
            NonUniqueIndex perThreadIndex = getAddedFromLocalStorage(txStorage);
            if (perThreadIndex == null)
            {
                perThreadIndex = this.createPerThreadAddedIndex(tx, txStorage).added;
            }
            return perThreadIndex.put(businessObject);
        }
        else
        {
            return this.nonTransactionalPut(businessObject);
        }
    }

    public Object nonTransactionalPut(Object businessObject)
    {
        return this.mainIndex.put(businessObject);
    }

    public Object remove(Object businessObject)
    {
        Object result = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
            NonUniqueIndex perThreadAdded = getAddedFromLocalStorage(txStorage);
            if (perThreadAdded != null)
            {
                result = perThreadAdded.remove(businessObject);
            }
            if (result == null)
            {
                FullUniqueIndex perThreadDeleted = getDeletedFromLocalStorage(txStorage);
                if (perThreadDeleted == null)
                {
                    perThreadDeleted = createPerThreadDeletedIndex(tx, txStorage);
                }
                perThreadDeleted.put(this.nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(businessObject));
            }
        }
        else
        {
            result = this.mainIndex.remove(businessObject);
        }
        return result;
    }

    public Object removeUsingUnderlying(Object underlyingObject)
    {
        return this.mainIndex.removeUsingUnderlying(underlyingObject);
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

    @Override
    public boolean prepareForReindex(Object businessObject, MithraTransaction tx)
    {
        boolean done = false;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        NonUniqueIndex threadIndex = getAddedFromLocalStorage(txStorage);
        if (tx.isCautious())
        {
            boolean mustDelete = true;
            if (threadIndex == null)
            {
                txStorage = createPerThreadAddedIndex(tx, txStorage);
            }
            else if (threadIndex.contains(businessObject))
            {
                mustDelete = false;
                threadIndex.remove(businessObject);
            }
            if (mustDelete)
            {
                FullUniqueIndex deletedIndex = txStorage.deleted;
                if (deletedIndex == null)
                {
                    deletedIndex = this.createPerThreadDeletedIndex(tx, txStorage);
                }
                deletedIndex.put(this.nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(businessObject));
            }
            done = true;
        }
        else
        {
            if (threadIndex != null &&
                    threadIndex.contains(businessObject))
            {
                threadIndex.remove(businessObject);
                done = true;
            }
        }
        return done;
    }

    @Override
    public void finishForReindex(Object businessObject, MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        NonUniqueIndex threadIndex = txStorage.added;
        threadIndex.put(businessObject);
    }

    @Override
    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        NonUniqueIndex threadIndex = getAddedFromLocalStorage(txStorage);
        if (threadIndex != null &&
            threadIndex.contains(businessObject))
        {
            threadIndex.remove(businessObject);
        }
        else
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
            deletedIndex.put(this.nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(businessObject));
        }
    }

    public Object putUsingUnderlying(Object businessObject, Object underlying)
    {
        throw new RuntimeException("not implemented");
    }

    private TransactionLocalStorage createPerThreadAddedIndex(MithraTransaction tx, TransactionLocalStorage txStorage)
    {
        NonUniqueIndex threadIndex = new NonUniqueIndex(null, this.pkExtractors, this.indexExtractors, this.pkHashStrategy, this.hashStrategy);
        threadIndex.setUnderlyingObjectGetter(transactionalUnderlyingObjectGetter);
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
        return this.mainIndex.putUsingUnderlying(object, newData);
    }

    public Object preparePut(Object object)
    {
        return null;
    }

    public void commitPreparedForIndex(Object index)
    {
        // nothing to do
    }

    public Object removeIgnoringTransaction(Object object)
    {
        return this.mainIndex.remove(object);
    }

    public Object getFromPreparedUsingData(Object data)
    {
        throw new RuntimeException("should never be called");
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        throw new RuntimeException("should never be called");
    }

    public void commit(MithraTransaction tx)
    {
    }

    public void rollback(MithraTransaction tx)
    {
    }

    public void clear()
    {
        this.mainIndex.clear();
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
            if (deletedIndex.getFromData(nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(o)) == null)
            {
                return procedure.execute(o);
            }
            return false;
        }
    }

    protected static class GetAllProcedure implements DoUntilProcedure
    {
        private FullUniqueIndex deletedIndex;
        private FastList result;

        public GetAllProcedure(FullUniqueIndex deletedIndex)
        {
            this.deletedIndex = deletedIndex;
            result = new FastList();
        }

        public boolean execute(Object o)
        {
            if (!deletedIndex.contains(nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(o)))
            {
                result.add(o);
            }
            return false;
        }

        public FastList getResult()
        {
            return result;
        }


    }

    protected static class AddAll implements DoUntilProcedure
    {
        private List result;

        public AddAll(List result)
        {
            this.result = result;
        }

        public boolean execute(Object o)
        {
            result.add(o);
            return false;
        }

        public List getResult()
        {
            return result;
        }
    }

    public boolean evictCollectedReferences()
    {
        return false;
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return false;
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

    protected static class AddNonDeleted implements DoUntilProcedure
    {
        private List result;
        private FullUniqueIndex deletedIndex;

        public AddNonDeleted(List result, FullUniqueIndex deletedIndex)
        {
            this.result = result;
            this.deletedIndex = deletedIndex;
        }

        public boolean execute(Object o)
        {
            if (deletedIndex.getFromData(nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(o)) == null)
            {
                result.add(o);
            }
            return false;
        }

        public List getResult()
        {
            return result;
        }
    }

    private static class TransactionLocalStorage
    {
        private NonUniqueIndex added;
        private FullUniqueIndex deleted;
    }
}
