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
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.transaction.TransactionLocal;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



public abstract class TransactionalUniqueIndex implements PrimaryKeyIndex, TransactionalIndex
{

    private String indexName;
    private Extractor[] extractors;
    private ExtractorBasedHashStrategy hashStrategy;
    private PrimaryKeyIndex mainIndex;
    private static final TransactionalUnderlyingObjectGetter transactionalUnderlyingObjectGetter = new TransactionalUnderlyingObjectGetter();
    private static final NonTransactionalUnderlyingObjectGetter nonTransactionalUnderlyingObjectGetter = new NonTransactionalUnderlyingObjectGetter();
    private TransactionLocal perTransactionStorage = new TransactionLocal();
    private final FastList preparedIndices = new FastList();

    public TransactionalUniqueIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive)
    {
        this.indexName = indexName;
        this.extractors = extractors;
        this.hashStrategy = ExtractorBasedHashStrategy.create(this.extractors);
        this.mainIndex = this.createMainIndex(indexName, extractors, timeToLive, relationshipTimeToLive);
        this.mainIndex.setUnderlyingObjectGetter(nonTransactionalUnderlyingObjectGetter);
    }

    protected abstract PrimaryKeyIndex createMainIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive);

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

    // for int indicies:
    public Object get(int indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(char indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    private Object checkDeletedIndex(Object result, TransactionLocalStorage txStorage)
    {
        if (result != null)
        {
            FullUniqueIndex perThreadDeletedIndex = txStorage == null ? null : txStorage.deleted;
            if (perThreadDeletedIndex != null &&
                    perThreadDeletedIndex.getFromData(this.nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(result)) != null)
            {
                result = null;
            }
        }
        return result;
    }

    public Object get(Object indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(byte[] indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(long indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(double indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(float indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(boolean indexValue)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(indexValue);
        }
        if (result == null)
        {
            result = this.mainIndex.get(indexValue);
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object get(Object dataHolder, List extractors) // for multi attribute indicies
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
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
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(dataHolder, extractors);
        }
        if (result == null)
        {
            result = this.mainIndex.get(dataHolder, extractors);
            if (txStorage != null&& result != null) result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public boolean contains(Object keyHolder, Extractor[] extractors, Filter2 filter)
    {
        Object result = this.get(keyHolder, extractors);
        return result != null && (filter == null || filter.matches(result, keyHolder));
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.get(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
        }
        if (result == null)
        {
            result = this.mainIndex.get(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
            if (txStorage != null&& result != null) result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public Object getNulls()
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        Index perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getNulls();
        }
        if (result == null)
        {
            result = this.mainIndex.getNulls();
            result = this.checkDeletedIndex(result, txStorage);
        }
        return result;
    }

    public boolean isUnique()
    {
        return true;
    }

    public int getAverageReturnSize()
    {
        return 1;
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        return multiplier;
    }

    public Extractor[] getExtractors()
    {
        return this.extractors;
    }

    public String getIndexName()
    {
        return indexName;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        throw new RuntimeException("not supported");
    }

    public Object getFromData(Object data)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullUniqueIndex perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getFromData(data);
        }
        if (result == null)
        {
            result = this.mainIndex.getFromData(data);
        }
        return result;
    }

    public Object getFromPreparedUsingData(Object data)
    {
        Object result = null;
        synchronized (this.preparedIndices)
        {
            for (int i = 0; i < preparedIndices.size() && result == null; i++)
            {
                FullUniqueIndex index = (FullUniqueIndex) preparedIndices.get(i);
                result = index.getFromData(data);
            }
        }
        return result;
    }

    public Object putWeak(Object businessObject)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            return putIntoPerThread(businessObject, tx);
        }
        else
        {
            return this.mainIndex.putWeak(businessObject);
        }
    }

    public Object putWeakUsingUnderlying(Object businessObject, Object underlying)
    {
        throw new RuntimeException("not implemented");
    }

    public Object put(Object businessObject)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            return putIntoPerThread(businessObject, tx);
        }
        else
        {
            return this.mainIndex.put(businessObject);
        }
    }

    private Object putIntoPerThread(Object businessObject, MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex perThreadIndex = txStorage == null ? null : txStorage.added;
        if (perThreadIndex == null)
        {
            perThreadIndex = this.createPerThreadAddedIndex(tx, txStorage).added;
        }
        return perThreadIndex.put(businessObject);
    }

    public Object remove(Object businessObject)
    {
        Object result = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
            FullUniqueIndex perThreadAdded = txStorage == null ? null : txStorage.added;
            if (perThreadAdded != null)
            {
                result = perThreadAdded.remove(businessObject);
            }
            if (result == null)
            {
                FullUniqueIndex perThreadDeleted = txStorage == null ? null : txStorage.deleted;
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

    public List removeAll(Filter filter)
    {
        return this.mainIndex.removeAll(filter);
    }

    public boolean sizeRequiresWriteLock()
    {
        return this.mainIndex.sizeRequiresWriteLock();
    }

    public void ensureCapacity(int capacity)
    {
        this.mainIndex.ensureCapacity(capacity);
    }

    public void ensureExtraCapacity(int capacity)
    {
        this.mainIndex.ensureExtraCapacity(capacity);
    }

    private FullUniqueIndex createPerThreadDeletedIndex(MithraTransaction tx, TransactionLocalStorage txStorage)
    {
        FullUniqueIndex perThreadDeleted = new FullUniqueIndex(this.hashStrategy);
        if (txStorage == null)
        {
            txStorage = new TransactionLocalStorage();
            perTransactionStorage.set(tx, txStorage);
        }
        txStorage.deleted = perThreadDeleted;
        return perThreadDeleted;
    }

    public void clear()
    {
        this.mainIndex.clear();
    }

    @Override
    public boolean prepareForReindex(Object businessObject, MithraTransaction tx)
    {
        boolean done = false;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex threadIndex = txStorage == null ? null : txStorage.added;
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
        FullUniqueIndex threadIndex = txStorage.added;
        threadIndex.put(businessObject);
    }

    @Override
    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex threadIndex = txStorage == null ? null : txStorage.added;
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
        FullUniqueIndex threadIndex = new FullUniqueIndex(this.hashStrategy);
        threadIndex.setUnderlyingObjectGetter(transactionalUnderlyingObjectGetter);
        if (txStorage == null)
        {
            txStorage = new TransactionLocalStorage();
            perTransactionStorage.set(tx, txStorage);
        }
        txStorage.added = threadIndex;
        return txStorage;
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex threadAddedIndex = txStorage == null ? null : txStorage.added;
        boolean done = false;
        if (threadAddedIndex != null)
        {
            done = threadAddedIndex.forAll(procedure);
        }
        if (!done)
        {
            FullUniqueIndex deletedIndex = txStorage == null ? null : txStorage.deleted;
            if (deletedIndex != null)
            {
                procedure = new IgnoreDeletedProcedure(procedure, deletedIndex);
            }
            done = this.mainIndex.forAll(procedure);
        }
        return done;
    }

    public HashStrategy getHashStrategy()
    {
        return this.hashStrategy;
    }

    public List getAll()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex deletedIndex = txStorage == null ? null : txStorage.deleted;
        List mainList;
        if (deletedIndex != null)
        {
            GetAllProcedure procedure = new GetAllProcedure(deletedIndex);
            this.mainIndex.forAll(procedure);
            mainList = procedure.getResult();
        }
        else
        {
            mainList = this.mainIndex.getAll();
        }
        FullUniqueIndex threadAddedIndex = txStorage == null ? null : txStorage.added;
        if (threadAddedIndex != null)
        {
            mainList.addAll(threadAddedIndex.getAll());
        }
        return mainList;
    }

    public Object putIgnoringTransaction(Object object, Object newData, boolean weak)
    {
        if (weak)
        {
            return this.mainIndex.putWeakUsingUnderlying(object, newData);
        }
        else
        {
            return this.mainIndex.putUsingUnderlying(object, newData);
        }
    }

    public Object preparePut(Object object)
    {
        // todo: rezaem: we don't need a big expensive index here. Maybe we can have a separate list for these one-off situations
        FullUniqueIndex perThreadAddedIndex = new FullUniqueIndex(this.hashStrategy);
        perThreadAddedIndex.setUnderlyingObjectGetter(transactionalUnderlyingObjectGetter);
        perThreadAddedIndex.putUsingUnderlying(object, ((MithraTransactionalObject) object).zGetNonTxData());
        synchronized (this.preparedIndices)
        {
            this.preparedIndices.add(perThreadAddedIndex);
        }
        return perThreadAddedIndex;
    }

    public void commitPreparedForIndex(Object index)
    {
        synchronized (this.preparedIndices)
        {
            this.preparedIndices.remove(index);
        }
    }

    public Object removeIgnoringTransaction(Object object)
    {
        return this.mainIndex.remove(object);
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex perThreadAddedIndex = txStorage == null ? null : txStorage.added;
        if (perThreadAddedIndex != null)
        {
            synchronized (this.preparedIndices)
            {
                this.preparedIndices.add(perThreadAddedIndex);
            }
        }
    }

    private void clearTransactionalState(MithraTransaction tx)
    {
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex addedIndex = txStorage == null ? null : txStorage.added;
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

    public PrimaryKeyIndex copy()
    {
        return this.mainIndex.copy();
    }

    public Object markDirty(MithraDataObject object)
    {
        return this.mainIndex.markDirty(object);
    }

    public Object getFromDataEvenIfDirty(Object data, NonNullMutableBoolean isDirty)
    {
        Object result = null;
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck());
        FullUniqueIndex perThreadAdded = txStorage == null ? null : txStorage.added;
        if (perThreadAdded != null)
        {
            result = perThreadAdded.getFromData(data);
        }
        if (result == null)
        {
            result = this.mainIndex.getFromDataEvenIfDirty(data, isDirty);
        }
        return result;
    }

    public int size()
    {
        int size = this.mainIndex.size();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        TransactionLocalStorage txStorage = (TransactionLocalStorage) perTransactionStorage.get(tx);
        FullUniqueIndex threadAddedIndex = txStorage == null ? null : txStorage.added;
        if (threadAddedIndex != null)
        {
            size += threadAddedIndex.size();
        }
        FullUniqueIndex threadDeletedIndex = txStorage == null ? null : txStorage.deleted;
        if (threadDeletedIndex != null)
        {
            size -= threadDeletedIndex.size();
        }
        return size;
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
            return !deletedIndex.contains(nonTransactionalUnderlyingObjectGetter.getUnderlyingObject(o)) && procedure.execute(o);
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

    public boolean evictCollectedReferences()
    {
        PrimaryKeyIndex index = this.mainIndex;
        if (index != null)
        {
            return index.evictCollectedReferences();
        }
        return false;
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        PrimaryKeyIndex index = this.mainIndex;
        if (index != null)
        {
            return index.needToEvictCollectedReferences();
        }
        return false;
    }

    private static class TransactionLocalStorage
    {
        private FullUniqueIndex added;
        private FullUniqueIndex deleted;
    }

    @Override
    public void destroy()
    {
        mainIndex.destroy();
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
