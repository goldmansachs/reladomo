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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;


public class TypedIndex implements Index, PrimaryKeyIndex, TransactionalIndex
{

    private Index index;
    private Class type;
    private Class underlyingType;

    public TypedIndex(Index index, Class type, Class underlyingType)
    {
        this.index = index;
        this.type = type;
        this.underlyingType = underlyingType;
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

    public Object get(Object dataHolder, List extractors)
    {
        return index.get(dataHolder, extractors);
    }

    public Object get(Object dataHolder, Extractor[] extractors) // for multi attribute indicies
    {
        return index.get(dataHolder, extractors);
    }

    @Override
    public boolean contains(Object dataHolder, Extractor[] extractors, Filter2 filter)
    {
        return index.contains(dataHolder, extractors, filter);
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return index.get(srcObject, srcData, relationshipHashStrategy, asOfDate0, asOfDate1);
    }

    public Object get(Object indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(byte[] indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(int indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(long indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(double indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(boolean indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(float indexValue)
    {
        return index.get(indexValue);
    }

    public Object get(char indexValue)
    {
        return index.get(indexValue);
    }

    public Object getNulls()
    {
        return index.getNulls();
    }

    public boolean isUnique()
    {
        return index.isUnique();
    }

    public Extractor[] getExtractors()
    {
        return index.getExtractors();
    }

    public int getAverageReturnSize()
    {
        return index.getAverageReturnSize();
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        return index.getMaxReturnSize(multiplier);
    }

    public Object put(Object businessObject)
    {
        if (type.isAssignableFrom(businessObject.getClass()))
        {
            return index.put(businessObject);
        }
        return null;
    }

    public Object putUsingUnderlying(Object businessObject, Object underlying)
    {
        if (underlyingType.isAssignableFrom(businessObject.getClass()))
        {
            return index.putUsingUnderlying(businessObject, underlying);
        }
        return null;
    }

    public Object putWeakUsingUnderlying(Object businessObject, Object underlying)
    {
        if (underlyingType.isAssignableFrom(businessObject.getClass()))
        {
            return ((PrimaryKeyIndex)index).putWeakUsingUnderlying(businessObject, underlying);
        }
        return null;
    }

    public Object remove(Object businessObject)
    {
        if (type.isAssignableFrom(businessObject.getClass()))
        {
            return index.remove(businessObject);
        }
        return null;
    }

    public Object removeUsingUnderlying(Object underlyingObject)
    {
        if (underlyingType.isAssignableFrom(underlyingObject.getClass()))
        {
            return index.removeUsingUnderlying(underlyingObject);
        }
        return null;
    }

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        index.setUnderlyingObjectGetter(underlyingObjectGetter);
    }

    @Override
    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx)
    {
        if (type.isAssignableFrom(businessObject.getClass()))
        {
            ((TransactionalIndex)index).prepareForReindexInTransaction(businessObject, tx);
        }
    }

    @Override
    public boolean prepareForReindex(Object businessObject, MithraTransaction tx)
    {
        if (type.isAssignableFrom(businessObject.getClass()))
        {
            return ((TransactionalIndex)index).prepareForReindex(businessObject, tx);
        }
        return false;
    }

    @Override
    public void finishForReindex(Object businessObject, MithraTransaction tx)
    {
        ((TransactionalIndex)index).finishForReindex(businessObject, tx);
    }

    public void clear()
    {
        index.clear();
    }

    public Object getFromData(Object data)
    {
        if (underlyingType.isAssignableFrom(data.getClass()))
        {
            return ((PrimaryKeyIndex)index).getFromData(data);
        }
        return null;
    }

    public List getAll()
    {
        return ((PrimaryKeyIndex)index).getAll();
    }

    public boolean forAll(DoUntilProcedure procedure)
    {
        return ((PrimaryKeyIndex)index).forAll(procedure);
    }

    public HashStrategy getHashStrategy()
    {
        return ((PrimaryKeyIndex)index).getHashStrategy();
    }

    public PrimaryKeyIndex copy()
    {
        return ((PrimaryKeyIndex)index).copy();
    }

    public int size()
    {
        return ((PrimaryKeyIndex)index).size();
    }

    public Object markDirty(MithraDataObject object)
    {
        if (underlyingType.isAssignableFrom(object.getClass()))
        {
            return ((PrimaryKeyIndex)index).markDirty(object);
        }
        return null;
    }

    public Object getFromDataEvenIfDirty(Object data, NonNullMutableBoolean isDirty)
    {
        return ((PrimaryKeyIndex)index).getFromDataEvenIfDirty(data, isDirty);
    }

    public Object putWeak(Object object)
    {
        if (type.isAssignableFrom(object.getClass()))
        {
            return ((PrimaryKeyIndex)index).putWeak(object);
        }
        return null;
    }

    public List removeAll(Filter filter)
    {
        return ((PrimaryKeyIndex)index).removeAll(new TypedFilter(filter, this.type, this.underlyingType));
    }

    public boolean sizeRequiresWriteLock()
    {
        return ((PrimaryKeyIndex)index).sizeRequiresWriteLock(); 
    }

    public void ensureCapacity(int capacity)
    {
        if (this.index instanceof PrimaryKeyIndex)
        {
            ((PrimaryKeyIndex)index).ensureCapacity(capacity);
        }
    }

    public void ensureExtraCapacity(int capacity)
    {
        if (this.index instanceof PrimaryKeyIndex)
        {
            ((PrimaryKeyIndex)index).ensureExtraCapacity(capacity);
        }
    }

    public Object putIgnoringTransaction(Object object, Object newData, boolean weak)
    {
        if (type.isAssignableFrom(object.getClass()))
        {
            return ((TransactionalIndex)index).putIgnoringTransaction(object, newData, weak);
        }
        return null;
    }

    public Object preparePut(Object object)
    {
        if (type.isAssignableFrom(object.getClass()))
        {
            return ((TransactionalIndex)index).preparePut(object);
        }
        return null;
    }

    public void commitPreparedForIndex(Object index)
    {
        ((TransactionalIndex)index).commitPreparedForIndex(index);
    }

    public Object removeIgnoringTransaction(Object object)
    {
        if (type.isAssignableFrom(object.getClass()))
        {
            return ((TransactionalIndex)index).removeIgnoringTransaction(object);
        }
        return null;
    }

    public Object getFromPreparedUsingData(Object data)
    {
        if (underlyingType.isAssignableFrom(data.getClass()))
        {
            return ((TransactionalIndex)index).getFromPreparedUsingData(data);
        }
        return null;
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        ((TransactionalIndex)index).prepareForCommit(tx);
    }

    public void commit(MithraTransaction tx)
    {
        ((TransactionalIndex)index).commit(tx);
    }

    public void rollback(MithraTransaction tx)
    {
        ((TransactionalIndex)index).rollback(tx);
    }

    public boolean evictCollectedReferences()
    {
        return this.index.evictCollectedReferences();
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return this.index.needToEvictCollectedReferences();
    }

    @Override
    public void destroy()
    {
        //nothing to do
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {
        this.index.reportSpaceUsage(logger, className);
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return this.index.getOffHeapAllocatedIndexSize();
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return this.index.getOffHeapUsedIndexSize();
    }
}
