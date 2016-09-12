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

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter2;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;


public class LazyIndex implements Index, TransactionalIndex
{
    private final Index index;

    public LazyIndex(Index index)
    {
        this.index = index;
    }

    @Override
    public boolean isInitialized()
    {
        return false;
    }

    public Index getInitialized(IterableIndex iterableIndex)
    {
        LazyIndex.this.index.ensureExtraCapacity(iterableIndex.size());

        iterableIndex.forAll(new DoUntilProcedure()
        {
            @Override
            public boolean execute(Object o)
            {
                LazyIndex.this.index.put(o);
                return false;
            }
        });
        return this.index;
    }

    @Override
    public void destroy()
    {
        //nothing to do
    }

    @Override
    public void reportSpaceUsage(Logger logger, String className)
    {

    }

    @Override
    public void ensureExtraCapacity(int size)
    {

    }

    @Override
    public Object get(Object dataHolder, List extractors)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(Object dataHolder, Extractor[] extractors)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object dataHolder, Extractor[] extractors, Filter2 filter)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(Object indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(byte[] indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(long indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(double indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(boolean indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(float indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(char indexValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDates0, Timestamp asOfDate1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getNulls()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUnique()
    {
        return this.index.isUnique();
    }

    @Override
    public Extractor[] getExtractors()
    {
        return this.index.getExtractors();
    }

    @Override
    public int getAverageReturnSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxReturnSize(int multiplier)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object put(Object businessObject)
    {
        return null;
    }

    @Override
    public Object putUsingUnderlying(Object businessObject, Object underlying)
    {
        return null;
    }

    @Override
    public Object remove(Object businessObject)
    {
        return null;
    }

    @Override
    public Object removeUsingUnderlying(Object underlyingObject)
    {
        return null;
    }

    @Override
    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter)
    {
        this.index.setUnderlyingObjectGetter(underlyingObjectGetter);
    }

    @Override
    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx)
    {
        // do nothing
    }

    @Override
    public void commit(MithraTransaction tx)
    {
        //do nothing
    }

    @Override
    public Object putIgnoringTransaction(Object object, Object newData, boolean weak)
    {
        return null;
    }

    @Override
    public Object preparePut(Object object)
    {
        return null;
    }

    @Override
    public void commitPreparedForIndex(Object index)
    {
        //do nothing
    }

    @Override
    public Object removeIgnoringTransaction(Object object)
    {
        return null;
    }

    @Override
    public Object getFromPreparedUsingData(Object data)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareForCommit(MithraTransaction tx)
    {
        //do nothing
    }

    @Override
    public void rollback(MithraTransaction tx)
    {
        //do nothing
    }

    @Override
    public boolean prepareForReindex(Object businessObject, MithraTransaction tx)
    {
        return false;
    }

    @Override
    public void finishForReindex(Object businessObject, MithraTransaction tx)
    {
        //do nothing
    }

    @Override
    public void clear()
    {
        //do nothing
    }

    @Override
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
        return 0;
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return 0;
    }
}