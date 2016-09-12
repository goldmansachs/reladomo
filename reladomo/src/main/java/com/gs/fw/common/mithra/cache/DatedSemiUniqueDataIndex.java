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

import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.Filter2;

import java.sql.Timestamp;
import java.util.List;


public class DatedSemiUniqueDataIndex extends DatedIndexWrapper implements TransactionalIndex
{

    private SemiUniqueDatedIndex semiUniqueDatedIndex;

    public DatedSemiUniqueDataIndex(SemiUniqueDatedIndex semiUniqueDatedIndex)
    {
        this.semiUniqueDatedIndex = semiUniqueDatedIndex;
    }

    public Extractor[] getExtractors()
    {
        return semiUniqueDatedIndex.getNonDatedExtractors();
    }

    public Object get(Object dataHolder, List extractors)
    {
        return semiUniqueDatedIndex.getFromSemiUnique(dataHolder, extractors);
    }

    public Object get(Object dataHolder, Extractor[] extractors) // for multi attribute indicies
    {
        return semiUniqueDatedIndex.getFromSemiUnique(dataHolder, extractors);
    }

    @Override
    public boolean contains(Object dataHolder, Extractor[] extractors, Filter2 filter)
    {
        return semiUniqueDatedIndex.containsInSemiUnique(dataHolder, extractors, filter);
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDates0, Timestamp asOfDate1)
    {
        throw new RuntimeException("not implemented. should not get here");
    }

    public Object removeUsingUnderlying(Object underlyingObject)
    {
        return this.semiUniqueDatedIndex.removeUsingUnderlying(underlyingObject);
    }

    @Override
    public Object put(Object businessObject)
    {
        return semiUniqueDatedIndex.putSemiUnique(businessObject);
    }

    public void clear()
    {
        this.semiUniqueDatedIndex.clear();
    }

    @Override
    public Object remove(Object businessObject)
    {
        return this.semiUniqueDatedIndex.remove(businessObject);
    }

    public Object putIgnoringTransaction(Object object, Object newData, boolean weak)
    {
        return ((TransactionalIndex)this.semiUniqueDatedIndex).putIgnoringTransaction(object, newData, weak);
    }

    public Object preparePut(Object object)
    {
        return ((TransactionalIndex)this.semiUniqueDatedIndex).preparePut(object);
    }

    public void commitPreparedForIndex(Object index)
    {
        ((TransactionalIndex)this.semiUniqueDatedIndex).commitPreparedForIndex(index);
    }

    public Object removeIgnoringTransaction(Object object)
    {
        return ((TransactionalIndex)this.semiUniqueDatedIndex).removeIgnoringTransaction(object);
    }

    public Object getFromPreparedUsingData(Object data)
    {
        return ((TransactionalIndex)this.semiUniqueDatedIndex).getFromPreparedUsingData(data);
    }

    public void prepareForCommit(MithraTransaction tx)
    {
        ((TransactionalIndex)this.semiUniqueDatedIndex).prepareForCommit(tx);
    }

    public void commit(MithraTransaction tx)
    {
        ((TransactionalIndex)this.semiUniqueDatedIndex).commit(tx);
    }

    public void rollback(MithraTransaction tx)
    {
        ((TransactionalIndex)this.semiUniqueDatedIndex).rollback(tx);
    }

    public Object getSemiUniqueAsOneWithDates(Object valueHolder, Extractor[] extractors, Timestamp[] dates, int nonDatedHash)
    {
        return semiUniqueDatedIndex.getSemiUniqueAsOneWithDates(valueHolder, extractors, dates, nonDatedHash);
    }

    public boolean evictCollectedReferences()
    {
        return this.semiUniqueDatedIndex.evictCollectedReferences();
    }

    @Override
    public boolean needToEvictCollectedReferences()
    {
        return this.semiUniqueDatedIndex.needToEvictCollectedReferences();
    }

    @Override
    public void finishForReindex(Object businessObject, MithraTransaction tx)
    {
        ((TransactionalIndex)this.semiUniqueDatedIndex).finishForReindex(businessObject, tx);
    }

    @Override
    public boolean prepareForReindex(Object businessObject, MithraTransaction tx)
    {
        return ((TransactionalIndex)this.semiUniqueDatedIndex).prepareForReindex(businessObject, tx);
    }

    @Override
    public void prepareForReindexInTransaction(Object businessObject, MithraTransaction tx)
    {
        ((TransactionalIndex)this.semiUniqueDatedIndex).prepareForReindexInTransaction(businessObject, tx);
    }

    public CommonExtractorBasedHashingStrategy getNonDatedPkHashStrategy()
    {
        return this.semiUniqueDatedIndex.getNonDatedPkHashStrategy();
    }

    public Object getSemiUniqueAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, Timestamp asOfDate0, Timestamp asOfDate1)
    {
        return semiUniqueDatedIndex.getSemiUniqueAsOne(srcObject, srcData, relationshipHashStrategy, nonDatedHash, asOfDate0, asOfDate1);
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        return semiUniqueDatedIndex.getOffHeapAllocatedIndexSize();
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return semiUniqueDatedIndex.getOffHeapUsedIndexSize();
    }
}
