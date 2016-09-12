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
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.Filter2;

import java.sql.Timestamp;
import java.util.List;


public class DatedDataIndex extends DatedIndexWrapper implements IterableNonUniqueIndex
{

    private SemiUniqueDatedIndex semiUniqueDatedIndex;

    public DatedDataIndex(SemiUniqueDatedIndex semiUniqueDatedIndex)
    {
        this.semiUniqueDatedIndex = semiUniqueDatedIndex;
    }

    public Object removeUsingUnderlying(Object underlyingObject)
    {
        return this.semiUniqueDatedIndex.removeUsingUnderlying(underlyingObject);
    }

    public Extractor[] getExtractors()
    {
        return semiUniqueDatedIndex.getExtractors();
    }

    public Object get(Object dataHolder, List extractors)
    {
        return semiUniqueDatedIndex.get(dataHolder, extractors);
    }

    public boolean contains(Object dataHolder, Extractor[] extractors, Filter2 filter)
    {
        return semiUniqueDatedIndex.contains(dataHolder, extractors, filter);
    }

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDates0, Timestamp asOfDate1)
    {
        throw new RuntimeException("not implemented. should not get here");
    }

    public Object get(Object dataHolder, Extractor[] extractors) // for multi attribute indicies
    {
        return semiUniqueDatedIndex.get(dataHolder, extractors);
    }

    @Override
    public void findAndExecute(Object dataHolder, Extractor[] extractors, DoUntilProcedure procedure)
    {
        Object dataObject = this.semiUniqueDatedIndex.get(dataHolder, extractors);
        if (dataObject != null)
        {
            procedure.execute(dataObject);
        }
    }

    public void clear()
    {
        this.semiUniqueDatedIndex.clear();
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
        return 0;
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        return 0;
    }

}
