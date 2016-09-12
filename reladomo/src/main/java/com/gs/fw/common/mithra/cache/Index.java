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
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.List;



public interface Index
{

    public Object get(Object dataHolder, List extractors); // for multi attribute indicies

    public Object get(Object dataHolder, Extractor[] extractors); // for multi attribute indicies

    public boolean contains(Object dataHolder, Extractor[] extractors, Filter2 filter);

    public Object get(Object indexValue);

    public Object get(byte[] indexValue);

    // for int indicies:
    public Object get(int indexValue);

    public Object get(long indexValue);

    public Object get(double indexValue);

    public Object get(boolean indexValue);

    public Object get(float indexValue);

    public Object get(char indexValue);

    public Object getNulls();

    public boolean isUnique();

    public Extractor[] getExtractors();

    /**
     * this is calculated as total entries/total unique keys
     * @return the average expected return size
     */
    public int getAverageReturnSize();

    public long getMaxReturnSize(int multiplier);

    public Object put(Object businessObject);

    public Object putUsingUnderlying(Object businessObject, Object underlying);

    public Object remove(Object businessObject);

    public Object removeUsingUnderlying(Object underlyingObject);

    public void setUnderlyingObjectGetter(UnderlyingObjectGetter underlyingObjectGetter);

    public void clear();

    public boolean evictCollectedReferences();

    public boolean needToEvictCollectedReferences();

    public Object get(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, Timestamp asOfDates0, Timestamp asOfDate1);

    public boolean isInitialized();

    public Index getInitialized(IterableIndex iterableIndex);

    public void destroy();

    public void reportSpaceUsage(Logger logger, String className);

    public void ensureExtraCapacity(int size);

    public long getOffHeapAllocatedIndexSize();

    public long getOffHeapUsedIndexSize();
}
