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

import java.sql.Timestamp;
import java.util.List;

import com.gs.fw.common.mithra.util.DoProcedure;

import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.RelationshipHashStrategy;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.Filter2;
import org.slf4j.Logger;



public interface SemiUniqueDatedIndex extends IterableIndex
{
    public Object get(Object dataHolder, List extractors); // for multi attribute indicies

    public Object get(Object dataHolder, Extractor[] extractors); // for multi attribute indicies

    public Extractor[] getExtractors();

    public Object remove(Object businessObject);

    public Object removeUsingUnderlying(Object businessObject);

    public void clear();

    // from PK index

    public Object getFromData(Object data, int nonDatedHashCode);

    public PrimaryKeyIndex copy();

    public int size();

    // new to differentiate
    public Object getFromSemiUnique(Object dataHolder, List extractors);

    public Object getFromSemiUnique(Object dataHolder, Extractor[] extractors);

    public Extractor[] getNonDatedExtractors();

    public Object removeOldEntry(Object data, Timestamp[] asOfDates);

    public List removeOldEntryForRange(Object data);

    public Object getSemiUniqueFromData(Object data, Timestamp[] asOfDates);

    public List getFromDataForAllDatesAsList(Object data);

    public boolean addSemiUniqueToContainer(Object data, TemporalContainer container);

    public Object getSemiUniqueAsOneWithDates(Object valueHolder, Extractor[] extractors, Timestamp[] dates, int nonDatedHash);

    public Object getSemiUniqueAsOne(Object srcObject, Object srcData, RelationshipHashStrategy relationshipHashStrategy, int nonDatedHash, Timestamp asOfDate0, Timestamp asOfDate1);

    public int getSemiUniqueSize();

    public boolean removeAllIgnoringDate(Object data, DoProcedure procedure);

    public Object put(Object key, int hashCode);

    public Object putSemiUnique(Object key);

    public List removeAll(Filter filter);

    public boolean evictCollectedReferences();

    public boolean needToEvictCollectedReferences();

    public CommonExtractorBasedHashingStrategy getNonDatedPkHashStrategy();

    public void forAllInParallel(ParallelProcedure procedure);

    public void ensureExtraCapacity(int extraCapacity);

    public List<Object> collectMilestoningOverlaps();

    public void destroy();

    public void reportSpaceUsage(Logger logger, String className);

    public long getOffHeapAllocatedIndexSize();

    public long getOffHeapUsedIndexSize();

    boolean contains(Object dataHolder, Extractor[] extractors, Filter2 filter);

    public boolean containsInSemiUnique(Object dataHolder, Extractor[] extractors, Filter2 filter);
}
