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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OffHeapableExtractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.TimestampPool;

import java.sql.Timestamp;
import java.util.List;

public class TwoOffHeapTimestampHashStrategy extends ExtractorBasedOffHeapHashStrategy
{
    private TimestampExtractor firstOnHeapExtractor;
    private TimestampExtractor secondOnHeapExtractor;
    private final int firstFieldOffset;
    private final int secondFieldOffset;

    public TwoOffHeapTimestampHashStrategy(Extractor[] onHeapExtractors)
    {
        firstFieldOffset = ((OffHeapTimestampExtractorWithOffset)((OffHeapableExtractor)onHeapExtractors[0]).zCreateOffHeapExtractor()).getFieldOffset();
        secondFieldOffset = ((OffHeapTimestampExtractorWithOffset)((OffHeapableExtractor)onHeapExtractors[1]).zCreateOffHeapExtractor()).getFieldOffset();
        firstOnHeapExtractor = (TimestampExtractor) onHeapExtractors[0];
        secondOnHeapExtractor = (TimestampExtractor) onHeapExtractors[1];
    }

    @Override
    public int computeHashCode(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return HashUtil.combineHashes(hashTimestampAsLong(dataStorage.getLong(dataOffset, firstFieldOffset)),
                hashTimestampAsLong(dataStorage.getLong(dataOffset, secondFieldOffset)));
    }

    private int hashTimestampAsLong(long one)
    {
        return one == TimestampPool.OFF_HEAP_NULL ? HashUtil.NULL_HASH : HashUtil.hash(one);
    }

    @Override
    public int computeHashCode(Object valueHolder, List extractors)
    {
        Extractor first = (Extractor) extractors.get(0);
        Extractor second = (Extractor) extractors.get(1);
        return HashUtil.combineHashes(first.valueHashCode(valueHolder), second.valueHashCode(valueHolder));
    }

    @Override
    public int computeHashCode(Object valueHolder, Extractor[] extractors)
    {
        return HashUtil.combineHashes(extractors[0].valueHashCode(valueHolder), extractors[1].valueHashCode(valueHolder));
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, List extractors)
    {
        return extractEquals(dataStorage, dataOffset, valueHolder, firstFieldOffset, (Extractor) extractors.get(0))
                && extractEquals(dataStorage, dataOffset, valueHolder, secondFieldOffset, (Extractor) extractors.get(1));
    }

    private boolean extractEquals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, int offset, Extractor extractorOne)
    {
        long time = dataStorage.getLong(dataOffset, offset);
        Timestamp timestamp = (Timestamp) extractorOne.valueOf(valueHolder);
        return timestamp == null ? time == TimestampPool.OFF_HEAP_NULL : timestamp.getTime() == time && (timestamp.getNanos() % 1000000) == 0;
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, Extractor[] extractors)
    {
        return extractEquals(dataStorage, dataOffset, valueHolder, firstFieldOffset, extractors[0])
                && extractEquals(dataStorage, dataOffset, valueHolder, secondFieldOffset, extractors[1]);
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder)
    {
        return extractEquals(dataStorage, dataOffset, valueHolder, firstFieldOffset, firstOnHeapExtractor)
                && extractEquals(dataStorage, dataOffset, valueHolder, secondFieldOffset, secondOnHeapExtractor);

    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int offsetOne, int offsetTwo)
    {
        return dataStorage.getLong(offsetOne, firstFieldOffset) == dataStorage.getLong(offsetTwo, firstFieldOffset) &&
                dataStorage.getLong(offsetOne, secondFieldOffset) == dataStorage.getLong(offsetTwo, secondFieldOffset);
    }

    @Override
    public int computeHashCode(Object object)
    {
        return HashUtil.combineHashes(firstOnHeapExtractor.valueHashCode(object),
                secondOnHeapExtractor.valueHashCode(object));
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return firstOnHeapExtractor.valueEquals(o1, o2) && secondOnHeapExtractor.valueEquals(o1, o2);
    }

    public int computeCombinedHashCode(OffHeapDataStorage dataStorage, int data, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, hashTimestampAsLong(dataStorage.getLong(data, firstFieldOffset)));
        h = HashUtil.combineHashes(h, hashTimestampAsLong(dataStorage.getLong(data, secondFieldOffset)));
        return h;
    }

    public int computeOnHeapCombinedHashCode(Object data, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, firstOnHeapExtractor.valueHashCode(data));
        h = HashUtil.combineHashes(h, secondOnHeapExtractor.valueHashCode(data));
        return h;
    }
}
