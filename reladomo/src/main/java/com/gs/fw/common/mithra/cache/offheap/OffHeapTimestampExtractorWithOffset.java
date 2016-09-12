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
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import com.gs.fw.common.mithra.util.TimestampPool;
import sun.misc.Unsafe;

import java.sql.Timestamp;

public class OffHeapTimestampExtractorWithOffset implements OffHeapTimestampExtractor
{
    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
    private final int fieldOffset;

    public OffHeapTimestampExtractorWithOffset(int fieldOffset)
    {
        this.fieldOffset = fieldOffset;
    }

    @Override
    public Timestamp timestampValueOf(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return TimestampPool.getInstance().getTimestampFromOffHeapTime(dataStorage.getLong(dataOffset, fieldOffset));
    }

    @Override
    public boolean isAttributeNull(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return dataStorage.getLong(dataOffset, fieldOffset) == TimestampPool.OFF_HEAP_NULL;
    }

    @Override
    public int computeHashFromValue(Object key)
    {
        if (key == null)
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(((Timestamp)key).getTime());
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, long otherDataAddress)
    {
        return dataStorage.getLong(dataOffset, fieldOffset) == UNSAFE.getLong(otherDataAddress + fieldOffset);
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, int secondOffset)
    {
        return dataStorage.getLong(dataOffset, fieldOffset) == dataStorage.getLong(secondOffset, fieldOffset);
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, Object key)
    {
        long time = dataStorage.getLong(dataOffset, fieldOffset);
        if (key == null)
        {
            return time == TimestampPool.OFF_HEAP_NULL;
        }
        Timestamp timestamp = (Timestamp) key;
        return timestamp.getTime() == time && timestamp.getNanos() % 1000000 == 0;
    }

    @Override
    public int computeHashFromOnHeapExtractor(Object valueHolder, Extractor onHeapExtractor)
    {
        if (onHeapExtractor.isAttributeNull(valueHolder))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(((TimestampExtractor)onHeapExtractor).timestampValueOfAsLong(valueHolder));
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, Extractor extractor)
    {
        return valueEquals(dataStorage, dataOffset, extractor.valueOf(valueHolder));
    }

    @Override
    public int computeHash(OffHeapDataStorage dataStorage, int dataOffset)
    {
        long time = dataStorage.getLong(dataOffset, fieldOffset);
        if (time == TimestampPool.OFF_HEAP_NULL)
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(time);
    }

    public int getFieldOffset()
    {
        return fieldOffset;
    }
}
