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


import com.gs.fw.common.mithra.extractor.DateExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import com.gs.fw.common.mithra.util.TimestampPool;
import sun.misc.Unsafe;

import java.util.Date;

public class OffHeapDateExtractorWithOffset implements OffHeapDateExtractor
{
    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
    private final int fieldOffset;
    private final int nullBitsOffset;
    private final int nullBitsPosition;

    public OffHeapDateExtractorWithOffset(int fieldOffset, int nullBitsOffset, int nullBitsPosition)
    {
        this.fieldOffset = fieldOffset;
        this.nullBitsOffset = nullBitsOffset;
        this.nullBitsPosition = nullBitsPosition;
    }

    @Override
    public Date dateValueOf(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return TimestampPool.getInstance().getTimestampFromOffHeapTime(dataStorage.getLong(dataOffset, fieldOffset));
    }

    @Override
    public boolean isAttributeNull(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return nullBitsOffset >= 0 && (dataStorage.getInt(dataOffset, nullBitsOffset) & (1 << nullBitsPosition)) != 0;
    }

    @Override
    public int computeHashFromValue(Object key)
    {
        if (key == null)
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(((Date)key).getTime());
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, long otherDataAddress)
    {
        if (isAttributeNull(dataStorage, dataOffset))
        {
            return (UNSAFE.getInt(otherDataAddress + nullBitsOffset) & (1 << nullBitsPosition)) != 0;
        }
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
        if (key == null)
        {
            return isAttributeNull(dataStorage, dataOffset);
        }
        Date timestamp = (Date) key;
        return timestamp.getTime() == dataStorage.getLong(dataOffset, fieldOffset);
    }

    @Override
    public int computeHashFromOnHeapExtractor(Object valueHolder, Extractor onHeapExtractor)
    {
        if (onHeapExtractor.isAttributeNull(valueHolder))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(((DateExtractor)onHeapExtractor).dateValueOfAsLong(valueHolder));
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, Extractor extractor)
    {
        return valueEquals(dataStorage, dataOffset, extractor.valueOf(valueHolder));
    }

    @Override
    public int computeHash(OffHeapDataStorage dataStorage, int dataOffset)
    {
        if (isAttributeNull(dataStorage, dataOffset))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(dataStorage.getLong(dataOffset, fieldOffset));
    }
}
