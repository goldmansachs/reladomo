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


import com.gs.fw.common.mithra.extractor.BooleanExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import sun.misc.Unsafe;

public class OffHeapBooleanExtractorWithOffset implements OffHeapBooleanExtractor
{
    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
    private final int fieldOffset;

    public OffHeapBooleanExtractorWithOffset(int fieldOffset)
    {
        this.fieldOffset = fieldOffset;
    }

    @Override
    public boolean isAttributeNull(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return dataStorage.isBooleanNull(dataOffset, fieldOffset);
    }

    public boolean booleanValueOf(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return dataStorage.getBoolean(dataOffset, fieldOffset);
    }

    @Override
    public int computeHashFromValue(Object key)
    {
        if (key == null)
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(((Boolean) key).booleanValue());
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, long otherDataAddress)
    {
        return dataStorage.getByte(dataOffset, fieldOffset) == UNSAFE.getByte(otherDataAddress + fieldOffset);
    }

    @Override
    public int computeHash(OffHeapDataStorage dataStorage, int dataOffset)
    {
        byte b = dataStorage.getByte(dataOffset, fieldOffset);
        if (b == 2)
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(b == 1);
    }

    @Override
    public int computeHashFromOnHeapExtractor(Object valueHolder, Extractor onHeapExtractor)
    {
        return onHeapExtractor.valueHashCode(valueHolder);
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, Extractor onHeapExtractor)
    {
        BooleanExtractor booleanExtractor = (BooleanExtractor) onHeapExtractor;
        if (onHeapExtractor.isAttributeNull(valueHolder))
        {
            return dataStorage.getByte(dataOffset, fieldOffset) == 2;
        }
        return booleanExtractor.booleanValueOf(valueHolder) == booleanValueOf(dataStorage, dataOffset);

    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, int secondOffset)
    {
        return booleanValueOf(dataStorage, dataOffset) == booleanValueOf(dataStorage, secondOffset);
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, Object key)
    {
        if (key == null)
        {
            return dataStorage.getByte(dataOffset, fieldOffset) == 2;
        }
        return ((Boolean)key).booleanValue() == booleanValueOf(dataStorage, dataOffset);
    }
}
