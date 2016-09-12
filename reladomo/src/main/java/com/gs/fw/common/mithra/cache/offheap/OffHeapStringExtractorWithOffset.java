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


import com.gs.fw.common.mithra.cache.StringIndex;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import com.gs.fw.common.mithra.util.StringPool;
import sun.misc.Unsafe;

public class OffHeapStringExtractorWithOffset implements OffHeapStringExtractor
{
    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
    private final int fieldOffset;

    public OffHeapStringExtractorWithOffset(int fieldOffset)
    {
        this.fieldOffset = fieldOffset;
    }

    @Override
    public String stringValueOf(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return StringPool.getInstance().getStringFromOffHeapAddress(dataStorage.getInt(dataOffset, fieldOffset));
    }

    @Override
    public void convertMasterStringToLocalString(long dataAddress, MasterCacheUplink uplink)
    {
        int masterStringRef = UNSAFE.getInt(dataAddress + this.fieldOffset);
        if (masterStringRef != StringIndex.NULL_STRING)
        {
            int convertedStringRef = uplink.mapMasterStringRefToLocalRef(masterStringRef);
            assert convertedStringRef != StringIndex.NULL_STRING;
            UNSAFE.putInt(dataAddress + this.fieldOffset, convertedStringRef);
        }
    }

    @Override
    public boolean isAttributeNull(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return dataStorage.getInt(dataOffset, fieldOffset) == 0;
    }

    @Override
    public int computeHashFromValue(Object key)
    {
        return HashUtil.offHeapHash((String)key);
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, long otherDataAddress)
    {
        return dataStorage.getInt(dataOffset, fieldOffset) == UNSAFE.getInt(otherDataAddress + fieldOffset);
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, int secondOffset)
    {
        return dataStorage.getInt(dataOffset, fieldOffset) == dataStorage.getInt(secondOffset, fieldOffset);
    }

    @Override
    public boolean valueEquals(OffHeapDataStorage dataStorage, int dataOffset, Object key)
    {
        if (key == null)
        {
            return isAttributeNull(dataStorage, dataOffset);
        }
        int keyAddress = StringPool.getInstance().getOffHeapAddressWithoutAdding((String) key);
        if (keyAddress == 0) return false;
        int address = dataStorage.getInt(dataOffset, fieldOffset);
        return keyAddress == address;
    }

    @Override
    public int computeHashFromOnHeapExtractor(Object valueHolder, Extractor onHeapExtractor)
    {
        return HashUtil.offHeapHash(((StringExtractor)onHeapExtractor).offHeapValueOf(valueHolder));
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, Extractor extractor)
    {
        int address = dataStorage.getInt(dataOffset, fieldOffset);
        int keyAddress = ((StringExtractor)extractor).offHeapValueOf(valueHolder);
        return keyAddress == address;
    }

    @Override
    public int computeHash(OffHeapDataStorage dataStorage, int dataOffset)
    {
        int address = dataStorage.getInt(dataOffset, fieldOffset);
        return HashUtil.offHeapHash(address);
    }
}
