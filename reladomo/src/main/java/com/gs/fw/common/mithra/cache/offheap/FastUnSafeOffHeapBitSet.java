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


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FastUnSafeOffHeapBitSet extends OffHeapMemoryReference
{
    private int pages;

    public FastUnSafeOffHeapBitSet(int totalPages)
    {
        super(totalPages << (FastUnsafeOffHeapDataStorage.PAGE_POWER_OF_TWO - 3));
        this.pages = totalPages;
    }

    public void set(int pageIndex, int bitIndex)
    {
        long address = computeAddress(pageIndex, bitIndex);
        int cur = UNSAFE.getInt(address);
        int bitPosition = bitIndex & 31;
        cur |= (1 << bitPosition);
        UNSAFE.putInt(address, cur);
    }

    public boolean get(int pageIndex, int bitIndex)
    {
        long address = computeAddress(pageIndex, bitIndex);
        int bitPosition = bitIndex & 31;
        return (UNSAFE.getInt(address) & (1 << bitPosition)) != 0;
    }

    public boolean get(int totalIndex)
    {
        long address = this.getBaseAddress() + intByteAddress(totalIndex);
        int bitPosition = totalIndex & 31;
        return (UNSAFE.getInt(address) & (1 << bitPosition)) != 0;
    }

    private int intByteAddress(int totalIndex)
    {
        return (totalIndex >> 5) << 2;
    }

    private long computeAddress(int pageIndex, int bitIndex)
    {
        return this.getBaseAddress() + (pageIndex << (FastUnsafeOffHeapDataStorage.PAGE_POWER_OF_TWO - 3)) + intByteAddress(bitIndex);
    }

    public void clearPage(int pageIndex)
    {
        UNSAFE.setMemory(computeAddress(pageIndex, 0), 1 << (FastUnsafeOffHeapDataStorage.PAGE_POWER_OF_TWO - 3), (byte) 0);
    }

    public void serializePage(ObjectOutput out, int pageIndex) throws IOException
    {
        long start = computeAddress(pageIndex, 0);
        long end = computeAddress(pageIndex + 1, 0);
        for(long index = start; index < end; index++)
        {
            out.writeByte(UNSAFE.getByte(index));
        }
    }

    public void deserializePage(ObjectInput in, int pageIndex) throws IOException
    {
        long start = computeAddress(pageIndex, 0);
        long end = computeAddress(pageIndex + 1, 0);
        assert start >= this.getBaseAddress();
        assert end <= this.getBaseAddress() + this.getAllocatedLength();
        for(long index = start; index < end; index++)
        {
            UNSAFE.putByte(index, in.readByte());
        }
    }
}
