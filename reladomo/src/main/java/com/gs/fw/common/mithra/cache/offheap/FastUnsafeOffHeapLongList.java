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


import com.gs.fw.common.mithra.util.MithraUnsafe;
import sun.misc.Unsafe;

public class FastUnsafeOffHeapLongList extends OffHeapMemoryReference
{
    private volatile int length;
    private int size;

    public FastUnsafeOffHeapLongList(int initialSize)
    {
        super(convertToBytes(initialSize));
        this.length = initialSize;
    }

    private static long convertToBytes(long sizeInLongs)
    {
        return sizeInLongs << 3;
    }

    public FastUnsafeOffHeapLongList(int initialSize, boolean registerForGc)
    {
        this(initialSize);
        if (registerForGc)
        {
            this.registerForGarbageCollection();
        }
    }

    public int size()
    {
        return size;
    }

    public long get(int arrayPosition)
    {
        if (arrayPosition > size)
        {
            throw new ArrayIndexOutOfBoundsException("index too large "+arrayPosition);
        }
        return UNSAFE.getLong(computeAddress(arrayPosition));
    }

    public void set(int arrayPosition, long value)
    {
        if (arrayPosition >= length)
        {
            resize(arrayPosition + 1);
        }
        UNSAFE.putLong(computeAddress(arrayPosition), value);
        if (size <= arrayPosition)
        {
            size = arrayPosition + 1;
        }
    }

    private void resize(int minNewSize)
    {
        long newSize = this.length * 3/2;
        if (newSize < minNewSize) newSize = minNewSize;
        assert newSize <= Integer.MAX_VALUE;
        reallocate(convertToBytes(newSize));
        this.length = (int) newSize;
    }

    private long computeAddress(long arrayPosition)
    {
        assert arrayPosition >= 0;
        assert arrayPosition < length;
        return this.getBaseAddress() + convertToBytes(arrayPosition);
    }

    public void clear()
    {
        UNSAFE.setMemory(getBaseAddress(), convertToBytes(size), (byte) 0);
    }

    public void add(long newValue)
    {
        this.set(size, newValue);
    }

    public void clearAndCopy(FastUnsafeOffHeapLongList copy)
    {
        if (this.length < copy.size())
        {
            this.resize(copy.size());
        }
        long copyLengthInBytes = convertToBytes(copy.size());
        UNSAFE.copyMemory(copy.getBaseAddress(), this.getBaseAddress(), copyLengthInBytes);
        UNSAFE.setMemory(this.getBaseAddress() + copyLengthInBytes, this.getAllocatedLength() - copyLengthInBytes, (byte) 0);
        this.size = copy.size();
    }

    public void addAll(FastUnsafeOffHeapLongList other)
    {
        if (other.size() == 0)
        {
            return;
        }
        if (this.length < this.size + other.size())
        {
            this.resize(this.size + other.size());
        }
        UNSAFE.copyMemory(other.getBaseAddress(), this.getBaseAddress() + convertToBytes(this.size), convertToBytes(other.size()));
        this.size += other.size();
    }
}
