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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.cache.offheap;


import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.IntIterator;

public class FastUnsafeOffHeapIntList extends OffHeapMemoryReference
{
    private static volatile int fence = 0;
    private volatile int length;
    private int size;

    public FastUnsafeOffHeapIntList(int initialSize)
    {
        super(convertToBytes(initialSize));
        this.length = initialSize;
    }

    private static long convertToBytes(long sizeInInts)
    {
        return sizeInInts << 2;
    }

    public FastUnsafeOffHeapIntList(FastUnsafeOffHeapIntList toCopy)
    {
        super(convertToBytes(toCopy.size()));
        this.length = toCopy.size();
        UNSAFE.copyMemory(toCopy.getBaseAddress(), this.getBaseAddress(), convertToBytes(toCopy.size()));
    }

    public FastUnsafeOffHeapIntList(int initialSize, boolean registerForGc)
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

    public int get(int arrayPosition)
    {
        if (arrayPosition > size)
        {
            throw new ArrayIndexOutOfBoundsException("index too large "+arrayPosition);
        }
        return fastGet(arrayPosition);
    }

    private int fastGet(int arrayPosition)
    {
        return UNSAFE.getInt(computeAddress(arrayPosition));
    }

    public void set(int arrayPosition, int value)
    {
        if (arrayPosition >= length)
        {
            resize(arrayPosition + 1);
        }
        fastSet(arrayPosition, value);
        if (size <= arrayPosition)
        {
            size = arrayPosition + 1;
        }
    }

    public void setWithFence(int arrayPosition, int value)
    {
        if (arrayPosition >= length)
        {
            resize(arrayPosition + 1);
        }
        fastSet(arrayPosition, value);
        fence++;
        if (size <= arrayPosition)
        {
            size = arrayPosition + 1;
        }
    }

    private void fastSet(int arrayPosition, int value)
    {
        UNSAFE.putInt(computeAddress(arrayPosition), value);
    }

    private void resize(int minNewSize)
    {
        int newSize = this.length * 3/2;
        if (newSize < minNewSize) newSize = minNewSize;
        reallocate(convertToBytes(newSize));
        this.length = newSize;
    }

    private long computeAddress(long arrayPosition)
    {
        assert arrayPosition < length;
        return this.getBaseAddress() + convertToBytes(arrayPosition);
    }

    public void add(int newValue)
    {
        this.set(size, newValue);
    }

    public void clear()
    {
        UNSAFE.setMemory(this.getBaseAddress(), convertToBytes(this.size), (byte) 0);
        this.size = 0;
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public void addAll(com.gs.collections.api.IntIterable intIterable)
    {
        for(com.gs.collections.api.iterator.IntIterator it = intIterable.intIterator(); it.hasNext();)
        {
            this.add(it.next());
        }
    }

    public void addAll(IntIterable intIterable)
    {
        for(IntIterator it = intIterable.intIterator(); it.hasNext();)
        {
            this.add(it.next());
        }
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer(this.size()*4);
        buf.append('[');
        for(int i=0;i<this.size;i++)
        {
            buf.append(this.fastGet(i)).append(", ");
        }
        buf.append(']');
        return buf.toString();
    }

    public void sort()
    {
        quickSort(0, size - 1);
    }

    private void quickSort (int low, int high)
    {
        if (low == high) return; // single element list
        if (low + 7 > high)
        {
            insertionSort(low, high);
        }
        else
        {
            int mid = (low + high) / 2;
            if (fastGet(mid) < fastGet(low)) swap(mid, low);
            if (fastGet(high) < fastGet(low)) swap(high, low);
            if (fastGet(high) < fastGet(mid)) swap(high, mid);
            int pivot = fastGet(mid);
            swap(mid, high - 1); // put pivot just before high
            int i = low, j = high;
            while (true)
            {
                while (fastGet(++i) < pivot)
                {
                    // scan right until we find something >= pivot
                }
                while (fastGet(--j) > pivot)
                {
                    // scan left until we find something <= pivot
                }
                if (i < j)
                    swap(i, j);
                else
                    break;
            }
            swap(i, high - 1); // restore pivot
            quickSort(low, i - 1); // sort L sublist
            quickSort(i , high); // sort R sublist
        }
    }

    private void insertionSort(int low, int high)
    {
        for (int i=low; i <= high; i++)
        {
   		    for (int j=i; j>low && fastGet(j-1)>fastGet(j); j--)
            {
   		        swap(j, j-1);
            }
        }
    }

    private void swap(int one, int two)
    {
        int v = fastGet(one);
        fastSet(one, fastGet(two));
        fastSet(two, v);
    }

    public void addAll(FastUnsafeOffHeapIntList toCopy)
    {
        if (toCopy.size() == 0)
        {
            return;
        }
        if (this.length < this.size() + toCopy.size())
        {
            this.resize(this.size + toCopy.size());
        }
        UNSAFE.copyMemory(toCopy.getBaseAddress(), this.computeAddress(this.size), convertToBytes(toCopy.size()));
        this.size += toCopy.size();
    }

    public int unusedCapacity()
    {
        return this.length - this.size();
    }
}
