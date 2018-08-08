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
import org.slf4j.Logger;
import sun.misc.Unsafe;

public class FastUnsafeOffHeapIntArrayStorage implements OffHeapIntArrayStorage
{
    private static final int MIN_SIZE = 3;
    private static final int MAX_SMALL_SIZE = 16;

    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
//    private static MithraUnsafe.AuditedMemory UNSAFE = MithraUnsafe.getAuditedMemory();
    private static final long MAX_INCREASE_SIZE = (1L << 26);

    private static FastUnsafeOffHeapMemoryInitialiser initialiser = new FastUnsafeOffHeapMemoryInitialiser();

    private int current = MAX_SMALL_SIZE - MIN_SIZE + 1;
    private long baseAddress;
    private long totalAllocated;
    private int totalFreed;
    private int largestFree = -1;
    private boolean destroyed;

    public FastUnsafeOffHeapIntArrayStorage()
    {
        //todo: add this to a weak list somewhere and free it when collected
        this.totalAllocated = 1 << 11; // 2 K
        baseAddress = UNSAFE.allocateMemory(totalAllocated);
        unsafeZeroMemory(this.baseAddress, this.totalAllocated);

    }

    private void unsafeZeroMemory(long address, long sizeInBytes)
    {
        initialiser.unsafeZeroMemory(address, sizeInBytes, baseAddress, totalAllocated);
    }

    private void unsafePutInt(long address, int value)
    {
        assert address >= baseAddress;
        assert address < baseAddress + totalAllocated;
        UNSAFE.putInt(address, value);
    }

    private int unsafeGetInt(long address)
    {
        assert address >= baseAddress;
        assert address < baseAddress + totalAllocated;
        return UNSAFE.getInt(address);
    }

    private void unsafeCopyMemory(long fromAddress, long toAddress, int sizeInBytes)
    {
        assert fromAddress >= baseAddress;
        assert fromAddress < baseAddress + totalAllocated;
        assert toAddress >= baseAddress;
        assert toAddress < baseAddress + totalAllocated;
        assert sizeInBytes < totalAllocated - (fromAddress - baseAddress);
        assert sizeInBytes < totalAllocated - (toAddress - baseAddress);

        UNSAFE.copyMemory(fromAddress, toAddress, sizeInBytes);
    }

    @Override
    public int allocate(int size)
    {
        assert size > 0;
        if (size <= MAX_SMALL_SIZE)
        {
            if (size < MIN_SIZE) size = MIN_SIZE;
            return allocateSmall(size);
        }
        return allocateLarge(size);
    }

    private int allocateLarge(int size)
    {
        if (largestFree > 0)
        {
            int freeSize = -unsafeGetInt(computeAddress(largestFree));
            if (freeSize == size)
            {
                int result = largestFree + 1;
                unsafePutInt(computeAddress(largestFree), size);
                unsafePutInt(computeAddress(result + size), size);
                totalFreed -= size + 2;
                unsafeZeroMemory(computeAddress(result), size << 2);
                return result;
            }
            else if (freeSize > size + 2)
            {
                int result = largestFree + 1;
                unsafePutInt(computeAddress(largestFree), size);
                unsafePutInt(computeAddress(result + size), size);
                totalFreed -= size + 2;
                int leftOver = freeSize - size - 2;
                largestFree += size + 2;
                unsafePutInt(computeAddress(largestFree), -leftOver);
                unsafePutInt(computeAddress(largestFree + leftOver + 1), -leftOver);
                unsafeZeroMemory(computeAddress(result), size << 2);
                return result;
            }
        }
        int max = (int) (totalAllocated >> 2);
        if (current + size + 2 > max)
        {
            reallocate(size << 2);
        }
        int result = current + 1;
        unsafePutInt(computeAddress(current), size);
        current += size + 2;
        unsafePutInt(computeAddress(current - 1), size);
        return result;
    }

    private int allocateSmall(int size)
    {
        long head = computeAddress(size - MIN_SIZE);
        int existing = unsafeGetInt(head);
        if (existing == 0)
        {
            return allocateLarge(size);
        }
        int next = getInt(existing, 0);
        unsafePutInt(head, next);
        unsafeZeroMemory(computeAddress(existing), size << 2);
        totalFreed -= size + 2;
        return existing;
    }

    private void reallocate(long minBytes)
    {
        long newSize = this.totalAllocated;
        long extra = 0;
        while(extra < minBytes)
        {
            if (newSize < MAX_INCREASE_SIZE)
            {
                newSize = newSize << 1;
            }
            else
            {
                newSize += MAX_INCREASE_SIZE;
            }
            extra = newSize - this.totalAllocated;
        }
        if (newSize > (1L << 33))
        {
            throw new RuntimeException("trying to allocate too much memory "+newSize);
        }
        this.baseAddress = UNSAFE.reallocateMemory(this.baseAddress, newSize);
        long uninitializedStart = this.baseAddress + this.totalAllocated;
        long unitializedSize = newSize - totalAllocated;
        totalAllocated = newSize;
        unsafeZeroMemory(uninitializedStart, unitializedSize);
    }

    @Override
    public void free(int ref)
    {
        assert ref > 2;
        assert ref < current;
        int size = unsafeGetInt(computeAddress(ref - 1));
        if (ref + size + 1 == current)
        {
            //freeing something at the end
            current = ref - 1;
            unsafeZeroMemory(computeAddress(current), (size + 2) << 2);
            return;
        }
        if (size <= MAX_SMALL_SIZE )
        {
            freeSmall(ref, size);
            return;
        }
        int adjacentLeftSize = unsafeGetInt(computeAddress(ref - 2));
        int adjacentRightSize = unsafeGetInt(computeAddress(ref + size + 1));
        int start = ref - 1;
        int end = ref + size + 1;
        int extraFree = 0;
        if (adjacentLeftSize < 0)
        {
            start += adjacentLeftSize - 2;
            extraFree += 2;
        }
        if (adjacentRightSize < 0)
        {
            end += - adjacentRightSize + 2;
            extraFree += 2;
        }
        int totalSize = end - start - 2;
        unsafePutInt(computeAddress(start), -totalSize);
        unsafePutInt(computeAddress(end - 1), -totalSize);
        if (largestFree < 0 || -unsafeGetInt(computeAddress(largestFree)) < totalSize)
        {
            largestFree = start;
        }
        totalFreed += size + extraFree;
    }

    private void freeSmall(int ref, int size)
    {
        long head = computeAddress(size - MIN_SIZE);
        int existing = unsafeGetInt(head);
        this.setInt(ref, 0, existing);
        unsafePutInt(head, ref);
        totalFreed += size + 2;
    }

    private long computeAddress(long ref)
    {
        assert (ref << 2) < totalAllocated;
        return this.baseAddress + (ref << 2);
    }

    @Override
    public boolean isFragmented()
    {
        return current > 1000 && totalFreed > (current >> 3);
    }

    @Override
    public int getLength(int arrayRef)
    {
        assert arrayRef > 2;
        assert arrayRef < current;
        return unsafeGetInt(this.computeAddress(arrayRef - 1));
    }

    @Override
    public void setInt(int arrayRef, int pos, int value)
    {
        assert pos >= 0;
        assert pos < getLength(arrayRef);
        unsafePutInt(this.computeAddress(arrayRef + pos), value);
    }

    @Override
    public int incrementAndGet(int arrayRef, int pos, int value)
    {
        long address = this.computeAddress(arrayRef + pos);
        int result = unsafeGetInt(address) + value;
        unsafePutInt(address, result);
        return result;
    }

    @Override
    public int reallocate(final int arrayRef, int newSize)
    {
        long initialAddress = computeAddress(arrayRef - 1);
        int size = unsafeGetInt(initialAddress);
        int max = (int) (totalAllocated >> 2);
        int delta = newSize - size;
        if (arrayRef + size + 1 == current)
        {
            if (delta + current < max)
            {
                unsafePutInt(initialAddress + ((size + 1) << 2), 0);
                current += delta;
                unsafePutInt(initialAddress, newSize);
                unsafePutInt(initialAddress + ((newSize + 1) << 2), newSize);
                return arrayRef;
            }
            else
            {
                return reallocByCopyAndFree(arrayRef, newSize);
            }
        }
        int adjacentRightSize = unsafeGetInt(computeAddress(arrayRef + size + 1));
        if (adjacentRightSize < 0 && -adjacentRightSize > delta)
        {
            unsafeZeroMemory(initialAddress + ((size + 1) << 2), delta << 2);
            unsafePutInt(initialAddress, newSize);
            unsafePutInt(initialAddress + ((newSize + 1) << 2), newSize);
            int newAdjacentSize = -adjacentRightSize - delta;
            long newAdjacentRef = computeAddress(arrayRef + newSize + 1);
            unsafePutInt(newAdjacentRef, -newAdjacentSize);
            unsafePutInt(newAdjacentRef + ((newAdjacentSize + 1) << 2), -newAdjacentSize);
            if (largestFree == arrayRef + size + 1)
            {
                largestFree = arrayRef + newSize + 1;
            }
            totalFreed -= delta;
            return arrayRef;
        }
        //todo: we could also look on the left
        return reallocByCopyAndFree(arrayRef, newSize);
    }

    @Override
    public void clear(int arrayRef)
    {
        long start = computeAddress(arrayRef);
        unsafeZeroMemory(start, getLength(arrayRef) << 2);
    }

    @Override
    public synchronized void destroy()
    {
        if (!destroyed)
        {
            destroyed = true;
            UNSAFE.freeMemory(this.baseAddress);
            this.baseAddress = -(1L << 40); // about a terabyte
        }
    }

    @Override
    public void reportSpaceUsage(Logger logger, String msg)
    {
        logger.debug(msg + " allocated bytes "+this.totalAllocated+" current pointer "+this.current*4L+" freed "+this.totalFreed*4L+" total unused "+(this.totalAllocated - this.current * 4L + this.totalFreed*4L));
    }

    @Override
    public void ensureCapacity(long sizeInBytes)
    {
        if (sizeInBytes > totalAllocated)
        {
            this.reallocate(sizeInBytes - totalAllocated);
        }
    }

    @Override
    public long getAllocatedSize()
    {
        return this.totalAllocated;
    }

    @Override
    public long getUsedSize()
    {
        return this.current*4L;
    }

    private int reallocByCopyAndFree(int arrayRef, int newSize)
    {
        int newRef = allocate(newSize);
        unsafeCopyMemory(computeAddress(arrayRef), computeAddress(newRef), getLength(arrayRef) << 2);
        free(arrayRef);
        return newRef;
    }

    @Override
    public int getInt(int arrayRef, int pos)
    {
//        if (pos >= getLength(arrayRef))
//        {
//            System.out.println("ohoh");
//        }
        assert pos >= 0;
        assert pos < getLength(arrayRef);
        return unsafeGetInt(this.computeAddress(arrayRef + pos));
    }
}
