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

package com.gs.fw.common.mithra.util;


import sun.misc.Unsafe;

public class PartitionFillerCounter
{
    private final int[] partitionedSize;
    private final int[] maxValue;

    private static final Unsafe UNSAFE;
    private static final long INT_ARRAY_BASE;
    private static final int INT_ARRAY_SHIFT;
    private static final int SIZE_BUCKETS = 8;

    static
    {
        try
        {
            UNSAFE = MithraUnsafe.getUnsafe();

            Class<?> intArrayClass = int[].class;
            INT_ARRAY_BASE = UNSAFE.arrayBaseOffset(intArrayClass);
            int intArrayScale = UNSAFE.arrayIndexScale(intArrayClass);
            if ((intArrayScale & (intArrayScale - 1)) != 0)
            {
                throw new AssertionError("data type scale not a power of two");
            }
            INT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(intArrayScale);

        }
        catch (SecurityException e)
        {
            throw new AssertionError(e);
        }
    }

    private static final long ARRAY_POS_OFFSET;

    static
    {
        try
        {
            ARRAY_POS_OFFSET = UNSAFE.objectFieldOffset(Count.class.getDeclaredField("arrayPos"));
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException("could not get head offset", e);
        }
    }

    public PartitionFillerCounter(int partitions, int totalSize)
    {
        if (partitions > totalSize)
        {
            partitions = totalSize;
        }
        this.partitionedSize = new int[partitions * 16]; // we want 64 bytes for each slot. int is 4 bytes, so 64 bytes is 16 ints.
        this.maxValue = new int[partitions];
        for(int i=0;i<partitions;i++)
        {
            maxValue[i] = (int)(((long)totalSize)*(i+1)/partitions);
            if (i > 0)
            {
                this.partitionedSize[i << 4] = maxValue[i - 1];
            }
        }
    }

    private boolean compareAndSetPartitionValue(int partitionAddress, int expected, int target)
    {
        return UNSAFE.compareAndSwapInt(this.partitionedSize, partitionAddress, expected, target);
    }

    private int getPartitionValue(int partitionAddress)
    {
        return UNSAFE.getIntVolatile(this.partitionedSize, partitionAddress);
    }

    public class Count
    {
        private volatile int arrayPos;


        public int fillAndIncrement()
        {
            while(true)
            {
                int address = arrayPos;
                int partitionAddress = address << 4;
                while (true)
                {
                    int localSize = getPartitionValue(partitionAddress);
                    if (localSize == maxValue[address])
                    {
                        break;
                    }
                    int target = localSize + 1;
                    if (compareAndSetPartitionValue(partitionAddress, localSize, target))
                    {
                        return localSize;
                    }
                }
                UNSAFE.compareAndSwapInt(this, ARRAY_POS_OFFSET, address, address + 1);
            }
        }
    }
}
