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

public class FastUnsafeOffHeapMemoryInitialiser
{
    // Protected for tests
    protected static Unsafe UNSAFE = MithraUnsafe.getUnsafe();

    public void unsafeZeroMemory(long address, long sizeInBytes, long baseAddress, long totalAllocated)
    {
        assert address >= baseAddress;
        assert address < baseAddress + totalAllocated;
        assert sizeInBytes <= totalAllocated - (address - baseAddress);

        // Based on performance profiling we determined that setMemory() is inefficient for small buffer sizes.
        // The overhead of each setMemory() call is offset by its more efficient performance on large buffer sizes.
        // The code used to perform the profiling is available at:
        // com.gs.fw.common.mithra.test.offheap.TestOffHeapDataStorage.testUnsafeZeroMemoryPerformance()

        if (sizeInBytes <= 256L)
        {
            unsafeZeroMemoryOptimisedForSmallBuffer(address, sizeInBytes);
        }
        else
        {
            unsafeZeroMemoryOptimisedForLargeBuffer(address, sizeInBytes);
        }
    }

    // protected for test purposes (this is also why the method cannot be static)
    protected void unsafeZeroMemoryOptimisedForLargeBuffer(long address, long sizeInBytes)
    {
        UNSAFE.setMemory(address, sizeInBytes, (byte) 0);
    }

    // protected for test purposes (this is also why the method cannot be static)
    protected void unsafeZeroMemoryOptimisedForSmallBuffer(long address, long sizeInBytes)
    {
        long endAddress = address + sizeInBytes;
        long endOfLastLong = address + ((sizeInBytes >> 3) << 3);
        for (long i = address; i < endOfLastLong; i += 8L)
        {
            UNSAFE.putLong(i, 0L);
        }
        for (long i = endOfLastLong; i < endAddress; i++)
        {
            UNSAFE.putByte(i, (byte) 0);
        }
    }
}
