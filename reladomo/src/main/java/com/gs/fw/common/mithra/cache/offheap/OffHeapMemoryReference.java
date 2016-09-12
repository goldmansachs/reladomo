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

public class OffHeapMemoryReference
{
    public static final long UNALLOCATED = Long.MIN_VALUE;
    protected static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
//    protected static MithraUnsafe.AuditedMemory UNSAFE = MithraUnsafe.getAuditedMemory();

    private long baseAddress = UNALLOCATED;
    private long lengthInBytes;
    private WeakOffHeapReference<OffHeapMemoryReference> finalizableReference;

    public long getBaseAddress()
    {
        return baseAddress;
    }

    public OffHeapMemoryReference(long initialSize)
    {
        if (initialSize > 0)
        {
            this.baseAddress = UNSAFE.allocateMemory(initialSize);
            UNSAFE.setMemory(baseAddress, initialSize, (byte) 0);
        }
        this.lengthInBytes = initialSize;
    }

    public long getAllocatedLength()
    {
        return lengthInBytes;
    }

    public synchronized void destroy()
    {
        if (baseAddress != OffHeapMemoryReference.UNALLOCATED)
        {
            UNSAFE.freeMemory(baseAddress);
            setDestroyed();
        }
    }

    protected void setDestroyed()
    {
        baseAddress = OffHeapMemoryReference.UNALLOCATED;
        this.lengthInBytes = 0;
        if (this.finalizableReference != null)
        {
            this.finalizableReference.setDestroyed();
        }
    }

    public void reallocate(long newSize)
    {
        assert newSize > 0;
        assert newSize > this.lengthInBytes : "bad new size of "+newSize+" existing length "+ lengthInBytes;
        long newAddress = UNSAFE.allocateMemory(newSize);
        if (this.baseAddress != UNALLOCATED)
        {
            UNSAFE.copyMemory(this.baseAddress, newAddress, this.lengthInBytes);
        }
        UNSAFE.setMemory(newAddress + this.lengthInBytes, newSize - this.lengthInBytes, (byte) 0);
        long oldAddress = this.baseAddress;
        this.baseAddress = newAddress;
        this.lengthInBytes = newSize;
        if (oldAddress != UNALLOCATED)
        {
            UNSAFE.freeMemory(oldAddress);
        }
        if (this.finalizableReference != null)
        {
            this.finalizableReference.resetAddress(this.baseAddress);
        }
    }

    public void registerForGarbageCollection()
    {
        this.finalizableReference = OffHeapCleaner.getInstance().allocateWeakReference(this.baseAddress, this);
    }

}

