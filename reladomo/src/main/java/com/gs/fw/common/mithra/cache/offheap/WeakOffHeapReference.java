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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

public class WeakOffHeapReference<T extends OffHeapMemoryReference> extends WeakReference<T>
{
    public static final long UNALLOCATED = Long.MIN_VALUE;
    protected static Unsafe UNSAFE = MithraUnsafe.getUnsafe();

    private volatile long baseAddress;

    public WeakOffHeapReference(T referent, ReferenceQueue q, long address)
    {
        super(referent, q);
        this.baseAddress = address;
    }

    public void setDestroyed()
    {
        this.baseAddress = UNALLOCATED;
        this.clear();
        OffHeapCleaner.getInstance().remove(this);
    }

    public void resetAddress(long newAddress)
    {
        this.baseAddress = newAddress;
    }

    protected void free()
    {
        if (baseAddress != UNALLOCATED)
        {
            UNSAFE.freeMemory(baseAddress);
            setDestroyed();
        }
    }
}
