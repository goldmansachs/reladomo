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


import com.gs.fw.common.mithra.cache.MithraReferenceThread;
import com.gs.fw.common.mithra.cache.ReferenceListener;

import java.lang.ref.ReferenceQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OffHeapCleaner implements ReferenceListener
{
    private static final OffHeapCleaner INSTANCE = new OffHeapCleaner();

    private final ReferenceQueue<WeakOffHeapReference<? extends OffHeapMemoryReference>> queue = new ReferenceQueue<WeakOffHeapReference<? extends OffHeapMemoryReference>>();
    private final ConcurrentOffHeapWeakHolder weakHolder = new ConcurrentOffHeapWeakHolder();

    private final AtomicLong freed = new AtomicLong();
    private final AtomicLong rateWindowStart = new AtomicLong();
    private final AtomicInteger rateWindowCount = new AtomicInteger();

    private OffHeapCleaner()
    {
        MithraReferenceThread.getInstance().addListener(this);
    }

    public static OffHeapCleaner getInstance()
    {
        return INSTANCE;
    }

    @Override
    public void evictCollectedReferences()
    {
        Object r;
        while ((r = queue.poll()) != null)
        {
            WeakOffHeapReference<OffHeapMemoryReference> weakRef = (WeakOffHeapReference<OffHeapMemoryReference>) r;
            weakRef.free();
            freed.incrementAndGet();
        }
    }

    public long getFreedCount()
    {
        return freed.get();
    }

    public WeakOffHeapReference<OffHeapMemoryReference> allocateWeakReference(long baseAddress, OffHeapMemoryReference ref)
    {
        long now = System.currentTimeMillis();
        WeakOffHeapReference<OffHeapMemoryReference> weakRef = new WeakOffHeapReference<OffHeapMemoryReference>(ref, this.queue, baseAddress);
        this.weakHolder.put(weakRef);
        if (now > rateWindowStart.get() + 1000)
        {
            rateWindowStart.set(now);
            rateWindowCount.set(0);
        }
        else
        {
            int count = rateWindowCount.incrementAndGet();
            if (count > 100)
            {
                rateWindowStart.set(now);
                rateWindowCount.set(0);
                MithraReferenceThread.getInstance().runNow();
            }
        }
        return weakRef;
    }

    public void remove(WeakOffHeapReference weakRef)
    {
        this.weakHolder.remove(weakRef);
    }
}
