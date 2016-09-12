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

import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.cache.offheap.MasterRetrieveStringResult;

public class StringPool implements ReferenceListener
{
    private static final StringPool instance = new StringPool();

    private boolean isOffHeap = false;
    private StringIndex stringIndex = new ConcurrentOnHeapStringIndex(20000);

    // singleton
    private StringPool()
    {
        MithraReferenceThread.getInstance().addListener(this);
    }

    public static StringPool getInstance() { return instance; }

    public String getOrAddToCache(String newValue, boolean hard)
    {
        if (newValue == null) return null;
        return this.stringIndex.getIfAbsentPut(newValue, hard);
    }

    public synchronized void enableOffHeapSupport()
    {
        if (isOffHeap) return;
        ConcurrentOffHeapStringIndex newWeakPool = new ConcurrentOffHeapStringIndex(20000);
        ((ConcurrentOnHeapStringIndex)stringIndex).copyTo(newWeakPool);
        this.stringIndex = newWeakPool;
        isOffHeap = true;
    }

    public void ensureCapacity(int capacity)
    {
        this.stringIndex.ensureCapacity(capacity);
    }

    public void evictCollectedReferences()
    {
        this.stringIndex.evictCollectedReferences();
    }

    public String getStringFromOffHeapAddress(int address)
    {
        if (address == 0) return null;
        return stringIndex.getStringFromOffHeapAddress(address);
    }

    public int getOffHeapAddressWithoutAdding(String key)
    {
        if (key == null) return StringIndex.NULL_STRING;
        return stringIndex.getOffHeapReference(key);
    }

    public int getOffHeapAddress(String key)
    {
        if (key == null) return 0;
        return stringIndex.getIfAbsentPutOffHeap(key);
    }

    public MasterRetrieveStringResult retrieveOffHeapStrings(int startIndex)
    {
        return stringIndex.retrieveStrings(startIndex);
    }
}
