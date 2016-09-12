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

import java.sql.Timestamp;

public class TimestampPool implements ReferenceListener
{
    private static final TimestampPool instance = new TimestampPool();

    private ConcurrentTempPool tempPool = new ConcurrentTempPool();

    private ConcurrentWeakPool weakPool = new ConcurrentWeakPool(new HardWeakFactory<Timestamp>()
    {
        @Override
        public Timestamp create(Timestamp original, boolean hard)
        {
            if (original instanceof CachedImmutableTimestamp) return original;
            return new CachedImmutableTimestamp(original);
        }

        @Override
        public Timestamp createTimestamp(long time)
        {
            return new CachedImmutableTimestamp(time);
        }
    }, 20000);
    public static final long OFF_HEAP_NULL = Long.MIN_VALUE / 2 - 1234567; // sybase/db2/postgres/oracle cannot store this value

    // singelton
    private TimestampPool()
    {
        MithraReferenceThread.getInstance().addListener(this);
    }

    public static TimestampPool getInstance() { return instance; }

    /**
     * return the pooled value
     *
     * @param newValue the value to look up in the pool and add if not there
     * @return the pooled value 
     */
    public Timestamp getOrAddToCache(Timestamp newValue, boolean hard)
    {
        if (newValue == null || newValue instanceof CachedImmutableTimestamp || newValue == NullDataTimestamp.getInstance())
        {
            return newValue;
        }
        return (Timestamp) weakPool.getIfAbsentPut(newValue, hard);
    }

    public Timestamp getOrAddToCache(Timestamp newValue, boolean hard, boolean offHeap)
    {
        if (newValue == null || newValue instanceof CachedImmutableTimestamp || newValue == NullDataTimestamp.getInstance())
        {
            return newValue;
        }
        if (offHeap)
        {
            if ((newValue.getNanos() % 1000000) != 0)
            {
                newValue = new ImmutableTimestamp(newValue.getTime());
            }
            return tempPool.getIfAbsentPut(newValue);
        }
        return (Timestamp) weakPool.getIfAbsentPut(newValue, hard);
    }

    public void evictCollectedReferences()
    {
        this.weakPool.evictCollectedReferences();
    }

    public Timestamp getTimestampFromOffHeapTime(long time)
    {
        if (time == OFF_HEAP_NULL) return null;
        return this.weakPool.getTimestampFromLong(time);
    }

    public Timestamp getOrAddToCache(long time, boolean hard)
    {
        if (time == OFF_HEAP_NULL)
        {
            return null;
        }
        Timestamp timestamp = this.weakPool.getTimestampFromLong(time);
        if (timestamp == null)
        {
            timestamp =(Timestamp) weakPool.getIfAbsentPut(new ImmutableTimestamp(time), hard);
        }
        return timestamp;
    }

    public Timestamp getOrAddToCacheForOffHeap(Timestamp value)
    {
        if ((value.getNanos() % 1000000) != 0)
        {
            value = new ImmutableTimestamp(value.getTime());
        }
//        if (value.getNanos() == 0)
//        {
//            return getOrAddToCache(value, true);
//        }
        return value;
    }

    private static class CachedImmutableTimestamp extends ImmutableTimestamp
    {
        private CachedImmutableTimestamp(Timestamp other)
        {
            super(other);
        }

        private CachedImmutableTimestamp(long time)
        {
            super(time);
        }

        private CachedImmutableTimestamp(long time, int nanos)
        {
            super(time, nanos);
        }
    }
}
