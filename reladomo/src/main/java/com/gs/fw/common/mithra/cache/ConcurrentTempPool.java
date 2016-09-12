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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.util.ImmutableTimestamp;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;



@SuppressWarnings({"unchecked"})
public class ConcurrentTempPool
{
    private static final int FIXED_SIZE = 1024;

    private static final AtomicReferenceFieldUpdater tableUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentTempPool.class, AtomicReferenceArray.class, "table");

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private volatile AtomicReferenceArray table;

    public ConcurrentTempPool()
    {
        table = new AtomicReferenceArray(FIXED_SIZE);
    }

    /*
     * Return the table after first expunging stale entries
     */
    private AtomicReferenceArray getTable()
    {
        return table;
    }

    public Timestamp getIfAbsentPut(Timestamp data)
    {
        int hash = data.hashCode();
        AtomicReferenceArray currentArray = getTable();
        int index = hash & (FIXED_SIZE - 1);
        Object o = currentArray.get(index);
        if (o != null && o.equals(data))
        {
            return (Timestamp) o;
        }
        Timestamp toPut = data instanceof ImmutableTimestamp ? data : new ImmutableTimestamp(data);
        currentArray.set(index, toPut);
        return toPut;
    }
}