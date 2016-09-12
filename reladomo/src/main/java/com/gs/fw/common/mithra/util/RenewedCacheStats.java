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

import java.util.List;

public class RenewedCacheStats
{
    public static final RenewedCacheStats EMPTY_STATS = new RenewedCacheStats();
    private List inserted;
    private List updated;
    private List deleted;
    private long howLongItTook;

    public RenewedCacheStats(List inserted, List updated, List deleted, long howLongItTook)
    {
        this.inserted = inserted;
        this.updated = updated;
        this.deleted = deleted;
        this.howLongItTook = howLongItTook;
    }

    private RenewedCacheStats()
    {
    }

    public List getInserted()
    {
        return this.inserted;
    }

    public List getUpdated()
    {
        return this.updated;
    }

    public List getDeleted()
    {
        return this.deleted;
    }

    public long getHowLongItTook()
    {
        return this.howLongItTook;
    }

    public String toString()
    {
        return "inserted: " + inserted.size() + " updated: " + updated.size() + " deleted: " + deleted.size() + " and took " + howLongItTook + " ms";
    }
}
