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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class MinExchange
{
    private static final AtomicReferenceFieldUpdater<MinExchange, MithraFastList> currentUpdater = AtomicReferenceFieldUpdater.newUpdater(MinExchange.class, MithraFastList.class, "current");

    private volatile MithraFastList current;
    private final int expectedSize;

    public MinExchange(MithraFastList current, int expectedSize)
    {
        this.current = current;
        this.expectedSize = expectedSize;
    }

    public List exchange(MithraFastList other)
    {
        while(true)
        {
            MithraFastList cur = current;
            if (other.size() > cur.size())
            {
                if (currentUpdater.compareAndSet(this, cur, other))
                {
                    cur.zEnsureCapacity(expectedSize);
                    return cur;
                }
            }
            else
            {
                return other;
            }
        }
    }


}
