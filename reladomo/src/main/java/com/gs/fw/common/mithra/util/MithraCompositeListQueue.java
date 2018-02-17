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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.util;


import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class MithraCompositeListQueue extends ListBasedQueue
{
    private static final AtomicIntegerFieldUpdater currentIndexUpdater = AtomicIntegerFieldUpdater.newUpdater(MithraCompositeListQueue.class, "current");

    private final FastList<List> lists;
    private final AtomicIntegerArray countArray;
    private final int chunkSize;
    private volatile int current = 0;

    public MithraCompositeListQueue(MithraCompositeList compositeList, int chunkSize)
    {
        this.lists = compositeList.getLists();
        this.countArray = new AtomicIntegerArray(lists.size());
        this.chunkSize = chunkSize;
    }

    public List borrow(List existing)
    {
        ResettableSubList existingSubList = (ResettableSubList) existing;
        if (existingSubList == null)
        {
            existingSubList = new ResettableSubList();
        }
        while(true)
        {
            int cur = current;
            if (cur < countArray.length()) // due to the concurrency on "current", this if statement can't be pulled out into the while clause
            {
                List target = lists.get(cur);
                int end = countArray.addAndGet(cur, chunkSize);
                int start = end - chunkSize;
                int targetSize = target.size();
                if (start < targetSize)
                {
                    if (end > targetSize)
                    {
                        end = targetSize;
                    }
                    existingSubList.reset(target, start, end - start);
                    return existingSubList;
                }
                currentIndexUpdater.compareAndSet(this, cur, cur + 1);
            }
            else break;
        }
        return null;
    }



}
