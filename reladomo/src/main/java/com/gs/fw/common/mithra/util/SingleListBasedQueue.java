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


import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class SingleListBasedQueue extends ListBasedQueue
{
    private static final AtomicIntegerFieldUpdater currentIndexUpdater = AtomicIntegerFieldUpdater.newUpdater(SingleListBasedQueue.class, "currentIndex");

    private final List target;
    private volatile int currentIndex;
    private final int chunkSize;

    public SingleListBasedQueue(List target, int chunkSize)
    {
        this.target = target;
        this.chunkSize = chunkSize;
    }

    public List borrow(List existing)
    {
        ResettableSubList existingSubList = (ResettableSubList) existing;
        if (existingSubList == null)
        {
            existingSubList = new ResettableSubList();
        }
        while(currentIndex < target.size())
        {
            int end = currentIndexUpdater.addAndGet(this, chunkSize);
            int start = end - chunkSize;
            if (start >= target.size())
            {
                break;
            }
            if (end > target.size())
            {
                end = target.size();
            }
            existingSubList.reset(target, start, end - start);
            return existingSubList;
        }
        return null;
    }

}
