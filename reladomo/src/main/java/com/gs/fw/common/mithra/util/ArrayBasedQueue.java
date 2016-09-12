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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class ArrayBasedQueue
{
    private static final AtomicIntegerFieldUpdater currentIndexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrayBasedQueue.class, "currentIndex");

    private final int targetLength;
    private volatile int currentIndex;
    private final int chunkSize;

    public ArrayBasedQueue(int targetLength, int chunkSize)
    {
        this.targetLength = targetLength;
        this.chunkSize = chunkSize;
    }

    public Segment borrow(Segment existing)
    {
        Segment existingSegment = existing;
        if (existingSegment == null)
        {
            existingSegment = new Segment();
        }
        if(currentIndex < targetLength)
        {
            int end = currentIndexUpdater.addAndGet(this, chunkSize);
            int start = end - chunkSize;
            if (start >= targetLength)
            {
                return null;
            }
            if (end > targetLength)
            {
                end = targetLength;
            }
            existingSegment.reset(start, end);
            return existingSegment;
        }
        return null;
    }

    public static class Segment
    {
        private int start;
        private int end;

        protected void reset(int start, int end)
        {
            this.start = start;
            this.end = end;
        }

        public int getEnd()
        {
            return end;
        }

        public int getStart()
        {
            return start;
        }
    }
}
