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

package com.gs.fw.common.mithra.notification.server;


import com.gs.collections.impl.set.mutable.UnifiedSet;
import java.util.Set;

public class GenerationalExpiringSet<T>
{
    private static final int GENERATIONS_COUNT = 4;

    private final Set<T>[] generationSets = new Set[GENERATIONS_COUNT];
    private final int generationTimeToLiveMillis;

    private long lastCleanUpTimeMillis;
    private int currentGenerationIndex;

    public GenerationalExpiringSet(int timeToLiveInSeconds)
    {
        if (timeToLiveInSeconds < 2)
        {
            throw new IllegalArgumentException("generationsCount must be at least 2s");
        }
        this.generationTimeToLiveMillis = 1000 * timeToLiveInSeconds / GENERATIONS_COUNT;
        for (int i = 0; i < GENERATIONS_COUNT; i++)
        {
            this.generationSets[i] = UnifiedSet.newSet();
        }
        this.lastCleanUpTimeMillis = System.currentTimeMillis();
    }

    public synchronized void add(T item)
    {
        this.switchBucketsIfNecessary();
        this.generationSets[this.currentGenerationIndex].add(item);
    }

    public synchronized boolean contains(T item)
    {
        boolean result = false;
        for (int i = 0; i < GENERATIONS_COUNT; i++)
        {
            if (this.generationSets[i].contains(item))
            {
                result = true;
                break;
            }
        }
        this.switchBucketsIfNecessary();
        return result;
    }

    public synchronized boolean remove(T item)
    {
        for (int i = 0; i < GENERATIONS_COUNT; i++)
        {
            if (this.generationSets[i].remove(item))
            {
                this.switchBucketsIfNecessary();
                return true;
            }
        }
        this.switchBucketsIfNecessary();
        return false;
    }

    public synchronized void clear()
    {
        for (int i = 0; i < GENERATIONS_COUNT; i++)
        {
            this.generationSets[i].clear();
        }
        this.lastCleanUpTimeMillis = System.currentTimeMillis();
    }

    private void switchBucketsIfNecessary()
    {
        long now = System.currentTimeMillis();
        if (now - this.lastCleanUpTimeMillis > this.generationTimeToLiveMillis)
        {
            this.currentGenerationIndex++;
            if (this.currentGenerationIndex == GENERATIONS_COUNT)
            {
                this.currentGenerationIndex = 0;
            }
            this.generationSets[this.currentGenerationIndex].clear();
            this.lastCleanUpTimeMillis = now;
        }
    }
}
