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

package com.gs.fw.common.mithra.cacheloader;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.List;

public abstract class DependentKeyIndex
{
    private final DependentLoadingTaskSpawner dependentLoadingTaskSpawner;
    private final FullUniqueIndex loadedOrQueuedOwnerIndex;
    private final CacheLoaderEngine cacheLoaderEngine;
    private final Extractor[] keyExtractors;
    private Operation ownerObjectFilter; // operation created from leftHandFilter on the filteredMapper.
    private LoadingTaskThreadPoolHolder loadingTaskThreadPoolHolder;

    public void setLoadingTaskThreadPoolHolder(LoadingTaskThreadPoolHolder loadingTaskThreadPoolHolder)
    {
        this.loadingTaskThreadPoolHolder = loadingTaskThreadPoolHolder;
    }

    protected DependentKeyIndex(CacheLoaderEngine cacheLoaderEngine, DependentLoadingTaskSpawner taskSpawner, Extractor[] keyExtractors)
    {
        this.cacheLoaderEngine = cacheLoaderEngine;
        this.dependentLoadingTaskSpawner = taskSpawner;
        this.keyExtractors = keyExtractors;
        this.loadedOrQueuedOwnerIndex = new FullUniqueIndex(null, this.keyExtractors);
    }

    public void addStripe(List stripe)
    {
        long takenFromStripes = 0;
        long addedToIndex = 0;
        takenFromStripes += stripe.size();
        if (this.ownerObjectFilter != null)
        {
            stripe = this.ownerObjectFilter.applyOperation(stripe);
        }

        List toAdd = this.dependentLoadingTaskSpawner.addOwnersToLoadIfAbsent(this.loadedOrQueuedOwnerIndex, stripe, this.getKeyExtractors());
        if (!toAdd.isEmpty())
        {
            addedToIndex += toAdd.size();
            this.addKeyHoldersToBeLoaded(toAdd);
        }
        this.cacheLoaderEngine.changeKeyIndexCount(addedToIndex);
        this.cacheLoaderEngine.changeStripedCount(-takenFromStripes);
    }

    public abstract long size();

    protected abstract void addKeyHoldersToBeLoaded(List keyHolders);

    public LoadingTaskRunner createTaskRunner(LoadingTaskRunner.State state)
    {
        List list = this.createListForTaskRunner();

        if (list.isEmpty())
        {
            return null;
        }

        LoadingTaskRunner taskRunner = this.dependentLoadingTaskSpawner.createTaskRunner(list, this.keyExtractors, state);

        this.cacheLoaderEngine.changeKeyIndexCount(-list.size());

        return taskRunner;
    }

    protected abstract List createListForTaskRunner();

    protected Extractor[] getKeyExtractors()
    {
        return this.keyExtractors;
    }

    public void orOwnerObjectFilter(Operation filter)
    {
        if (this.ownerObjectFilter == null || filter == null)
        {
            this.ownerObjectFilter = null;
        }
        else if (!this.ownerObjectFilter.equals(filter))
        {
            this.ownerObjectFilter = this.ownerObjectFilter.or(filter);
        }
    }

    public void setOwnerObjectFilter(Operation filter)
    {
        this.ownerObjectFilter = filter;
    }

    public boolean hasOwnerQueuedOrLoaded(Object owner, Extractor[] keyExtractors)
    {
        return this.loadedOrQueuedOwnerIndex.contains(owner, keyExtractors, null);
    }

    public void putStripeOnQueue(List stripe)
    {
        this.loadingTaskThreadPoolHolder.getThreadPool().addToCPUQueue(new QueueStripe(stripe, this));
    }

    public CacheLoaderEngine getCacheLoaderEngine()
    {
        return this.cacheLoaderEngine;
    }

    public int getLoadedOrQueuedOwnerCount()
    {
        return this.loadedOrQueuedOwnerIndex.size();
    }

    public String toString()
    {
        return new StringBuilder(this.getClass().getSimpleName())
                .append(" for [").append(FastList.newListWith(this.keyExtractors))
                .append("] / ").append(this.dependentLoadingTaskSpawner)
                .toString();
    }
}