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

package com.gs.fw.common.mithra.cacheloader;


import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.ListFactory;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;

public final class DependentSingleKeyIndex extends DependentKeyIndex
{
    private final List<List> dequeuedKeyHolders = FastList.newList();
    private volatile long size = 0L;

    public DependentSingleKeyIndex(CacheLoaderEngine cacheLoaderEngine, DependentLoadingTaskSpawner dependentObjectLoader, Extractor keyExtractors[])
    {
        super(cacheLoaderEngine, dependentObjectLoader, keyExtractors);
    }

    public synchronized void addKeyHoldersToBeLoaded(List keyHolders)
    {
        List list;
        if (dequeuedKeyHolders.size() == 0)
        {
            list = FastList.newList();
            this.dequeuedKeyHolders.add(list);
        }
        else
        {
            list = this.dequeuedKeyHolders.get(this.dequeuedKeyHolders.size() - 1);
            if (list.size() >= DependentLoadingTaskSpawner.TASK_SIZE)
            {
                list = FastList.newList();
                this.dequeuedKeyHolders.add(list);
            }
        }
        list.addAll(keyHolders);
        this.size+=keyHolders.size();
    }

    public long size()
    {
        return this.size;
    }

    public synchronized List createListForTaskRunner()
    {
        List list = this.dequeuedKeyHolders.size() == 0 ? ListFactory.EMPTY_LIST : this.dequeuedKeyHolders.remove(0);
        this.size -= list.size();
        return list;
    }
}