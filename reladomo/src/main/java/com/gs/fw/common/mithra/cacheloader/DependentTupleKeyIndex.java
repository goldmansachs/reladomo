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
import com.gs.fw.common.mithra.cache.NonUniqueIdentityIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.DoUntilProcedure;

import java.util.List;

public final class DependentTupleKeyIndex extends DependentKeyIndex
{
    private NonUniqueIdentityIndex bestSingleKeyIndex = null;
    private List sampleForBestSingleKeyIndex = FastList.newList();
    private long size = 0L;

    public DependentTupleKeyIndex(CacheLoaderEngine cacheLoaderEngine, DependentLoadingTaskSpawner dependentObjectLoader, Extractor keyExtractors[])
    {
        super (cacheLoaderEngine, dependentObjectLoader, keyExtractors);
    }

    protected synchronized void addKeyHoldersToBeLoaded(List keyHolders)
    {
        if (this.bestSingleKeyIndex == null)
        {
            this.sampleForBestSingleKeyIndex.addAll(keyHolders);

            if (this.sampleForBestSingleKeyIndex.size() >= DependentLoadingTaskSpawner.TASK_SIZE)
            {
                this.bestSingleKeyIndex = this.createLeastMutatingIndex(this.sampleForBestSingleKeyIndex, this.getKeyExtractors());
                this.sampleForBestSingleKeyIndex = null;
            }
        }
        if (this.bestSingleKeyIndex != null)
        {
            for(int i=0;i<keyHolders.size();i++)
            {
                this.bestSingleKeyIndex.put(keyHolders.get(i));
            }
        }

        this.resetSize();
    }


    protected synchronized List createListForTaskRunner()
    {
        List list;
        if (this.bestSingleKeyIndex == null)
        {
            list = this.sampleForBestSingleKeyIndex;
            this.sampleForBestSingleKeyIndex = FastList.newList();
        }
        else
        {
            list = this.convinientChunkToLoad(this.bestSingleKeyIndex);
        }

        this.resetSize();
        return list;
    }

    private void resetSize ()
    {
        this.size = this.bestSingleKeyIndex == null ? this.sampleForBestSingleKeyIndex.size() : this.bestSingleKeyIndex.size();
    }

    public long size()
    {
        return this.size;
    }

    private List convinientChunkToLoad(NonUniqueIdentityIndex bestSingleKeyIndex)
    {
        final List[] resultListHolder = new List[1];
        resultListHolder[0] = FastList.newList();
        bestSingleKeyIndex.doUntil(new DoUntilProcedure()
        {
            public boolean execute(Object each)
            {
                if (each instanceof List)
                {
                    List list = (List) each;
                    if (list.size() >= DependentLoadingTaskSpawner.TASK_SIZE)
                    {
                        resultListHolder[0] = list;
                        return true;
                    }
                    else
                    {
                        if (resultListHolder[0].size() < DependentLoadingTaskSpawner.TASK_SIZE)
                        {
                            resultListHolder[0].addAll (list);
                        }
                    }
                }
                else
                {
                    if (resultListHolder[0].size() < DependentLoadingTaskSpawner.TASK_SIZE)
                    {
                        resultListHolder[0].add (each);
                    }
                }
                return false;
            }
        });

        List list = resultListHolder[0];
        for (Object aList : list)
        {
            bestSingleKeyIndex.remove(aList);
        }
        return list;
    }
    private NonUniqueIdentityIndex createLeastMutatingIndex(final List sampleKeyHolders, final Extractor[] keyExtractors)
    {
        NonUniqueIdentityIndex[] indexes = new NonUniqueIdentityIndex[keyExtractors.length];
        for (int n = 0; n < keyExtractors.length; n++)
        {
            Extractor[] nsExtractor = {keyExtractors[n]};
            indexes[n] = new NonUniqueIdentityIndex(nsExtractor);
        }

        int sampleLimit = Math.min(sampleKeyHolders.size(), DependentLoadingTaskSpawner.TASK_SIZE);
        for (int i=0; i<sampleLimit; i++)
        {
            Object each = sampleKeyHolders.get(i);
            for (int n = 0; n < keyExtractors.length; n++)
            {
                indexes[n].put(each);
            }
        }

        NonUniqueIdentityIndex bestKeyIndex = indexes[0];
        for (int n = 1; n < keyExtractors.length; n++)
        {
            if (indexes[n].getAverageReturnSize() > bestKeyIndex.getAverageReturnSize())
            {
                bestKeyIndex = indexes[n];
            }
        }

        for (int i=sampleLimit; i<sampleKeyHolders.size(); i++)
        {
            bestKeyIndex.put(sampleKeyHolders.get(i));
        }

        return bestKeyIndex;
    }
}
