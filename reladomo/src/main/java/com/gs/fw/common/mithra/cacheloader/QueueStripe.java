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


import java.util.List;
import java.util.concurrent.Callable;

/**
 * a stripe of key holders produced by cursor and queued by Executor consumer
 * Callable is executed on the CPU Queue and would return a IO Bound Task if a lot of data is accumulated on the DependentKeyIndex
 */
public class QueueStripe implements Callable<Runnable>
{
    private List stripe;
    private DependentKeyIndex dependentKeyIndex;

    public QueueStripe(List stripe, DependentKeyIndex dependentKeyIndex)
    {
        this.stripe = stripe;
        this.dependentKeyIndex = dependentKeyIndex;
    }

    @Override
    public Runnable call() throws Exception
    {
        this.dependentKeyIndex.addStripe(this.stripe);
        if (this.dependentKeyIndex.size() > DependentLoadingTaskSpawner.TASK_SIZE)
        {
            return this.dependentKeyIndex.createTaskRunner(LoadingTaskRunner.State.QUEUED);
        }
        return null;
    }
}
