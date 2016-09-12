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


import java.util.concurrent.atomic.AtomicInteger;

public class FixedCountTaskFactory implements TaskFactory
{
    private CpuBoundTask tasks[];
    private AtomicInteger current = new AtomicInteger();

    public FixedCountTaskFactory(CpuBoundTask[] tasks)
    {
        this.tasks = tasks;
    }

    @Override
    public CpuBoundTask createTask()
    {
        int next = current.incrementAndGet();
        if (next == tasks.length)
        {
            MithraCpuBoundThreadPool.getInstance().setFactoryDone(this);
        }
        if (next >= tasks.length)
        {

            return null;
        }
        return tasks[next];
    }

    @Override
    public void startAndWorkUntilFinished()
    {
        MithraCpuBoundThreadPool.getInstance().submitTaskFactory(this);

        CpuBoundTask task = tasks[0];
        while(task != null)
        {
            task.run();
            task = this.createTask();
        }

        for(CpuBoundTask cpuBoundTask: tasks)
        {
            cpuBoundTask.waitUntilDoneWithExceptionHandling();
        }
    }
}
