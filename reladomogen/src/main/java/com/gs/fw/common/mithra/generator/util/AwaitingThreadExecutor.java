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

package com.gs.fw.common.mithra.generator.util;

import com.gs.fw.common.mithra.generator.util.AutoShutdownThreadExecutor;

import java.util.concurrent.atomic.AtomicInteger;


public class AwaitingThreadExecutor
{

    private AutoShutdownThreadExecutor executor;
    private AtomicInteger todo = new AtomicInteger();

    public AwaitingThreadExecutor(int maxThreads, String name)
    {
        executor = new AutoShutdownThreadExecutor(maxThreads, name);
    }

    public void submit(Runnable task)
    {
        executor.submit(new CountingRunnable(task));
    }

    public void shutdown()
    {
        executor.shutdown();
    }

    public void setExceptionHandler(AutoShutdownThreadExecutor.ExceptionHandler exceptionHandler)
    {
        executor.setExceptionHandler(exceptionHandler);
    }

    public void waitUntilDone()
    {
        synchronized (todo)
        {
            while(todo.get() != 0 && !executor.isAborted())
            {
                try
                {
                    todo.wait(1000);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }
    }

    private class CountingRunnable implements Runnable
    {
        private Runnable target;

        private CountingRunnable(Runnable target)
        {
            this.target = target;
            todo.incrementAndGet();
        }

        public void run()
        {
            try
            {
                target.run();
            }
            finally
            {
                int remaining = todo.decrementAndGet();
                if (remaining == 0)
                {
                    synchronized (todo)
                    {
                        todo.notify();
                    }
                }
            }
        }
    }
}
