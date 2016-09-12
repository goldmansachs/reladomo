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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.cacheloader.ExternalQueueThreadExecutor;
import com.gs.fw.common.mithra.cacheloader.IOTaskThreadPoolWithCpuTaskConveyor;
import junit.framework.TestCase;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class IOTaskThreadPoolWithCpuTaskConveyorTest extends TestCase
{
    private volatile Throwable lastException = null;
    private AtomicLong completedCPUTasks;
    private AtomicLong completedIOTasks;
    private int currentState;
    private CountDownLatch completedState;

    public void tearDown()
    {
        lastException = null;
    }

    public void testConveyor() throws InterruptedException
    {

        completedCPUTasks = new AtomicLong(0);
        completedIOTasks = new AtomicLong(0);

        IOTaskThreadPoolWithCpuTaskConveyor conveyor =
                new IOTaskThreadPoolWithCpuTaskConveyor(new IoClassBuilder(), new ExceptionHandler(), this.getName(), 10);

        int taskCount = 1000;
        completedState = new CountDownLatch(taskCount * (taskCount - 1) / 2);

        conveyor.startThreads();
        for (int i = 0; i < taskCount; i++)
        {
            conveyor.addToCPUQueue(new CpuTask(i));
        }

        completedState.await();
        assertEquals(null, this.lastException);

        conveyor.shutdown();

        assertEquals(taskCount, completedCPUTasks.get());
        assertConveyorStopped(conveyor);
    }

    public void testShutdown() throws InterruptedException
    {

        completedCPUTasks = new AtomicLong(0);
        completedIOTasks = new AtomicLong(0);

        IOTaskThreadPoolWithCpuTaskConveyor conveyor =
                new IOTaskThreadPoolWithCpuTaskConveyor(new IoClassBuilder(), new ExceptionHandler(), this.getName(), 2);

        int taskCount = 10000;
        completedState = new CountDownLatch(taskCount * (taskCount - 1) / 2);

        conveyor.startThreads();
        for (int i = 0; i < taskCount; i++)
        {
            conveyor.addToCPUQueue(new CpuTask(i));
        }

        conveyor.shutdown();
        assertConveyorStopped(conveyor);
    }

    private void assertConveyorStopped(IOTaskThreadPoolWithCpuTaskConveyor conveyor) throws InterruptedException
    {   // ugly: thread stops asynchronously. give it some time to finish.
        for (int i=0; i<100; i++)
        {
            if(conveyor.isStopped()) return;
            Thread.sleep(2);
        }
        fail("conveyor thread did not stop.");
    }

    private class CpuTask implements Callable<Runnable>
    {
        private final int n;

        private CpuTask(int n)
        {
            this.n = n;
        }


        @Override
        public Runnable call()
        {
            busyCPU();

            currentState += n;
            completedCPUTasks.incrementAndGet();

            return null;
        }
    }

    private class IoTask implements Runnable
    {
        private final int n;

        private IoTask(int n)
        {
            this.n = n;
        }

        @Override
        public void run()
        {
            completedIOTasks.incrementAndGet();
            for (int i=0; i<n; i++) completedState.countDown();
        }
    }

    private class IoClassBuilder implements IOTaskThreadPoolWithCpuTaskConveyor.Builder
    {

        @Override
        public Runnable build()
        {
            if (currentState == 0)
            {
                return null;
            }
            else
            {
                int n = currentState;
                currentState = 0;
                return new IoTask(n);
            }
        }
    }

    private class ExceptionHandler implements ExternalQueueThreadExecutor.ExceptionHandler
    {
        @Override
        public void handleException(Runnable task, Throwable exception)
        {
            lastException = exception;
        }
    }

    private double z = 1000.0;
    private void busyCPU ()
    {
        for (int i=0; i<1000; i++)
        {
            z = Math.pow(z, 0.999);
        }
    }
}
