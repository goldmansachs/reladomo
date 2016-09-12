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


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

public class IOTaskThreadPoolWithCpuTaskConveyor implements Runnable
{
    private volatile boolean stopped = false;
    private final BlockingQueue<Callable> cpuTaskQueue = new LinkedBlockingQueue<Callable>();
    private final DualCapacityBlockingQueue<Runnable> ioTaskQueue;
    private final ExternalQueueThreadExecutor ioThreadExecutor;
    private final Builder ioBoundTaskBuilder;
    private Thread consumerThread;
    private final ExternalQueueThreadExecutor.ExceptionHandler exceptionHandler;

    public IOTaskThreadPoolWithCpuTaskConveyor(Builder ioBoundTaskBuilder, ExternalQueueThreadExecutor.ExceptionHandler exceptionHandler, String name, int threadCount)
    {
        this.ioBoundTaskBuilder = ioBoundTaskBuilder;
        this.ioTaskQueue = new DualCapacityBlockingQueue<Runnable>(threadCount); // keep ready 1 extra task per thread.
        ioThreadExecutor = new ExternalQueueThreadExecutor(this.ioTaskQueue, exceptionHandler, name, threadCount);
        this.exceptionHandler = exceptionHandler;
        consumerThread = new Thread(this, name + " CPU Task Conveyor");
        consumerThread.setDaemon(true);
    }

    @Override
    public void run()
    {
        try
        {
            while (!stopped)
            {
                convey();
            }
        }
        catch (Throwable t)
        {
            this.exceptionHandler.handleException(this, t);
        }
    }

    private void convey() throws Exception
    {
        Callable<Runnable> cpuTask;
        Runnable ioBoundTask;

        // block until there is something to produce
        while ((ioBoundTask = this.ioBoundTaskBuilder.build()) == null)
        {
            cpuTask = cpuTaskQueue.take();
            if (this.stopped) return;
            ioBoundTask = cpuTask.call();
            if (ioBoundTask != null)
            {
                break;
            }
        }

        // block until there is space on consuming queue
        ioTaskQueue.put(ioBoundTask);
        if (this.stopped) return;

        // drain the stripeQueue
        while ((cpuTask = cpuTaskQueue.poll()) != null)
        {
            if (this.stopped) return;

            ioBoundTask = cpuTask.call();
            if (ioBoundTask != null)
            {
                ioTaskQueue.addUnlimited(ioBoundTask);
            }
        }
    }

    public interface Builder
    {
        Runnable build();
    }

    public void addToCPUQueue(Callable<Runnable> cpuTask)
    {
        if (this.stopped)
        {
            throw new RuntimeException("stopped");
        }
        this.cpuTaskQueue.add(cpuTask);
    }

    public void addToIOQueue(Runnable ioTask)
    {
        if (this.stopped)
        {
            throw new RuntimeException("stopped");
        }
        this.ioTaskQueue.addUnlimited(ioTask);
    }

    public void awaitForAbandonedThreads ()
    {
        this.ioThreadExecutor.awaitForAbandonedThreads();
    }
    public void startThreads()
    {
        this.consumerThread.start();
        this.ioThreadExecutor.startThreads();
    }

    public void shutdown()
    {
        this.stopped = true;
        this.cpuTaskQueue.clear();
        this.cpuTaskQueue.add(new Callable<Runnable>()
        {
            @Override
            public Runnable call()
            {
                // do nothing. this is just a signal so the thread reads something from the queue.
                return null;
            }
        });

        ioThreadExecutor.shutdown();
    }

    public boolean isStopped()
    {
        return !consumerThread.isAlive();
    }

    public void updateMonitor(LoadingTaskThreadPoolMonitor monitor)
    {
        monitor.setBusyIOThreads(this.ioThreadExecutor.getBusyThreadCount());
        monitor.setIoTaskQueue(ioTaskQueue.size());
        monitor.setCpuTaskQueue(this.cpuTaskQueue.size());
    }
}
