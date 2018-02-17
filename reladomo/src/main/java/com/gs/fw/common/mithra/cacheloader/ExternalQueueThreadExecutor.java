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

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;


public class ExternalQueueThreadExecutor implements Executor
{
    private DualCapacityBlockingQueue<Runnable> blockingQueue;
    private WorkerThread[] threads;
    private ExceptionHandler exceptionHandler;
    private final String name;
    private AtomicInteger busyCount = new AtomicInteger(0);
    private volatile boolean stopped = false;

    private AbandonedThreadCountLatch abandonedThreadCountLatch;
    private static Map<String, AbandonedThreadCountLatch> abandonedThreadCountLatches = new ConcurrentHashMap();

    public ExternalQueueThreadExecutor(DualCapacityBlockingQueue<Runnable> blockingQueue, ExceptionHandler exceptionHandler, String name, int threadCount)
    {
        this.blockingQueue = blockingQueue;
        this.exceptionHandler = exceptionHandler;
        this.name = name;
        this.threads = new WorkerThread[threadCount];

        this.initAbandonedThreadCountLatch();
    }

    private AbandonedThreadCountLatch initAbandonedThreadCountLatch()
    {
        this.abandonedThreadCountLatch = abandonedThreadCountLatches.get(name);
        if (this.abandonedThreadCountLatch == null)
        {
            int toleratedAbandonedThreadCount = threads.length/2; // the connection_pool = 2*thread_count. Extra connections are used for BCP upload. Connection pool will barely survive with abandoned_threads = thread_count/2
            this.abandonedThreadCountLatch = new AbandonedThreadCountLatch(toleratedAbandonedThreadCount);
            abandonedThreadCountLatches.put(name, this.abandonedThreadCountLatch);
        }

        return this.abandonedThreadCountLatch;
    }

    public void awaitForAbandonedThreads()
    {
        this.abandonedThreadCountLatch.await();
    }

    public int getAbandonedCountForTest()
    {
        return this.abandonedThreadCountLatch.count;
    }

    public void startThreads()
    {
        for (int i = 0; i < this.threads.length; i++)
        {
            WorkerThread thread = new WorkerThread(this.name + "-" + i);
            thread.setDaemon(true);
            thread.start();
            this.threads[i] = thread;
        }
    }

    public void execute(Runnable command)
    {
        if (stopped)
        {
            throw new RuntimeException("The executor is dead.");
        }
        this.blockingQueue.addUnlimited(command);
    }

    public void shutdown()
    {
        this.stopped = true;
        this.blockingQueue.shutdown();

        for (Thread each : this.threads)
        {
            if (each.isAlive())
            {
                each.setName("Abandoned " + each.getName());
            }
        }

        this.blockingQueue = null;
        this.threads = null;
        this.exceptionHandler = null;
    }

    private class WorkerThread extends Thread
    {
        private WorkerThread(String name)
        {
            this.setDaemon(true);
            this.setName(name);
        }

        public void run()
        {
            abandonedThreadCountLatch.countUp();
            try
            {
                while (!stopped)
                {
                    try
                    {
                        Runnable runnable = blockingQueue.take();
                        if (stopped)
                        {
                            break;
                        }
                        busyCount.incrementAndGet();
                        runnable.run();
                    }
                    catch (Throwable t)
                    {
                        exceptionHandler.handleException(this, t);
                    }
                    busyCount.decrementAndGet();
                }
            }
            finally
            {
                abandonedThreadCountLatch.countDown();
            }
        }
    }

    public static interface ExceptionHandler
    {
        public void handleException(Runnable task, Throwable exception);
    }

    public int getBusyThreadCount()
    {
        return this.busyCount.get();
    }

    private static class AbandonedThreadCountLatch
    {
        private final int toleratedAbandonedThreadCount;
        private int count;

        AbandonedThreadCountLatch(int toleratedAbandonedThreadCount)
        {
            this.toleratedAbandonedThreadCount = toleratedAbandonedThreadCount;
        }

        synchronized void await()
        {
            while (count >= toleratedAbandonedThreadCount)
            {
                try
                {
                    this.wait();
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
            }
        }

        synchronized void countUp()
        {
            count++;
        }

        synchronized void countDown()
        {
            count--;
            if (count <= toleratedAbandonedThreadCount)
            {
                this.notifyAll();
            }
        }
    }
}
