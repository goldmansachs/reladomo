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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.cacheloader.DualCapacityBlockingQueue;
import com.gs.fw.common.mithra.cacheloader.ExternalQueueThreadExecutor;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ExternalQueueThreadExecutorTest extends TestCase
{
    private List<Throwable> handledExceptions = FastList.<Throwable>newList().asSynchronized();
    private ExternalQueueThreadExecutor.ExceptionHandler exceptionHandler = new ExternalQueueThreadExecutor.ExceptionHandler()
    {
        @Override
        public void handleException(Runnable task, Throwable exception)
        {
            handledExceptions.add(exception);
        }
    };

    private Set<Thread> threads = UnifiedSet.<Thread>newSet().asSynchronized();
    private volatile CountDownLatch completedTasks;

    private class Task implements Runnable
    {
        private CountDownLatch sync;

        private Task(CountDownLatch sync)
        {
            this.sync = sync;
        }

        @Override
        public void run()
        {
            try
            {
                if (this.sync != null)
                {
                    this.sync.await();
                }
                Thread.sleep(1);  // give a chance for the second thread to start up.
            }
            catch (InterruptedException e)
            {

            }
            threads.add(Thread.currentThread());
            completedTasks.countDown();
        }
    }

    public void testThreadPoolLifecycle() throws InterruptedException
    {
        DualCapacityBlockingQueue<Runnable> queue = new DualCapacityBlockingQueue<Runnable>(5);
        ExternalQueueThreadExecutor threadPool = new ExternalQueueThreadExecutor(queue, exceptionHandler, "nancy", 2);
        int initialTaskCount = 10;
        completedTasks= new CountDownLatch(initialTaskCount);
        for (int i=0; i<initialTaskCount; i++)
        {
            threadPool.execute(new Task(null));
        }
        assertEquals(initialTaskCount, completedTasks.getCount());

        threadPool.awaitForAbandonedThreads();
        threadPool.startThreads();
        completedTasks.await();

        assertEquals(2, threads.size());

        for (Thread each: threads)
        {
            assertTrue(each.isAlive());
        }

        CountDownLatch delayedRelease = new CountDownLatch(1);
        CountDownLatch permanentHold = new CountDownLatch(1);
        completedTasks= new CountDownLatch(2);

        for (int i=0; i<10; i++)
        {
            threadPool.execute(new Task(delayedRelease));
        }
        for (int i=0; i<10; i++)
        {
            threadPool.execute(new Task(permanentHold));
        }
        for (Thread each: threads)
        {
            assertTrue(each.isAlive());
        }
        completedTasks.await(5, TimeUnit.MILLISECONDS);
        threadPool.shutdown();

        for (Thread each: threads)
        {
            assertTrue(each.isAlive());
            assertTrue(each.getName().startsWith("Abandoned "));
        }

        assertEquals(2, threadPool.getAbandonedCountForTest());
        completedTasks= new CountDownLatch(2);
        delayedRelease.countDown();
        completedTasks.await(10, TimeUnit.MILLISECONDS);
        Thread.sleep(20); // race

        for (Thread each: threads)
        {
            assertFalse(each.isAlive());
        }
    }
}
