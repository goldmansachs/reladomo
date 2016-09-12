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

package com.gs.fw.common.mithra.test;

import com.gs.collections.impl.set.mutable.primitive.LongHashSet;
import com.gs.fw.common.mithra.util.*;
import junit.framework.TestCase;

public class TestThreadExecutor extends TestCase
{

    public void testOneThread()
    {
        runExecutor(1);
    }

    public void testTwoThreads()
    {
        runExecutor(2);
    }

    public void testFourThreads()
    {
        runExecutor(4);
    }

    public void testSlowQueuing()
    {
        AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(1, "TestThreadExecutor");
        executor.setTimeoutInMilliseconds(10);
        TestRunnable[] runnables = new TestRunnable[100];
        for(int i=0;i<runnables.length;i++)
        {
            runnables[i] = new TestRunnable();
        }
        for(int i=0;i<runnables.length;i++)
        {
            executor.submit(runnables[i]);
            sleep(20);
        }
        executor.shutdownAndWaitUntilDone();
        LongHashSet set = new LongHashSet(100);
        for(int i=0;i<runnables.length;i++)
        {
            assertTrue(runnables[i].isDone());
            set.add(runnables[i].getThreadId());
        }
        assertTrue(set.size() > 10);
    }

    public void runExecutor(int threads)
    {
        AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(threads, "TestThreadExecutor"+threads);
        TestRunnable[] runnables = new TestRunnable[10000];
        for(int i=0;i<runnables.length;i++)
        {
            runnables[i] = new TestRunnable();
        }
        for(int i=0;i<runnables.length;i++)
        {
            executor.submit(runnables[i]);
        }
        executor.shutdownAndWaitUntilDone();
        LongHashSet set = new LongHashSet(threads);
        for(int i=0;i<runnables.length;i++)
        {
            assertTrue(runnables[i].isDone());
            set.add(runnables[i].getThreadId());
        }
        assertEquals(threads, set.size());
    }

    public void testExceptionHandling()
    {
        int threads = 4;
        AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(threads, "TestThreadExecutor"+threads);
        Runnable[] runnables = new Runnable[3];
        for(int i=0;i<2;i++)
        {
            runnables[i] = new TestRunnableWithWait();
        }
        runnables[2] = new TestRunnableWithException();
        for(int i=0;i<runnables.length;i++)
        {
            executor.submit(runnables[i]);
        }
        executor.shutdownAndWaitUntilDone();
        assertTrue(((TestRunnableWithWait)runnables[0]).isDone());
        assertTrue(executor.isAborted());
    }

    public static void sleep(long millis)
    {
        long now = System.currentTimeMillis();
        long target = now + millis;
        while(now < target)
        {
            try
            {
                Thread.sleep(target-now);
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            now = System.currentTimeMillis();
        }
    }

    private static class TestRunnable implements Runnable
    {
        private long threadId = -12345678;

        public void run()
        {
            threadId = Thread.currentThread().getId();
        }

        public long getThreadId()
        {
            return threadId;
        }

        public boolean isDone()
        {
            return threadId != -12345678;
        }
    }

    private static class TestRunnableWithWait implements Runnable
    {
        private long threadId = -12345678;

        public void run()
        {
            sleep(1000);
            threadId = Thread.currentThread().getId();
        }

        public long getThreadId()
        {
            return threadId;
        }

        public boolean isDone()
        {
            return threadId != -12345678;
        }
    }

    private static class TestRunnableWithException implements Runnable
    {
        private long threadId = -12345678;

        public void run()
        {
            throw new RuntimeException("for testing only");
        }

        public long getThreadId()
        {
            return threadId;
        }

        public boolean isDone()
        {
            return threadId != -12345678;
        }
    }

    public void testCpuThreadPool()
    {
        CpuBoundTask[] tasks = new CpuBoundTask[60];
        final boolean[] todo = new boolean[60];
        for(int i=0;i<tasks.length;i++)
        {
            final int count = i;
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    todo[count] = true;
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
        for(int i=0;i<todo.length;i++) assertTrue(todo[i]);
    }
}
