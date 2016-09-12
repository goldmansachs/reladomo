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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.util.MutableBoolean;
import com.gs.fw.common.mithra.util.ThreadConservingExecutor;
import com.gs.fw.common.mithra.test.ExceptionCatchingCountDownLatch;
import junit.framework.TestCase;

import java.util.concurrent.TimeUnit;
import java.util.Comparator;


public class ThreadConservingExecutorTest  extends TestCase
{
    private static final int WAIT_TIME = 10000;
    private static final TimeUnit WAIT_UNIT = TimeUnit.MILLISECONDS;
    private final int workerThreadCount = 3;

    private ThreadConservingExecutor createExecutor()
    {
        return new ThreadConservingExecutor(workerThreadCount);
    }

    public void testSubmittedRunnableDoesWork()
    {
        ThreadConservingExecutor executor = createExecutor();
        final MutableBoolean runnableDidWork = new MutableBoolean(false);
        executor.submit(new Runnable()
        {
            public void run()
            {
                runnableDidWork.replace(true);
            }
        });
        executor.finish();
        assertTrue("Executor did not run submitted runnable", runnableDidWork.getValue());
    }

    public void testSubmittedRunnablesWorkInParallel()
    {
        ThreadConservingExecutor executor = createExecutor();
        final MutableBoolean firstRunnableStepOne = new MutableBoolean(false);
        final MutableBoolean firstRunnableStepTwo = new MutableBoolean(false);
        final ExceptionCatchingCountDownLatch blockMainUntilFirstRunnableStepOne = new ExceptionCatchingCountDownLatch(1);
        final ExceptionCatchingCountDownLatch blockFirstUntilMainAsserts = new ExceptionCatchingCountDownLatch(1);


        executor.submit(new Runnable()
        {
            public void run()
            {
                firstRunnableStepOne.replace(true);
                blockMainUntilFirstRunnableStepOne.countDown();
                blockFirstUntilMainAsserts.await(WAIT_TIME, WAIT_UNIT);
                firstRunnableStepTwo.replace(true);
            }
        });

        final MutableBoolean secondRunnableStepOne = new MutableBoolean(false);
        final MutableBoolean secondRunnableStepTwo = new MutableBoolean(false);
        final ExceptionCatchingCountDownLatch blockMainUntilSecondRunnableStepOne = new ExceptionCatchingCountDownLatch(1);
        final ExceptionCatchingCountDownLatch blockSecondUntilMainAsserts = new ExceptionCatchingCountDownLatch(1);
        executor.submit(new Runnable()
        {
            public void run()
            {
                secondRunnableStepOne.replace(true);
                blockMainUntilSecondRunnableStepOne.countDown();
                blockSecondUntilMainAsserts.await(WAIT_TIME, WAIT_UNIT);
                secondRunnableStepTwo.replace(true);
            }
        });
        blockMainUntilFirstRunnableStepOne.await(WAIT_TIME, WAIT_UNIT);
        blockMainUntilSecondRunnableStepOne.await(WAIT_TIME, WAIT_UNIT);
        assertTrue("First runnable is not running", firstRunnableStepOne.getValue());
        assertTrue("Second runnable is not running", secondRunnableStepOne.getValue());
        assertFalse("First runnable did not run in parallel", firstRunnableStepTwo.getValue());
        assertFalse("Second runnable did not run in parallel", secondRunnableStepTwo.getValue());

        blockFirstUntilMainAsserts.countDown();
        blockSecondUntilMainAsserts.countDown();
        executor.finish();

        assertTrue("First runnable did not finish", firstRunnableStepTwo.getValue());
        assertTrue("Second runnable did not finish", secondRunnableStepTwo.getValue());
    }

    public void testAbortWithException() throws InterruptedException
    {
        ThreadConservingExecutor executor = createExecutor();
        final SignaledRunnable runnable = new SignaledRunnable();
        executor.submit(runnable);
        runnable.waitUntilDone();
        Thread.sleep(1000); // Need a little time for runnable to execute and set error
        executor.abort(new AbortException());
        this.assertExceptionType(executor, TestException.class);
    }

    private static class SignaledRunnable implements Runnable
    {
        private boolean done = false;
        @Override
        public void run()
        {
            synchronized (this)
            {
                TestException e = new TestException();
                this.done = true;
                this.notify();
                throw e;
            }
        }

        public synchronized void waitUntilDone()
        {
            while(!done)
            {
                try
                {
                    this.wait();
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }
    }

    public void testAbortWithoutException() throws InterruptedException
    {
        ThreadConservingExecutor executor = createExecutor();
        executor.submit(new Runnable()
        {
            public void run()
            {
                // Do nothing
            }
        });
        executor.abort(new AbortException());
        this.assertExceptionType(executor, AbortException.class);
    }

    private void assertExceptionType(ThreadConservingExecutor executor, Class exceptionType)
    {
        RuntimeException error = null;
        try
        {
            executor.finish();
        }
        catch (RuntimeException e)
        {
            error = e;
        }
        assertEquals(exceptionType, error.getClass());
    }

    public void testErrorThrowsOnFinish()
    {
        ThreadConservingExecutor executor = createExecutor();
        executor.submit(new Runnable()
        {
            public void run()
            {
                throw new TestException();
            }
        });

        TestException error = null;
        try
        {
            executor.finish();
        }
        catch (TestException e)
        {
            error = e;
        }
        assertNotNull("Error was not propagated", error);
    }

    public void testFifoOrdering()
    {
        ThreadConservingExecutor executor = createExecutor();

        final ControlsForSubmittedRunnable first = this.submitRunnable(executor);
        final ControlsForSubmittedRunnable second = this.submitRunnable(executor);
        final ControlsForSubmittedRunnable third = this.submitRunnable(executor);
        final ControlsForSubmittedRunnable fourth = this.submitRunnable(executor);

        first.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);
        second.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);

        assertTrue("First runnable is not running", first.getRunnableDidWork().getValue());
        assertTrue("Second runnable is not running", second.getRunnableDidWork().getValue());
        //these are only a best effort assertion - technically, the third and fourth _could_ have been scheduled but not yet run; but it's unlikely!
        assertFalse("Third runnable is running too soon", third.getRunnableDidWork().getValue());
        assertFalse("Fourth runnable is running too soon", fourth.getRunnableDidWork().getValue());

        first.getBlockOfSubmittedRunnable().countDown();

        third.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);
        assertTrue("Third runnable is not running", third.getRunnableDidWork().getValue());
        assertFalse("Fourth runnable is running too soon", fourth.getRunnableDidWork().getValue());

        second.getBlockOfSubmittedRunnable().countDown();

        fourth.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);
        assertTrue("Fourth runnable is not running", fourth.getRunnableDidWork().getValue());

        third.getBlockOfSubmittedRunnable().countDown();
        fourth.getBlockOfSubmittedRunnable().countDown();
        executor.finish();
    }

    public void testNaturalOrderingHighPriorityRunnableRunsBeforeLowPriority()
    {
        ThreadConservingExecutor priorityQueueExecutor = ThreadConservingExecutor.priorityQueued(workerThreadCount);

        this.doHighPriorityRunnableRunsBeforeLowPriority(priorityQueueExecutor, new RunnableThatHasIdFactory()
        {
            public RunnableThatHasId create(int id, final ControlsForSubmittedRunnable control)
            {
                return new RunnableComparableById(id)
                {
                    public void run()
                    {
                        doWork(control);
                    }
                };
            }
        });
    }

    public void testComparatorOrderingHighPriorityRunnableRunsBeforeLowPriority()
    {
        ThreadConservingExecutor priorityQueueExecutor = ThreadConservingExecutor.priorityQueued(workerThreadCount, new Comparator()
        {
            public int compare(Object o1, Object o2)
            {
                return ((RunnableThatHasId)o1).id - ((RunnableThatHasId)o2).id;
            }
        });

        this.doHighPriorityRunnableRunsBeforeLowPriority(priorityQueueExecutor, new RunnableThatHasIdFactory()
        {
            public RunnableThatHasId create(int id, final ControlsForSubmittedRunnable control)
            {
                return new RunnableThatHasId(id)
                {
                    public void run()
                    {
                        doWork(control);
                    }
                };
            }
        });
    }

    private void doHighPriorityRunnableRunsBeforeLowPriority(ThreadConservingExecutor priorityQueueExecutor, RunnableThatHasIdFactory factory)
    {
        final ControlsForSubmittedRunnable first = this.submitRunnableThatHasId(2, factory, priorityQueueExecutor);

        //submitter.submit(2, priorityQueueExecutor);
        final ControlsForSubmittedRunnable second = this.submitRunnableThatHasId(2, factory, priorityQueueExecutor);

        first.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);
        second.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);

        final ControlsForSubmittedRunnable lowPriority = this.submitRunnableThatHasId(2, factory, priorityQueueExecutor);
        final ControlsForSubmittedRunnable highPriority = this.submitRunnableThatHasId(1, factory, priorityQueueExecutor);

        assertTrue("First runnable is not running", first.getRunnableDidWork().getValue());
        assertTrue("Second runnable is not running", second.getRunnableDidWork().getValue());
        //these are only a best effort assertion - technically, the low and hight priority runnables _could_ have been scheduled but not yet run; but it's unlikely!
        assertFalse("Low priority runnable is running too soon", lowPriority.getRunnableDidWork().getValue());
        assertFalse("High priority runnable is running too soon - all threads should be busy with first two runnables!", highPriority.getRunnableDidWork().getValue());

        first.getBlockOfSubmittedRunnable().countDown();

        highPriority.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);
        assertTrue("High priority runnable is not running", highPriority.getRunnableDidWork().getValue());
        assertFalse("Low priority runnable is running too soon", lowPriority.getRunnableDidWork().getValue());

        second.getBlockOfSubmittedRunnable().countDown();

        lowPriority.getBlockOfMainThread().await(WAIT_TIME, WAIT_UNIT);
        assertTrue("Low priority runnable is not running", lowPriority.getRunnableDidWork().getValue());

        highPriority.getBlockOfSubmittedRunnable().countDown();
        lowPriority.getBlockOfSubmittedRunnable().countDown();
        priorityQueueExecutor.finish();
    }

    private ControlsForSubmittedRunnable submitRunnableThatHasId(int id, RunnableThatHasIdFactory factory, ThreadConservingExecutor priorityQueueExecutor)
    {
        ControlsForSubmittedRunnable control = new ControlsForSubmittedRunnable();
        priorityQueueExecutor.submit(factory.create(id, control));
        return control;
    }

    private ControlsForSubmittedRunnable submitRunnable(ThreadConservingExecutor executor)
    {
        final ControlsForSubmittedRunnable control = new ControlsForSubmittedRunnable();
        executor.submit(new Runnable()
        {
            public void run()
            {
                doWork(control);
            }
        });
        return control;
    }


    private void doWork(ControlsForSubmittedRunnable control)
    {
        control.getRunnableDidWork().replace(true);
        control.getBlockOfMainThread().countDown();
        control.getBlockOfSubmittedRunnable().await(WAIT_TIME, WAIT_UNIT);
    }

    private static class TestException extends RuntimeException{}

    private static class AbortException extends RuntimeException{}

    private static class ControlsForSubmittedRunnable
    {
        private final MutableBoolean runnableDidWork = new MutableBoolean(false);
        private final ExceptionCatchingCountDownLatch blockOfMainThread = new ExceptionCatchingCountDownLatch(1);
        private final ExceptionCatchingCountDownLatch blockOfSubmittedRunnable = new ExceptionCatchingCountDownLatch(1);

        public MutableBoolean getRunnableDidWork()
        {
            return runnableDidWork;
        }

        public ExceptionCatchingCountDownLatch getBlockOfMainThread()
        {
            return blockOfMainThread;
        }

        public ExceptionCatchingCountDownLatch getBlockOfSubmittedRunnable()
        {
            return blockOfSubmittedRunnable;
        }
    }

    private static abstract class RunnableThatHasId implements Runnable
    {
        protected final int id;

        protected RunnableThatHasId(int id)
        {
            this.id = id;
        }
    }

    private static abstract class RunnableComparableById extends RunnableThatHasId implements Comparable
    {
        protected RunnableComparableById(int id)
        {
            super(id);
        }

        public int compareTo(Object o)
        {
            return this.id - ((RunnableThatHasId)o).id;
        }
    }

    private static interface RunnableThatHasIdFactory
    {
        public abstract RunnableThatHasId create(int id, ControlsForSubmittedRunnable control);
    }
}
