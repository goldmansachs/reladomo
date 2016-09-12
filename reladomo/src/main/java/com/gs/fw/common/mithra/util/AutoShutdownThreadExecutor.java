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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;


public class AutoShutdownThreadExecutor implements Executor
{

    private static AtomicInteger globalPoolCounter = new AtomicInteger();

    private int timeoutInMilliseconds = 60000;
    private final int maxThreads;
    private int poolNumber;
    private final String name;
    private AtomicLong combinedState = new AtomicLong(); // lower 32 bits are (queue size - idle threads). upper 32 bits are current threads
    private AtomicInteger threadId = new AtomicInteger();
    private volatile boolean isAborted;
    private final Object endSignal = new Object();
    private LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
    private ExceptionHandler exceptionHandler;
    private ThreadGroup threadGroup;

    public AutoShutdownThreadExecutor(int maxThreads, String name)
    {
        this.maxThreads = maxThreads;
        this.name = name;
        this.threadGroup = new ThreadGroup(name);
        poolNumber = globalPoolCounter.incrementAndGet();
    }

    /**
     * Executes given runnable at some time in the future.
     */
    public void execute (Runnable runnable)
    {
        this.submit(runnable);
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
    }

    public ExceptionHandler getExceptionHandler()
    {
        if (exceptionHandler == null)
        {
            exceptionHandler = new DefaultExceptionHandler();
        }
        return exceptionHandler;
    }

    public void setTimeoutInMilliseconds(int timeoutInMilliseconds)
    {
        this.timeoutInMilliseconds = timeoutInMilliseconds;
    }

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     */
    public void shutdown()
    {
        while(true)
        {
            long cur = combinedState.get();
            long newValue = cur | 0x8000000000000000L;
            if (combinedState.compareAndSet(cur, newValue))
            {
                break;
            }
        }

    }

    public boolean isAborted()
    {
        return isAborted;
    }

    public void shutdownNow()
    {
        shutdown();
        isAborted = true;
    }

    /**
     * Shuts down the executor and blocks until all tasks have completed execution
     *
     */
    public void shutdownAndWaitUntilDone()
    {
        shutdown();
        while(true)
        {
            synchronized (endSignal)
            {
                try
                {
                    if ((combinedState.get() & 0x7FFFFFFFFFFFFFFFL) == 0) break;
                    endSignal.wait(100);
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
            }
        }
    }

    /**
     * Submits a Runnable task for execution and returns a Future
     * representing that task. The Future's <tt>get</tt> method will
     * return <tt>null</tt> upon <em>successful</em> completion.
     *
     * @param task the task to submit
     */
    public void submit(Runnable task)
    {
        queue.add(task);
        while(true)
        {
            long cur = combinedState.get();
            int shutdown = (int)(cur >> 63) & 1;
            if (shutdown != 0)
            {
                throw new RejectedExecutionException("ThreadExecutor is already shutdown");
            }
            int currentThreads = ((int) (cur >> 32)) & 0x7FFFFFFF;
            int sizeMinusIdle = (int)(cur & 0xFFFFFFFFL);
            if (currentThreads < maxThreads && sizeMinusIdle >= 0)
            {
                currentThreads++;
                long next = combineStates(currentThreads, sizeMinusIdle, shutdown);
                if (combinedState.compareAndSet(cur, next))
                {
                    ThreadGroup localThreadGroup = threadGroup;
                    if (localThreadGroup.isDestroyed())
                    {
                        synchronized (this)
                        {
                            if (this.threadGroup.isDestroyed())
                            {
                                localThreadGroup = new ThreadGroup(this.name);
                                this.threadGroup = localThreadGroup;
                            }
                        }
                    }
                    new ExecutorThread(name + "-" + poolNumber + "-" + threadId.incrementAndGet()).start();
                    break;
                }
            }
            else
            {
                sizeMinusIdle++;
                if (trySettingState(cur, currentThreads, sizeMinusIdle, shutdown)) break;
            }
        }
    }

    private boolean trySettingState(long cur, int currentThreads, int sizeMinusIdle, int shutdown)
    {
        long next = combineStates(currentThreads, sizeMinusIdle, shutdown);
        return combinedState.compareAndSet(cur, next);
    }

    private long combineStates(int currentThreads, int sizeMinusIdle, int shutdown)
    {
        return ((long)currentThreads << 32) | (((long) sizeMinusIdle) & 0xFFFFFFFFL) | (((long)shutdown) << 63);
    }

    private class ExecutorThread extends Thread
    {
        private ExecutorThread(String name)
        {
            super(threadGroup, name);
            this.setDaemon(true);
        }

        public void run()
        {
            while (true)
            {
                Runnable target = null;
                try
                {
                    target = queue.poll(timeoutInMilliseconds, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
                if (target != null)
                {
                    if (!isAborted)
                    {
                        try
                        {
                            target.run();
                        }
                        catch(Throwable t)
                        {
                            getExceptionHandler().handleException(AutoShutdownThreadExecutor.this, target, t);
                        }
                    }
                    cleanUpAfterTask();
                    if (terminateThread(true, true)) return;
                }
                else
                {
                    if (terminateThread(false, false)) return;
                }
            }
        }

        private boolean terminateThread(boolean careAboutShutdown, boolean reduceQueueSize)
        {
            while(true)
            {
                long cur = combinedState.get();
                int shutdown = (int)(cur >> 63) & 1;
                int currentThreads = ((int) (cur >> 32)) & 0x7FFFFFFF;
                int sizeMinusIdle = (int)(cur & 0xFFFFFFFFL);
                if (reduceQueueSize) sizeMinusIdle--;
                if ((!careAboutShutdown | (shutdown != 0)) && sizeMinusIdle < 0)
                {
                    sizeMinusIdle++;
                    currentThreads--;
                    long next = combineStates(currentThreads, sizeMinusIdle, shutdown);
                    if (combinedState.compareAndSet(cur, next))
                    {
                        synchronized (endSignal)
                        {
                            endSignal.notify();
                        }
                        return true;
                    }
                }
                else
                {
                    if (trySettingState(cur, currentThreads, sizeMinusIdle, shutdown)) break;
                }
            }
            return false;
        }
    }

    protected void cleanUpAfterTask()
    {
        //subclass may override
    }

    public static interface ExceptionHandler
    {
        public void handleException(AutoShutdownThreadExecutor executor, Runnable target, Throwable exception);
    }

    private static class DefaultExceptionHandler implements ExceptionHandler
    {
        private static Logger logger = LoggerFactory.getLogger(AutoShutdownThreadExecutor.class.getName());

        public void handleException(AutoShutdownThreadExecutor executor, Runnable target, Throwable exception)
        {
            executor.shutdownNow();
            logger.error("Error in runnable target. Shutting down queue", exception);
        }
    }
}
