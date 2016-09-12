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

import java.util.Map;
import java.util.concurrent.*;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;


public class ThreadConservingExecutor implements ExecutorWithFinish
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadConservingExecutor.class);

    private final BlockingQueue queue;
    private Future[] executorRunnables;
    private final int threads;
    private final Comparator comparator;
    private Throwable throwable;
    private int queuedWork;

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool(new ThreadFactory()
    {
        private final AtomicInteger threadNumber = new AtomicInteger(0);

        public Thread newThread(Runnable r)
        {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("ThreadConservingThread-" + threadNumber.incrementAndGet());
            return t;
        }
    });

    public static ThreadConservingExecutor fifoQueued(int threads)
    {
        return new ThreadConservingExecutor(threads);
    }

    public static ThreadConservingExecutor priorityQueued(int threads)
    {
        return priorityQueued(threads, new Comparator()
        {
            public int compare(Object o1, Object o2)
            {
                return ((Comparable)o1).compareTo(o2);
            }
        });
    }

    public static ThreadConservingExecutor priorityQueued(int threads, Comparator comparator)
    {
        return new ThreadConservingExecutor(threads, new PriorityBlockingQueue(), comparator);
    }

    /**
     * instantiates (threads - 1) threads to do its work. When finish() is called, the calling thread
     * is also used to do the work.
     * @param threads
     */
    public ThreadConservingExecutor(int threads)
    {
        this(threads, new LinkedBlockingQueue(), null);
    }

    private ThreadConservingExecutor(int threads, BlockingQueue queue, Comparator comparator)
    {
        this.threads = threads;
        this.queue = queue;
        this.comparator = comparator;
    }

    private synchronized void setError(Throwable t)
    {
        this.throwable = t;
        this.notify();
    }

    private synchronized boolean hasError()
    {
        return this.throwable != null;
    }

    private void createThreads()
    {
        if (this.executorRunnables == null && threads > 1)
        {
            this.executorRunnables = new Future[threads - 1];
            for(int i = 0;i < threads - 1;i++)
            {
                this.executorRunnables[i] = EXECUTOR_SERVICE.submit(new ExecutorRunnable());
            }
        }
    }

    public void checkForError()
    {
        if (throwable != null)
        {
            if (throwable instanceof RuntimeException)
            {
                throw (RuntimeException) throwable;
            }
            else
            {
                throw new RuntimeException(throwable);
            }
        }
    }

    public void abort(Throwable e)
    {
        if (!hasError())
        {
            this.setError(e);
        }
    }

    public void execute(Runnable command)
    {
        this.submit(command);
    }

    public void submit(Runnable runnable)
    {
        checkForError();
        createThreads();
        queue.add(new ErrorCatchingRunnable(runnable));
        incrementQueuedWork();
    }

    private synchronized void incrementQueuedWork()
    {
        this.queuedWork++;
        this.notify();
    }

    private synchronized void decrementQueuedWork()
    {
        this.queuedWork--;
        this.notify();
    }

    private int getRunningThreadCount()
    {
        int result = 1;
        if (this.executorRunnables != null)
        {
            result += this.executorRunnables.length;
        }
        return result;
    }

    public void finish()
    {
        checkForError();
        boolean done = false;
        while(!done)
        {
            checkForError();
            QueueRunnable runnable = (QueueRunnable) queue.poll();
            if (runnable != null)
            {
                if (runnable.mustShutdown())
                {
                    done = true;
                }
                else
                {
                    runnable.run();
                }
            }
            else
            {
                synchronized (this)
                {
                    checkForError();
                    if (queuedWork == 0)
                    {
                        done = true;
                    }
                    else
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
        }
        int toFinish = this.getRunningThreadCount();
        for(int i=0;i<toFinish-1;i++)
        {
            queue.add(new ShutdownRunnable());
        }
        joinOtherThreads();
    }

    private void joinOtherThreads()
    {
        if (this.executorRunnables != null)
        {
            for(int i=0;i<this.executorRunnables.length;i++)
            {
                try
                {
                    this.executorRunnables[i].get();
                }
                catch (InterruptedException e)
                {
                    i--; // ignore the exception and wait for this thread again.
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                checkForError();
            }
        }
    }

    private interface QueueRunnable extends Runnable, Comparable
    {
        public boolean mustShutdown();
        public Map getInheritedState();
    }

    private class ErrorCatchingRunnable implements QueueRunnable
    {
        private Runnable runnable;
        private Map inheritedState;

        public ErrorCatchingRunnable(Runnable runnable)
        {
            this.runnable = runnable;
            this.inheritedState = ThreadLocalRegistry.getInstance().getLocalStateCopy();
        }

        public int compareTo(Object o)
        {
            return o instanceof ErrorCatchingRunnable ? comparator.compare(this.runnable, ((ErrorCatchingRunnable) o).runnable) : -1;
        }

        public boolean mustShutdown()
        {
            return hasError();
        }

        public Map getInheritedState()
        {
            return inheritedState;
        }

        public void run()
        {
            try
            {
                runnable.run();
                decrementQueuedWork();
            }
            catch(Throwable t)
            {
                logger.error("error while running job.", t);
                setError(t);
            }
        }
    }

    private static class ShutdownRunnable implements QueueRunnable
    {
        public boolean mustShutdown()
        {
            return true;
        }

        public int compareTo(Object o)
        {
            return o instanceof ShutdownRunnable ? 0 : 1;
        }

        public Map getInheritedState()
        {
            return null;
        }

        public void run()
        {
            // nothing to do
        }
    }

    private class ExecutorRunnable implements Runnable
    {
        @Override
        public void run()
        {
            boolean done = hasError();
            while(!done)
            {
                QueueRunnable runnable = null;
                try
                {
                    runnable = (QueueRunnable) queue.poll(100, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
                if (runnable != null)
                {
                    if (runnable.mustShutdown())
                    {
                        done = true;
                    }
                    else
                    {
                        ThreadLocalRegistry.getInstance().setLocalState(runnable.getInheritedState());
                        try
                        {
                            runnable.run();
                        }
                        finally
                        {
                            ThreadLocalRegistry.getInstance().clearAllInstancesForThread();
                        }
                    }
                }
                done |= hasError();
            }
        }
    }
}
