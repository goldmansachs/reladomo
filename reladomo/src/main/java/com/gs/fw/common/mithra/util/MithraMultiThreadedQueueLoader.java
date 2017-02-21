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

import com.gs.fw.common.mithra.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * The use of this class is completely optional. A MithraList will normally resolve itself on demand.
 * This class provides a mechanism to resolve multiple lists simultaneously in multiple threads.
 * Any exceptions are properly propagated back from all threads.
 * When in a transaction, this class does not multi-thread the operation to comply with the
 * one-transaction to one-thread model.
 */
public class MithraMultiThreadedQueueLoader
{

    private final int maxThreads;
    private final Object poolLock = new Object();
    private LinkedBlockingQueue output = new LinkedBlockingQueue();
    private boolean errorOccured;
    private ArrayList allExceptions;
    private AtomicInteger expectedResults = new AtomicInteger();
    private AutoShutdownThreadExecutor executor;

    private Logger logger = LoggerFactory.getLogger(MithraMultiThreadedQueueLoader.class.getName());
    private final boolean loadWithinTransactions;

    public MithraMultiThreadedQueueLoader(int maxThreads)
    {
        this(maxThreads, false);
    }

    public MithraMultiThreadedQueueLoader(int maxThreads, boolean loadWithinTransactions)
    {
        this.maxThreads = maxThreads;
        this.loadWithinTransactions = loadWithinTransactions;
    }

    protected void createPool()
    {
        synchronized(poolLock)
        {
            if (executor == null)
            {
                executor = new AutoShutdownThreadExecutor(maxThreads, "Mithra Pooled Thread");
            }
        }
    }

    public void addQueueItem(MithraListQueueItem item)
    {
        this.checkForErrorAndThrowExceptionIfAny();
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            // we must not do this in a different thread
            item.getMithraListToResolve().forceResolve();
            boolean done = false;
            while(!done)
            {
                try
                {
                    this.output.put(item);
                    expectedResults.incrementAndGet();
                    done = true;
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }
        else
        {
            createPool();
            LoaderRunnable runnable = new LoaderRunnable(item, this.output);
            executor.submit(runnable);
            expectedResults.incrementAndGet();
        }
    }

    private synchronized void checkForErrorAndThrowExceptionIfAny()
    {
        if (this.isErrorOccured())
        {
            this.shutdownPool();
            throw (MithraException) this.allExceptions.get(0);
        }
    }

    public boolean hasMoreExpectedResults()
    {
        this.checkForErrorAndThrowExceptionIfAny();
        return expectedResults.get() > 0;
    }

    public MithraListQueueItem takeResult()
    {
        MithraListQueueItem result = null;
        while (this.hasMoreExpectedResults())
        {
            this.checkForErrorAndThrowExceptionIfAny();
            try
            {
                result = (MithraListQueueItem) this.output.poll(100, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                //ignore
            }
            if (result != null)
            {
                expectedResults.decrementAndGet();
                return result;
            }
        }
        return result;
    }

    public synchronized boolean isErrorOccured()
    {
        return errorOccured;
    }

    public synchronized void setErrorOccured(MithraException exception)
    {
        this.errorOccured = true;
        if (this.allExceptions == null)
        {
            this.allExceptions = new ArrayList();
        }
        allExceptions.add(exception);
    }

    public synchronized ArrayList getAllExceptions()
    {
        return allExceptions;
    }

    public void shutdownPool()
    {
        synchronized(poolLock)
        {
            if (executor != null)
            {
                executor.shutdown();
                executor = null;
            }
        }
    }

    public Logger getLogger()
    {
        return this.logger;
    }

    public void setLogger(Logger newLogger)
    {
        this.logger = newLogger;
    }

    private class LoaderRunnable
    implements Runnable
    {
        private MithraListQueueItem mithraListQueueItem;
        private LinkedBlockingQueue output;

        public LoaderRunnable(MithraListQueueItem mithraListQueueItem, LinkedBlockingQueue output)
        {
            this.mithraListQueueItem = mithraListQueueItem;
            this.output = output;
        }

        public void run()
        {
            try
            {
                // Retrieve the dataRows.
                if (!isErrorOccured())
                {
                    if(MithraMultiThreadedQueueLoader.this.loadWithinTransactions)
                    {
                        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Object>()
                        {
                            @Override
                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                LoaderRunnable.this.mithraListQueueItem.getMithraListToResolve().forceResolve();
                                return null;
                            }
                        });
                    }
                    else
                    {
                        this.mithraListQueueItem.getMithraListToResolve().forceResolve();
                    }
                    synchronized (this.mithraListQueueItem)
                    {
                        output.put(this.mithraListQueueItem);
                    }
                }
            }
            catch(MithraException e)
            {
                handleException(e);
            }
            catch (Throwable ex)
            {
                handleException(new MithraException("unexpected error in multithreaded loader", ex));
            }
        }

        private void handleException(MithraException ex)
        {
            // Store the exception for the parent thread to pick up.
            // We have to wrap it in a LoaderException as it could be any
            // type of checked or runtime exception.
            setErrorOccured(ex);
            logger.error("could not load ", ex);
        }

    }

}
