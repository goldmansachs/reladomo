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
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheWithOptimisticLockingTxParticipationMode;
import com.gs.fw.common.mithra.extractor.HashableValueSelector;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.list.InsertAllTransactionalCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * This class allows the multi-threaded execution of insert/update/terminate (or delete) operations on a set of objects.
 * To reduce database contention, the objects are bucketed based on the hashBucketExtractor. It is recommended that
 * the first column in the unique database index be used for hashing. All Mithra object attributes can be used
 * for hashing.
 *
 * This class is not synchronized. It expects to be called from a single thread or be externally synchronized.
 *
 * The typical usage of this class is as follows:
 *
 * MultiQueueExecutor executor = new MultiQueueExecutor(5, ProductFinder.cusip(), 200, ProductFinder.getFinderInstance());
 *
 * executor.addForInsert(someProduct); // called many times
 * executor.addForUpdate(productFromDatabase, inMemoryProductWithNewValues); // called many times
 * executor.addForTermination(someProduct); // called many times
 *
 * executor.waitUntilFinished();
 *
 * The order in which the add methods are called is not relevant. Because of the hashing and batching,
 * order is also not preserved.
 * Updates, inserts and terminations are done in separate transactions. It is expected that the same object
 * class is used in all calls to the add methods.
 *
 * If a failure occurs in any of the execution threads, all pending tasks are cancelled and the caller
 * thread will get an exception. No additional tasks can be added if one fails.
 *
 * There is no transactional guarantee across the various threads and batches. The calling application must
 * ensure it can easily recover from any exceptions.
 *
 */
public class MultiQueueExecutor implements QueueExecutor
{
    private static Logger logger = LoggerFactory.getLogger(MultiQueueExecutor.class.getName());


    protected ThreadPoolExecutor[] executors;
    protected final List[] terminateLists;
    protected final List[] insertLists;
    protected final List[] dbUpdateLists;
    protected final List[] fileUpdateLists;
    private HashableValueSelector hashBucketExtractor;
    private int numberOfQueues;
    private int totalUpdates;
    private int totalInserts;
    private int totalTerminates;
    private AtomicInteger processedUpdates = new AtomicInteger();
    private AtomicInteger processedInserts = new AtomicInteger();
    private AtomicInteger processedTerminates = new AtomicInteger();
    private int updatesQueued;
    private int insertsQueued;
    private int terminatesQueued;
    private Throwable error = null;
    private RelatedFinder finder;
    private final boolean isDated;
    private final boolean hasOptimisticLocking;
    private int logInterval = 30000;
    private volatile long lastLogTime;
    private boolean useBulkInsert;

    private final int batchSize;


    /**
     * Constructor.
     * @param numberOfQueues      Total number of queues. There is one thread  for every queue, so this is also the number of threads.
     * @param hashBucketExtractor Hashing algorithm for the object. You can use a Mithra attribute, for example ProductFinder.cusip()
     * @param batchSize           Number of objects to put in each batch.
     * @param finder              An instance of the finder class. For example, ProductFinder.getFinderInstance()
     *
     */
    public MultiQueueExecutor(int numberOfQueues, HashableValueSelector hashBucketExtractor,
                              int batchSize, RelatedFinder finder)
    {
        this.batchSize = batchSize;
        this.finder = finder;
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();
        isDated = asOfAttributes != null;
        hasOptimisticLocking = asOfAttributes != null && (asOfAttributes.length == 2 || asOfAttributes[0].isProcessingDate());
        this.numberOfQueues = numberOfQueues;
        executors = new ThreadPoolExecutor[numberOfQueues];
        for (int i = 0; i < numberOfQueues; i++)
        {
            executors[i] = createExecutor();
        }
        this.hashBucketExtractor = hashBucketExtractor;
        terminateLists = allocateLists();
        insertLists = allocateLists();
        dbUpdateLists = allocateLists();
        fileUpdateLists = allocateLists();
        lastLogTime = System.currentTimeMillis();
    }

    protected ThreadPoolExecutor createExecutor()
    {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue());
    }

    /**
     * Makes the queue use bulk inserts. This is only recommended for stand alone loader programs.
     * The objects being used for updates will be in a terminated state until the final insertion
     * is done. Bulk insert is done at the very end in a single call, and is not batched. This method
     * should be called before adding any objects for insertion or update.
     */
    public void setUseBulkInsert()
    {
        this.useBulkInsert = true;
    }

    /**
     * set the interval in milliseconds that the queue prints to the log at INFO level
     * @param logIntervalInMilliseconds in millisecons
     */
    public void setLogInterval(int logIntervalInMilliseconds)
    {
        this.logInterval = logIntervalInMilliseconds;
    }

    protected List[] allocateLists()
    {
        List[] result = new List[this.numberOfQueues];
        for (int i = 0; i < numberOfQueues; i++)
        {
            result[i] = new ArrayList(batchSize);
        }
        return result;
    }

    public Future submitTaskToQueue(Callable task, int queueNumber, AtomicInteger counter)
    {
        return executors[queueNumber].submit(new CallableWrapper(task, this, counter));
    }

    public void addForTermination(Object o)
    {
        checkFailed();
        int bucket = getBucket(o);
        this.totalTerminates++;
        terminateLists[bucket].add(o);
        if (terminateLists[bucket].size() == batchSize)
        {
            this.submitTaskToQueue(this.createTerminateTask(terminateLists[bucket]), bucket, processedTerminates);
            this.terminateLists[bucket] = new ArrayList(batchSize);
            this.terminatesQueued++;
        }
    }

    private void checkFailed()
    {
        if (this.error != null)
        {
            if (error instanceof RuntimeException)
            {
                throw (RuntimeException) error;
            }
            throw new MithraBusinessException("One of the tasks to the MultiQueueExecutor failed", error);
        }
    }

    private int getBucket(Object o)
    {
        return (hashBucketExtractor.valueHashCode(o) & 0x7fffffff) % this.numberOfQueues;
    }

    public void addForInsert(Object o)
    {
        checkFailed();
        int bucket = 0;
        if (!useBulkInsert)
        {
            bucket = getBucket(o);
        }
        this.totalInserts++;
        insertLists[bucket].add(o);
        if (!useBulkInsert && insertLists[bucket].size() == batchSize)
        {
            this.submitTaskToQueue(this.createInsertTask(insertLists[bucket]), bucket, processedInserts);
            this.insertLists[bucket] = new ArrayList(batchSize);
            this.insertsQueued++;
        }
    }

    public void addForUpdate(Object dbObject, Object fileObject)
    {
        checkFailed();
        int bucket = getBucket(dbObject);
        this.totalUpdates++;
        dbUpdateLists[bucket].add(dbObject);
        fileUpdateLists[bucket].add(fileObject);
        if (useBulkInsert)
        {
            insertLists[0].add(fileObject);
        }
        if (dbUpdateLists[bucket].size() == batchSize)
        {
            this.submitTaskToQueue(this.createUpdateTask(dbUpdateLists[bucket], fileUpdateLists[bucket]), bucket, processedUpdates);
            this.dbUpdateLists[bucket] = new ArrayList(batchSize);
            this.fileUpdateLists[bucket] = new ArrayList(batchSize);
            this.updatesQueued++;
        }

    }

    /**
     * flushes any remaining objects. called automatically from waitUntilFinished().
     */
    public void flushTermination()
    {
        for (int i = 0; i < numberOfQueues; i++)
        {
            if (!terminateLists[i].isEmpty())
            {
                this.submitTaskToQueue(this.createTerminateTask(terminateLists[i]), i, processedTerminates);
                this.terminateLists[i] = new ArrayList(batchSize);
                this.terminatesQueued++;
            }
        }
    }

    /**
     * flushes any remaining objects. called automatically from waitUntilFinished(). Has no effect when in bulk insert mode.
     */
    public void flushInsert()
    {
        if (!useBulkInsert)
        {
            for (int i = 0; i < numberOfQueues; i++)
            {
                if (!insertLists[i].isEmpty())
                {
                    this.submitTaskToQueue(this.createInsertTask(insertLists[i]), i, processedInserts);
                    this.insertLists[i] = new ArrayList(batchSize);
                    this.insertsQueued++;
                }
            }
        }
    }

    /**
     * flushes any remaining objects. called automatically from waitUntilFinished().
     */
    public void flushUpdate()
    {
        for (int i = 0; i < numberOfQueues; i++)
        {
            if (!dbUpdateLists[i].isEmpty())
            {
                this.submitTaskToQueue(this.createUpdateTask(dbUpdateLists[i], fileUpdateLists[i]), i, processedUpdates);
                this.dbUpdateLists[i] = new ArrayList(batchSize);
                this.fileUpdateLists[i] = new ArrayList(batchSize);
                this.updatesQueued++;
            }
        }
    }

    /**
     * Calls flushTermination(), flushUpdate(), flushInsert() and shutdown() and waits for all pending tasks
     * to finish.
     *
     * To ensure proper error handling, this method must be called.
     */
    public void waitUntilFinished()
    {
        flushTermination();
        flushUpdate();
        flushInsert();
        shutdown();
        for (int i = 0; i < numberOfQueues; i++)
        {
            while (!executors[i].isTerminated())
            {
                try
                {
                    executors[i].awaitTermination(10000000, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
            }
        }
        if (useBulkInsert)
        {
            bulkInsert();
        }
        logFinalResults();
        if (this.error != null)
        {
            if (error instanceof RuntimeException)
            {
                throw (RuntimeException) error;
            } else
            {
                throw new RuntimeException("One or more parallel tasks failed", this.error);
            }
        }
    }

    protected void bulkInsert()
    {
        MithraManager manager = MithraManagerProvider.getMithraManager();
        manager.executeTransactionalCommand(new InsertAllTransactionalCommand(insertLists[0], 1), 0);
    }

    /**
     * Does an orderly shutsdown of the underlying threads. All pending tasks will be executed.
     * No additional tasks can be scheduled after calling this.
     */
    public void shutdown()
    {
        for (int i = 0; i < numberOfQueues; i++)
        {
            executors[i].shutdown();
        }
    }

    public boolean anyFailed()
    {
        return this.isFailed();
    }

    public boolean isFailed()
    {
        return this.error != null;
    }

    public Throwable getError()
    {
        return error;
    }

    protected void setFailed(Throwable t)
    {
        this.error = t;
        for (int i = 0; i < numberOfQueues; i++)
        {
            executors[i].getQueue().clear();
            executors[i].shutdown();
        }
    }

    public int getTotalUpdates()
    {
        return totalUpdates;
    }

    public int getTotalInserts()
    {
        return totalInserts;
    }

    public int getTotalTerminates()
    {
        return totalTerminates;
    }

    protected void logResults()
    {
        long now = System.currentTimeMillis();
        if (now > lastLogTime + logInterval)
        {
            lastLogTime = now;
            int total = this.insertsQueued + this.terminatesQueued + this.updatesQueued;
            int done = this.processedInserts.get() + this.processedTerminates.get() + this.processedUpdates.get();
            logger.info("Processed " + done + " of " + total + " tasks. " + (total - done) + " still in queue.");
        }
    }

    protected void logFinalResults()
    {
        if (this.error != null)
        {
            logger.error("All tasks did not complete. See the exceptions above");
        }
        int total = this.insertsQueued + this.terminatesQueued + this.updatesQueued;
        logger.info("Finished processing " + total + " tasks. Inserted " + this.totalInserts + ", updated " + this.totalUpdates +
                " and terminated " + this.totalTerminates + " objects.");
    }

    public static class CallableWrapper implements Callable
    {
        private Callable callable;
        private MultiQueueExecutor executor;
        private AtomicInteger counter;

        public CallableWrapper(Callable callable, MultiQueueExecutor executor, AtomicInteger counter)
        {
            this.callable = callable;
            this.executor = executor;
            this.counter = counter;
        }

        public Object call() throws Exception
        {
            Object returnValue = null;
            try
            {
                returnValue = callable.call();
                counter.incrementAndGet();
                executor.logResults();
            }
            catch (Throwable t)
            {
                logger.error("Failure in " + callable.getClass().getName(), t);
                executor.setFailed(t);
            }
            finally
            {
                callable = null;
            }
            return returnValue;
        }
    }

    protected void setTransactionOptions(MithraTransaction tx)
    {
        if (hasOptimisticLocking)
        {
            tx.setRetryOnOptimisticLockFailure(true);
            tx.setTxParticipationMode(finder.getMithraObjectPortal(),
                    ReadCacheWithOptimisticLockingTxParticipationMode.getInstance());
        }
    }

    protected Callable createInsertTask(List objects)
    {
        final InsertAllTransactionalCommand command = new InsertAllTransactionalCommand(objects);
        return createTransactionalCallable(command);
    }

    protected Callable createTransactionalCallable(final TransactionalCommand command)
    {
        return new Callable()
        {
            public Object call() throws Exception
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(command);
                return null;
            }
        };
    }

    protected Callable createUpdateTask(final List dbObjects, final List fileObjects)
    {
        final TransactionalCommand command = new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                setTransactionOptions(tx);
                if (useBulkInsert)
                {
                    for (int i = 0; i < dbObjects.size(); i++)
                    {
                        MithraDatedTransactionalObject dbObject = (MithraDatedTransactionalObject) dbObjects.get(i);
                        dbObject.terminate();
                    }
                } else
                {
                    for (int i = 0; i < dbObjects.size(); i++)
                    {
                        MithraTransactionalObject dbObject = (MithraTransactionalObject) dbObjects.get(i);
                        MithraTransactionalObject fileObject = (MithraTransactionalObject) fileObjects.get(i);
                        dbObject.copyNonPrimaryKeyAttributesFrom(fileObject);
                    }
                }
                return null;
            }

        };
        return createTransactionalCallable(command);
    }

    protected Callable createTerminateTask(final List objects)
    {
        TransactionalCommand command = null;
        if (isDated)
        {
            command = new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    setTransactionOptions(tx);
                    for(int i=0;i<objects.size();i++)
                    {
                        MithraDatedTransactionalObject object = (MithraDatedTransactionalObject) objects.get(i);
                        object.terminate();
                    }
                    return null;
                }
            };
        }
        else
        {
            command = new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    setTransactionOptions(tx);
                    for(int i=0;i<objects.size();i++)
                    {
                        MithraTransactionalObject object = (MithraTransactionalObject) objects.get(i);
                        object.delete();
                    }
                    return null;
                }
            };
        }
        return createTransactionalCallable(command);
    }

}

