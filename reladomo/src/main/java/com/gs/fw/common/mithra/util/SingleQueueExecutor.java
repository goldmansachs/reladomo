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
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheWithOptimisticLockingTxParticipationMode;
import com.gs.fw.common.mithra.database.SyslogChecker;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.AttributeBasedOrderBy;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class allows the multi-threaded execution of insert/update/terminate (or delete) operations on a set of objects.
 * To reduce database contention, the objects are bucketed based on the hashBucketExtractor. It is recommended that
 * the first column in the unique database index be used for hashing. All Mithra object attributes can be used
 * for hashing.
 *
 * This class is not synchronized. It expects to be called from a single thread or be extrenally synchronized.
 *
 * The typical usage of this class is as follows:
 *
 * SingleQueueExecutor executor = new SingleQueueExecutor(...); // see the constructor javadoc
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
public class SingleQueueExecutor implements QueueExecutor
{
    private static Logger logger = LoggerFactory.getLogger(SingleQueueExecutor.class.getName());

    protected ThreadPoolExecutor[] executor;
    protected ThreadPoolExecutor insertExecutor;
    protected MithraFastList updateAndTerminateList;
    protected MithraFastList insertList;
    protected MithraFastList insertForUpdateList;
    protected LinkedBlockingQueue insertQueue = new LinkedBlockingQueue();
    private Comparator orderBy;
    private Comparator transactionOperationComparator = new TransactionOperationComparator();
    private int numberOfUpdateThreads;
    private int numberOfInsertThreads;
    private int totalUpdates;
    private int totalInserts;
    private int totalTerminates;
    protected final AtomicInteger processedUpdatesAndTerminates = new AtomicInteger();
    protected final AtomicInteger processedInserts = new AtomicInteger();
    private volatile int updatesAndTerminatesQueued;
    private volatile int insertsQueued;
    private Throwable error = null;
    private RelatedFinder finder;
    private final boolean hasOptimisticLocking;
    private int logInterval = 30000;
    private volatile long lastLogTime;
    private boolean useBulkInsert;
    private boolean updateWithTerminateAndInsert = true;
    private Timestamp exclusiveUntil = null;
    private int insertBatchSize;
    private int minBatchesBeforeQueuing;
    private AtomicLong timeInUpdateAndTerminate = new AtomicLong();
    private AtomicLong timeInInsert = new AtomicLong();
    private ExecutorErrorHandler errorHandler = new DefaultExecutorErrorHandler();
    private boolean retryOnTimeout = true;
    private TransactionStyle transactionStyle;

    private final int batchSize;
    private int maxRetriesBeforeRequeue = 3;
    private SyslogChecker syslogChecker = new SyslogChecker(110.0, -1); // ignore

    /**
     * Constructor.
     * @param numberOfThreads Total number of queues. Use 3 for good results. There is one thread  for every queue, so this is also the number of threads.
     * @param orderBy Used for sorting. You can use a Mithra attribute, for example ProductFinder.cusip().ascendingOrderBy()
     * @param batchSize Number of objects to put in each batch.
     * @param finder An instance of the finder class. For example, ProductFinder.getFinderInstance()
     * @param insertThreads number of insert threads. These can go pretty high (10 or more) when using bcp. Use 12 for good results with bcp.
     * @param sysLogPercentThreshold percentage of syslog to wait for before inserting/updating - 100% turns waiting off
     * @param sysLogMaxWaitTimeMillis number of milliseconds to wait for syslog to drain
     *
     */
    public SingleQueueExecutor(int numberOfThreads, Comparator orderBy,
            int batchSize, RelatedFinder finder, int insertThreads,
            double sysLogPercentThreshold, long sysLogMaxWaitTimeMillis)
    {
        this(numberOfThreads, orderBy, batchSize, finder, insertThreads);
        this.syslogChecker = new SyslogChecker(sysLogPercentThreshold, sysLogMaxWaitTimeMillis);
    }

    /**
     * Constructor.
     * @param numberOfThreads Total number of queues. Use 3 for good results. There is one thread  for every queue, so this is also the number of threads.
     * @param orderBy Used for sorting. You can use a Mithra attribute, for example ProductFinder.cusip().ascendingOrderBy()
     * @param batchSize Number of objects to put in each batch.
     * @param finder An instance of the finder class. For example, ProductFinder.getFinderInstance()
     * @param insertThreads number of insert threads. These can go pretty high (10 or more) when using bcp. Use 12 for good results with bcp.
     *
     */
    public SingleQueueExecutor(int numberOfThreads, Comparator orderBy,
            int batchSize, RelatedFinder finder, int insertThreads)
    {
        this.batchSize = batchSize;
        this.insertBatchSize = 2000;
        this.minBatchesBeforeQueuing = 10*numberOfThreads;
        this.finder = finder;
        AsOfAttribute[] asOfAttributes = finder.getAsOfAttributes();

        VersionAttribute versionAttribute = finder.getVersionAttribute();

        hasOptimisticLocking = versionAttribute != null || ((asOfAttributes != null) &&
                               ((asOfAttributes.length == 2 || asOfAttributes[0].isProcessingDate())));


        this.numberOfUpdateThreads = numberOfThreads;
        this.numberOfInsertThreads = insertThreads;
        this.executor = new ThreadPoolExecutor[numberOfThreads];
        LinkedBlockingQueue lastUpdateQueue = null;
        ThreadPoolExecutor lastExecutor = null;
        for(int i=0;i<numberOfThreads; i++)
        {
            lastUpdateQueue = new LinkedBlockingQueue();
            lastExecutor = new ThreadPoolExecutor(1, 1,
                                        0L, TimeUnit.MILLISECONDS,
                                        lastUpdateQueue);
            this.executor[i] = lastExecutor;
        }
        if (insertThreads == 0)
        {
            if (numberOfThreads != 1)
            {
                throw new RuntimeException("can only do zero insert threads if there is a single update/terminate thread");
            }
            insertExecutor = lastExecutor;
            insertQueue = lastUpdateQueue;
            this.updateWithTerminateAndInsert = false;
        }
        else
        {
            this.insertExecutor = new ThreadPoolExecutor(numberOfInsertThreads, numberOfInsertThreads,
                                    0L, TimeUnit.MILLISECONDS,
                                    insertQueue);
        }
        this.orderBy = orderBy;
        insertList = new MithraFastList(insertBatchSize);
        insertForUpdateList = new MithraFastList(insertBatchSize);
        updateAndTerminateList = new MithraFastList(minBatchesBeforeQueuing * batchSize);
        lastLogTime = System.currentTimeMillis();
        this.transactionStyle = new TransactionStyle(MithraManagerProvider.getMithraManager().getTransactionTimeout(), maxRetriesBeforeRequeue, this.retryOnTimeout);
    }

    public void setInsertBatchSize(int insertBatchSize)
    {
        this.insertBatchSize = insertBatchSize;
    }

    public int getNumberOfThreads()
    {
        return numberOfUpdateThreads;
    }

    /**
     * default value is 3.
     * @param maxRetriesBeforeRequeue the number of retries before the particular chunk of work is requeued at the end
     * of the queue
     */
    public void setMaxRetriesBeforeRequeue(int maxRetriesBeforeRequeue)
    {
        this.maxRetriesBeforeRequeue = maxRetriesBeforeRequeue;
        this.transactionStyle = new TransactionStyle(MithraManagerProvider.getMithraManager().getTransactionTimeout(), maxRetriesBeforeRequeue, this.retryOnTimeout);
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
        this.insertBatchSize = 5000;
    }

    /**
     * Execute operations until specified business date, instead of till infinity.
     * @param exclusiveUntil the exclusive until date.
     */
    public void setExclusiveUntil(Timestamp exclusiveUntil)
    {
        this.exclusiveUntil = exclusiveUntil;
    }

    /**
     * Used to prevent the separate terminating then inserting of update records.  This method should be used if
     * you need to ensure that the terminate and insert halves of an update are within the same transaction.
     * @param updateWithTerminateAndInsert if true, updates will be translated to terminate and insert
     */
    public void setUpdateWithTerminateAndInsert(boolean updateWithTerminateAndInsert)
    {
        if (this.numberOfInsertThreads != 0)
        {
            this.updateWithTerminateAndInsert = updateWithTerminateAndInsert;
        }
        else if (updateWithTerminateAndInsert == true)
        {
            logger.warn("cannot update with terminate and insert without any insert threads!");
        }
    }

    private boolean useUpdateWithTerminateAndInsert()
    {
        return useBulkInsert && updateWithTerminateAndInsert;
    }

    public void setRetryOnTimeout(boolean retryOnTimeout)
    {
        this.retryOnTimeout = retryOnTimeout;
        this.transactionStyle = new TransactionStyle(MithraManagerProvider.getMithraManager().getTransactionTimeout(), maxRetriesBeforeRequeue, this.retryOnTimeout);
    }

    /**
     * set the interval in milliseconds that the queue prints to the log at INFO level
     * @param logIntervalInMilliseconds in millisecons
     */
    public void setLogInterval(int logIntervalInMilliseconds)
    {
        this.logInterval = logIntervalInMilliseconds;
    }

    public double getSysLogPercentThreshold()
    {
        return this.syslogChecker.getSysLogPercentThreshold();
    }

    public void setSysLogPercentThreshold(double sysLogPercentThreshold)
    {
        this.syslogChecker.setSysLogPercentThreshold(sysLogPercentThreshold);
    }

    public long getSysLogMaxWaitTimeMillis()
    {
        return this.syslogChecker.getSysLogMaxWaitTimeMillis();
    }

    public void setSysLogMaxWaitTimeMillis(long sysLogMaxWaitTimeMillis)
    {
        this.syslogChecker.setSysLogMaxWaitTimeMillis(sysLogMaxWaitTimeMillis);
    }

    /**
     * Use a custom error handler for non-retriable exceptions.
     * @param errorHandler the custom error handler
     */
    public void setErrorHandler(ExecutorErrorHandler errorHandler)
    {
        this.errorHandler = errorHandler;
    }


    public ExecutorErrorHandler getErrorHandler()
    {
        return errorHandler;
    }

    public void submitUpdateTaskToQueue(CallableTask task, AtomicInteger counter, int queueNumber)
    {
        executor[queueNumber].execute(new CallableWrapper(task, this, counter, executor[queueNumber], this.retryOnTimeout));
    }

    public void submitInsertTaskToQueue(CallableTask task, AtomicInteger counter)
    {
        this.insertsQueued++;
        insertExecutor.execute(new CallableWrapper(task, this, counter, insertExecutor, this.retryOnTimeout));
    }

    public synchronized void addForTermination(Object o)
    {
        checkFailed();
        this.totalTerminates++;
        updateAndTerminateList.add(createTerminateOperation((MithraTransactionalObject) o));
        checkForUpdateSubmission();
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

    public synchronized void addForInsert(Object o)
    {
        checkFailed();
        this.totalInserts++;
        insertList.add(o);
        checkForInsertSubmission();
    }

    public synchronized void addAllForInsert(List list)
    {
        checkFailed();
        this.totalInserts += list.size();
        insertList.addAll(list);
        checkForInsertSubmission();
    }

    public synchronized void addUpdatesForInsert(List operations)
    {
        for(int i=0;i<operations.size();i++)
        {
            TransactionOperation op = (TransactionOperation) operations.get(i);
            op.addUpdatesForInsert(insertForUpdateList);
        }
        if (insertForUpdateList.size() >= insertBatchSize)
        {
            int sent = submitInsert(insertForUpdateList);
            MithraFastList newList = new MithraFastList(insertBatchSize);
            if (sent < insertForUpdateList.size())
            {
                newList.addAll(insertForUpdateList.subList(sent, insertForUpdateList.size()));
            }
            this.insertForUpdateList = newList;
        }
    }

    private int submitInsert(MithraFastList insertList)
    {
        int batches = insertList.size() / insertBatchSize;
        insertList.sortThis(orderBy);
        int end = 0;
        if (batches > 3 * numberOfInsertThreads && numberOfInsertThreads > 2)
        {
            int start = 0;
            int firstEnd = submitInsertBatches(start, insertList, 3);
            start = insertBatchSize;
            int secondEnd = submitInsertBatches(start, insertList, 3);
            end = Math.max(firstEnd, secondEnd);
            start = insertBatchSize*2;
            secondEnd = submitInsertBatches(start, insertList, 3);
            end = Math.max(end, secondEnd);
        }
        else if (batches > 2 * numberOfInsertThreads && numberOfInsertThreads > 1)
        {
            int start = 0;
            int firstEnd = submitInsertBatches(start, insertList, 2);
            start = insertBatchSize;
            int secondEnd = submitInsertBatches(start, insertList, 2);
            end = Math.max(firstEnd, secondEnd);
        }
        else
        {
            int start = 0;
            while(end + insertBatchSize <= insertList.size())
            {
                end = getGoodBatchingPoint(start, orderBy, insertList, insertBatchSize);
                this.submitInsertTaskToQueue(this.createInsertTaskForObjects(insertList.subList(start, end)), processedInserts);
                start = end;
            }
        }
        return end;
    }

    private int submitInsertBatches(int start, MithraFastList insertList, int stride)
    {
        int end = 0;
        while(start + insertBatchSize <= insertList.size())
        {
            end = start + insertBatchSize;
            this.submitInsertTaskToQueue(this.createInsertTaskForObjects(insertList.subList(start, end)), processedInserts);
            start += stride*insertBatchSize;
        }
        return end;
    }

    public synchronized void addForUpdate(Object dbObject, Object fileObject)
    {
        checkFailed();
        this.totalUpdates++;
        UpdateOperation pair = createUpdateOperation((MithraTransactionalObject) dbObject,
                (MithraTransactionalObject) fileObject);
        updateAndTerminateList.add(pair);
        checkForUpdateSubmission();
    }

    private void checkForUpdateSubmission()
    {
//        if (processedUpdatesAndTerminates.get() + 2*numberOfUpdateThreads >= this.updatesAndTerminatesQueued)
        {
            if (updateAndTerminateList.size() >= minBatchesBeforeQueuing*batchSize)
            {
                int sent = submitUpdatesAndTerminates();
                MithraFastList newList = new MithraFastList(batchSize * minBatchesBeforeQueuing);
                if (sent < updateAndTerminateList.size())
                {
                    newList.addAll(updateAndTerminateList.subList(sent, updateAndTerminateList.size()));
                }
                this.updateAndTerminateList = newList;
            }
        }
    }

    private void checkForInsertSubmission()
    {
        if (insertList.size() >= insertBatchSize && insertQueue.size() < this.numberOfInsertThreads+3)
        {
            int sent = submitInsert(insertList);
            MithraFastList newList = new MithraFastList(insertBatchSize);
            if (sent < insertList.size())
            {
                newList.addAll(insertList.subList(sent, insertList.size()));
            }
            this.insertList = newList;
        }
    }

    private int submitUpdatesAndTerminates()
    {
        int batches = updateAndTerminateList.size() / batchSize;
        int tasksPerThread = batches / numberOfUpdateThreads;

        int end = 0;
        if (numberOfUpdateThreads <= 1 || tasksPerThread >= minBatchesBeforeQueuing / numberOfUpdateThreads)
        {
            updateAndTerminateList.sortThis(transactionOperationComparator);
            for(int i=0;i<numberOfUpdateThreads;i++)
            {
                this.submitUpdateAndTerminateForThread(i, tasksPerThread);
            }
            end = tasksPerThread * numberOfUpdateThreads * batchSize;
        }
        return end;
    }

    private void submitUpdateAndTerminateForThread(int threadNumber, int tasksPerThread)
    {
        for(int j=0;j<tasksPerThread;j++)
        {
            int start = threadNumber * batchSize * tasksPerThread + j * batchSize;
            this.submitUpdateAndTerminate(start, start + batchSize, threadNumber);
        }
    }

    private void submitRemainingUpdateAndTerminate(int start)
    {
        int batchEnd = Math.min(updateAndTerminateList.size(), start + batchSize);
        int curQueue = 0;
        while(batchEnd > start)
        {
            this.submitUpdateAndTerminate(start, batchEnd, curQueue);
            start = batchEnd;
            batchEnd = Math.min(updateAndTerminateList.size(), start + batchSize);
            curQueue = (curQueue + 1) % numberOfUpdateThreads;
        }
    }

    private void submitUpdateAndTerminate(int start, int end, int queueNumber)
    {
        List operations = updateAndTerminateList.subList(start, end);
        this.submitUpdateTaskToQueue(this.createUpdateAndTerminateTask(operations), processedUpdatesAndTerminates, queueNumber);
        this.updatesAndTerminatesQueued++;
    }

    private int getGoodBatchingPoint(int start, Comparator comparator, MithraFastList list, int batchSize)
    {
        int guess = start + batchSize;
        boolean done = false;
        while(!done && guess + 1 < list.size())
        {
            Object current = list.get(guess);
            Object next = list.get(guess + 1);
            if (comparator.compare(current, next) == 0)
            {
                guess++;
            }
            else
            {
                done = true;
            }
        }
        return guess;
    }

    /**
     * flushes any remaining objects. called automatically from waitUntilFinished().
     */
    public void flushTermination()
    {
        flushUpdatesAndTerminates();
    }

    /**
     * flushes any remaining objects. called automatically from waitUntilFinished(). Has no effect when in bulk insert mode.
     */
    public synchronized void flushInsert()
    {
        if (!insertForUpdateList.isEmpty())
        {
            int sent = this.submitInsert(insertForUpdateList);
            if (sent < insertForUpdateList.size())
            {
                this.submitInsertTaskToQueue(this.createInsertTaskForObjects(insertForUpdateList.subList(sent, insertForUpdateList.size())), processedInserts);
            }
            this.insertForUpdateList = new MithraFastList(insertBatchSize);
        }

        if (!insertList.isEmpty())
        {
            int sent = this.submitInsert(insertList);
            if (sent < insertList.size())
            {
                this.submitInsertTaskToQueue(this.createInsertTaskForObjects(insertList.subList(sent, insertList.size())), processedInserts);
            }
            this.insertList = new MithraFastList(insertBatchSize);
        }
    }

    /**
     * flushes any remaining objects. called automatically from waitUntilFinished().
     */
    public void flushUpdate()
    {
        flushUpdatesAndTerminates();
    }

    public synchronized void flushUpdatesAndTerminates()
    {
        if (!updateAndTerminateList.isEmpty())
        {
            int sent = this.submitUpdatesAndTerminates();
            if (sent < updateAndTerminateList.size())
            {
                this.submitRemainingUpdateAndTerminate(sent);
            }
            this.updateAndTerminateList = new MithraFastList(batchSize * numberOfUpdateThreads);
        }
    }

    public synchronized void queueMorePendingTasks()
    {
        checkForInsertSubmission();
        checkForUpdateSubmission();
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
        for (int i = 0; i < numberOfUpdateThreads; i++)
        {
            while(!executor[i].isTerminated())
            {
                try
                {
                    executor[i].awaitTermination(10000000, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
            }
        }
        flushInsert();
        insertExecutor.shutdown();
        while(!insertExecutor.isTerminated())
        {
            try
            {
                insertExecutor.awaitTermination(10000000, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                // ignore
            }
        }
        logFinalResults();
        if (this.error != null)
        {
            if (error instanceof RuntimeException)
            {
                throw (RuntimeException) error;
            }
            else
            {
                throw new RuntimeException("One or more parallel tasks failed", this.error);
            }
        }
    }

    /**
     * Does an orderly shutsdown of the underlying threads. All pending tasks will be executed.
     * No additional tasks can be scheduled after calling this.
     */
    public void shutdown()
    {
        for (int i = 0; i < numberOfUpdateThreads; i++)
        {
            executor[i].shutdown();
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
        for (int i = 0; i < numberOfUpdateThreads; i++)
        {
            executor[i].getQueue().clear();
            executor[i].shutdown();
        }
        insertExecutor.getQueue().clear();
        insertExecutor.shutdown();
    }

    public AtomicInteger getInsertCounter()
    {
        return this.processedInserts;
    }

    public AtomicInteger getUpdateCounter()
    {
        return this.processedUpdatesAndTerminates;
    }

    public AtomicInteger getCounterForTask(CallableTask task)
    {
        if (task instanceof InsertTask)
        {
            return this.processedInserts;
        }
        return this.processedUpdatesAndTerminates;
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

    public int getPendingUpdatesAndTerminates()
    {
        return updateAndTerminateList.size();
    }

    public int getPendingInserts()
    {
        return insertList.size();
    }

    public int getProcessedUpdatesAndTerminates()
    {
        return totalUpdates+totalTerminates-updateAndTerminateList.size();
    }

    public int getProcessedInserts()
    {
        return totalInserts-insertList.size();
    }

    protected void logResults()
    {
        long now = System.currentTimeMillis();
        if (now > lastLogTime + logInterval)
        {
            lastLogTime = now;
            int total = this.insertsQueued + this.updatesAndTerminatesQueued;
            int done = this.processedInserts.get() + this.processedUpdatesAndTerminates.get();
            SingleQueueExecutor.logger.info("Processed "+done+" of "+total+" tasks. "+(total-done)+" still in queue.");
        }
    }

    protected void logFinalResults()
    {
        if (this.error != null)
        {
            logger.error("All tasks did not complete. See the exceptions above");
        }
        int total = this.insertsQueued + this.updatesAndTerminatesQueued;
        String msg = "Finished processing " + total + " tasks. Inserted " + this.totalInserts + ", updated " + this.totalUpdates +
                " and terminated " + this.totalTerminates + " objects.\n";
        if (timeInInsert.get() > 0)
        {
            double inserts = (double) totalInserts;
            if (this.useUpdateWithTerminateAndInsert())
            {
                inserts += totalUpdates;
            }
            msg += (inserts /timeInInsert.get()*1000)+" inserts per second per thread. ";
        }
        if (timeInUpdateAndTerminate.get() > 0)
        {
            msg += (((double)totalTerminates+totalUpdates)/timeInUpdateAndTerminate.get()*1000)+" update/terminates per second per thread. ";
        }
        if (this.syslogChecker.getTotalSyslogWaitTime() > 0)
        {
            msg += "Total syslog wait time: " + (int)(this.syslogChecker.getTotalSyslogWaitTime()/1000) + "seconds. ";
        }
        SingleQueueExecutor.logger.info(msg);
    }

    public static class CallableWrapper implements Runnable
    {
        private CallableTask callable;
        private SingleQueueExecutor sqe;
        private final AtomicInteger counter;
        private final ThreadPoolExecutor executor;
        private boolean retryOnTimeout = true;

        public CallableWrapper(CallableTask callable, SingleQueueExecutor sqe, AtomicInteger counter,
                ThreadPoolExecutor executor)
        {
            this.callable = callable;
            this.sqe = sqe;
            this.counter = counter;
            this.executor = executor;
        }

        public CallableWrapper(CallableTask callable, SingleQueueExecutor sqe, AtomicInteger counter, ThreadPoolExecutor executor, boolean retryOnTimeout)
        {
            this.callable = callable;
            this.sqe = sqe;
            this.counter = counter;
            this.executor = executor;
            this.retryOnTimeout = retryOnTimeout;
        }

        public void run()
        {
            try
            {
                if (this.sqe.syslogChecker.requiresCheck())
                {
                    this.waitForSyslog();
                }
                callable.call();
                counter.incrementAndGet();
                sqe.logResults();
                callable = null;
            }
            catch (MithraBusinessException e)
            {
                if (e.isRetriable() || (this.retryOnTimeout && e.isTimedOut() ))
                {
                    logger.warn("too many retries for this batch, putting it in the back of the queue");
                    this.executor.getQueue().add(this);
                }
                else
                {
                    sqe.getErrorHandler().handle(e, logger, sqe, this.executor, callable);
                }
            }
            catch(Throwable t)
            {
                sqe.getErrorHandler().handle(t, logger, sqe, this.executor, callable);
            }
        }

        private void waitForSyslog() throws MithraBusinessException
        {
            MithraTransactionalObject txObject = this.callable.getOperations().get(0).getTxObject();    // Use first object in transaction
            sqe.syslogChecker.checkAndWaitForSyslog(txObject);
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
        if (getBulkInsertThreshold() >= 0)
        {
            tx.setBulkInsertThreshold(getBulkInsertThreshold());
        }
    }

    protected CallableTask createInsertTaskForObjects(final List objects)
    {
        final List<TransactionOperation> operations = new MithraFastList<TransactionOperation>();
        for (int i = 0; i < objects.size(); i++)
        {
            operations.add(createInsertOperation((MithraTransactionalObject) objects.get(i)));
        }
        return createInsertTask(operations);
    }

    protected CallableTask createInsertTask(final List<TransactionOperation> operations)
    {
        final TransactionalCommand command =  new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    if (getBulkInsertThreshold() >= 0)
                    {
                        tx.setBulkInsertThreshold(getBulkInsertThreshold());
                    }
                    for(int i=0;i<operations.size();i++)
                    {
                        operations.get(i).performOperation(useBulkInsert, exclusiveUntil);
                    }
                    return null;
                }
            };
        return new InsertTask(operations, command);
    }

    protected int getBulkInsertThreshold()
    {
        if (useBulkInsert)
        {
            return 1;
        }
        return -1;
    }

    protected CallableTask createUpdateAndTerminateTask(final List operations)
    {
        final TransactionalCommand command =  new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    setTransactionOptions(tx);
                    for(int i=0;i<operations.size();i++)
                    {
                        TransactionOperation op = (TransactionOperation) operations.get(i);
                        op.performOperation(useUpdateWithTerminateAndInsert(), exclusiveUntil);
                    }
                    return null;
                }
            };
        return new UpdateAndTerminateTask(operations, command);
    }

    public abstract static class TransactionOperation
    {
        protected MithraTransactionalObject txObject;

        protected TransactionOperation(MithraTransactionalObject object)
        {
            this.txObject = object;
        }

        public MithraTransactionalObject getTxObject()
        {
            return txObject;
        }

        protected void terminateOrDeleteMithraTransactionalObject(Timestamp businessDateTo)
        {
            if (txObject instanceof MithraDatedTransactionalObject )
            {
                if (businessDateTo == null)
                {
                    ((MithraDatedTransactionalObject)txObject).terminate();
                }
                else
                {
                    ((MithraDatedTransactionalObject)txObject).terminateUntil(businessDateTo);
                }
            }
            else
            {
                txObject.delete();
            }
        }

        public abstract void performOperation(boolean updateWithTerminateAndInsert, Timestamp businessDateTo);

        public abstract void addUpdatesForInsert(List insertList);
    }


    protected UpdateOperation createUpdateOperation(MithraTransactionalObject dbObject, MithraTransactionalObject fileObject)
    {
        return new UpdateOperation(dbObject, fileObject);
    }

    protected static class UpdateOperation extends TransactionOperation
    {
        private MithraTransactionalObject fileObject;

        public UpdateOperation(MithraTransactionalObject dbObject, MithraTransactionalObject fileObject)
        {
            super(dbObject);
            this.fileObject = fileObject;
        }

        public Object getDbObject()
        {
            return this.getTxObject();
        }

        public Object getFileObject()
        {
            return fileObject;
        }

        @Override
        public void performOperation(boolean updateWithTerminateAndInsert, Timestamp businessDateTo)
        {
            if (updateWithTerminateAndInsert)
            {
                terminateOrDeleteMithraTransactionalObject(businessDateTo);
            }
            else
            {
                if (businessDateTo == null)
                {
                    txObject.copyNonPrimaryKeyAttributesFrom(fileObject);
                }
                else
                {
                    ((MithraDatedTransactionalObject)txObject).copyNonPrimaryKeyAttributesUntilFrom((MithraDatedTransactionalObject) fileObject, businessDateTo);
                }
            }
        }

        @Override
        public void addUpdatesForInsert(List insertList)
        {
            insertList.add(fileObject);
        }
    }

    protected TerminateOperation createTerminateOperation(MithraTransactionalObject txObject)
    {
        return new TerminateOperation(txObject);
    }

    protected static class TerminateOperation extends TransactionOperation
    {
        public TerminateOperation(MithraTransactionalObject object)
        {
            super(object);
        }

        @Override
        public void performOperation(boolean updateWithTerminateAndInsert, Timestamp businessDateTo)
        {
            terminateOrDeleteMithraTransactionalObject(businessDateTo);
        }

        @Override
        public void addUpdatesForInsert(List insertList)
        {
            // nothing to do
        }
    }

    protected InsertOperation createInsertOperation(MithraTransactionalObject txObject)
    {
        return new InsertOperation(txObject);
    }

    protected static class InsertOperation extends TransactionOperation
    {
        public InsertOperation(MithraTransactionalObject object)
        {
            super(object);
        }

        @Override
        public void performOperation(boolean updateWithTerminateAndInsert, Timestamp businessDateTo)
        {
            if (businessDateTo == null)
            {
                txObject.insert();
            }
            else
            {
                ((MithraDatedTransactionalObject) txObject).insertUntil(businessDateTo);
            }
        }

        @Override
        public void addUpdatesForInsert(List insertList)
        {
            // nothing to do
        }
    }

    protected class TransactionOperationComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            TransactionOperation left = (TransactionOperation) o1;
            TransactionOperation right = (TransactionOperation) o2;
            return orderBy.compare(left.getTxObject(), right.getTxObject());
        }
    }

    public abstract class CallableTask implements Callable
    {
        protected final List<TransactionOperation>  operations;
        protected final TransactionalCommand command;

        public CallableTask(List<TransactionOperation> operations, TransactionalCommand command)
        {
            this.operations = operations;
            this.command = command;
        }

        public List<TransactionOperation> getOperations()
        {
            return operations;
        }

        public Object call() throws Exception
        {
            if (orderBy instanceof AttributeBasedOrderBy)
            {
                Extractor extractor = ((AttributeBasedOrderBy) orderBy).getAttribute();
                logger.info(this.getMessage() +extractor.valueOf(operations.get(0).getTxObject())+
                        " to "+extractor.valueOf(operations.get(operations.size() - 1).getTxObject()));
            }
            else
            {
                logger.info(this.getMessage()+operations.size());
            }
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(command, transactionStyle);
            this.getTime().getAndAdd((System.currentTimeMillis() - start));
            return null;
        }

        protected abstract String getMessage();

        protected abstract AtomicLong getTime();

        public abstract CallableTask createTaskCloneForOperations(List<TransactionOperation> ops);
    }

    private class UpdateAndTerminateTask extends CallableTask
    {
        public UpdateAndTerminateTask(List operations, TransactionalCommand command)
        {
            super(operations, command);
        }

        @Override
        protected AtomicLong getTime()
        {
            return timeInUpdateAndTerminate;
        }

        @Override
        protected String getMessage()
        {
            return "updating ";
        }

        @Override
        public Object call() throws Exception
        {
            super.call();
            if (useUpdateWithTerminateAndInsert())
            {
                addUpdatesForInsert(operations);
            }
            return null;
        }

        @Override
        public CallableTask createTaskCloneForOperations(List<TransactionOperation> ops)
        {
            return createUpdateAndTerminateTask(ops);
        }
    }

    private class InsertTask extends CallableTask
    {
        public InsertTask(List operations, TransactionalCommand command)
        {
            super(operations, command);
        }

        @Override
        protected AtomicLong getTime()
        {
            return timeInInsert;
        }

        @Override
        protected String getMessage()
        {
            return "inserting ";
        }

        @Override
        public CallableTask createTaskCloneForOperations(List<TransactionOperation> ops)
        {
            return createInsertTask(ops);
        }
    }
}
