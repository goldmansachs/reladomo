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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.finder.Navigation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadedBatchProcessor <T, TL extends MithraList<T>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedBatchProcessor.class);

    private final Set<Object> shards;
    private final RelatedFinder<T> finderInstance;
    private final Operation mainOperation;
    private final Consumer<T, TL> consumer;
    private final List<Navigation<T>> deepFetches;

    private Map<Object, Operation> additionalPerShardRetrievalOperations = UnifiedMap.newMap();
    private ErrorHandler<T, TL> errorHandler = new DefaultErrorHandler<T, TL>();
    private int batchSize = 2000;
    private int retrievalThreads = -1; // -1 means equal to the number of shards

    private AtomicLong totalQueued = new AtomicLong();
    private AtomicLong totalDeepFetchedTime = new AtomicLong();
    private AtomicLong totalDeepFetched = new AtomicLong();

    public MultiThreadedBatchProcessor(RelatedFinder<T> finderInstance, Operation mainOperation, List<Navigation<T>> deepFetches, Consumer<T, TL> consumer, Set<Object> shards)
    {
        this.shards = shards;
        this.finderInstance = finderInstance;
        this.mainOperation = mainOperation;
        this.consumer = consumer;
        this.deepFetches = deepFetches;
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public void setErrorHandler(ErrorHandler<T, TL> errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public void setRetrievalThreads(int retrievalThreads)
    {
        this.retrievalThreads = retrievalThreads;
    }

    public void setAdditionalPerShardRetrievalOperations(Map<Object, Operation> additionalPerShardRetrievalOperations)
    {
        this.additionalPerShardRetrievalOperations = additionalPerShardRetrievalOperations;
    }

    public long getTotalDeepFetched()
    {
        return totalDeepFetched.get();
    }

    public long getTotalDeepFetchedTime()
    {
        return totalDeepFetchedTime.get();
    }

    public long getTotalQueued()
    {
        return totalQueued.get();
    }

    public void process()
    {
        consumer.startConsumption(this);

        load(consumer);

        consumer.endConsumption(this);
    }

    public void load(Consumer<T, TL> consumer)
    {
        int threads = this.retrievalThreads;
        if (threads == -1)
        {
            threads = shards == null ? 1 : shards.size();
        }
        AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(threads, "MTBP load");
        executor.setTimeoutInMilliseconds(10);
        AutoShutdownThreadExecutor deepFetchAndBatchProcessor = new AutoShutdownThreadExecutor(threads, "MTBP process");
        deepFetchAndBatchProcessor.setTimeoutInMilliseconds(10);
        int deepFetchAndBatchProcessorThreads = threads * 3;
        final LinkedBlockingQueue<TL> listBeforeDeepFetchesQueue = new LinkedBlockingQueue<TL>(deepFetchAndBatchProcessorThreads + deepFetchAndBatchProcessorThreads/10 + 10);
        final CountDownLatch loadLatch = new CountDownLatch(shards == null ? 1 : shards.size());
        final CountDownLatch deepFetchLatch = new CountDownLatch(deepFetchAndBatchProcessorThreads);

        for (int i=0;i<deepFetchAndBatchProcessorThreads;i++)
        {
            deepFetchAndBatchProcessor.submit(new DeepFetchAndBatchProcessorRunnable(loadLatch, listBeforeDeepFetchesQueue, executor, deepFetchAndBatchProcessor,
                    deepFetchLatch, consumer, errorHandler));
        }
        deepFetchAndBatchProcessor.shutdown();
        if (shards != null)
        {
            for(final Object shard: shards)
            {
                executor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        queueForDeepFetchAndProcessing(shard, listBeforeDeepFetchesQueue);
                        loadLatch.countDown();
                    }
                });
            }
        }
        else
        {
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    queueForDeepFetchAndProcessing(null, listBeforeDeepFetchesQueue);
                    loadLatch.countDown();
                }
            });
        }
        executor.shutdownAndWaitUntilDone();
        if (executor.isAborted())
        {
            deepFetchAndBatchProcessor.shutdownNow();
            throw new RuntimeException("unrecoverable error while processing. See logs above");
        }
        deepFetchAndBatchProcessor.shutdownAndWaitUntilDone();
        if (deepFetchAndBatchProcessor.isAborted())
        {
            throw new RuntimeException("unrecoverable error while processing. See logs above.");
        }
        LOGGER.info("Total read from DB: " + totalQueued);
    }

    protected long deepFetchBatch(TL list)
    {
        long start = System.currentTimeMillis();
        addDeepFetches(list);
        list.forceResolve();
        return System.currentTimeMillis() - start;
    }

    protected void queueForDeepFetchAndProcessing(final Object shardId, final LinkedBlockingQueue<TL> listBeforeDeepFetchQueue)
    {
        final AtomicLong total = new AtomicLong();
        queueWithOp(shardId, listBeforeDeepFetchQueue, total);
        String msg = "";
        if (shardId != null)
        {
            msg = "Source " + shardId + " ";
        }
        LOGGER.info(msg + "finished reading. " + total.get() + " queued for output.");
    }

    protected void queueWithOp(final Object shardId, final LinkedBlockingQueue<TL> listQueue, final AtomicLong total)
    {
        Operation op = mainOperation;
        if (shardId != null)
        {
            op = op.and(finderInstance.getSourceAttribute().nonPrimitiveEq(shardId));
        }
        Operation additionalOperation = additionalPerShardRetrievalOperations.get(shardId);
        if (additionalOperation != null)
        {
            op = op.and(additionalOperation);
        }
        final List accumulator = FastList.newList(batchSize);
        MithraList many = ((RelatedFinder)finderInstance).findMany(op);
        many.forEachWithCursor(new DoWhileProcedure()
        {
            @Override
            public boolean execute(Object obj)
            {
                T result = (T) obj;
                accumulator.add(result);
                if (accumulator.size() == batchSize)
                {
                    queueResultsWithoutDeepFetch(accumulator, listQueue, shardId);
                    total.addAndGet(accumulator.size());
                    accumulator.clear();
                }
                return true;
            }
        });
        if (!accumulator.isEmpty())
        {
            queueResultsWithoutDeepFetch(accumulator, listQueue, shardId);
            total.addAndGet(accumulator.size());
        }
    }

    protected void queueResultsWithoutDeepFetch(List<T> accumulator, LinkedBlockingQueue<TL> listQueue, Object shardId)
    {
        TL list = (TL) finderInstance.constructEmptyList();
        list.addAll(accumulator);
        try
        {
            listQueue.put(list); // must not touch tradeList after queuing, as another thread may be manipulating it.
            String msg = "";
            if (shardId != null)
            {
                msg = " for source " + shardId;
            }
            LOGGER.info("queued " + accumulator.size() + msg);
            totalQueued.addAndGet(accumulator.size());
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Unexpected exception", e);
        }
    }

    protected void addDeepFetches(TL list)
    {
        for(int i=0;i<deepFetches.size();i++)
        {
            list.deepFetch(deepFetches.get(i));
        }
    }

    private class DeepFetchAndBatchProcessorRunnable implements Runnable
    {
        private final CountDownLatch loadLatch;
        private final LinkedBlockingQueue<TL> listBeforeDeepFetchesQueue;
        private final AutoShutdownThreadExecutor loadExecutor;
        private final AutoShutdownThreadExecutor deepFetchExecutor;
        private final CountDownLatch deepFetchLatch;
        private final Consumer<T, TL> consumer;
        private final ErrorHandler<T, TL> errorHandler;

        public DeepFetchAndBatchProcessorRunnable(CountDownLatch loadLatch, LinkedBlockingQueue<TL> listBeforeDeepFetchesQueue,
                AutoShutdownThreadExecutor loadExecutor, AutoShutdownThreadExecutor deepFetchExecutor, CountDownLatch deepFetchLatch,
                Consumer<T, TL> consumer, ErrorHandler<T, TL> errorHandler)
        {
            this.loadLatch = loadLatch;
            this.listBeforeDeepFetchesQueue = listBeforeDeepFetchesQueue;
            this.loadExecutor = loadExecutor;
            this.deepFetchExecutor = deepFetchExecutor;
            this.deepFetchLatch = deepFetchLatch;
            this.consumer = consumer;
            this.errorHandler = errorHandler;
        }

        public void processDeepFetchQueue()
        {
            TL list = null;
            try
            {
                list = listBeforeDeepFetchesQueue.poll(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                //ignore
            }
            if (list != null)
            {
                totalDeepFetchedTime.addAndGet(deepFetchBatch(list));
                totalDeepFetched.addAndGet(list.size());

                try
                {
                    consumer.consume(list);
                }
                catch (Throwable e)
                {
                    this.errorHandler.handleError(e, MultiThreadedBatchProcessor.this, list);
                }
            }
        }

        @Override
        public void run()
        {
            while (true)
            {
                if (loadLatch.getCount() == 0)
                {
                    // the end
                    while (!listBeforeDeepFetchesQueue.isEmpty())
                    {
                        this.processDeepFetchQueue();
                    }
                    break;
                }
                else if (loadExecutor.isAborted() || deepFetchExecutor.isAborted())
                {
                    deepFetchExecutor.shutdownNow();
                    break;
                }
                else
                {
                    this.processDeepFetchQueue();
                }
            }
            deepFetchLatch.countDown();
        }
    }

    public interface Consumer<T, TL extends MithraList<T>>
    {
        public void startConsumption(MultiThreadedBatchProcessor<T, TL> processor);

        public void consume(TL list) throws Exception;

        public void endConsumption(MultiThreadedBatchProcessor<T, TL> processor);
    }

    public interface ErrorHandler<T, TL extends MithraList<T>>
    {
        public void handleError(Throwable t, MultiThreadedBatchProcessor<T, TL> processor, TL batch);
    }

    private static class DefaultErrorHandler<T, TL extends MithraList<T>> implements ErrorHandler<T, TL>
    {
        @Override
        public void handleError(Throwable t, MultiThreadedBatchProcessor<T, TL> processor, TL batch)
        {
            if (t instanceof RuntimeException)
            {
                throw (RuntimeException)t;
            }
            throw new RuntimeException("Unhandled exception", t);
        }
    }
}
