/*
  Copyright 2018 Goldman Sachs.
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

package com.gs.reladomo.jms;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.reladomo.txid.ReladomoTxIdInterface;
import com.gs.reladomo.txid.ReladomoTxIdInterfaceFinder;
import com.gs.reladomo.util.InterruptableBackoff;
import org.slf4j.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.transaction.RollbackException;
import javax.transaction.xa.XAException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
/**
The batch JMS message loop combines best practices for dealing with transaction work:
- Messages are batched and expected to be processed in batch to minimize IO. For example, instead doing reference data
lookup per message, the batch processor is expected to do a single aggregate lookup, e.g. via an in-clause.
- The loop manages the batch size to maximize the throughput within the transaction timeout limit.
- The loop implements a sophisticated error handling framework, capable of finding messages that cause unexpected
exceptions.
- It's expected that a JVM will run many loops (each in its own thread), processing many queues/topics with similar
content. If something goes drastically wrong with one of the loops, signalling a system wide issue, the entire process
can be shutdown via the "poisoning" of the loops.
 */
public class BatchJmsMessageLoop<T extends InFlightBatch>
{
    private final static AtomicInteger consecutiveUnhandledRejections = new AtomicInteger();
    private static volatile boolean POISONED;

    private static final AtomicInteger threadNumber = new AtomicInteger();
    protected final MultiThreadedTm multiThreadedTm;
    protected final BatchProcessor<T, BatchJmsMessageLoop<T>> batchProcessor;
    protected final InterruptableBackoff interruptableBackoff;
    protected final JmsTopicConfig incomingTopicConfig;

    protected final Logger logger;
    protected final String incomingTopicName;
    protected final TopicResourcesWithTransactionXid topicResourcesWithTransactionXid;
    protected boolean markNextMessageInvalid = false;
    protected final LoopTimingStatistics loopTimingStatistics = new LoopTimingStatistics();
    protected Throwable lastMessageError;
    protected boolean splitBatchMode = false;
    protected volatile boolean running = true;
    protected int batchSizeUpperLimit = 200;
    protected int maxBatchSize = batchSizeUpperLimit;
    protected int timeouts;
    protected int underTimes;
    protected Thread runningThread;
    protected boolean batchBackoffEnabled;
    protected IncomingTopic incomingTopic;
    protected long maxMessageWaitTime = Integer.MAX_VALUE;

    private static long INITIAL_BATCH_DELAY_IN_MILLIS = 1* 1000;
    private static long MAX_BATCH_DELAY_IN_MILLIS = 5 * 60 * 1000;

    public BatchJmsMessageLoop(JmsTopicConfig incomingTopicConfig, long maxWaitTimeToConnect, MultiThreadedTm multiThreadedTm,
            Logger logger, BatchProcessor batchProcessor,
            String incomingTopicName, String uniqueXidPrefix, Object sourceAttribute, ReladomoTxIdInterfaceFinder txIdFinder)
    {
        this(incomingTopicConfig, maxWaitTimeToConnect, multiThreadedTm, logger, batchProcessor, incomingTopicName,
                uniqueXidPrefix, sourceAttribute, txIdFinder,
                new InterruptableBackoff(INITIAL_BATCH_DELAY_IN_MILLIS, MAX_BATCH_DELAY_IN_MILLIS, logger));
    }

    public BatchJmsMessageLoop(JmsTopicConfig incomingTopicConfig, long maxWaitTimeToConnect, MultiThreadedTm multiThreadedTm,
            Logger logger, BatchProcessor batchProcessor,
            String incomingTopicName, String uniqueXidPrefix, Object sourceAttribute, ReladomoTxIdInterfaceFinder txIdFinder, InterruptableBackoff interruptableBackoff)
    {
        this.multiThreadedTm = multiThreadedTm;
        this.logger = logger;
        this.batchProcessor = batchProcessor;
        this.interruptableBackoff = interruptableBackoff;
        this.incomingTopicConfig = incomingTopicConfig;
        this.incomingTopicName = incomingTopicName;
        this.topicResourcesWithTransactionXid = new TopicResourcesWithTransactionXid(txIdFinder, maxWaitTimeToConnect,
                multiThreadedTm, uniqueXidPrefix + incomingTopicName, sourceAttribute);
    }

    public int getBatchSizeUpperLimit()
    {
        return batchSizeUpperLimit;
    }

    public void setBatchSizeUpperLimit(int batchSizeUpperLimit)
    {
        this.batchSizeUpperLimit = batchSizeUpperLimit;
    }

    public LoopTimingStatistics getLoopTimingStatistics()
    {
        return loopTimingStatistics;
    }

    public long getMaxMessageWaitTime()
    {
        return maxMessageWaitTime;
    }

    public void setMaxMessageWaitTime(long maxMessageWaitTime)
    {
        this.maxMessageWaitTime = maxMessageWaitTime;
    }

    public boolean isBatchBackoffEnabled()
    {
        return batchBackoffEnabled;
    }

    public void setBatchBackoffEnabled(boolean batchBackoffEnabled)
    {
        this.batchBackoffEnabled = batchBackoffEnabled;
    }

    public OutgoingAsyncTopic connectOutgoingTopicWithRetry(JmsTopicConfig topicConfig, String outgoingThreadNamePostFix, OutgoingTopicListener outgoingTopicListener)
    {
        return topicResourcesWithTransactionXid.connectOutgoingTopicWithRetry(topicConfig, outgoingThreadNamePostFix, outgoingTopicListener);
    }

    public IncomingTopic connectIncomingTopicWithRetry(JmsTopicConfig incomingTopicConfig, InterruptableBackoff interruptableBackoff)
    {
        return topicResourcesWithTransactionXid.connectIncomingTopicWithRetry(incomingTopicConfig, interruptableBackoff);
    }

    protected MithraTransaction startTransaction()
    {
        MithraTransaction mithraTransaction = this.topicResourcesWithTransactionXid.startTransaction();
        setupOptimisticReads(mithraTransaction);
        return mithraTransaction;
    }

    protected void commitTransaction(MithraTransaction mithraTransaction, T inFlightBatch, List<JmsTopic> topicsToRestartAndRecover) throws RestartTopicException
    {
        long start = System.currentTimeMillis();
        this.topicResourcesWithTransactionXid.commit(mithraTransaction, inFlightBatch.getOutgoingMessageFutures(), topicsToRestartAndRecover);
        this.loopTimingStatistics.setFlushAndCommitTime(System.currentTimeMillis() - start);
    }

    protected void setupOptimisticReads(MithraTransaction tx)
    {
        batchProcessor.setupOptimisticReads(this, tx);
    }

    protected void recover() throws XAException
    {
        ReladomoTxIdInterface transactionXid = this.topicResourcesWithTransactionXid.recover();
        byte[] globalXid = this.topicResourcesWithTransactionXid.getGlobalTransactionId(transactionXid);
        if (globalXid != null)
        {
            boolean committed = transactionXid.isCommitted();
            recoverResources(globalXid, committed);
        }
    }

    protected void processIncoming()
    {
        this.batchProcessor.notifyLoopReady(this);
        List<JmsTopic> topicsToRestartAndRecover = FastList.newList();
        int batchSize = maxBatchSize;
        while(running)
        {
            if (shutdownIfNeeded())
            {
                this.logger.info("Message processor shutting down from listener");
                break;
            }
            loopTimingStatistics.reset();
            preTransaction();
            if (!running)
            {
                this.logger.info("Message processor shutting down");
                break;
            }
            MithraTransaction mithraTransaction = startTransaction();
            T inFlightBatch = null;
            try
            {
                inFlightBatch = fillBatch(batchSize, topicsToRestartAndRecover);
                this.logger.debug("Filled batch of size {}", inFlightBatch.size());
                processBatch(inFlightBatch);
                commitTransaction(mithraTransaction, inFlightBatch, topicsToRestartAndRecover);
                /*
                    Queue writes for elastic search only after the tx has committed
                    If the tx is not successful, the accumulated messages (in the inflight batch) get GCed
                 */
                this.batchProcessor.processPostCommit(this, inFlightBatch);
                batchSize = resetState(mithraTransaction, batchSize, inFlightBatch);
                if (inFlightBatch.validAndNotSubsumedInFlightRecordsSize() > 0)
                {
                    consecutiveUnhandledRejections.set(0);
                }
            }
            catch (MithraBusinessException e)
            {
                batchSize = handleBusinessException(e, inFlightBatch, batchSize);
                inFlightBatch = null;
            }
            catch (RollbackException e)
            {
                handleRollback(e, inFlightBatch);
                inFlightBatch = null;
            }
            catch (RestartTopicException e)
            {
                handleTopicRestart(e, inFlightBatch, topicsToRestartAndRecover);
                inFlightBatch = null;
            }
            catch (Throwable e)
            {
                batchSize = handleThrowable(e, inFlightBatch, batchSize);
                inFlightBatch = null;
            }
            if (running)
            {
                postTransaction(inFlightBatch);
                backoff(inFlightBatch);
            }
            if (running)
            {
                running = !POISONED;
            }
        }
    }

    protected void backoff(T inFlightBatch)
    {
        if (inFlightBatch != null)
        {
            backoffBeforeNextBatch(inFlightBatch.size());
        }
    }
    
    protected void backoffBeforeNextBatch(int currentBatchSize)
    {
        if (!batchBackoffEnabled)
        {
            return;
        }
        if (currentBatchSize != 0 )
        {
            interruptableBackoff.reset();
            return;
        }
        interruptableBackoff.sleep();
    }

    protected int handleThrowable(Throwable e, T inFlightBatch, int batchSize)
    {
        rollbackNow(inFlightBatch);
        this.logger.error("rolled back after unexpected exception", e);
        return splitBatchIfNeeded(inFlightBatch, batchSize, e);
    }

    protected void handleTopicRestart(RestartTopicException e, T inFlightBatch, List<JmsTopic> topicsToRestartAndRecover)
    {
        rollbackNow(inFlightBatch);
        this.logger.warn("rolled back with dead topic(s)", e);
        this.topicResourcesWithTransactionXid.restartTopics(topicsToRestartAndRecover);
        topicsToRestartAndRecover.clear();
    }

    protected void handleRollback(RollbackException e, T inFlightBatch)
    {
        rollbackNow(inFlightBatch);
        this.logger.warn("rolled back", e);
    }

    protected int handleBusinessException(MithraBusinessException e, T inFlightBatch, int batchSize)
    {
        rollbackNow(inFlightBatch);
        this.logger.error("unexpected exception. rolled back", e);
        if (e.isRetriable())
        {
            e.waitBeforeRetrying();
            return batchSize;
        }
        else
        {
            return splitBatchIfNeeded(inFlightBatch, batchSize, e);
        }
    }

    public static void setPoisoned(boolean poisoned)
    {
        BatchJmsMessageLoop.POISONED = poisoned;
    }

    public static boolean isPoisoned()
    {
        return POISONED;
    }

    protected int splitBatchIfNeeded(T inFlightBatch, int batchSize, Throwable e)
    {
        if (batchSize == 1 || (inFlightBatch != null && inFlightBatch.size() == 1))
        {
            if (sanityCheck())
            {
                this.markNextMessageInvalid = true;
                this.lastMessageError = e;
                handleUnexpectedRejection(this.consecutiveUnhandledRejections.incrementAndGet(), e, inFlightBatch);
            }
            else
            {
                logger.error("Failed sanity check, quitting");
                POISONED = true;
            }
            return batchSize;
        }
        else
        {
            if (e instanceof MithraBusinessException && ((MithraBusinessException)e).isTimedOut())
            {
                this.timeouts++;
                if (this.timeouts == 3)
                {
                    this.logger.error("Reducing max batch size from {} to {} due to timeout", maxBatchSize, maxBatchSize/2);
                    maxBatchSize = batchSize/2;
                    timeouts = 0;
                }
            }
            int newBatchSize = batchSize/2;
            splitBatchMode = true;
            this.logger.error("Reducing batch size from {} to {}", batchSize, newBatchSize);
            return newBatchSize;
        }
    }

    protected boolean sanityCheck()
    {
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < 5 * 60 * 1000)
        {
            logger.info("doing a sanity check for trade read");
            try
            {
                this.batchProcessor.sanityCheck(this);
                return true;
            }
            catch (Exception e)
            {
                logger.error("Could not read trade and minimal ref data", e);
            }
            try
            {
                Thread.sleep(10*1000); // every 10 seconds
            }
            catch (InterruptedException e)
            {
                // ignore
            }
        }
        return false;

    }

    protected void handleUnexpectedRejection(int rejectionsSoFar, Throwable e, T inFlightRecord)
    {
        this.batchProcessor.handleUnexpectedRejection(this, rejectionsSoFar, e, inFlightRecord);
    }

    private void rollbackNow(T inFlightBatch)
    {
        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (currentTransaction != null)
        {
            try
            {
                currentTransaction.rollback();
            }
            catch (MithraTransactionException e)
            {
                this.logger.error("trouble rolling back. Restart the process if this persists", e);
            }
        }
        this.batchProcessor.transactionRolledBack(this, inFlightBatch);
    }

    private void processBatch(T inFlightBatch) throws Exception
    {
        if (inFlightBatch != null && inFlightBatch.size() > 0)
        {
            this.batchProcessor.processRecords(this, inFlightBatch);
        }
    }

    private int resetState(MithraTransaction mithraTransaction, int batchSize, T inFlightBatch)
    {
        int newBatchSize = batchSize;
        this.markNextMessageInvalid = false;
        if (batchSize < maxBatchSize && !splitBatchMode)
        {
            newBatchSize *= 2;
        }
        this.lastMessageError = null;
        splitBatchMode = false;
        long end = System.currentTimeMillis();
        long txTime = end - mithraTransaction.getRealStartTime();
        this.loopTimingStatistics.setFullTransactionTime(txTime);
        if (maxBatchSize < batchSizeUpperLimit &&
                inFlightBatch.size() == this.maxBatchSize && txTime < MithraManagerProvider.getMithraManager().getTransactionTimeout()*1000/3)
        {
            this.underTimes++;
            if (this.underTimes == 5)
            {
                maxBatchSize *= 2;
                this.logger.info("setting max batch size higher {}", maxBatchSize);
                this.underTimes = 0;
            }
        }
        return newBatchSize;
    }

    protected boolean shutdownIfNeeded()
    {
        return this.batchProcessor.shutdownNow(this);
    }

    public void shutdown()
    {
        this.running = false;
        shutdownStart();
        try
        {
            this.runningThread.join(MithraManagerProvider.getMithraManager().getTransactionTimeout()*1000);
        }
        catch (InterruptedException e)
        {
            //ignore
        }
        if (this.runningThread.isAlive())
        {
            throw new RuntimeException("Could not shutdown topic processor");
        }
        closeNonJmsResources();
        this.topicResourcesWithTransactionXid.closeTopics();
    }

    protected class ProcessRunnable implements Runnable
    {
        @Override
        public void run()
        {
            logger.info("Starting Topic processor for "+incomingTopicName + " with batch size " + maxBatchSize);
            try
            {
                setupTopics();
            }
            catch (Throwable e)
            {
                logger.error("could not setup topics", e);
                setPoisoned(true);
                throw new RuntimeException(e);
            }
            try
            {
                recover();
            }
            catch (Throwable e)
            {
                logger.error("could not recover", e);
                setPoisoned(true);
                throw new RuntimeException(e);
            }
            try
            {
                waitTillSourceIsOpen();
                processIncoming();
            }
            catch (Throwable e)
            {
                logger.error("unhandled exception", e);
                setPoisoned(true);
            }
        }
    }

    public void start()
    {
        this.runningThread = new Thread(new ProcessRunnable(), "Incoming "+this.incomingTopicName+" "+threadNumber.incrementAndGet());
        runningThread.start();
    }

    public void join()
    {
        while (runningThread != null)
        {
            try
            {
                this.runningThread.join();
                return;
            }
            catch (InterruptedException e)
            {
                //ignore
            }
        }
    }

    protected void setupTopics()
    {
        this.incomingTopic = this.connectIncomingTopicWithRetry(this.incomingTopicConfig, this.interruptableBackoff);
        this.batchProcessor.setupTopicsAndResources(this);
    }

    protected void closeNonJmsResources()
    {
        this.batchProcessor.closeNonJmsResources(this);
    }

    protected void waitTillSourceIsOpen()
    {
        this.batchProcessor.waitTillSourceIsOpen(this);
    }

    protected T fillBatch(int batchSize, List<JmsTopic> topicsToRestartAndRecover) throws RestartTopicException, RollbackException
    {
        int timeoutMillis = MithraManagerProvider.getMithraManager().getTransactionTimeout() * 1000;
        try
        {
            long start = System.currentTimeMillis();
            T inFlightBatch = readMessages(timeoutMillis, batchSize);
            this.loopTimingStatistics.setReceiveAndParseTime(System.currentTimeMillis() - start);
            return inFlightBatch;
        }
        catch (RestartTopicException e)
        {
            topicsToRestartAndRecover.add(e.getTopic());
            throw e;
        }
    }

    protected T readMessages(int timeoutMillis, int batchSize) throws RollbackException, RestartTopicException
    {
        this.batchProcessor.enlistNonJmsResources(this);
        int receivedMessages = 0;
        this.incomingTopic.enlistIntoTransaction();
        T inFlightBatch = this.batchProcessor.createInFlightBatch(this);
        long start = System.currentTimeMillis();
        while(true)
        {
            if (receivedMessages == batchSize)
            {
                break;
            }
            Message message = receiveMessage(timeoutMillis, receivedMessages);
            if (message == null) break;
            if (receivedMessages == 0)
            {
                this.loopTimingStatistics.setFirstMessageWaitTime(System.currentTimeMillis() - start);
            }
            if (receivedMessages == 0 && markNextMessageInvalid)
            {
                inFlightBatch.addInvalidMessage(message, this.incomingTopicConfig.getTopicName(), this.lastMessageError);
            }
            else
            {
                inFlightBatch.addMessage(message, this.incomingTopicConfig.getTopicName());
            }
            receivedMessages++;
            if (batchProcessor.isLastMessageEndOfBatch(this, inFlightBatch))
            {
                break;
            }
        }
        this.incomingTopic.delistFromTransaction(true);
        return inFlightBatch;
    }

    private Message receiveMessage(int timeoutMillis, int receivedMessages) throws RestartTopicException
    {
        try
        {
            long waitTime = receivedMessages == 0 ? timeoutMillis/2 : 10;
            if (waitTime > this.maxMessageWaitTime)
            {
                waitTime = this.maxMessageWaitTime;
            }
            return this.incomingTopic.receive(waitTime);
        }
        catch (JMSException e)
        {
            throw new RestartTopicException("Could not read message from topic "+this.incomingTopicConfig.getTopicName(), e, this.incomingTopic);
        }
    }

    protected void postTransaction(T inFlightBatch)
    {
        this.batchProcessor.postTransaction(this, inFlightBatch);
    }

    protected void recoverResources(byte[] globalXid, boolean committed) throws XAException
    {
        this.batchProcessor.recoverNonJmsResources(this, globalXid, committed);
    }

    protected void preTransaction()
    {
        this.batchProcessor.preTransaction(this);
    }

    protected void shutdownStart()
    {
        this.batchProcessor.shutdownStart(this);
    }


}
