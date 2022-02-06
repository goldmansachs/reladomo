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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.reladomo.jms;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.reladomo.txid.ReladomoTxIdFinder;
import junit.framework.TestCase;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BatchJmsMessageLoopTest extends TestCase
{
    private static final Logger logger = LoggerFactory.getLogger(BatchJmsMessageLoopTest.class);

    protected String INCOMING_TOPIC = "RDO::JMS:TEST";
    protected static final String ECHO_TOPIC = "RDO::TEST:ECHO";

    private MultiThreadedTm multiThreadedTm = new MultiThreadedTm();
    private MithraTestResource mithraTestResource;
    private BatchJmsMessageLoop batchJmsMessageLoop;

    @Before
    public void setUp() throws NamingException, JMSException, XAException
    {
        final MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        final String mithraRuntimeConfig = "ReladomoJmsTestConfig.xml";
        mithraTestResource = new MithraTestResource(mithraRuntimeConfig);
        ConnectionManagerForTests connectionManagerForTestTradeDb = ConnectionManagerForTests.getInstance("test_trade_db");
        mithraTestResource.createSingleDatabase(connectionManagerForTestTradeDb);

        this.mithraTestResource.setUp();

        multiThreadedTm = new MultiThreadedTm();
        mithraManager.setJtaTransactionManagerProvider(new JtaProvider()
        {
            @Override
            public TransactionManager getJtaTransactionManager()
            {
                return multiThreadedTm;
            }
        });
        mithraManager.setTransactionTimeout(180); // short timeout for testing topic processor
    }

    @After
    public void tearDown()
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        InMemoryBroker.getInstance().clear();
    }

    @Test
    public void testOneMessage() throws NamingException, JMSException
    {
        JmsTopicConfig incomingTopicConfig = createIncomingConfig();
        JmsTopicConfig echoTopic = createOutputTopic(ECHO_TOPIC);

        TestBatchProcessor batchProcessor = new TestBatchProcessor(echoTopic, 1);
        batchProcessor.setDiscardEmptyBatches(true);
        this.batchJmsMessageLoop = new BatchJmsMessageLoop<TestInFlightBatch>(incomingTopicConfig, 10,
                multiThreadedTm, logger, batchProcessor, INCOMING_TOPIC, "TST1", null,
                ReladomoTxIdFinder.getFinderInstance());
        this.batchJmsMessageLoop.setBatchBackoffEnabled(false);
        this.batchJmsMessageLoop.start();
        batchProcessor.waitUntilProcessorStart();

        String testMessage = "Hello world";
        this.injectMessages(FastList.newListWith(testMessage), INCOMING_TOPIC);
        batchProcessor.waitForBatchNumber(1, 30000);
        List<byte[]> bytes = this.consumeResult(ECHO_TOPIC, 10 , 10);
        Assert.assertEquals(1, bytes.size());
        Assert.assertEquals(testMessage, new String(bytes.get(0)));
        Assert.assertFalse(batchProcessor.rolledBack);
    }

    @Test
    public void testTwoMessagesInOneTransaction() throws NamingException, JMSException
    {
        JmsTopicConfig incomingTopicConfig = createIncomingConfig();
        JmsTopicConfig echoTopic = createOutputTopic(ECHO_TOPIC);

        TestBatchProcessor batchProcessor = new TestBatchProcessor(echoTopic, 2);
        batchProcessor.setDiscardEmptyBatches(true);
        this.batchJmsMessageLoop = new BatchJmsMessageLoop<TestInFlightBatch>(incomingTopicConfig, 10,
                multiThreadedTm, logger, batchProcessor, INCOMING_TOPIC, "TST1", null,
                ReladomoTxIdFinder.getFinderInstance());
        this.batchJmsMessageLoop.setBatchBackoffEnabled(false);
        this.batchJmsMessageLoop.start();
        batchProcessor.waitUntilProcessorStart();

        String testMessage = "Hello world";
        String testMessage2 = "The sky is blue";
        this.injectMessages(FastList.newListWith(testMessage, testMessage2), INCOMING_TOPIC);
        batchProcessor.waitForBatchNumber(1, 30000);
        List<byte[]> bytes = this.consumeResult(ECHO_TOPIC, 10 , 10);
        Assert.assertEquals(2, bytes.size());
        Assert.assertEquals(testMessage, new String(bytes.get(0)));
        Assert.assertEquals(testMessage2, new String(bytes.get(1)));
        Assert.assertFalse(batchProcessor.rolledBack);
    }

    @Test
    public void testTwoMessagesInTwoTransactions() throws NamingException, JMSException
    {
        JmsTopicConfig incomingTopicConfig = createIncomingConfig();
        JmsTopicConfig echoTopic = createOutputTopic(ECHO_TOPIC);

        TestBatchProcessor batchProcessor = new TestBatchProcessor(echoTopic, 2);
        batchProcessor.setDiscardEmptyBatches(true);
        this.batchJmsMessageLoop = new BatchJmsMessageLoop<TestInFlightBatch>(incomingTopicConfig, 10,
                multiThreadedTm, logger, batchProcessor, INCOMING_TOPIC, "TST1", null,
                ReladomoTxIdFinder.getFinderInstance());
        this.batchJmsMessageLoop.setBatchBackoffEnabled(false);
        this.batchJmsMessageLoop.start();
        batchProcessor.waitUntilProcessorStart();

        String testMessage = "Hello world";
        String testMessage2 = "The sky is blue";
        this.injectMessages(FastList.newListWith(testMessage), INCOMING_TOPIC);
        batchProcessor.waitForBatchNumber(1, 30000);

        this.injectMessages(FastList.newListWith(testMessage2), INCOMING_TOPIC);
        batchProcessor.waitForBatchNumber(2, 30000);

        List<byte[]> bytes = this.consumeResult(ECHO_TOPIC, 10 , 10);
        Assert.assertEquals(2, bytes.size());
        Assert.assertEquals(testMessage, new String(bytes.get(0)));
        Assert.assertEquals(testMessage2, new String(bytes.get(1)));
        Assert.assertFalse(batchProcessor.rolledBack);
    }

    public void injectMessages(List<String> messages, String topicName) throws JMSException, NamingException
    {
        final List<byte[]> bytes = FastList.newList();
        for (int i = 0; i < messages.size(); i++)
        {
            bytes.add(messages.get(i).getBytes());
        }
        final OutgoingAsyncTopic outgoingAsyncTopic = new OutgoingAsyncTopic(new InMemoryTopicConfig(null, topicName), this.multiThreadedTm, "", null);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Future<Void> voidFuture = outgoingAsyncTopic.asyncSendMessages(bytes, null);
                voidFuture.get();
                return null;
            }
        });
        outgoingAsyncTopic.close();
    }

    protected JmsTopicConfig createIncomingConfig()
    {
        return createTopicForConsumer(INCOMING_TOPIC);
    }

    protected JmsTopicConfig createTopicForConsumer(String topicName)
    {
        return new InMemoryTopicConfig(consumerFor(topicName), topicName);
    }

    protected String consumerFor(String topic)
    {
        return "consumer:" + topic;
    }

    protected JmsTopicConfig createOutputTopic(String topicName) throws JMSException, NamingException
    {
        JmsTopicConfig topicConfig = new InMemoryTopicConfig(null, topicName);
        //register durable consumers so we can later read from these topics:
        new IncomingTopic(createTopicForConsumer(topicName), this.multiThreadedTm).close();
        return topicConfig;
    }

    public MutableList<byte[]> consumeResult(String topic, final int firstWait, final int lastWait)
    {
        try
        {
            final IncomingTopic incomingTopic = new IncomingTopic(createTopicForConsumer(topic), this.multiThreadedTm);
            final MutableList<byte[]> result = FastList.newList();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                @Override
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    incomingTopic.enlistIntoTransaction();
                    while (true)
                    {
                        long timeout = result.size() > 0 ? 10 : firstWait;
                        // Wait for a message
                        BytesMessage message = (BytesMessage) incomingTopic.receive(timeout);
                        if (message == null)
                        {
                            break;
                        }
                        int bodyLength = (int) message.getBodyLength();
                        byte[] msgBytes = new byte[bodyLength];
                        message.readBytes(msgBytes);
                        result.add(msgBytes);
                    }
                    incomingTopic.delistFromTransaction(true);
                    return null;
                }
            }, new TransactionStyle(20));
            incomingTopic.close();
            return result;
        }
        catch (Exception e)
        {
            throw new RuntimeException("failed to read messages", e);
        }
    }

    protected class TestInFlightBatch implements InFlightBatch
    {
        private List<Message> messages = FastList.newList();
        private List<Future> outgoingFutures = FastList.newList();

        @Override
        public void addMessage(Message message, String topicName)
        {
            this.messages.add(message);
        }

        public List<Message> getMessages()
        {
            return messages;
        }

        @Override
        public void addInvalidMessage(Message message, String topicName, Throwable lastMessageError)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public List<Future> getOutgoingMessageFutures()
        {
            return this.outgoingFutures;
        }

        public void addOutgingFuture(Future future)
        {
            this.outgoingFutures.add(future);
        }

        @Override
        public int size()
        {
            return this.messages.size();
        }

        @Override
        public int validAndNotSubsumedInFlightRecordsSize()
        {
            return this.messages.size();
        }
    }

    protected class TestBatchProcessor implements BatchProcessor<TestInFlightBatch, BatchJmsMessageLoop<TestInFlightBatch>>
    {
        private IntObjectHashMap<InFlightBatch> batches = IntObjectHashMap.newMap();
        private CountDownLatch latch = new CountDownLatch(1);
        private JmsTopicConfig echoTopicConfig;
        private int expectedTotalRecords;
        private boolean discardEmptyBatches = false;
        private int batchCounter = 0;
        private int totalRecords;
        private long loopStart;
        private OutgoingAsyncTopic echoTopic;
        private boolean rolledBack = false;

        public TestBatchProcessor(JmsTopicConfig echoTopicConfig, int totalRecordsToExpect)
        {
            this.echoTopicConfig = echoTopicConfig;
            this.expectedTotalRecords = totalRecordsToExpect;
        }

        public boolean isDiscardEmptyBatches()
        {
            return discardEmptyBatches;
        }

        public void setDiscardEmptyBatches(boolean discardEmptyBatches)
        {
            this.discardEmptyBatches = discardEmptyBatches;
        }

        @Override
        public void setupTopicsAndResources(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            this.echoTopic = batchJmsMessageLoop.connectOutgoingTopicWithRetry(this.echoTopicConfig, "Echo", null);
        }

        @Override
        public void recoverNonJmsResources(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, byte[] globalXid, boolean committed)
        {
            //nothing to do
        }

        @Override
        public void waitTillSourceIsOpen(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            //nothing to do
        }

        @Override
        public void notifyLoopReady(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            latch.countDown();
            this.loopStart = System.currentTimeMillis();
        }

        @Override
        public boolean shutdownNow(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            if (this.expectedTotalRecords == this.totalRecords)
            {
                return true;
            }
            if (this.loopStart < System.currentTimeMillis() - 60*1000)
            {
                return true;
            }
            return false;
        }

        @Override
        public void preTransaction(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            // nothing to do
        }

        @Override
        public void setupOptimisticReads(BatchJmsMessageLoop<TestInFlightBatch> loop, MithraTransaction tx)
        {
            //nothing to do
        }

        @Override
        public void enlistNonJmsResources(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            //nothing to do
        }

        @Override
        public TestInFlightBatch createInFlightBatch(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            return new TestInFlightBatch();
        }

        @Override
        public boolean isLastMessageEndOfBatch(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, TestInFlightBatch inFlightBatch)
        {
            return false;
        }

        @Override
        public void processRecords(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, TestInFlightBatch inFlightBatch)
        {
            this.echoTopic.sendSyncMessageClones(inFlightBatch.getMessages());
        }

        @Override
        public void processPostCommit(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, TestInFlightBatch batch)
        {
            if (discardEmptyBatches && batch.size() == 0)
            {
                return;
            }
            batchCounter++;
            batches.put(batchCounter, batch);
            this.totalRecords += batch.size();
        }

        @Override
        public void transactionRolledBack(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, TestInFlightBatch inFlightBatch)
        {
            rolledBack = true;
        }

        @Override
        public void sanityCheck(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop) throws Exception
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void handleUnexpectedRejection(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, int consequetiveUnhandledRejections, Throwable e, TestInFlightBatch inFlightBatch)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void postTransaction(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop, TestInFlightBatch inFlightBatch)
        {
            //nothing to do
        }

        @Override
        public void shutdownStart(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            //
        }

        @Override
        public void closeNonJmsResources(BatchJmsMessageLoop<TestInFlightBatch> batchJmsMessageLoop)
        {
            //
        }

        public void waitUntilProcessorStart()
        {
            while(true)
            {
                try
                {
                    if (!latch.await(1, TimeUnit.MINUTES))
                    {
                        throw new RuntimeException("Loop didn't start!");
                    }
                    return;
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }

        public void setExpectedTotalRecords(int expectedTotalRecords)
        {
            this.expectedTotalRecords = expectedTotalRecords;
        }

        public int getExpectedTotalRecords()
        {
            return expectedTotalRecords;
        }

        public synchronized InFlightBatch waitForBatchNumber(int num, long waitTime) // 1 means 1
        {
            long targetTime = System.currentTimeMillis() + waitTime;
            while(!this.batches.containsKey(num) && System.currentTimeMillis() < targetTime)
            {
                long nextWaitTime = Math.max(10, (targetTime - System.currentTimeMillis())/10);
                try
                {
                    this.wait(nextWaitTime);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
                if (this.batches.containsKey(num)) return batches.get(num);
            }
            if (this.batches.containsKey(num)) return batches.get(num);
            throw new RuntimeException("did not get "+num+" requested batches. only got "+this.batches.size()+" batches");
        }

    }

}
