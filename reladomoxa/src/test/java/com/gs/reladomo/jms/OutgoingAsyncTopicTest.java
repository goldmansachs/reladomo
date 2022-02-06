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

import com.gs.fw.common.mithra.JtaProvider;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.reladomo.txid.ReladomoTxIdFinder;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.NamingException;
import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class OutgoingAsyncTopicTest extends TestCase
{
    private MultiThreadedTm multiThreadedTm = new MultiThreadedTm();
    private MithraTestResource mithraTestResource;
    private IncomingTopic incomingTopic;
    private TopicResourcesWithTransactionXid topicResourcesWithTransactionXid;
    private OutgoingAsyncTopic outTopic;
    private OutgoingAsyncTopic asyncOutTopic;
    private MithraTransaction mithraTransaction;

    @Before
    public void setUp() throws NamingException, JMSException, XAException
    {
        setupMithra();
        JmsTopicConfig outTopicCfg = createOutputTopic("out");
        JmsTopicConfig asyncOutTopicCfg = createOutputTopic("out");
        asyncOutTopicCfg.setAsync(true);
        outTopic = new OutgoingAsyncTopic(outTopicCfg, this.multiThreadedTm, "", null);
        asyncOutTopic = new OutgoingAsyncTopic(asyncOutTopicCfg, this.multiThreadedTm, "", null);
        topicResourcesWithTransactionXid = new TopicResourcesWithTransactionXid(ReladomoTxIdFinder.getFinderInstance(), 100,
                multiThreadedTm, "test"+outTopic.getTopicName(), null);
        topicResourcesWithTransactionXid.connectOutgoingTopicWithRetry(outTopic.getConfig(), "a", null);
        topicResourcesWithTransactionXid.recover();
    }

    @After
    public void tearDown()
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
    }

    protected void setupMithra()
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
        incomingTopic = new IncomingTopic(createTopicForConsumer(topicName), this.multiThreadedTm);
        incomingTopic.close();
        return topicConfig;
    }

    @Test
    public void testSendSetsProperties() throws NamingException, JMSException, RollbackException, RestartTopicException, XAException
    {
        mithraTransaction = topicResourcesWithTransactionXid.startTransaction();

        UnifiedMap<String, Object> msgProperties = UnifiedMap.<String, Object>newMap();
        msgProperties.put("Hello", "World");
        Future<Void> voidFuture = outTopic.asyncSendMessages(FastList.newListWith("msg1".getBytes()), msgProperties);
        topicResourcesWithTransactionXid.commit(mithraTransaction, FastList.<Future>newListWith(voidFuture), FastList.<JmsTopic>newList());

        Message messageRecvd = incomingTopic.receive(100);
        assertEquals("msg1", JmsUtil.getMessageBodyAsString(messageRecvd));
        assertEquals("World", messageRecvd.getStringProperty("Hello"));
    }

    @Test
    public void testSendSetsPropertiesAsync() throws NamingException, JMSException, RollbackException, RestartTopicException, XAException
    {
        mithraTransaction = topicResourcesWithTransactionXid.startTransaction();

        UnifiedMap<String, Object> msgProperties = UnifiedMap.<String, Object>newMap();
        msgProperties.put("Hello", "World");
        Future<Void> voidFuture = asyncOutTopic.asyncSendMessages(FastList.newListWith("msg1".getBytes()), msgProperties);
        topicResourcesWithTransactionXid.commit(mithraTransaction, FastList.<Future>newListWith(voidFuture), FastList.<JmsTopic>newList());

        Message messageRecvd = incomingTopic.receive(100);
        assertEquals("msg1", JmsUtil.getMessageBodyAsString(messageRecvd));
        assertEquals("World", messageRecvd.getStringProperty("Hello"));
    }

    @Test
    public void testCloneSendPreservesNullProperties() throws NamingException, JMSException, RollbackException, RestartTopicException, XAException
    {
        mithraTransaction = topicResourcesWithTransactionXid.startTransaction();

        InMemoryBytesMessage messageToSend = new InMemoryBytesMessage();
        messageToSend.writeBytes("msg1".getBytes());

        Future<Void> voidFuture = outTopic.sendSyncMessageClones(FastList.<Message>newListWith(messageToSend));
        topicResourcesWithTransactionXid.commit(mithraTransaction, FastList.<Future>newListWith(voidFuture), FastList.<JmsTopic>newList());

        Message messageRecvd = incomingTopic.receive(100);
        assertNull(messageRecvd.getStringProperty("Hello"));
    }

    @Test
    public void testCloneSendPreservesNonNullProperty() throws NamingException, JMSException, RollbackException, RestartTopicException, XAException
    {
        mithraTransaction = topicResourcesWithTransactionXid.startTransaction();

        InMemoryBytesMessage messageToSend = new InMemoryBytesMessage();
        messageToSend.writeBytes("msg1".getBytes());
        messageToSend.setStringProperty("Hello", "World");

        Future<Void> voidFuture = outTopic.sendSyncMessageClones(FastList.<Message>newListWith(messageToSend));
        topicResourcesWithTransactionXid.commit(mithraTransaction, FastList.<Future>newListWith(voidFuture), FastList.<JmsTopic>newList());

        Message messageRecvd = incomingTopic.receive(100);
        assertEquals("World", messageRecvd.getStringProperty("Hello"));
    }

}