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

package com.gs.fw.common.mithra.test.gsintegrator;


import com.gs.fw.aig.intgr.IntgrException;
import com.gs.fw.aig.intgr.bus.IPublisher;
import com.gs.fw.aig.intgr.bus.ITxManager;
import com.gs.fw.aig.intgr.message.MessageProperties;
import com.gs.fw.aig.intgr.store.SafeStoreDatum;
import com.gs.fw.aig.intgr.store.SafeStoreResourceName;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.gsintegrator.IntegratorMithraPublisher;
import com.gs.fw.common.mithra.gsintegrator.PublisherPlugin;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.mockobjects.sql.MockConnection;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.TimeZone;

public class TestGsIntegratorTransport extends TestCase
{
    private static Logger logger = LoggerFactory.getLogger(TestGsIntegratorTransport.class.getName());

    private MithraTestResource mithraTestResource;

    protected void setUp()
    throws Exception
    {
        mithraTestResource = new MithraTestResource("MithraConfigGsIntegrator.xml");

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(TimeZone.getDefault());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "A", "hsqldb/mithraGsIntegratorTestData.txt");

         mithraTestResource.setUp();
    }

    protected void tearDown() throws Exception
    {
        mithraTestResource.tearDown();
    }

    public void testOneMessage()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPlugin.class.getName());
            con = new MockConnection();
            con.setExpectedCommitCalls(1);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            SafeStoreDatum datum = new SafeStoreDatum();
            datum.resourceName = safeStoreResourceName.getResourceName();
            datum.seqNo = 1;
            datum.transportProps = new MessageProperties();
            datum.transportProps.setIntProperty("orderId", 15);

            ITxManager txManager = publisher.createTxManager(con);
            txManager.begin();
            publisher.send(datum);
            txManager.commit();
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(1, plugin.getMessagesProcessed());
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(15)));
        assertEquals(1, plugin.getBeginCalled());
        assertEquals(1, plugin.getEndCalled());
    }

    public void testDupeDetection()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPlugin.class.getName());
            con = new MockConnection();
            con.setExpectedCommitCalls(2);
            ITxManager txManager = publisher.createTxManager(con);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            SafeStoreDatum datum = new SafeStoreDatum();
            datum.resourceName = safeStoreResourceName.getResourceName();
            datum.seqNo = 1;
            datum.transportProps = new MessageProperties();
            datum.transportProps.setIntProperty("orderId", 15);

            txManager.begin();
            publisher.send(datum);
            txManager.commit();

            datum = new SafeStoreDatum();
            datum.resourceName = safeStoreResourceName.getResourceName();
            datum.seqNo = 1;
            datum.transportProps = new MessageProperties();
            datum.transportProps.setIntProperty("orderId", 15);

            txManager.begin();
            publisher.send(datum);
            txManager.commit();
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(1, plugin.getMessagesProcessed());
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(15)));
        assertEquals(2, plugin.getBeginCalled());
        assertEquals(2, plugin.getEndCalled());
    }

    public void testTenMessagesSeparately()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPlugin.class.getName());
            con = new MockConnection();
            con.setExpectedCommitCalls(10);
            ITxManager txManager = publisher.createTxManager(con);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            for(int i=0;i<10;i++)
            {
                SafeStoreDatum datum = new SafeStoreDatum();
                datum.resourceName = safeStoreResourceName.getResourceName();
                datum.seqNo = 1+i;
                datum.transportProps = new MessageProperties();
                datum.transportProps.setIntProperty("orderId", 15 + i);

                txManager.begin();
                publisher.send(datum);
                txManager.commit();
            }
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(10, plugin.getMessagesProcessed());
        assertEquals(10, plugin.getBeginCalled());
        assertEquals(10, plugin.getEndCalled());
    }

    public void testTenMessagesTogether()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPlugin.class.getName());
            con = new MockConnection();
            con.setExpectedCommitCalls(1);
            ITxManager txManager = publisher.createTxManager(con);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            txManager.begin();
            for(int i=0;i<10;i++)
            {
                SafeStoreDatum datum = new SafeStoreDatum();
                datum.resourceName = safeStoreResourceName.getResourceName();
                datum.seqNo = 1+i;
                datum.transportProps = new MessageProperties();
                datum.transportProps.setIntProperty("orderId", 15 + i);

                publisher.send(datum);
            }
            txManager.commit();
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(10, plugin.getMessagesProcessed());
        assertEquals(1, plugin.getBeginCalled());
        assertEquals(1, plugin.getEndCalled());
    }

    public void testRollback()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPlugin.class.getName());
            con = new MockConnection();
            con.setExpectedRollbackCalls(1);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            SafeStoreDatum datum = new SafeStoreDatum();
            datum.resourceName = safeStoreResourceName.getResourceName();
            datum.seqNo = 1;
            datum.transportProps = new MessageProperties();
            datum.transportProps.setIntProperty("orderId", 15);

            ITxManager txManager = publisher.createTxManager(con);
            txManager.begin();
            publisher.send(datum);
            txManager.rollback();
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(1, plugin.getMessagesProcessed());
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(15)));
        assertEquals(1, plugin.getBeginCalled());
        assertEquals(0, plugin.getEndCalled());
    }

    public void testRollbackWithFlush()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPlugin.class.getName());
            con = new MockConnection();
            con.setExpectedRollbackCalls(1);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            SafeStoreDatum datum = new SafeStoreDatum();
            datum.resourceName = safeStoreResourceName.getResourceName();
            datum.seqNo = 1;
            datum.transportProps = new MessageProperties();
            datum.transportProps.setIntProperty("orderId", 15);

            ITxManager txManager = publisher.createTxManager(con);
            txManager.begin();
            publisher.send(datum);
            MithraManagerProvider.getMithraManager().getCurrentTransaction().executeBufferedOperations();
            txManager.rollback();
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(1, plugin.getMessagesProcessed());
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(15)));
        assertEquals(1, plugin.getBeginCalled());
        assertEquals(0, plugin.getEndCalled());
    }

    public void testRetry()
    {
        MockConnection con = null;
        try
        {
            IPublisher publisher = new IntegratorMithraPublisher(TestPluginWithRetryException.class.getName());
            con = new MockConnection();
            con.setExpectedCommitCalls(1);

            SafeStoreResourceName safeStoreResourceName = new SafeStoreResourceName("TEST_INSTANCE", "TEST_C_TRANSPORT", "TEST_TOPIC", "TEST_P_TRANSPORT", "200603131635");
            SafeStoreDatum datum = new SafeStoreDatum();
            datum.resourceName = safeStoreResourceName.getResourceName();
            datum.seqNo = 1;
            datum.transportProps = new MessageProperties();
            datum.transportProps.setIntProperty("orderId", 15);

            ITxManager txManager = publisher.createTxManager(con);
            txManager.begin();
            publisher.send(datum);
            txManager.commit();
        }
        catch (IntgrException e)
        {
            logger.error("got exception", e);
            fail("should not have gotten exception");
        }

        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        con.verify();
        TestPlugin plugin = TestPlugin.getLastInstance();
        assertEquals(1, plugin.getMessagesProcessed());
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(15)));
        assertEquals(2, plugin.getBeginCalled());
        assertEquals(1, plugin.getEndCalled());
    }

    public static class TestPlugin implements PublisherPlugin
    {
        private static TestPlugin lastInstance;
        private int messagesProcessed;
        private int beginCalled;
        private int endCalled;

        public TestPlugin()
        {
            lastInstance = this;
        }

        public static TestPlugin getLastInstance()
        {
            return lastInstance;
        }

        public int getMessagesProcessed()
        {
            return messagesProcessed;
        }

        public void send(SafeStoreDatum datum)
        {
            this.messagesProcessed++;
            int orderId = -1;
            try
            {
                orderId = datum.transportProps.getIntProperty("orderId");
            }
            catch (IntgrException e)
            {
                logger.error("Unexpected exception", e);
                fail("Unexpected exception: " + e.getMessage());
            }
            Assert.assertTrue(orderId != -1);
            Order order = new Order();
            order.setDescription("Test order " + orderId);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setOrderId(orderId);
            order.setState("New order");
            order.setUserId(10+orderId);
            order.insert();
        }

        public void beginBatch()
        {
            this.beginCalled++;
        }

        public void endBatch()
        {
            this.endCalled++;
        }

        public int getBeginCalled()
        {
            return beginCalled;
        }

        public int getEndCalled()
        {
            return endCalled;
        }
    }

    public static class TestPluginWithRetryException extends TestPlugin
    {
        private boolean threwException = false;

        public void send(SafeStoreDatum datum)
        {
            if (!threwException)
            {
                threwException = true;
                MithraDatabaseException mithraDatabaseException = new MithraDatabaseException("test retry exception");
                mithraDatabaseException.setRetriable(true);
                throw mithraDatabaseException;
            }
            super.send(datum);
        }
    }
}
