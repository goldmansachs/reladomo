/*
 Copyright 2019 Goldman Sachs.
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import junit.framework.TestCase;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class TestUtf8TestCharset extends TestCase
{
    private static final Logger logger = LoggerFactory.getLogger(TestUtf8TestCharset.class.getName());

    protected static final String MITHRA_TEST_DATA_FILE_PATH = "testdata/";
    private MithraTestResource mithraTestResource;

    protected void setUp() throws Exception
    {
        mithraTestResource = buildMithraTestResource();
        mithraTestResource.setCharset(Charset.forName("UTF-8"));
        mithraTestResource.setRestrictedClassList(new Class[] { Order.class });

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "A", MITHRA_TEST_DATA_FILE_PATH + "testUtf8.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();

    }

    protected MithraTestResource buildMithraTestResource()
    {
        String xmlFile = System.getProperty("mithra.xml.config");
        return new MithraTestResource(xmlFile);
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        mithraTestResource.tearDown();
    }

    public void testUtfCharacters() throws Exception
    {
        Order o = OrderFinder.findOne(OrderFinder.orderId().eq(55));
        byte[] bytes = new byte[3];
        bytes[0] = -23;
        bytes[1] = -96;
        bytes[2] = -111;
        String x = new String(bytes, "UTF-8");
        if (!o.getDescription().startsWith(x))
        {
            String msg = "";
            for(byte b: o.getDescription().getBytes("UTF-8"))
            {
                msg += b + " ";
            }
            logger.info("Chars for order 55: "+msg);
        }
        OrderList orders = OrderFinder.findMany(OrderFinder.description().startsWith(x));
        Assert.assertEquals(2, orders.size());
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(4));
        bytes[0] = -32;
        bytes[1] = -92;
        bytes[2] = -81;
        Assert.assertTrue(order.getDescription().endsWith(new String(bytes, "UTF-8")));
    }
}
