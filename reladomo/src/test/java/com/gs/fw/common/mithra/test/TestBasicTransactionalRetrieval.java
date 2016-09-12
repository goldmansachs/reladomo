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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import java.sql.SQLException;



public class TestBasicTransactionalRetrieval extends MithraTestAbstract
{

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class
        };
    }

    public void testRetrieveOneRow()
            throws SQLException
    {
        int orderId = 1;
        String sql = "select * from APP.ORDERS where ORDER_ID = " + orderId;

        OrderList list = new OrderList(OrderFinder.orderId().eq(orderId));

        this.genericRetrievalTest(sql, list);
        assertEquals(list.size(), 1);
        Order order = list.getOrderAt(0);
        assertEquals(order.getOrderId(), 1);
    }

    public void testRetrieveOneRowAndCache()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        int dbHits = getRetrievalCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertEquals(dbHits, getRetrievalCount());
        assertSame(order, order2);
        Order order3 = OrderFinder.findOne(OrderFinder.description().eq("First order"));
        assertSame(order3, order);
    }

}
