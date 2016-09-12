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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.*;


public class TestTransactionalObjectAttributesBehavior extends MithraTestAbstract
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
            Order.class,
            OrderItem.class          
        };
    }
    public void testReadonlyAttributeInMemoryNoTx() throws SQLException
    {
        OrderItem orderitem = new OrderItem();
        int orderitemId = 1019;
        orderitem.setId(orderitemId);

        int productId = 23;
        orderitem.setProductId(productId);

        assertEquals(productId, orderitem.getProductId());
    }

    public void testReadonlyAttributeInMemoryTx() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderItem orderitem = new OrderItem();
        int orderitemId = 1019;
        orderitem.setId(orderitemId);

        int productId = 23;
        orderitem.setProductId(productId);

        assertEquals(productId, orderitem.getProductId());
        tx.commit();
        assertEquals(productId, orderitem.getProductId());
    }


    public void testReadonlyAttributePersistNoTx()
    {
        int orderId = 1;
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        OrderItem orderItem = order.getItems().getOrderItemAt(0);

        int productID = orderItem.getProductId();

        try
        {
            orderItem.setProductId(productID+1);
            fail("should have thrown an exception");
        }
        catch (MithraBusinessException e)
        {
            // expected
        }
    }

    public void testReadonlyAttributePersistSameTx()
    {
        int orderId = 1;

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));

        OrderItem orderItem = order.getItems().getOrderItemAt(0);

        int productID = orderItem.getProductId();

        try
        {
            orderItem.setProductId(productID+1);
            fail("should have thrown an exception");
        }
        catch (MithraBusinessException e)
        {
            // expected
        }
        finally
        {
            tx.rollback();
        }
    }
}

