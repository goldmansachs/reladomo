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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.math.BigDecimal;



public class TestCalculated extends TestSqlDatatypes
{

    public void testAbsoluteValueRetrievalForInteger()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        sql = "select * from PARA_DESK where abs(TAG_INT)  = 827";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().absoluteValue().eq(827));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(TAG_INT)  <> 827";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().absoluteValue().notEq(827));
        this.genericRetrievalTest(sql, desks);
        
        sql = "select * from PARA_DESK where abs(TAG_INT)  < 8";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().absoluteValue().lessThan(8));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(TAG_INT)  <= 7";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().absoluteValue().lessThanEquals(7));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(TAG_INT)  > 99";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().absoluteValue().greaterThan(99));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(TAG_INT)  >= 827";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().absoluteValue().greaterThanEquals(827));
        this.genericRetrievalTest(sql, desks);
    }

    public void testAbsoluteValueRetrievalForDouble()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        sql = "select * from PARA_DESK where abs(SIZE_DOUBLE)  = 10.54";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().eq(10.54));
        this.genericRetrievalTest(sql, desks);
        
        sql = "select * from PARA_DESK where abs(SIZE_DOUBLE)  <> 45";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().notEq(45));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(SIZE_DOUBLE)  < 43.23";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().lessThan(43.23));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(SIZE_DOUBLE)  <= 43.23";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().lessThanEquals(43.23));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(SIZE_DOUBLE)  > 10.54";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().greaterThan(10.54));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(SIZE_DOUBLE)  >= 45";
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().greaterThanEquals(45));
        this.genericRetrievalTest(sql, desks);
    }

    public void testAbsoluteValueRetrievalForBigDecimal()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        sql = "select * from PARA_DESK where abs(BIG_DOUBLE)  = 999.99";
        desks = new ParaDeskList(ParaDeskFinder.bigDouble().absoluteValue().eq(999.99));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(BIG_DOUBLE)  <> 999.99";
        desks = new ParaDeskList(ParaDeskFinder.bigDouble().absoluteValue().notEq(999.99));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(BIG_DOUBLE)  < 999.99";
        desks = new ParaDeskList(ParaDeskFinder.bigDouble().absoluteValue().lessThan(999.99));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(BIG_DOUBLE)  <= 1.9999";
        desks = new ParaDeskList(ParaDeskFinder.bigDouble().absoluteValue().lessThanEquals(1.9999));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(BIG_DOUBLE)  > 0.99999";
        desks = new ParaDeskList(ParaDeskFinder.bigDouble().absoluteValue().greaterThan(0.99999));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(BIG_DOUBLE)  >= 0.99999";
        desks = new ParaDeskList(ParaDeskFinder.bigDouble().absoluteValue().greaterThanEquals(0.99999));
        this.genericRetrievalTest(sql, desks);
    }

    public void testAbsoluteValueRetrievalForLong()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        sql = "select * from PARA_DESK where abs(CONNECTION_LONG)  = 3000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().absoluteValue().eq((long)3000000));
        this.genericRetrievalTest(sql, desks);
        
        sql = "select * from PARA_DESK where abs(CONNECTION_LONG)  <> 3000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().absoluteValue().notEq((long)3000000));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(CONNECTION_LONG)  < 2000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().absoluteValue().lessThan((long)2000000));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(CONNECTION_LONG)  <= 2000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().absoluteValue().lessThanEquals((long)2000000));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(CONNECTION_LONG)  > 2000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().absoluteValue().greaterThan((long)2000000));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(CONNECTION_LONG)  >= 3000000";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().absoluteValue().greaterThanEquals((long)3000000));
        this.genericRetrievalTest(sql, desks);
    }

    public void testAbsoluteValueRetrievalForFloat()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        sql = "select * from PARA_DESK where abs(MAX_FLOAT)  = 654.25";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().absoluteValue().eq((float)654.25));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(MAX_FLOAT)  <> 654.25";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().absoluteValue().notEq((float)654.25));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(MAX_FLOAT)  < 23423.25";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().absoluteValue().lessThan((float)23423.25));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(MAX_FLOAT)  <= 23423.25";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().absoluteValue().lessThanEquals((float)23423.25));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(MAX_FLOAT)  > 43446546";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().absoluteValue().greaterThan((float)43446546));
        this.genericRetrievalTest(sql, desks);

        sql = "select * from PARA_DESK where abs(MAX_FLOAT)  >= 654.25";
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().absoluteValue().greaterThanEquals((float)654.25));
        this.genericRetrievalTest(sql, desks);
    }

    public void testAbsoluteValueWithDated()
    {
        Operation op = AuditedOrderItemFinder.originalPrice().absoluteValue().greaterThan(12);
        AuditedOrderItemList list = new AuditedOrderItemList(op);
        assertTrue(list.size() > 0);
        for(int i= 0; i < list.size(); i++)
        {
            assertTrue(Math.abs(list.getAuditedOrderItemAt(i).getOriginalPrice()) > 12);
        }
    }

    public void testMod()
    {
        Operation op = OrderFinder.orderId().mod(3).eq(1);
        OrderList orders = new OrderList(op);

        assertTrue(orders.size() > 0);
        for(int i=0;i<orders.size();i++)
        {
            assertEquals(1, orders.get(i).getOrderId() % 3);
        }
    }
    
    public void testIntDivision()
    {
        Operation op = OrderFinder.orderId().dividedBy(3).eq(1);
        OrderList orders = new OrderList(op);

        assertTrue(orders.size() > 0);
        for(int i=0;i<orders.size();i++)
        {
            assertEquals(1, orders.get(i).getOrderId() / 3);
        }
    }

    public void testIntAddition()
    {
        Operation op = OrderFinder.orderId().plus(3).eq(4);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(1, orders.get(0).getOrderId());
    }

    public void testIntMultiplication()
    {
        Operation op = OrderFinder.orderId().times(3).eq(6);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(2, orders.get(0).getOrderId());
    }

    public void testBigDecimalDivision()
    {
        Operation op = BigOrderItemFinder.id().dividedBy(3).eq(1);
        BigOrderItemList items = new BigOrderItemList(op);

        assertTrue(items.size() > 0);
        for(int i=0;i<items.size();i++)
        {
            assertEquals(1, items.get(i).getId().intValue() / 3);
        }
    }

    public void testBigDecimalAddition()
    {
        Operation op = BigOrderItemFinder.id().plus(3).eq(4);
        BigOrderItemList items = new BigOrderItemList(op);

        assertEquals(1, items.size());
        assertEquals(1, items.get(0).getId().intValue());
    }

    public void testBigDecimalMultiplication()
    {
        Operation op = BigOrderItemFinder.id().times(3).eq(6);
        BigOrderItemList items = new BigOrderItemList(op);

        assertEquals(1, items.size());
        assertEquals(2, items.get(0).getId().intValue());
    }
    
    public void testSubtractWithAbsolute()
    {
        Operation op = OrderFinder.orderId().minus(3).absoluteValue().greaterThan(0);
        OrderList orders = new OrderList(op);

        assertTrue(orders.size() > 0);
        for(int i=0;i<orders.size();i++)
        {
            assertTrue(Math.abs(orders.get(i).getOrderId() - 3) > 0);
        }
    }

    public void testIntDivisionDouble()
    {
        Operation op = OrderFinder.orderId().dividedBy(3.0).eq(1);
        OrderList orders = new OrderList(op);

        assertTrue(orders.size() > 0);
        for(int i=0;i<orders.size();i++)
        {
            assertEquals(1.0, orders.get(i).getOrderId() / 3.0, 0.0);
        }
    }

    public void testIntAdditionDouble()
    {
        Operation op = OrderFinder.orderId().plus(3.0).eq(4);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(1, orders.get(0).getOrderId());
    }

    public void testIntAdditionBigDecimal()
    {
        Operation op = OrderFinder.orderId().plus(BigDecimal.valueOf(3.0)).eq(4);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(1, orders.get(0).getOrderId());
    }

    public void testIntMultiplicationDouble()
    {
        Operation op = OrderFinder.orderId().times(3.0).eq(6);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(2, orders.get(0).getOrderId());
    }

    public void testIntMultiplicationBigDecimal()
    {
        Operation op = OrderFinder.orderId().times(BigDecimal.valueOf(3.0)).eq(6);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(2, orders.get(0).getOrderId());
    }

    public void testSubtractWithAbsoluteDouble()
    {
        Operation op = OrderFinder.orderId().minus(3.0).absoluteValue().greaterThan(0);
        OrderList orders = new OrderList(op);

        assertTrue(orders.size() > 0);
        for(int i=0;i<orders.size();i++)
        {
            assertTrue(Math.abs(orders.get(i).getOrderId() - 3.0) > 0);
        }
    }

    public void testIntAttributeAddition()
    {
        Operation op = OrderFinder.orderId().plus(OrderFinder.userId()).eq(2);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(1, orders.get(0).getOrderId());
    }

    public void testSubtractWithAbsoluteDoubleWithRelationship()
    {
        Operation op = OrderItemFinder.order().orderId().minus(3.0).absoluteValue().greaterThan(0);
        OrderItemList items = new OrderItemList(op);

        assertTrue(items.size() > 0);
        for(int i=0;i<items.size();i++)
        {
            assertTrue(Math.abs(items.get(i).getOrderId() - 3.0) > 0);
        }
    }

    //todo: fix this.it's very because it violates one of the core assumptions about attributes. this attributes belongs to multiple portals
    public void xtestIntAttributeAdditionWithRelationship()
    {
        Operation op = OrderFinder.orderId().plus(OrderFinder.items().productId()).eq(2);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(1, orders.get(0).getOrderId());
    }

    //todo: fix this.it's very because it violates one of the core assumptions about attributes. this attributes belongs to multiple portals
    public void xtestIntAttributeAdditionWithRelationshipReverse()
    {
        Operation op = OrderFinder.items().productId().plus(OrderFinder.orderId()).eq(2);
        OrderList orders = new OrderList(op);

        assertEquals(1, orders.size());
        assertEquals(1, orders.get(0).getOrderId());
    }

}
