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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.AuditedOrder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderList;
import com.gs.fw.common.mithra.test.domain.Book;
import com.gs.fw.common.mithra.test.domain.BookFinder;
import com.gs.fw.common.mithra.test.domain.BookList;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.DoubleHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.Set;


public class TestCalculatedString
extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderItem.class,
                AuditedOrder.class,
                Book.class
        };
    }

    public TestCalculatedString()
    {
        super("Mithra Object Tests");
    }

    public void testToLower()
    {
        assertEquals(0, new OrderList(OrderFinder.description().eq("first order")).size());
        OrderList orderList = new OrderList(OrderFinder.description().toLowerCase().eq("first order"));
        assertEquals(1, orderList.size());
        Order order = orderList.getOrderAt(0);
        assertEquals(1, order.getOrderId());

        orderList = new OrderList(OrderFinder.description().toLowerCase().notEq("first order"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertFalse(order.getDescription().toLowerCase().equals("first order"));
        }

        orderList = new OrderList(OrderFinder.description().toLowerCase().startsWith("f"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().toLowerCase().startsWith("f"));
        }

        BookList bookList = new BookList(BookFinder.author().toLowerCase().endsWith("bloch"));
        assertEquals(1, bookList.size());
        assertEquals("Joshua Bloch", bookList.getBookAt(0).getAuthor());

        bookList = new BookList(BookFinder.author().toLowerCase().contains("bl"));
        assertEquals(1, bookList.size());
        assertEquals("Joshua Bloch", bookList.getBookAt(0).getAuthor());

        orderList = new OrderList(OrderFinder.description().toLowerCase().notStartsWith("f"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertFalse(order.getDescription().toLowerCase().startsWith("f"));
        }

        bookList = new BookList(BookFinder.author().toLowerCase().notEndsWith("bloch"));
        assertTrue(bookList.size() > 1);
        for(int i=0;i<bookList.size();i++)
        {
            assertFalse(bookList.getBookAt(i).getAuthor().toLowerCase().endsWith("bloch"));
        }

        bookList = new BookList(BookFinder.author().toLowerCase().notContains("bl"));
        assertTrue(bookList.size() > 1);
        for(int i=0;i<bookList.size();i++)
        {
            assertTrue(bookList.getBookAt(i).getAuthor().toLowerCase().indexOf("bl") < 0);
        }

        orderList = new OrderList(OrderFinder.description().toLowerCase().greaterThan("first order"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().toLowerCase().compareTo("first order") > 0);
        }

        orderList = new OrderList(OrderFinder.description().toLowerCase().greaterThanEquals("second order"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().toLowerCase().compareTo("second order") >= 0);
        }

        orderList = new OrderList(OrderFinder.description().toLowerCase().lessThan("third order"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().toLowerCase().compareTo("third order") < 0);
        }

        orderList = new OrderList(OrderFinder.description().toLowerCase().lessThanEquals("second order"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().toLowerCase().compareTo("second order") <= 0);
        }
    }

    public void testMappedToLower()
    {
        OrderItemList orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().eq("first order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().equals("first order"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().notEq("first order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(!order.getDescription().toLowerCase().equals("first order"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().startsWith("f"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().startsWith("f"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().endsWith("t order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().endsWith("t order"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().contains("t ord"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().indexOf("t ord") > 0);
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().notStartsWith("f"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(!order.getDescription().toLowerCase().startsWith("f"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().notEndsWith("user"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(!order.getDescription().toLowerCase().endsWith("user"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().notContains("use"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().indexOf("use") < 0);
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().greaterThan("first order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().compareTo("first order") > 0);
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().greaterThanEquals("second order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().compareTo("second order") >= 0);
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().lessThan("third order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().compareTo("third order") < 0);
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().toLowerCase().lessThanEquals("second order"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().toLowerCase().compareTo("second order") <= 0);
        }


    }

    public void testSubstring()
    {
        assertEquals(0, new OrderList(OrderFinder.description().eq("First")).size());
        OrderList orderList = new OrderList(OrderFinder.description().substring(0, 5).eq("First"));
        assertEquals(1, orderList.size());
        Order order = orderList.getOrderAt(0);
        assertEquals(1, order.getOrderId());

        assertEquals(0, new OrderList(OrderFinder.description().eq("first")).size());
        orderList = new OrderList(OrderFinder.description().substring(0, 5).toLowerCase().eq("first"));
        assertEquals(1, orderList.size());
        order = orderList.getOrderAt(0);
        assertEquals(1, order.getOrderId());

        orderList = new OrderList(OrderFinder.description().substring(0, 5).notEq("First"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertFalse(order.getDescription().substring(0, 5).equals("First"));
        }

        orderList = new OrderList(OrderFinder.description().substring(1, 5).startsWith("ir"));
        assertTrue(orderList.size() > 0);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().substring(1, 5).startsWith("ir"));
        }

        BookList bookList = new BookList(BookFinder.author().substring(7, -1).endsWith("Bloch"));
        assertEquals(1, bookList.size());
        assertEquals("Joshua Bloch", bookList.getBookAt(0).getAuthor());

        bookList = new BookList(BookFinder.author().substring(4,-1).contains("Bl"));
        assertEquals(1, bookList.size());
        assertEquals("Joshua Bloch", bookList.getBookAt(0).getAuthor());

        orderList = new OrderList(OrderFinder.description().substring(0, 20).notStartsWith("F"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertFalse(order.getDescription().substring(0, order.getDescription().length()).startsWith("F"));
        }

        bookList = new BookList(BookFinder.author().substring(4,-1).notEndsWith("Bloch"));
        assertTrue(bookList.size() > 1);
        for(int i=0;i<bookList.size();i++)
        {
            assertFalse(bookList.getBookAt(i).getAuthor().substring(4).endsWith("Bloch"));
        }

        bookList = new BookList(BookFinder.author().substring(4,20).notContains("Bl"));
        assertTrue(bookList.size() > 1);
        for(int i=0;i<bookList.size();i++)
        {
            assertTrue(bookList.getBookAt(i).getAuthor().substring(4).indexOf("Bl") < 0);
        }

        orderList = new OrderList(OrderFinder.description().substring(0,5).greaterThan("First"));
        assertTrue(orderList.size() > 1);
        for(int i=0;i<orderList.size();i++)
        {
            order = orderList.getOrderAt(i);
            assertTrue(order.getDescription().substring(0,5).compareTo("First") > 0);
        }

    }

    public void testMappedSubstring()
    {
        OrderItemList orderItemList = new OrderItemList(OrderItemFinder.order().description().substring(0, 5).eq("First"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().substring(0, 5).equals("First"));
        }

        orderItemList = new OrderItemList(OrderItemFinder.order().description().substring(0, 5).toLowerCase().eq("first"));
        assertTrue(orderItemList.size() > 0);
        for(int i=0;i<orderItemList.size();i++)
        {
            Order order = orderItemList.getOrderItemAt(i).getOrder();
            assertTrue(order.getDescription().substring(0, 5).toLowerCase().equals("first"));
        }

    }

    public void testAsOfAttributesWithToLower()
    {
        assertEquals(0, new AuditedOrderList(AuditedOrderFinder.description().eq("first order")).size());
        AuditedOrderList orderList = new AuditedOrderList(AuditedOrderFinder.description().toLowerCase().eq("first order"));
        assertEquals(1, orderList.size());
        AuditedOrder order = orderList.getAuditedOrderAt(0);
        assertEquals(1, order.getOrderId());
    }

    public void testIntegerToString()
    {
        StringAttribute orderIdAsString = OrderFinder.orderId().convertToStringAttribute();
        assertEquals(1, OrderFinder.findOne(orderIdAsString.eq("1")).getOrderId());
        Set<String> ids = UnifiedSet.newSet();
        ids.add("1");
        ids.add("2");
        ids.add("3");
        OrderList list = OrderFinder.findMany(orderIdAsString.in(ids));
        assertEquals(3, list.size());
    }

    public void testIntegerToStringInMapper()
    {
        Mapper mapper = OrderFinder.orderId().convertToStringAttribute().constructEqualityMapper(OrderItemFinder.orderId().convertToStringAttribute());
        mapper.setAnonymous(false);
        DoubleHashSet doubleSet = new DoubleHashSet(2000);
        for(int i = 0; i < 2000; i++)
        {
            doubleSet.add(i);
        }

        Operation op = new MappedOperation(mapper, OrderItemFinder.quantity().greaterThan(0));
        op = op.and(new MappedOperation(mapper, OrderItemFinder.discountPrice().lessThan(10000)));
        op = op.and(new MappedOperation(mapper, OrderItemFinder.quantity().in(doubleSet)));

        assertEquals(3, OrderFinder.findMany(op).size());
    }

    public void testStringToInteger()
    {
        IntegerAttribute trackingIdAsInt = OrderFinder.trackingId().convertToIntegerAttribute();
        assertEquals(1, OrderFinder.findOne(trackingIdAsInt.eq(123)).getOrderId());
        IntHashSet set = new IntHashSet();
        set.add(123);
        set.add(124);
        set.add(125);

        OrderList list = OrderFinder.findMany(trackingIdAsInt.in(set));
        assertEquals(3, list.size());
    }
}
