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


import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.extractor.AbstractStringExtractor;

import java.util.List;
import java.util.ArrayList;
import java.sql.Timestamp;
import java.lang.reflect.Method;

public class TestMithraInterfaces extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            OrderWi.class,
            OrderClassificationWi.class,
            OrderItemWi.class,
            OrderStatusWi.class,
            ReadOnlyOrderWi.class,
            ReadOnlyOrderItemWi.class,
            ReadOnlyOrderStatusWi.class,
            BitemporalOrderWi.class,
            BitemporalOrderClassificationWi.class,
            BitemporalOrderItemWi.class,
            BitemporalOrderStatusWi.class,
            ReadOnlyBitemporalOrderWi.class,
            ReadOnlyBitemporalOrderItemWi.class,
            ReadOnlyBitemporalOrderStatusWi.class,
            Order.class,
            OrderItem.class,
            OrderStatus.class
        };
    }

    public void testRetrieval() throws Exception
    {
        Operation op = OrderWiFinder.orderId().eq(1);
        OrderWi order = OrderWiFinder.findOne(op);
        assertTrue(order instanceof MithraTransactionalObject);
        assertTrue(order instanceof OrderWiImpl);

        assertEquals(1, order.getOrderId());
        assertEquals(1, order.getUserId());
        assertEquals("First order", order.getDescription());
        assertEquals("In-Progress", order.getState());
        try
        {
            OrderWi.class.getMethod("delete");
            fail("Should not get here");
        }
        catch(NoSuchMethodException e)
        {
            getLogger().info("Expected exception, "+OrderWi.class.getName()+" does not have a method called \"delete\"");
        }
    }

    public void testReadOnlyRetrieval() throws Exception
    {
        Operation op = ReadOnlyOrderWiFinder.orderId().eq(1);
        ReadOnlyOrderWi order = ReadOnlyOrderWiFinder.findOne(op);
        assertTrue(order instanceof MithraObject);
        assertTrue(order instanceof ReadOnlyOrderWiImpl);

        assertEquals(1, order.getOrderId());
        assertEquals(1, order.getUserId());
        assertEquals("First order", order.getDescription());
        assertEquals("In-Progress", order.getState());
        try
        {
            ReadOnlyOrderWi.class.getMethod("delete");
            fail("Should not get here");
        }
        catch(NoSuchMethodException e)
        {
            getLogger().info("Expected exception, "+ReadOnlyOrderWi.class.getName()+" does not have a method called \"delete\"");
        }
    }

    public void testDatedRetrieval() throws Exception
    {
        Operation op = BitemporalOrderWiFinder.orderId().eq(1);
        op = op.and(BitemporalOrderWiFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        BitemporalOrderWi order = BitemporalOrderWiFinder.findOne(op);
        assertTrue(order instanceof MithraTransactionalObject);
        assertTrue(order instanceof BitemporalOrderWiImpl);

        assertEquals(1, order.getOrderId());
        assertEquals(1, order.getUserId());
        assertEquals("First order", order.getDescription());
        assertEquals("In-Progress", order.getState());
        try
        {
            BitemporalOrderWi.class.getMethod("delete");
            fail("Should not get here");
        }
        catch(NoSuchMethodException e)
        {
            getLogger().info("Expected exception, "+BitemporalOrderWi.class.getName()+" does not have a method called \"delete\"");
        }
    }

    public void testReadOnlyDatedRetrieval() throws Exception
    {
        Operation op = ReadOnlyBitemporalOrderWiFinder.orderId().eq(1);
        op = op.and(ReadOnlyBitemporalOrderWiFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        ReadOnlyBitemporalOrderWi order = ReadOnlyBitemporalOrderWiFinder.findOne(op);
        assertTrue(order instanceof MithraObject);
        assertTrue(order instanceof ReadOnlyBitemporalOrderWiImpl);

        assertEquals(1, order.getOrderId());
        assertEquals(1, order.getUserId());
        assertEquals("First order", order.getDescription());
        assertEquals("In-Progress", order.getState());
        try
        {
            ReadOnlyBitemporalOrderWi.class.getMethod("delete");
            fail("Should not get here");
        }
        catch(NoSuchMethodException e)
        {
            getLogger().info("Expected exception, "+ReadOnlyBitemporalOrderWi.class.getName()+" does not have a method called \"delete\"");
        }
    }

    public void testTransactionalInterfaces()
    {
        Method[] methods = OrderWi.class.getMethods();
        List<String> methodNames = populateActualMethodNames(methods);
        validateInterface(expectedOrderWiMethods(), methodNames);
    }

    public void testTransactionalObjectReadOnlyInterfaces()
    {
        Method[] methods = ObjectWiRo.class.getMethods();
        List<String> methodNames = populateActualMethodNames(methods);
        validateInterface(expectedObjectWiRoMethods(), methodNames);
    }

    public void testReadOnlyInterfaces()
    {
        Method[] methods = ReadOnlyOrderWi.class.getMethods();
        List<String> methodNames = populateActualMethodNames(methods);
        validateInterface(expectedReadOnlyOrderWiMethods(), methodNames);
    }

    public void testReadOnlyObjectReadOnlyInterfaces()
    {
        Method[] methods = ReadOnlyObjectWiRo.class.getMethods();
        List<String> methodNames = populateActualMethodNames(methods);
        validateInterface(expectedReadOnlyObjectWiRoMethods(), methodNames);
    }

    public void testDatedInterfaces()
    {
        Method[] methods = BitemporalOrderWi.class.getMethods();
        List<String> methodNames = populateActualMethodNames(methods);
        validateInterface(expectedBitemporalOrderWiMethods(), methodNames);
    }

    public void testReadOnlyDatedInterfaces()
    {
        Method[] methods = ReadOnlyBitemporalOrderWi.class.getMethods();
        List<String> methodNames = populateActualMethodNames(methods);
        validateInterface(expectedReadOnlyBitemporalOrderWiMethods(), methodNames);
    }

    private List<String> populateActualMethodNames(Method[] methods)
    {
        List<String> methodNames = new ArrayList<String>();
        for(int i = 0; i < methods.length; i++)
        {
            methodNames.add(methods[i].getName());
        }
        return methodNames;
    }

    private List<String> expectedBitemporalOrderWiMethods()
    {
        List<String> l = new ArrayList<String>();
        l.add("getBusinessDateFrom");
        l.add("setBusinessDateFrom");
        l.add("isBusinessDateFromNull");
        l.add("getBusinessDateTo");
        l.add("setBusinessDateTo");
        l.add("isBusinessDateToNull");
        l.add("getProcessingDateFrom");
        l.add("setProcessingDateFrom");
        l.add("isProcessingDateFromNull");
        l.add("getProcessingDateTo");
        l.add("setProcessingDateTo");
        l.add("isProcessingDateToNull");
        l.add("getDescription");
	    l.add("setDescription");
        l.add("isDescriptionNull");
        l.add("setDescriptionUntil");
        l.add("getOrderDate");
        l.add("setOrderDate");
        l.add("isOrderDateNull");
        l.add("setOrderDateUntil");
        l.add("getOrderId");
        l.add("setOrderId");
        l.add("setOrderIdUntil");
        l.add("isOrderIdNull");
        l.add("getState");
        l.add("setState");
        l.add("isStateNull");
        l.add("setStateUntil");
        l.add("getTrackingId");
        l.add("setTrackingId");
        l.add("isTrackingIdNull");
        l.add("setTrackingIdUntil");
        l.add("getUserId");
        l.add("setUserId");
        l.add("isUserIdNull");
        l.add("setUserIdNull");
        l.add("setUserIdUntil");
        l.add("getItems");
        l.add("setItems");
        l.add("getItemsWithoutInterfaces");
        l.add("setItemsWithoutInterfaces");
        l.add("getOrderStatus");
        l.add("setOrderStatus");
        l.add("getOrderStatusWithoutInterfaces");
        l.add("setOrderStatusWithoutInterfaces");
        l.add("getBusinessDate");
        l.add("getProcessingDate");
        l.add("getClassifications");
        l.add("setClassifications");
        l.add("getClassificationById");
        return l;
    }

    private List<String> expectedReadOnlyBitemporalOrderWiMethods()
    {
        List<String> l = new ArrayList<String>();
        l.add("getBusinessDateFrom");
        l.add("setBusinessDateFrom");
        l.add("isBusinessDateFromNull");
        l.add("getBusinessDateTo");
        l.add("setBusinessDateTo");
        l.add("isBusinessDateToNull");
        l.add("getProcessingDateFrom");
        l.add("setProcessingDateFrom");
        l.add("isProcessingDateFromNull");
        l.add("getProcessingDateTo");
        l.add("setProcessingDateTo");
        l.add("isProcessingDateToNull");
        l.add("getDescription");
        l.add("setDescription");
        l.add("isDescriptionNull");

        l.add("getOrderDate");
        l.add("setOrderDate");
        l.add("isOrderDateNull");

        l.add("getOrderId");
        l.add("setOrderId");

        l.add("isOrderIdNull");
        l.add("getState");
        l.add("setState");
        l.add("isStateNull");

        l.add("getTrackingId");
        l.add("setTrackingId");
        l.add("isTrackingIdNull");

        l.add("getUserId");
        l.add("setUserId");
        l.add("isUserIdNull");
        l.add("setUserIdNull");

        l.add("getItems");
        l.add("getItemsWithoutInterface");
        l.add("getOrderStatus");
        l.add("getOrderStatusWithoutInterface");
        l.add("getBusinessDate");
        l.add("getProcessingDate");
        return l;
    }


    private List<String> expectedReadOnlyOrderWiMethods()
    {
        List<String> l = new ArrayList<String>();
        l.add("getDescription");
        l.add("setDescription");
        l.add("isDescriptionNull");
        l.add("getOrderDate");
        l.add("setOrderDate");
        l.add("isOrderDateNull");
        l.add("getOrderId");
        l.add("setOrderId");
        l.add("isOrderIdNull");
        l.add("getState");
        l.add("setState");
        l.add("isStateNull");
        l.add("getTrackingId");
        l.add("setTrackingId");
        l.add("isTrackingIdNull");
        l.add("getUserId");
        l.add("setUserId");
        l.add("isUserIdNull");
        l.add("setUserIdNull");
        l.add("getItems");
        l.add("getItemsWithoutInterfaces");
        l.add("getOrderStatus");
        l.add("getOrderStatusWithoutInterfaces");
        return l;
    }

    private List<String> expectedOrderWiMethods()
    {
        List<String> l = new ArrayList<String>();
        l.add("getDescription");
        l.add("setDescription");
        l.add("isDescriptionNull");
        l.add("getOrderDate");
        l.add("setOrderDate");
        l.add("isOrderDateNull");
        l.add("getOrderId");
        l.add("setOrderId");
        l.add("isOrderIdNull");
        l.add("getState");
        l.add("setState");
        l.add("isStateNull");
        l.add("getTrackingId");
        l.add("setTrackingId");
        l.add("isTrackingIdNull");
        l.add("getUserId");
        l.add("setUserId");
        l.add("isUserIdNull");
        l.add("setUserIdNull");
        l.add("getItems");
        l.add("setItems");
        l.add("getItemsWithoutInterfaces");
        l.add("setItemsWithoutInterfaces");
        l.add("getOrderStatus");
        l.add("setOrderStatus");
        l.add("getOrderStatusWithoutInterfaces");
        l.add("setOrderStatusWithoutInterfaces");
        l.add("getCheapItems");
        l.add("getCheapItemsWithoutInterfaces");
        l.add("getClassifications");
        l.add("setClassifications");
        l.add("getClassificationById");
        return l;
    }

    private List<String> expectedObjectWiRoMethods()
    {
        List<String> l = new ArrayList<String>();
        l.add("getObjectId");
        l.add("isObjectIdNull");
        l.add("getObjectDate");
        l.add("isObjectDateNull");
        l.add("getObjectValue");
        l.add("isObjectValueNull");
        return l;
    }

    private List<String> expectedReadOnlyObjectWiRoMethods()
    {
        List<String> l = new ArrayList<String>();
        l.add("getObjectId");
        l.add("isObjectIdNull");
        l.add("getObjectDate");
        l.add("isObjectDateNull");
        l.add("getObjectValue");
        l.add("isObjectValueNull");
        return l;
    }

    private void validateInterface(List<String> expectedMethodNames, List<String> actualMethodNames)
    {
        List<String> copy = new ArrayList(actualMethodNames);
        copy.removeAll(expectedMethodNames);
        if (!copy.isEmpty())
        {
            fail("Actual method names not found in the expected list: " + copy.toString());
        }
        assertEquals(expectedMethodNames.size(), actualMethodNames.size());
        for(int i = 0; i < expectedMethodNames.size(); i++)
        {
            String expectedMethodName = expectedMethodNames.get(i);
            assertTrue("Expected method "+expectedMethodName+" was not found", actualMethodNames.remove(expectedMethodName));
        }
        assertTrue(actualMethodNames.isEmpty());
    }

    public static class MethodNameExtractor extends AbstractStringExtractor
    {
        public void setStringValue(Object o, String newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public String stringValueOf(Object o)
        {
            return ((Method)o).getName();
        }
    }

    public void testInsert()
    {
        OrderWi order = createOrderWi(999, "OrderWi 999", "New-Order", "12345", 1);
        OrderItemWiList itemList = new OrderItemWiListImpl();
        itemList.add(createOrderItemWi(1234, 1, 100, 99.99, 95.00, "Completed"));
        itemList.add(createOrderItemWi(1235, 1, 100, 99.99, 95.00, "Completed"));
        order.setItems(itemList);
        order.setOrderStatus(createOrderStatus(99, "Foo",new Timestamp(System.currentTimeMillis()) ));
        insertOrder(order);

        OrderWi foundOrder = findOrder(999);
        assertNotNull(foundOrder);

        List<OrderItemWi> items = foundOrder.getItems();
        assertEquals(2, items.size());

        OrderStatusWi status = foundOrder.getOrderStatus();
        
    }

    private OrderWi findOrder(int i)
    {
        return OrderWiFinder.findOne(OrderWiFinder.orderId().eq(i));
    }

    private void insertOrder(OrderWi order)
    {
        ((OrderWiImpl)order).cascadeInsert();
    }

    private OrderStatusWi createOrderStatus(int statusId, String user, Timestamp updateTime)
    {
        OrderStatusWi status = new OrderStatusWiImpl();
        status.setStatus(statusId);
        status.setLastUser(user);
        status.setLastUpdateTime(updateTime);
        return status;
    }

    private OrderWi createOrderWi(int orderId, String desc, String state, String trackingId, int userId)
    {
        OrderWi order = new OrderWiImpl();
        order.setOrderId(orderId);
        order.setDescription(desc);
        order.setState(state);
        order.setTrackingId(trackingId);
        order.setUserId(userId);
        return order;
    }

    private OrderItemWi createOrderItemWi(int itemId, int prodId, double qty, double price, double discountPrice, String state )
    {
        OrderItemWi item = new OrderItemWiImpl();
        item.setId(itemId);
        item.setProductId(prodId);
        item.setQuantity(qty);
        item.setOriginalPrice(price);
        item.setDiscountPrice(discountPrice);
        item.setState(state);
        return item;
    }


    public void testBitemporalOptimizedParameterizedRelationship()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        BitemporalOrderWi order = BitemporalOrderWiFinder.findByPrimaryKey(1, now, now);
        assertNotNull(order);
        BitemporalOrderClassificationWiList classifications = order.getClassifications();
        assertEquals(1, classifications.size());
        assertEquals("Class 42", classifications.get(0).getDescription());
        assertNull(order.getClassificationById(3));
        BitemporalOrderClassificationWi classificationById = order.getClassificationById(42);
        assertNotNull(classificationById);
        assertEquals("Class 42", classificationById.getDescription());
    }

    public void testNoTemporalOptimizedParameterizedRelationship()
    {
        OrderWi order = OrderWiFinder.findByPrimaryKey(1);
        assertNotNull(order);
        OrderClassificationWiList classifications = order.getClassifications();
        assertEquals(1, classifications.size());
        assertEquals("Iowa Class", classifications.get(0).getDescription());
        assertNull(order.getClassificationById(42));
        OrderClassificationWi classificationById = order.getClassificationById(62);
        assertNotNull(classificationById);
        assertEquals("Iowa Class", classificationById.getDescription());
    }
}
