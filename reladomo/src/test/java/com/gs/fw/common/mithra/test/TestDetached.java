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


import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;
import junit.framework.Assert;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.*;
import java.util.List;

public class TestDetached extends MithraTestAbstract
{
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            OptimisticOrder.class,
            Order.class,
            OrderParentToChildren.class,
            OrderItem.class,
            OrderStatus.class,
            OrderWi.class,
            OrderItemWi.class,
            OrderStatusWi.class,
            OrderItemStatus.class,
            Employee.class,
            ControlDefinition.class,
            ControlListDefinitionMapping.class,
            ListDefinition.class,
            TestControlledEntry.class,
            TestRestrictedEntity.class,
            TestRestrictedItem.class,
            TestRestrictedItemComment.class,
            MithraTestSequence.class,
            ExchangeRate.class,
            ExchangeRateChild.class,
            FileDirectory.class,
            DatedAccount.class,
                AuditedOrder.class,
                AuditedOrderStatusTwo.class,
                AuditedOrderStatus.class
        };
    }

    public void testOneObjectDetachedInsert() throws SQLException
    {
        int orderId = 1234000;
        Order order = new Order();
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setOrderId(orderId);
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        Order inserted = order.copyDetachedValuesToOriginalOrInsertIfNew();
        int dbCount = this.getRetrievalCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertSame(inserted, order2);
        assertNotSame(order, order2);
        assertEquals(dbCount, this.getRetrievalCount());
        String sql = "select * from APP.ORDERS where ORDER_ID = " + orderId;

        OrderList list = new OrderList(OrderFinder.orderId().eq(orderId));

        this.genericRetrievalTest(sql, list);
    }

    public void testOneObjectDetachedUpdate() throws SQLException
    {
        int orderId = 1;
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertNotSame(order, originalOrder);
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        assertFalse(originalOrder.getDescription().equals(order.getDescription()));
        Order updated = order.copyDetachedValuesToOriginalOrInsertIfNew();
        assertEquals(order.getDescription(), originalOrder.getDescription());
        assertEquals(order.getState(), originalOrder.getState());
        assertEquals(order.getTrackingId(), originalOrder.getTrackingId());
        assertEquals(order.getOrderDate(), originalOrder.getOrderDate());
        assertEquals(order.getUserId(), originalOrder.getUserId());
        int dbCount = this.getRetrievalCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertSame(updated, order2);
        assertSame(originalOrder, order2);
        assertEquals(dbCount, this.getRetrievalCount());
        String sql = "select * from APP.ORDERS where ORDER_ID = " + orderId;

        OrderList list = new OrderList(OrderFinder.orderId().eq(orderId));

        this.genericRetrievalTest(sql, list);

    }

    private void checkOrderDoesNotExist(int orderId)
            throws SQLException
    {
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(orderId)));
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();
    }

    public void testOneObjectDetachedDelete() throws SQLException
    {
        int orderId = 1;
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertTrue(order != originalOrder);
        order.delete();
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertNull(order2);
        checkOrderDoesNotExist(orderId);
    }

    public void testOneObjectDetachedDeleteWithMutablePk() throws SQLException
    {
        ExchangeRate originalRate = findExchangeRate("A", "USD", 10, new Timestamp(dateTimeFormatter.parseMillis("2004-09-30 18:30:00.0")));
        assertNotNull(originalRate);
        ExchangeRate rate = originalRate.getDetachedCopy();
        rate.delete();
        rate.copyDetachedValuesToOriginalOrInsertIfNew();
        ExchangeRateFinder.clearQueryCache();
        assertNull(findExchangeRate("A", "USD", 10, new Timestamp(dateTimeFormatter.parseMillis("2004-09-30 18:30:00.0"))));
    }

    public void testDetachedList() throws SQLException
    {
        OrderList ol = new OrderList(OrderFinder.orderId().greaterThan(1));
        OrderList detached = ol.getDetachedCopy();
        int orderId = detached.getOrderAt(0).getOrderId();
        detached.remove(0);
        Order order = detached.getOrderAt(0);
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        order = new Order();
        order.setDescription("test 2");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()+1000));
        order.setOrderId(10001);
        order.setState("test state2");
        order.setTrackingId("t10001");
        order.setUserId(18);
        detached.add(order);

        order = new Order();
        detached.add(order);
        detached.remove(detached.size() - 1);

        detached.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        checkOrderDoesNotExist(orderId);
        ol = new OrderList(OrderFinder.orderId().greaterThan(1));
        String sql = "select * from APP.ORDERS where ORDER_ID > " + 1;
        this.genericRetrievalTest(sql, ol);
    }

    public void testDetachedListRemoveByObject() throws SQLException
    {
        OrderList ol = new OrderList(OrderFinder.orderId().greaterThan(1));
        OrderList detached = ol.getDetachedCopy();
        Order o = detached.getOrderAt(0);
        int orderId = o.getOrderId();
        detached.remove(o);
        Order order = detached.getOrderAt(0);
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        order = new Order();
        order.setDescription("test 2");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()+1000));
        order.setOrderId(10001);
        order.setState("test state2");
        order.setTrackingId("t10001");
        order.setUserId(18);
        detached.add(order);

        order = new Order();
        detached.add(order);
        detached.remove(detached.size() - 1);

        detached.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        checkOrderDoesNotExist(orderId);
        ol = new OrderList(OrderFinder.orderId().greaterThan(1));
        String sql = "select * from APP.ORDERS where ORDER_ID > " + 1;
        this.genericRetrievalTest(sql, ol);
    }

    public void testDetachedListRemoveAll() throws SQLException
    {
        OrderList ol = new OrderList(OrderFinder.orderId().greaterThan(1));
        ol.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        OrderList detached = ol.getDetachedCopy();
        List toRemove = ol.subList(0, ol.size()/2);
        int lastToRemove = ((Order)toRemove.get(toRemove.size() -1)).getOrderId();
        detached.removeAll(toRemove);

        detached.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        checkOrderDoesNotExist(lastToRemove);
        ol = new OrderList(OrderFinder.orderId().greaterThan(1));
        String sql = "select * from APP.ORDERS where ORDER_ID > " + 1;
        this.genericRetrievalTest(sql, ol);
    }


    public void testSimpleDetachedList() throws SQLException
    {
        OrderList ol = new OrderList();
        ol.add(OrderFinder.findOne(OrderFinder.orderId().eq(1)));
        ol.add(OrderFinder.findOne(OrderFinder.orderId().eq(2)));

        OrderList detached = ol.getDetachedCopy();

        int orderId = detached.getOrderAt(0).getOrderId();
        detached.remove(0);
        Order order = detached.getOrderAt(0);
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        order = new Order();
        order.setDescription("test 2");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()+1000));
        order.setOrderId(10001);
        order.setState("test state2");
        order.setTrackingId("t10001");
        order.setUserId(18);
        detached.add(order);

        order = new Order();
        detached.add(order);
        detached.remove(detached.size() - 1);

        detached.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        checkOrderDoesNotExist(orderId);

    }

    public void testObjectWithSourceAttribute() throws SQLException
    {
        Employee emp = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().eq(1)));
        assertNotNull(emp);

        Employee detached = emp.getDetachedCopy();
        detached.setName("detached test");
        detached.copyDetachedValuesToOriginalOrInsertIfNew();
        assertEquals(emp.getName(), "detached test");
        Employee other = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(1).and(EmployeeFinder.id().eq(1)));
        assertNotNull(other);

        assertFalse(other.getName().equals(emp.getName()));
    }

    public void testOriginalObjectDeleted() throws SQLException
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        originalOrder.delete();

        order.setDescription("test 1");
        try
        {
            order.copyDetachedValuesToOriginalOrInsertIfNew();
            fail("erroneously updated a deleted object");
        }
        catch(MithraDeletedException e)
        {
            // good
        }
    }

    public void testDetachUpdateCommit()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("detached test");
        tx.commit();
        assertEquals("detached test", order.getDescription());
    }

    public void testDetachUpdateRollback()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("detached test");
        tx.rollback();
        assertEquals(originalOrder.getDescription(), order.getDescription());
    }

    public void testDetachDeleteCommit() throws SQLException
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.delete();
        tx.commit();
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        checkOrderDoesNotExist(1);
    }

    public void testDetachDeleteRollback()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.delete();
        tx.rollback();
        assertEquals(originalOrder.getDescription(), order.getDescription());
    }

    public void testDetachedReset()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        order.setDescription("test 1700");
        assertEquals(order.getDescription(), "test 1700");
        order.resetFromOriginalPersistentObject();
        assertEquals(originalOrder.getDescription(), order.getDescription());
    }

    public void testDetachedResetInTransactionWithCommit()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        order.setDescription("test 1700");
        assertEquals(order.getDescription(), "test 1700");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.commit();
        assertEquals(originalOrder.getDescription(), order.getDescription());

    }

    public void testDetachedResetInTransactionWithRollback()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        order.setDescription("test 1700");
        assertEquals(order.getDescription(), "test 1700");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.rollback();
        assertEquals(order.getDescription(), "test 1700");
    }

    public void testDetachDeleteReset()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        order.delete();
        order.resetFromOriginalPersistentObject();
        assertEquals(originalOrder.getDescription(), order.getDescription());
    }

    public void testDetachDeleteResetInTransactionWithCommit()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        order.delete();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.commit();
        assertEquals(originalOrder.getDescription(), order.getDescription());
    }

    public void testDetachDeleteResetInTransactionWithRollback() throws SQLException
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        order.delete();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.rollback();
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        checkOrderDoesNotExist(1);
    }

    public void testNullRelationship()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        Order order = originalOrder.getDetachedCopy();
        OrderItemList orderItemList = order.getItems();
        orderItemList.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        OrderItem item = orderItemList.get(0);
        OrderItemStatus orderItemStatus = item.getOrderItemStatus();
        assertNull(orderItemStatus);
    }

    public void testIsModified()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment());
        order.setDescription("test 1800");
        assertTrue(order.isModifiedSinceDetachment());
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment());
        order.delete();
        assertTrue(order.isModifiedSinceDetachment());
    }

    public void testIsModifiedByModifyingMutablePK()
    {
        ExchangeRate originalRate = findExchangeRate("A", "USD", 10, new Timestamp(dateTimeFormatter.parseMillis("2004-09-30 18:30:00.0")));
        assertNotNull(originalRate);
        ExchangeRate rate = originalRate.getDetachedCopy();
        assertFalse(rate.isModifiedSinceDetachment());
        rate.setSource(11);
        assertTrue(rate.isModifiedSinceDetachment());
        rate.resetFromOriginalPersistentObject();
        assertFalse(rate.isModifiedSinceDetachment());
        rate.setDate(new Timestamp(System.currentTimeMillis()));
        assertTrue(rate.isModifiedSinceDetachment());
        rate.resetFromOriginalPersistentObject();
        rate.setSourceNull();
        assertTrue(rate.isModifiedSinceDetachment());
    }

    public void testIsModifiedInTransactionWithCommit()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        assertTrue(order.isModifiedSinceDetachment());
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment());
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        assertFalse(order.isModifiedSinceDetachment());
    }

    public void testIsModifiedInTxWithCommitByModifyingMutablePK()
    {
        ExchangeRate originalRate = findExchangeRate("A", "USD", 10, new Timestamp(dateTimeFormatter.parseMillis("2004-09-30 18:30:00.0")));
        assertNotNull(originalRate);
        ExchangeRate rate = originalRate.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertFalse(rate.isModifiedSinceDetachment());
        rate.setSource(11);
        assertTrue(rate.isModifiedSinceDetachment());
        tx.commit();
        assertTrue(rate.isModifiedSinceDetachment());
        rate.copyDetachedValuesToOriginalOrInsertIfNew();
        assertFalse(rate.isModifiedSinceDetachment());
    }

    public void testIsModifiedInTransactionWithRollback()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        assertTrue(order.isModifiedSinceDetachment());
        tx.rollback();
        assertFalse(order.isModifiedSinceDetachment());
    }

    public void testIsModifiedInTransactionWithRollbackModifyingMutablePK()
    {
        ExchangeRate originalRate = findExchangeRate("A", "USD", 10, new Timestamp(dateTimeFormatter.parseMillis("2004-09-30 18:30:00.0")));
        assertNotNull(originalRate);
        ExchangeRate rate = originalRate.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertFalse(rate.isModifiedSinceDetachment());
        rate.setSource(11);
        assertTrue(rate.isModifiedSinceDetachment());
        tx.rollback();
        assertFalse(rate.isModifiedSinceDetachment());
    }

    public void testIsModifiedByAttribute()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.description()));
        order.setDescription("test 1800");
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.description()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.userId()));
        order.setUserIdNull();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.userId()));
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.description()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.userId()));
        Order inmemory = new Order();
        inmemory.setOrderId(12349738);
        assertTrue(inmemory.isModifiedSinceDetachment(OrderFinder.description()));
    }

    public void testIsModifiedByAttribute2()
    {
        ExchangeRate originalRate = findExchangeRate("A", "USD", 10, new Timestamp(dateTimeFormatter.parseMillis("2004-09-30 18:30:00.0")));
        assertNotNull(originalRate);
        ExchangeRate rate = originalRate.getDetachedCopy();

        rate.setSource(99);
        assertTrue(rate.isModifiedSinceDetachment(ExchangeRateFinder.source()));
        assertFalse(rate.isModifiedSinceDetachment(ExchangeRateFinder.date()));
        rate.resetFromOriginalPersistentObject();
        rate.setSourceNull();
        assertTrue(rate.isModifiedSinceDetachment(ExchangeRateFinder.source()));
        rate.resetFromOriginalPersistentObject();
        assertFalse(rate.isModifiedSinceDetachment(ExchangeRateFinder.source()));
        assertFalse(rate.isModifiedSinceDetachment(ExchangeRateFinder.date()));
    }

    public void testIsModifiedByAttributeInTransactionWithCommit()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        order.setUserIdNull();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.description()));
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.userId()));
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.description()));
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.userId()));
    }

    public void testIsModifiedByAttributeInTransactionWithRollback()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.description()));
        order.setUserIdNull();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.userId()));
        tx.rollback();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.description()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.userId()));
    }

    public void testIsModifiedByRelatedObjects()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
        OrderItemList orderItemList = order.getItems();
        orderItemList.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        OrderItem item = orderItemList.get(0);
        item.setQuantity(999999.99);
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
        item.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
        item.delete();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
        order.getItems().remove(0);
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        order.getItems().add(new OrderItem());
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
        assertNotNull(originalOrder.getOrderStatus());
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
        order.setOrderStatus(null);
        order.setOrderStatus(null);
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.orderStatus()));
    }

    public void testIsModifiedByRelatedObjectsInTransactionWithCommit()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        OrderItemList orderItemList = order.getItems();
        orderItemList.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        OrderItem item = orderItemList.get(0);
        item.setQuantity(999999.99);
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));

        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        order.getItems();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));

        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        order.getItems().add(new OrderItem());
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
    }

    public void testIsModifiedByRelatedObjectsInTransactionWithRollback()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        OrderItemList orderItemList = order.getItems();
        orderItemList.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        OrderItem item = orderItemList.get(0);
        item.delete();
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));
        tx.rollback();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
    }

    public void testIsModifiedByDeeplyRelatedObjects()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        OrderItemList orderItemList = order.getItems();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        orderItemList.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        OrderItem item = orderItemList.get(0);
        OrderItemStatus orderItemStatus = item.getOrderItemStatus();
        assertFalse(order.isModifiedSinceDetachment(OrderFinder.items()));
        orderItemStatus.setLastUser("gonzra");
        assertTrue(order.isModifiedSinceDetachment(OrderFinder.items()));

        Order originalOrder2 = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        Order order2 = originalOrder2.getDetachedCopy();
        assertFalse(order2.isModifiedSinceDetachment(OrderFinder.items()));
        OrderItemList orderItemList2 = order.getItems();
        assertFalse(order2.isModifiedSinceDetachment(OrderFinder.items()));
        orderItemList2.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        OrderItem item2 = orderItemList2.get(0);
        item2.setOrderItemStatus(null);
        assertFalse(order2.isModifiedSinceDetachment(OrderFinder.items()));
    }

    public void testManyToManyInsertNew()
    {
        final ControlDefinitionList cdlist = new ControlDefinitionList();
        final ControlDefinition cd = new ControlDefinition(InfinityTimestamp.getParaInfinity());
        cd.setControlCode(1);
        cdlist.add(cd);

        final ListDefinition ld = new ListDefinition(InfinityTimestamp.getParaInfinity());
        ld.setListId("xyz");

        ControlListDefinitionMapping mapping = new ControlListDefinitionMapping(InfinityTimestamp.getParaInfinity());
        mapping.setControlDefinition(cd);
        mapping.setListDefinition(ld);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                cdlist.cascadeInsertAll();
                ld.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });

        assertNotNull(ListDefinitionFinder.findOneBypassCache(ListDefinitionFinder.listId().eq("xyz")));
        assertNotNull(ControlDefinitionFinder.findOneBypassCache(ControlDefinitionFinder.controlCode().eq(1)));
        assertEquals(1, ld.getControlDefinitionMappings().size());
    }

    public void testManyToManyAddNewMappingWithNewRelated()
    {
        final ControlDefinitionList cdlist = new ControlDefinitionList();

        final ListDefinition ld2 = ListDefinitionFinder.findOne(ListDefinitionFinder.listId().eq("abc")).getDetachedCopy();
        final ControlDefinition cd2 = new ControlDefinition(InfinityTimestamp.getParaInfinity());
        cd2.setControlCode(2);
        cdlist.add(cd2);

        ControlListDefinitionMapping mapping2 = new ControlListDefinitionMapping(InfinityTimestamp.getParaInfinity());
        mapping2.setControlDefinition(cd2);
        mapping2.setListDefinition(ld2);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                cdlist.cascadeInsertAll();
                ld2.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });

        ListDefinitionFinder.clearQueryCache();
        ControlDefinitionFinder.clearQueryCache();
        ControlListDefinitionMappingFinder.clearQueryCache();

        assertNotNull(ControlDefinitionFinder.findOneBypassCache(ControlDefinitionFinder.controlCode().eq(2)));
        assertEquals(1, ListDefinitionFinder.findOne(ListDefinitionFinder.listId().eq("abc")).getControlDefinitionMappings().size());
    }

    public void testManyToManyAddNewMappingWithNewRelated2()
    {
        final ControlDefinitionList cdlist = new ControlDefinitionList();

        final ListDefinition ld = ListDefinitionFinder.findOne(ListDefinitionFinder.listId().eq("abc")).getDetachedCopy();
        final ControlDefinition cd2 = new ControlDefinition(InfinityTimestamp.getParaInfinity());
        cd2.setControlCode(2);
        cdlist.add(cd2);

        ControlListDefinitionMapping mapping = new ControlListDefinitionMapping(InfinityTimestamp.getParaInfinity());
        mapping.setControlDefinition(cd2);
        mapping.setListDefinition(ld);

        ControlDefinition existing = ControlDefinitionFinder.findOne(ControlDefinitionFinder.controlCode().eq(1000)).getDetachedCopy();
        ControlListDefinitionMapping mapping2 = new ControlListDefinitionMapping(InfinityTimestamp.getParaInfinity());
        mapping2.setControlDefinition(existing);
        mapping2.setListDefinition(ld);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                cdlist.cascadeInsertAll();
                ld.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });

        ListDefinitionFinder.clearQueryCache();
        ControlDefinitionFinder.clearQueryCache();
        ControlListDefinitionMappingFinder.clearQueryCache();

        assertNotNull(ControlDefinitionFinder.findOneBypassCache(ControlDefinitionFinder.controlCode().eq(2)));
        assertEquals(2, ListDefinitionFinder.findOne(ListDefinitionFinder.listId().eq("abc")).getControlDefinitionMappings().size());
    }

    public void testManyToManyRemoveMapping()
    {
        final ListDefinition ld = ListDefinitionFinder.findOne(ListDefinitionFinder.listId().eq("def")).getDetachedCopy();
        ld.getControlDefinitionMappings().remove(0);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                ld.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });

        ListDefinitionFinder.clearQueryCache();
        ControlDefinitionFinder.clearQueryCache();
        ControlListDefinitionMappingFinder.clearQueryCache();

        assertEquals(0, ListDefinitionFinder.findOneBypassCache(ListDefinitionFinder.listId().eq("def")).getControlDefinitionMappings().size());
    }

    public void testDetachedWithoutDependent()
    {
        Order originalOrder = OrderFinder.findOne(OrderFinder.orderId().eq(4));
        Order order = originalOrder.getDetachedCopy();
        assertNull(order.getOrderStatus());
    }

    public void testRemoveFromListDoesCascadeMarkDeleted()
    {
        TestControlledEntry entry = createControlledEntry();
        entry.cascadeInsert();

        TestRestrictedItem item = entry.getRestrictedEntities().get(0).getRestrictedItem().getNonPersistentCopy();

        TestControlledEntry detached = entry.getDetachedCopy();
        TestRestrictedEntity restricted = detached.getRestrictedEntities().get(0);
        TestRestrictedItem detachedItem = restricted.getRestrictedItem();

        detached.getRestrictedEntities().remove(0);
        assertTrue(restricted.isDeletedOrMarkForDeletion());
        assertTrue(detachedItem.isDeletedOrMarkForDeletion());


        detached.copyDetachedValuesToOriginalOrInsertIfNew();

        assertNull(TestRestrictedItemFinder.findOne(TestRestrictedItemFinder.eq(item).and(TestRestrictedItemFinder.businessDate().eq(item.getBusinessDate()))));
    }

    public void testSetNullMarksDeleted()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1)).getDetachedCopy();
        OrderStatus orderStatus = order.getOrderStatus();
        assertNotNull(orderStatus);
        order.setOrderStatus(null);
        assertTrue(orderStatus.isDeletedOrMarkForDeletion());
    }

    public void testAddControlledEntryUpdateItemComment() throws Exception
    {
        String testComment2 = "Test Comment 2";
        String updatedTestComment = "A new value";
        // save
        MithraManagerProvider.getMithraManager().setTransactionTimeout(10000);
        TestControlledEntry aEntry = createControlledEntry();
        this.saveControlledEntry(aEntry);

        // get
        TestControlledEntryList tcel = this.getControlledEntryList(aEntry.getControlledEntryId());
        TestControlledEntry entry = tcel.get(0);


        assertTrue("ControlledEntryEntityComments not found", entry
                .getRestrictedEntities().getRestrictedItems().getComments().size() == 1);

        // update
        TestRestrictedEntityList entityList = entry.getRestrictedEntities();
        TestRestrictedEntity entity = entityList.get(0);
        entity.getRestrictedItem().getComments().get(0).setComment(updatedTestComment);
        TestRestrictedItemCommentList tril = entity.getRestrictedItem().getComments();
        tril.add( createRestrictedItemComment(testComment2) );

        this.saveControlledEntryList(tcel);

        // get 2
        TestControlledEntryList entryList2 = this.getControlledEntryList(aEntry.getControlledEntryId());


        TestControlledEntry entry2 = entryList2.get(0);
        TestRestrictedEntityList trel2 = entry2.getRestrictedEntities();
        assertEquals(1, trel2.size());

        TestRestrictedItem item = trel2.get(0).getRestrictedItem();

        TestRestrictedItemCommentList tricl2 = item.getComments();
        assertEquals(2, tricl2.size());
        for(int i =0; i < tricl2.size();i++)
        {
            TestRestrictedItemComment comment = tricl2.get(i);
            if(comment.getRestrictedCommentId() == 1)
            {
                assertTrue(comment.getComment().equals(updatedTestComment));
            }
            else
            {
               assertTrue(comment.getComment().equals(testComment2));
            }
            assertEquals(item.getRestrictedItemId(), comment.getRestrictedItemId());
        }
    }

    private TestRestrictedItemComment createRestrictedItemComment(String s)
    {
        TestRestrictedItemComment tric = new TestRestrictedItemComment(new Timestamp(System.currentTimeMillis()));
        tric.setComment(s);
        tric.setCommentType("Comment Type 2");
        tric.setUpdateBy("Rafael");

        return tric;
    }


    public TestControlledEntry createControlledEntry()
    {

        TestRestrictedItemComment tric = new TestRestrictedItemComment(new Timestamp(System.currentTimeMillis()));
        tric.setComment("Comment Type 1");
        tric.setComment("Test Comment 1");
        tric.setUpdateBy("Rafael");
        TestRestrictedItemCommentList list = new TestRestrictedItemCommentList();
        list.add(tric);

        TestRestrictedItem tri = new TestRestrictedItem(new Timestamp(System.currentTimeMillis()));
        tri.setItemType("Item Type");
        tri.setUpdateBy("Rafael");
        tri.setComments(list);

        TestRestrictedEntity tre = new TestRestrictedEntity(new Timestamp(System.currentTimeMillis()));
        tre.setEntityId(1234);
        tre.setEntityName("Entity 1234");
        tre.setEntityType("Entity 1234 Type");
        tre.setRestrictedItem(tri);
        TestRestrictedEntityList trel = new TestRestrictedEntityList();
        trel.add(tre);

        TestControlledEntry ce = new TestControlledEntry(new Timestamp(System.currentTimeMillis()));
        ce.setMnpiType("MnpiTpe");
        ce.setApproveOverrideFlag(true);
        ce.setQuietPeriodLength(100);
        ce.setCreateBy("Rafael");
        ce.setUpdateBy("Rafael");
        ce.setRestrictedEntities(trel);
        return ce;
    }

    public void saveControlledEntry(final TestControlledEntry entry) throws Exception
    {

        if (entry == null)
        {
            throw new Exception(" saveControlledEntry - controlledEntry cannot be null.");
        }

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                entry.copyDetachedValuesToOriginalOrInsertIfNew();
                return entry;
            }
        });
        }
        catch (Exception e)
        {

            throw new Exception("ControlledEntryServiceImpl.save failed", e);
        }
    }

    public void saveControlledEntryList(final TestControlledEntryList entryList) throws Exception
    {

        if (entryList == null)
        {
            throw new Exception(" saveControlledEntry - controlledEntry cannot be null.");
        }

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    entryList.copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved();
                    return null;
                }
            });
        }
        catch (Exception e)
        {

            throw new Exception("ControlledEntryServiceImpl.save failed", e);
        }
    }

    public TestControlledEntryList getControlledEntryList(long controlledEntryId)
    {
        Operation op = TestControlledEntryFinder.controlledEntryId().eq(controlledEntryId);
        op = op.and(TestControlledEntryFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        TestControlledEntryList entries = new TestControlledEntryList(op);
        entries.setOrderBy(TestControlledEntryFinder.controlledEntryId().descendingOrderBy());
        entries.deepFetch(TestControlledEntryFinder.restrictedEntities().restrictedItem().comments());

        return entries.getDetachedCopy();
    }

    private ExchangeRate findExchangeRate(String sourceId, String currency, int source, Timestamp ts)
    {
        Operation op = ExchangeRateFinder.acmapCode().eq(sourceId);
        op = op.and(ExchangeRateFinder.currency().eq(currency));
        op = op.and(ExchangeRateFinder.source().eq(source));
        op = op.and(ExchangeRateFinder.date().eq(ts));

        return ExchangeRateFinder.findOne(op);
    }

    public void testOptimisitcLockExceptionWithDetached()
    {
        OptimisticOrder order3 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertNotNull(order3);
        final OptimisticOrder detachedOne = order3.getDetachedCopy();
        detachedOne.setUserId(102);

        detachedOne.copyDetachedValuesToOriginalOrInsertIfNew();
    }

    public void testSetOrderOnExistingOrderItem()
    {
        OrderItem item = OrderItemFinder.findOne(OrderItemFinder.id().eq(1)).getDetachedCopy();
        item.setOrder(OrderFinder.findOne(OrderFinder.orderId().eq(2)).getDetachedCopy());
        assertEquals(2, item.getOrderId());
        item.copyDetachedValuesToOriginalOrInsertIfNew();
        assertEquals(2, item.zFindOriginal().getOrderId());
    }

    public void testDetachedDeleteChildren()
    {
        Order o = OrderFinder.findOne(OrderFinder.orderId().eq(55)).getDetachedCopy();
        o.delete();
        o.copyDetachedValuesToOriginalOrInsertIfNew();
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(55)));
        assertEquals(0, OrderParentToChildrenFinder.findMany(OrderParentToChildrenFinder.parentOrderId().eq(55)).size());
    }

    public void testDetachedMoveManyToMany()
    {
        Order childOrder = OrderFinder.findOne(OrderFinder.orderId().eq(56)).getDetachedCopy();
        Order newParentOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1)).getDetachedCopy();
        Order oldParentOrder = childOrder.getParentToChildAsChild().getParentOrder();

        OrderParentToChildren parentToChildAsChild = childOrder.getParentToChildAsChild();
        oldParentOrder.getParentToChildAsParent().remove(parentToChildAsChild);
        parentToChildAsChild.delete();
        OrderParentToChildren newParentToChildAsChild = new OrderParentToChildren();
        childOrder.setParentToChildAsChild(newParentToChildAsChild);
        newParentToChildAsChild.setParentOrder(newParentOrder);

        parentToChildAsChild.copyDetachedValuesToOriginalOrInsertIfNew();
        newParentToChildAsChild.copyDetachedValuesToOriginalOrInsertIfNew();

        assertEquals(1, childOrder.getParentToChildAsChild().getParentOrder().getOrderId());
        assertEquals(1, OrderFinder.findOne(OrderFinder.orderId().eq(56)).getParentToChildAsChild().getParentOrder().getOrderId());
    }

    public void testDetachedToManyMoveChild()
    {
        final Order firstOrder = OrderFinder.findOne(OrderFinder.orderId().eq(1)).getDetachedCopy();
        int firstSize = firstOrder.getItems().size();
        final Order secondOrder = OrderFinder.findOne(OrderFinder.orderId().eq(2)).getDetachedCopy();

        secondOrder.getItems().setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        int size = secondOrder.getItems().size();
        OrderItem toMove = secondOrder.getItems().get(0);
        toMove.setOrder(firstOrder);

        assertEquals(size - 1, secondOrder.getItems().size());
        assertEquals(firstOrder, toMove.getOrder());
        assertEquals(firstOrder.getOrderId(), toMove.getOrderId());

        assertFalse(toMove.isDeletedOrMarkForDeletion());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                firstOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                secondOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        assertEquals(1 + firstSize, OrderFinder.findOne(OrderFinder.orderId().eq(1)).getItems().size());
        assertEquals(size - 1, OrderFinder.findOne(OrderFinder.orderId().eq(2)).getItems().size());
    }

    public void testDetachedToManyMoveChildDeleteOriginal()
    {
        FileDirectory fd = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1)).getDetachedCopy(); // c:\
        FileDirectoryList childDirectories = fd.getChildDirectories();
        childDirectories.setOrderBy(FileDirectoryFinder.fileDirectoryId().ascendingOrderBy());
        int projectsIndex = 1;
        assertEquals("projects", childDirectories.get(projectsIndex).getName());
        FileDirectoryList allChildDirsOfChildDirectories = childDirectories.getChildDirectories();
        allChildDirsOfChildDirectories.setOrderBy(FileDirectoryFinder.name().ascendingOrderBy());
        FileDirectory para40Dir = allChildDirsOfChildDirectories.get(0);
        assertEquals("PARA4.0", para40Dir.getName());
        para40Dir.setParentDirectory(fd); // move c:\projects\PARA4.0 to c:\PARA4.0
        childDirectories.remove(projectsIndex); // delete c:\projects

        fd.copyDetachedValuesToOriginalOrInsertIfNew();
        FileDirectory newFd = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1));
        childDirectories = newFd.getChildDirectories();
        childDirectories.setOrderBy(FileDirectoryFinder.fileDirectoryId().ascendingOrderBy());
        assertEquals("PARA4.0", childDirectories.get(1).getName());
    }

    public void testDetachedToManyMoveChildToUninsertedParent()
    {
        final Order firstOrder = new Order();
        firstOrder.setOrderId(1000);
        final Order secondOrder = OrderFinder.findOne(OrderFinder.orderId().eq(2)).getDetachedCopy();

        secondOrder.getItems().setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        int size = secondOrder.getItems().size();
        OrderItem toMove = secondOrder.getItems().get(0);
        toMove.setOrder(firstOrder);

        assertEquals(size - 1, secondOrder.getItems().size());
        assertEquals(firstOrder, toMove.getOrder());
        assertEquals(firstOrder.getOrderId(), toMove.getOrderId());

        assertFalse(toMove.isDeletedOrMarkForDeletion());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                firstOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                secondOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        assertEquals(1 , OrderFinder.findOne(OrderFinder.orderId().eq(1000)).getItems().size());
        assertEquals(size - 1, OrderFinder.findOne(OrderFinder.orderId().eq(2)).getItems().size());
    }

    public void testDetachedDeepFetch()
    {
        OrderList detachedList = OrderFinder.findMany(OrderFinder.orderId().lessThan(3)).getDetachedCopy();
        detachedList.deepFetch(OrderFinder.items());
        detachedList.deepFetch(OrderFinder.orderStatus());

        assertTrue(detachedList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        for(int i=0;i<detachedList.size();i++)
        {
            itemCount += detachedList.get(i).getItems().size();
            detachedList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }
    
    public void testDeleteAllOnDependent()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(2)).getDetachedCopy();
        assertTrue(order.getItems().size() > 0);
        order.getItems().deleteAll();
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        OrderItemList orderItemList = order.getItems();
        orderItemList.setBypassCache(true);
        assertEquals(0, orderItemList.size());
    }

    public void testEmptyDetachedDeepFetch()
    {
        OrderList detachedList = OrderFinder.findMany(OrderFinder.orderId().greaterThan(1000)).getDetachedCopy();
        detachedList.forceResolve();
        detachedList.deepFetch(OrderFinder.items());
        detachedList.forceResolve();
        detachedList.deepFetch(OrderFinder.orderStatus());
        assertEquals(0, detachedList.size());
        assertEquals(0, detachedList.getOrderStatus().size());
        assertEquals(0, detachedList.getItems().size());
    }

    public void testNonDatedDetachedAssociationSetToNull()
    {
        Order detachedOrder = OrderFinder.findOne(OrderFinder.orderId().eq(4)).getDetachedCopy();
        Assert.assertNull(detachedOrder.getOrderStatus());

        OrderStatus orderStatus = new OrderStatus();
        orderStatus.setStatus(16);
        orderStatus.setLastUser("aUser");

        detachedOrder.setOrderStatus(orderStatus);

        detachedOrder.setOrderStatus(null);

        Assert.assertNull(detachedOrder.getOrderStatus());
    }

    public void testDatedDetachedAssociationSetToNull()
    {
        AuditedOrder detachedOrder = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(4)).getDetachedCopy();
        Assert.assertNull(detachedOrder.getOrderStatus());

        AuditedOrderStatus orderStatus = new AuditedOrderStatus();
        orderStatus.setStatus(16);
        orderStatus.setLastUser("aUser");

        detachedOrder.setOrderStatus(orderStatus);

        detachedOrder.setOrderStatus(null);

        Assert.assertNull(detachedOrder.getOrderStatus());
    }
}
