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

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.block.procedure.primitive.ObjectIntProcedure;
import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;

import com.gs.collections.api.list.MutableList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.*;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;



public class TestTransactionalList extends MithraTestAbstract
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderParentToChildren.class,
            OrderStatus.class,
            OrderStatusWi.class,
            OrderItemStatus.class,
            BitemporalOrder.class,
            AuditedOrder.class,
            OrderItem.class,
            OrderItemWi.class,
            Employee.class,
            TinyBalance.class,
            FileDirectory.class,
            AuditOnlyBalance.class,
            NonAuditedBalance.class,
            ParaBalance.class
        };
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

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
    }

    public void testBulkRefresh()
    {
        int startOrderId = 5000;
        int countToInsert = 10;
        IntHashSet orderIdSet = new IntHashSet();
        OrderList list = new OrderList();
        for(int i=0;i<countToInsert;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            list.add(order);
            orderIdSet.add(i+startOrderId);
        }
        list.insertAll();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int count = this.getRetrievalCount();
        OrderList list2 = new OrderList(OrderFinder.orderId().in(orderIdSet));
        list2.forceResolve();
        assertTrue(list2.getOrderAt(1).zIsParticipatingInTransaction(tx));
        tx.commit();
        assertEquals(count + 1, this.getRetrievalCount() );
    }

    public void testRefresh() throws SQLException
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        list.forceResolve();
        assertTrue(list.size() > 0);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int count = this.getRetrievalCount();
        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        list2.forceResolve();
        assertEquals(count + 1, this.getRetrievalCount() );
        assertTrue(list2.getOrderAt(1).zIsParticipatingInTransaction(tx));
        tx.commit();

    }

    public void testInsertAll() throws SQLException
    {
        int startOrderId = 5000;
        int count = 1010;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            list.add(order);
        }
        list.insertAll();
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.ORDERS where ORDER_ID >= ? AND ORDER_ID < ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, startOrderId);
        ps.setInt(2, startOrderId+count);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();

        int dbCount = this.getRetrievalCount();
        for(int i=0;i<count;i++)
        {
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(i+startOrderId));
            assertSame(order, list.getOrderAt(i));
        }
        assertEquals(dbCount, this.getRetrievalCount());

    }

    public void xtestDeepFetchManyWithLimit() throws SQLException
    {
        OrderFinder.getMithraObjectPortal().reloadCache();
        int startOrderId = 5000;
        int count = 1010;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            list.add(order);
        }
        list.insertAll();
        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<count*2;i++)
        {
            OrderItem item = new OrderItem();
            item.setOrderId(startOrderId + i/2);
            item.setId(startOrderId + i);
            item.setOriginalPrice(15);
            item.setProductId(7);
            itemList.add(item);
        }
        itemList.insertAll();
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();

        OrderItemList all = new OrderItemList(OrderItemFinder.all());
        all.deepFetch(OrderItemFinder.order());
        all.setMaxObjectsToRetrieve(1000);
        all.forceResolve();

        for(int i=0;i<all.size();i++)
        {
            all.getOrderItemAt(i).getOrder();
        }
    }

    public void testMockBulkInsertAll() throws Exception
    {
        int startOrderId = 5000;
        int count = 1010;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            list.add(order);
        }
        list.bulkInsertAll();

        for(int i=0;i<count;i++)
        {
            Order order = list.getOrderAt(i);
            assertTrue(!order.isInMemory());
        }

        int dbCount = this.getRetrievalCount();
        for(int i=0;i<count;i++)
        {
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(i+startOrderId));
            assertSame(order, list.getOrderAt(i));
        }
        assertEquals(dbCount, this.getRetrievalCount());
        list = new OrderList(OrderFinder.orderId().greaterThanEquals(startOrderId));
        list.setBypassCache(true);
        assertEquals(list.size(), count);
    }

    public void testInsertAllWithMultiInsert() throws SQLException
    {
        H2DatabaseType.getInstance().setUseMultiValueInsert(true);
        int startOrderId = 5000;
        int count = 1010;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            list.add(order);
        }
        list.insertAll();
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.ORDERS where ORDER_ID >= ? AND ORDER_ID < ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, startOrderId);
        ps.setInt(2, startOrderId+count);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();

        int dbCount = this.getRetrievalCount();
        for(int i=0;i<count;i++)
        {
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(i+startOrderId));
            assertSame(order, list.getOrderAt(i));
        }
        assertEquals(dbCount, this.getRetrievalCount());
        H2DatabaseType.getInstance().setUseMultiValueInsert(false);

    }

    public void testInsertAllWithSourceId() throws SQLException
    {
        int startId = 5000;
        int count = 1012;
        EmployeeList list = new EmployeeList();
        for(int i=0;i<count;i++)
        {
            Employee emp = new Employee();
            emp.setId(startId+i);
            emp.setName("Employee "+i);
            emp.setSourceId(i % 2);
            list.add(emp);
        }
        list.insertAll();
        Connection con = this.getConnection(0);
        String sql = "select count(*) from EMPLOYEE where ID >= ? AND ID < ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, startId);
        ps.setInt(2, startId+count);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(count/2, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();

        con = this.getConnection(1);
        ps = con.prepareStatement(sql);
        ps.setInt(1, startId);
        ps.setInt(2, startId+count);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(count/2, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();
    }

    public void testUpdateBatching() throws SQLException
    {
        int startOrderId = 5000;
        int count = 1000;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setState("state "+i);
            list.add(order);
        }
        list.insertAll();
        list = new OrderList(OrderFinder.orderId().greaterThanEquals(startOrderId).and(OrderFinder.orderId().lessThan(startOrderId+count)));
        assertEquals(count,  list.size());
        OrderList detached = list.getDetachedCopy();
        for(int i=0;i<count;i++)
        {
            Order order = detached.getOrderAt(i);
            order.setDescription("same for all");
            order.setState("some state");
        }
        detached.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.ORDERS where ORDER_ID >= ? AND ORDER_ID < ? and DESCRIPTION = ? and STATE = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, startOrderId);
        ps.setInt(2, startOrderId+count);
        ps.setString(3, "same for all");
        ps.setString(4, "some state");
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();

    }

    public void testDeleteAllOneByOne() throws SQLException
    {
        OrderList list = new OrderList(OrderFinder.all());

        OrderList toBeDeleted = new OrderList();

        IntHashSet deletedIds = new IntHashSet();
        for(int i=0;i<list.size();i+=2) // delete every other one
        {
            toBeDeleted.add(list.get(i));
            deletedIds.add(list.getOrderAt(i).getOrderId());
        }

        toBeDeleted.deleteAll();
        IntIterator it = deletedIds.intIterator();
        while(it.hasNext())
        {
            this.checkOrderDoesNotExist(it.next());
        }
        OrderList list2 = new OrderList(OrderFinder.all());
        assertEquals(list.size() - deletedIds.size(), list2.size());
    }

    public void testDeleteAll()
    {
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        firstList.deleteAll();
        OrderList secondList = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, secondList.size());
    }

    public void testDeleteAllInBatches()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(5000, 1000);
        insertTestData(list);
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(500);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());
    }

    public void testDeleteAllInBatchesFromDbAndCache()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(5000, 1000);
        list.bulkInsertAll();

        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(5000, firstList.size());

        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        secondList.deleteAllInBatches(500);
        OrderList thirdList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, thirdList.size());
    }

    public void testDeleteAllInBatchesAfterInsert()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(5000, 1000);
        list.insertAll();
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(500);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());        
    }

    public void testDeleteAllInBatchesWithInvalidBatchSize()
    {
        try
        {
           OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
           firstList.deleteAllInBatches(0);
           fail();
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception", e);
        }

        try
        {
           OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
           firstList.deleteAllInBatches(-500);
           fail();
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception", e);
        }


        TinyBalanceList tinyBalanceList = this.getPurgeAllTinyBalanceList(50);
        try
        {
           tinyBalanceList.purgeAllInBatches(0);
            fail();
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception", e);
        }

        try
        {
           tinyBalanceList.purgeAllInBatches(-5);
            fail();
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception", e);
        }

    }

    public void testDeleteAllInBatchesOneByOne()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(5000, 1000);
        list.insertAll();
        list.deleteAllInBatches(500);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());
    }

    public void testDeleteAllInBatchesOneByOneWithBatchGreaterThanExisitingRows()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(1000, 1000);
        list.insertAll();
        list.deleteAllInBatches(5000);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());
    }

    public void testDeleteAllInBatchesOneByOneWithLastBatchSmaller()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(1875, 1000);
        list.insertAll();
        list.deleteAllInBatches(500);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());
    }

    public void testDeleteListAndRelatedObjectOneByOneInBatches()
    {
        OrderList list = createOrderListWithCountAndInitialIndex(3, 1000);
        list.insertAll();
        OrderItemList itemList = createItemListWithCountAndIndexForOrder(1000, 1000, 1000);
        itemList.insertAll();
        OrderItemList itemList2 = createItemListWithCountAndIndexForOrder(1000, 2000, 1001);
        itemList2.insertAll();
        OrderItemList itemList3 = createItemListWithCountAndIndexForOrder(900, 3000, 1002);
        itemList3.insertAll();

        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        OrderList orderList = new OrderList(OrderFinder.all());
        orderList.getItems().deleteAllInBatches(500);
        orderList.deleteAllInBatches(500);

        OrderList secondOrderList = new OrderList(OrderFinder.all());
        assertEquals(0, secondOrderList.size());
        OrderItemList secondOrderItemList = secondOrderList.getItems();
        assertEquals(0, secondOrderItemList.size());
    }

    public void testPurgeAllInBatches()
    {
        TinyBalanceList tinyBalanceList = this.getPurgeAllTinyBalanceList(50);
        AuditOnlyBalanceList auditOnlyList = this.getPurgeAllAuditOnlyList(2);
        NonAuditedBalanceList nonAuditBalanceList = this.getPurgeAllNonAuditedList(50);

        assertTrue(tinyBalanceList.size() > 0);
        assertTrue(auditOnlyList.size() > 0);
        assertTrue(nonAuditBalanceList.size() > 0);

        tinyBalanceList = this.getPurgeAllTinyBalanceList(50);
        auditOnlyList = this.getPurgeAllAuditOnlyList(2);
        nonAuditBalanceList = this.getPurgeAllNonAuditedList(50);

        tinyBalanceList.purgeAllInBatches(2);
        auditOnlyList.purgeAllInBatches(1);
        nonAuditBalanceList.purgeAllInBatches(1);

        TinyBalanceList tinyBalanceListCheck = this.getPurgeAllTinyBalanceList(50);
        AuditOnlyBalanceList auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        NonAuditedBalanceList nonAuditBalanceListCheck = this.getPurgeAllNonAuditedList(50);
        assertEquals(0, tinyBalanceListCheck.size());
        assertEquals(0, auditOnlyListCheck.size());
        assertEquals(0, nonAuditBalanceListCheck.size());
    }

    private OrderList createOrderListWithCountAndInitialIndex(int count, int initialOrderId)
    {
        OrderList orderList = new OrderList();
        for(int i = 0; i < count; i++)
        {
            Order order = new Order();
            order.setOrderId(initialOrderId + i);
            order.setUserId(999);
            orderList.add(order);            
        }
        return orderList;
    }

    private OrderItemList createItemListWithCountAndIndexForOrder(int count, int initialItemId, int orderId)
    {
        OrderItemList itemList = new OrderItemList();
        for(int i = 0; i < count; i++)
        {
            OrderItem item = new OrderItem();
            item.setId(initialItemId+i);
            item.setOrderId(orderId);
            item.setProductIdNull();
            item.setDiscountPriceNull();
            item.setOriginalPriceNull();
            itemList.add(item);
        }
        return itemList;
    }

    public void testDeleteAllPreCached() throws SQLException
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));

        IntHashSet deletedIds = new IntHashSet();
        for(int i=0;i<list.size();i++)
        {
            deletedIds.add(list.getOrderAt(i).getOrderId());
        }

        list.deleteAll();
        IntIterator it = deletedIds.intIterator();
        while(it.hasNext())
        {
            this.checkOrderDoesNotExist(it.next());
        }
        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
    }

    public void testCascadeDeleteAll() throws SQLException
    {
        int startOrderId = 5000;
        int count = 1000;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(1);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setState("state "+i);
            list.add(order);
        }
        list.insertAll();

        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        list2.cascadeDeleteAll();
    }

    public void testCascadeDeleteSelfRelationship()
    {
        FileDirectoryList list = new FileDirectoryList(FileDirectoryFinder.fileDirectoryId().eq(1));
        list.cascadeDeleteAll();
    }

    public void testDeleteAllPreCachedInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        list.forceResolve();
        this.testDeleteAllPreCached();
        tx.commit();
    }

    public void testDeleteAllAfterInsert()
    {
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        assertTrue(firstList.size() > 0);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order newOrder = new Order();
        newOrder.setOrderId(100000);
        newOrder.setOrderDate(new Timestamp(System.currentTimeMillis()));
        newOrder.setUserId(1);
        newOrder.setDescription("Fourth order");
        newOrder.setState("In-Progress");
        newOrder.setTrackingId("134");
        newOrder.insert();

        firstList = new OrderList(OrderFinder.userId().eq(1));
        firstList.deleteAll();

        assertEquals(0, firstList.size());

        tx.commit();

        firstList = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, firstList.size());
    }

    public void testPurgeAllAuditOnly()
    {
        AuditOnlyBalanceList auditOnlyList = this.getPurgeAllAuditOnlyList(2);

        assertTrue(auditOnlyList.size() > 0);

        //begin transaction
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        auditOnlyList = this.getPurgeAllAuditOnlyList(2);

        auditOnlyList.purgeAll();

        AuditOnlyBalanceList auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        assertEquals(0, auditOnlyListCheck.size());

        tx.commit();
        //end transaction

        auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        assertEquals(0, auditOnlyListCheck.size());
    }

    public void testPurgeAll()
    {
        TinyBalanceList tinyBalanceList = this.getPurgeAllTinyBalanceList(50);
        AuditOnlyBalanceList auditOnlyList = this.getPurgeAllAuditOnlyList(2);
        NonAuditedBalanceList nonAuditBalanceList = this.getPurgeAllNonAuditedList(50);

        assertTrue(tinyBalanceList.size() > 0);
        assertTrue(auditOnlyList.size() > 0);
        assertTrue(nonAuditBalanceList.size() > 0);

        //begin transaction
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tinyBalanceList = this.getPurgeAllTinyBalanceList(50);
        auditOnlyList = this.getPurgeAllAuditOnlyList(2);
        nonAuditBalanceList = this.getPurgeAllNonAuditedList(50);

        tinyBalanceList.purgeAll();
        auditOnlyList.purgeAll();
        nonAuditBalanceList.purgeAll();

        TinyBalanceList tinyBalanceListCheck = this.getPurgeAllTinyBalanceList(50);
        AuditOnlyBalanceList auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        NonAuditedBalanceList nonAuditBalanceListCheck = this.getPurgeAllNonAuditedList(50);
        assertEquals(0, tinyBalanceListCheck.size());
        assertEquals(0, auditOnlyListCheck.size());
        assertEquals(0, nonAuditBalanceListCheck.size());

        tx.commit();
        //end transaction

        tinyBalanceListCheck = this.getPurgeAllTinyBalanceList(50);
        auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        nonAuditBalanceListCheck = this.getPurgeAllNonAuditedList(50);
        assertEquals(0, tinyBalanceListCheck.size());
        assertEquals(0, auditOnlyListCheck.size());
        assertEquals(0, nonAuditBalanceListCheck.size());
    }

    public void testPurgeAllAfterInsert() throws Exception
    {
        TinyBalanceList balanceList = this.getPurgeAllTinyBalanceList(50);
        NonAuditedBalanceList nonAuditBalanceList = this.getPurgeAllNonAuditedList(50);
        assertTrue(balanceList.size() > 0);
        assertTrue(nonAuditBalanceList.size() > 0);

        //open transaction
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();

        //Add new balance
        TinyBalance newBalance = new TinyBalance(new Timestamp(timestampFormat.parse("2001-10-01 00:00:00.0").getTime()));
        newBalance.setBusinessDateTo(new Timestamp(timestampFormat.parse("2003-01-01 18:30:00.0").getTime()));
        newBalance.setAcmapCode("A");
        newBalance.setBalanceId(50);
        newBalance.setQuantity(100);
        newBalance.setNullablePrimitiveAttributesToNull();
        newBalance.insert();

        NonAuditedBalance newNonAuditedBalance = new NonAuditedBalance(new Timestamp(timestampFormat.parse("2001-10-01 00:00:00.0").getTime()));
        newNonAuditedBalance.setBusinessDateTo(new Timestamp(timestampFormat.parse("2001-11-01 00:00:00.0").getTime()));
        newNonAuditedBalance.setAcmapCode("A");
        newNonAuditedBalance.setBalanceId(50);
        newNonAuditedBalance.setQuantity(100);
        newNonAuditedBalance.setNullablePrimitiveAttributesToNull();
        newNonAuditedBalance.insert();

        balanceList = this.getPurgeAllTinyBalanceList(50);
        nonAuditBalanceList = this.getPurgeAllNonAuditedList(50);
        balanceList.purgeAll();
        nonAuditBalanceList.purgeAll();

        TinyBalanceList balanceListCheck = this.getPurgeAllTinyBalanceList(50);
        NonAuditedBalanceList nonAuditBalanceListCheck = this.getPurgeAllNonAuditedList(50);
        assertEquals(0, balanceListCheck.size());
        assertEquals(0, nonAuditBalanceListCheck.size());
        tx.commit();
        // close transaction

        balanceListCheck = this.getPurgeAllTinyBalanceList(50);
        nonAuditBalanceListCheck = this.getPurgeAllNonAuditedList(50);
        balanceListCheck.forceResolve();

        assertEquals(0, balanceListCheck.size());
        assertEquals(0, nonAuditBalanceListCheck.size());
    }

    public void testPurgeAllAfterUpdate() throws Exception
    {
        AuditOnlyBalanceList auditOnlyList = this.getPurgeAllAuditOnlyList(2);
        assertTrue(auditOnlyList.size() > 0);

        //begin transaction
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();

        Operation auditOnlyOp = AuditOnlyBalanceFinder.balanceId().eq(2);
        auditOnlyOp = auditOnlyOp.and(AuditOnlyBalanceFinder.acmapCode().eq("A"));
        auditOnlyOp = auditOnlyOp.and(AuditOnlyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));
        AuditOnlyBalance auditOnly = AuditOnlyBalanceFinder.findOne(auditOnlyOp);
        auditOnly.setInterest(3400);

        auditOnlyList = this.getPurgeAllAuditOnlyList(2);
        auditOnlyList.purgeAll();

        AuditOnlyBalanceList auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        assertEquals(0, auditOnlyListCheck.size());
        tx.commit();
        // close transaction

        auditOnlyListCheck = this.getPurgeAllAuditOnlyList(2);
        assertEquals(0, auditOnlyListCheck.size());
    }

    private TinyBalanceList getPurgeAllTinyBalanceList(int balanceId)
    {
        Operation tinyBalanceOp = TinyBalanceFinder.balanceId().eq(balanceId);
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());

        return new TinyBalanceList(tinyBalanceOp);
    }

    private AuditOnlyBalanceList getPurgeAllAuditOnlyList(int balanceId)
    {
        Operation auditOnlyOp = AuditOnlyBalanceFinder.balanceId().eq(balanceId);
        auditOnlyOp = auditOnlyOp.and(AuditOnlyBalanceFinder.acmapCode().eq("A"));
        auditOnlyOp = auditOnlyOp.and(AuditOnlyBalanceFinder.processingDate().equalsEdgePoint());

        return new AuditOnlyBalanceList(auditOnlyOp);
    }

    private NonAuditedBalanceList getPurgeAllNonAuditedList(int balanceId)
    {
        Operation nonAuditBalanceOp = NonAuditedBalanceFinder.balanceId().eq(balanceId);
        nonAuditBalanceOp = nonAuditBalanceOp.and(NonAuditedBalanceFinder.acmapCode().eq("A"));
        nonAuditBalanceOp = nonAuditBalanceOp.and(NonAuditedBalanceFinder.businessDate().equalsEdgePoint());

        return new NonAuditedBalanceList(nonAuditBalanceOp);
    }

    public void testQueryExpirationInTransaction()
    {
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        firstList.forceResolve();
        int firstListSize = firstList.size();
        assertTrue(firstListSize > 0);
        assertFalse(firstList.isStale());
        Order order = firstList.getOrderAt(0);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setUserId(order.getUserId()+1000);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(1));
        secondList.forceResolve();
        int secondListSize = secondList.size();
        assertTrue(secondList.size() < firstListSize);
        order = secondList.getOrderAt(0);
        order.setUserId(order.getUserId()+1000);
        assertTrue(secondList.isStale());

        tx.commit();
        assertTrue(firstList.isStale());
        OrderList thirdList = new OrderList(OrderFinder.userId().eq(1));
        thirdList.forceResolve();
        assertTrue(thirdList.size() < secondListSize);
    }

    public void testTransactionalParticipation()
    {
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        firstList.forceResolve();
        int originalSize = firstList.size();
        assertTrue(originalSize > 0);
        assertFalse(firstList.isStale());
        Order order = firstList.getOrderAt(0);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setUserId(order.getUserId()+1000);
        firstList.forceResolve();
        assertTrue(firstList.size() < originalSize); // testing that list was refreshed in transaction
        for(int i=0;i<firstList.size();i++)
        {
            assertTrue(firstList.getOrderAt(i).zIsParticipatingInTransaction(MithraManagerProvider.getMithraManager().getCurrentTransaction()));
        }
        tx.commit();
    }

    public void testMultithreadedLoader()
    {
        MithraMultiThreadedLoader loader = new MithraMultiThreadedLoader(5);
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        OrderList secondList = new OrderList(OrderFinder.userId().eq(2));
        ArrayList allLists = new ArrayList();
        allLists.add(firstList);
        allLists.add(secondList);
        for(int i=3;i<1000;i++)
        {
            allLists.add(new OrderList(OrderFinder.userId().eq(i)));
        }
        loader.loadMultipleLists(allLists);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertNotNull(firstList.getOrderAt(0));
        assertNotNull(secondList.getOrderAt(0));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testMultithreadedLoaderWithException()
    {
        MithraMultiThreadedLoader loader = new MithraMultiThreadedLoader(5);
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        OrderList secondList = new OrderList(OrderFinder.userId().eq(2));
        ArrayList allLists = new ArrayList();
        allLists.add(firstList);
        allLists.add(secondList);
        for(int i=3;i<1000;i++)
        {
            allLists.add(new OrderList(OrderFinder.userId().eq(i)));
        }
        allLists.add(new TinyBalanceList(TinyBalanceFinder.balanceId().eq(1))); // causes an exception because acmap and business date are not specified
        try
        {
            loader.loadMultipleLists(allLists);
            fail();
        }
        catch (MithraException e)
        {
            // good
        }
    }

    public void testMultithreadedQueueLoader()
    {
        MithraMultiThreadedQueueLoader loader = new MithraMultiThreadedQueueLoader(5);
        //try shutting down the loader's pool before adding any items
        try
        {
            loader.shutdownPool();
        }
        catch (Exception e)
        {
            fail("should not get here...");
        }

        final OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        final OrderList secondList = new OrderList(OrderFinder.userId().eq(2));
        loader.addQueueItem(new MithraListQueueItem() {
            public MithraList getMithraListToResolve()
            {
                return firstList;
            }
        });
        loader.addQueueItem(new MithraListQueueItem() {
            public MithraList getMithraListToResolve()
            {
                return secondList;
            }
        });
        MithraListQueueItem one = loader.takeResult();
        MithraListQueueItem two = loader.takeResult();
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertNotNull(firstList.getOrderAt(0));
        assertNotNull(secondList.getOrderAt(0));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        boolean gotBoth = (one.getMithraListToResolve() == firstList && two.getMithraListToResolve() == secondList)
            || (one.getMithraListToResolve() == secondList && two.getMithraListToResolve() == firstList);
        assertTrue(gotBoth);

        loader = new MithraMultiThreadedQueueLoader(5, true);
        final AtomicBoolean workerThreadWasInTransaction = new AtomicBoolean(false);
        final OrderList transactionProtectedList = new OrderList(OrderFinder.userId().eq(1))
        {
            @Override
            public void forceResolve()
            {
                workerThreadWasInTransaction.set(MithraManagerProvider.getMithraManager().isInTransaction());
                super.forceResolve();
            }
        };
        loader.addQueueItem(new MithraListQueueItem()
        {
            @Override
            public MithraList getMithraListToResolve()
            {
                return transactionProtectedList;
            }
        });
        loader.takeResult();
        assertTrue(workerThreadWasInTransaction.get());
    }

    public void testMaxObjectsToRetrieve()
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
            firstList.setMaxObjectsToRetrieve(1);
            assertEquals(1, firstList.size());
            assertTrue(firstList.reachedMaxObjectsToRetrieve());
            // make sure we don't cache the partial result
            OrderList secondList = new OrderList(OrderFinder.userId().eq(1));
            assertTrue(secondList.size() > 1);
        }
        else
        {
            OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
            firstList.setMaxObjectsToRetrieve(1);
            assertTrue(firstList.size() > 1);
        }
    }

    public void testUpdatingSimpleInMemoryList()
    {
        int startOrderId = 5000;
        int count = 1000;
        OrderList list = new OrderList();
        for(int i=0;i<count;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setState("state "+i);
            list.add(order);
        }

        list.setUserId(999);
        for(int i = 0; i < list.size(); i++)
        {
            Order order = list.getOrderAt(i);
            assertEquals(999, order.getUserId());
        }
    }

    public void testUpdatingDatedSimpleInMemoryList()
    {
        int startBalanceId = 5000;
        int count = 10;
        TinyBalanceList list = new TinyBalanceList();
        for(int i = 0; i < count; i++)
        {
            TinyBalance balance = new TinyBalance(InfinityTimestamp.getParaInfinity());
            balance.setAcmapCode("A");
            balance.setBalanceId(startBalanceId + i);
            balance.setQuantity(100.0 + i);
            balance.setNullablePrimitiveAttributesToNull();
            list.add(balance);
        }
        list.setQuantity(900);
        for(int i = 0; i < list.size(); i++)
        {
            TinyBalance balance = list.getTinyBalanceAt(i);
            assertEquals(900, balance.getQuantity(), 0);
        }
    }

    public void testUpdatingOperationBasedList()
    {
        Operation op = OrderFinder.state().eq("In-Progress");
        Operation op2 = OrderFinder.state().eq("Completed");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderList completedOrderList0 = new OrderList(op2);
        assertEquals(0, completedOrderList0.size());
        OrderList inProgressOrderList0 = new OrderList(op);
        inProgressOrderList0.setState("Completed");
        tx.commit();

        OrderList inProgressOrderList1 = new OrderList(op);
        assertEquals(0, inProgressOrderList1.size());

        OrderList completedOrderList1 = new OrderList(op2);
        for (int i = 0; i < completedOrderList1.size(); i++)
        {
            Order order = completedOrderList1.get(i);
            assertEquals("Completed", order.getState());
        }
    }

    public void testUpdatingDatedOperationBasedList()
            throws Exception
    {
        Operation op = TinyBalanceFinder.acmapCode().eq("A");
        op = op.and(TinyBalanceFinder.quantity().lessThan(0.0));
        op = op.and(TinyBalanceFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime())));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalanceList negQtyList = new TinyBalanceList(op);
        assertTrue(negQtyList.size() > 0);
        negQtyList.setQuantity(1.0);
        tx.commit();
        TinyBalanceList negQtyList2 = new TinyBalanceList(op);
        assertEquals(0, negQtyList2.size());
    }

    public void testUpdatingMultipleAttributesOperationBasedList()
    {
        Operation op = OrderFinder.state().eq("In-Progress");
        Operation op2 = OrderFinder.state().eq("Completed").and(OrderFinder.userId().eq(987));


        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderList completedOrderList0 = new OrderList(op2);
        assertEquals(0, completedOrderList0.size());
        OrderList inProgressOrderList0 = new OrderList(op);
        inProgressOrderList0.setState("Completed");
        inProgressOrderList0.setUserId(987);
        tx.commit();

        OrderList inProgressOrderList1 = new OrderList(op);
        assertEquals(0, inProgressOrderList1.size());

        OrderList completedOrderList1 = new OrderList(op2);
        for (int i = 0; i < completedOrderList1.size(); i++)
        {
            Order order = completedOrderList1.get(i);
            assertEquals("Completed", order.getState());
            assertEquals(987, order.getUserId());
        }
    }

    public void testRefreshInTransaction() throws SQLException
    {
        OrderList list1 = new OrderList(OrderFinder.orderId().eq(2000));
        assertTrue(list1.isEmpty());

        int updatedRows = this.insertOneRowToOrder(2000, new Timestamp(System.currentTimeMillis()), 2222, "test desc", "teststate", "testid");
        assertEquals(updatedRows, 1);
        OrderList refreshedList = (OrderList) MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderList newList = new OrderList(OrderFinder.orderId().eq(2000));
                newList.forceResolve();
                return newList;
            }
        });
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        assertEquals(1, refreshedList.size());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testNoneOperationBypassingCache()
    {
        Operation op = (BitemporalOrderFinder.userId().in(new IntHashSet()));
        op = op.and(BitemporalOrderFinder.orderId().eq(4));
        op = op.and(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        BitemporalOrderList list = new BitemporalOrderList(op);
        list.setBypassCache(true);
        assertTrue(list.isEmpty());
    }

    public void testPurgeBeforeBusinessDateNonCached() throws ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2007-06-29 18:30:00.0").getTime());

        //purge everything for balance id 9876 that happened on or before 06-29-2007 18:30:00
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation tinyBalanceOp = TinyBalanceFinder.balanceId().eq(8765);
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate));
                TinyBalanceList tinyBalanceList = new TinyBalanceList(tinyBalanceOp);
                tinyBalanceList.purgeAll();
                return null;
            }
        });

        Timestamp before = new Timestamp(timestampFormat.parse("2006-03-15 18:30:00.0").getTime());
        Operation op = TinyBalanceFinder.balanceId().eq(8765);
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));
        op = op.and(TinyBalanceFinder.businessDate().eq(before));
        TinyBalance balance = TinyBalanceFinder.findOne(op);
        assertNull(balance);

        Timestamp before2 = new Timestamp(timestampFormat.parse("2007-01-15 18:30:00.0").getTime());
        Operation op2 = TinyBalanceFinder.balanceId().eq(8765);
        op2 = op2.and(TinyBalanceFinder.acmapCode().eq("A"));
        op2 = op2.and(TinyBalanceFinder.businessDate().eq(before2));
        TinyBalance balance2 = TinyBalanceFinder.findOne(op2);
        assertNull(balance2);

        Operation op3 = TinyBalanceFinder.balanceId().eq(8765);
        op3 = op3.and(TinyBalanceFinder.acmapCode().eq("A"));
        op3 = op3.and(TinyBalanceFinder.businessDate().eq(businessDate));
        TinyBalance balance3 = TinyBalanceFinder.findOne(op3);
        assertNotNull(balance3);

        Timestamp after = new Timestamp(timestampFormat.parse("2007-06-30 18:30:00.0").getTime());
        Operation op4 = TinyBalanceFinder.balanceId().eq(8765);
        op4 = op4.and(TinyBalanceFinder.acmapCode().eq("A"));
        op4 = op4.and(TinyBalanceFinder.businessDate().eq(after));
        TinyBalance balance4 = TinyBalanceFinder.findOne(op4);
        assertNotNull(balance4);

        Operation tinyBalanceOp1 = TinyBalanceFinder.balanceId().eq(8765);
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate));
        TinyBalanceList tinyBalanceList1 = new TinyBalanceList(tinyBalanceOp1);
        assertEquals(0, tinyBalanceList1.size());

        Operation tinyBalanceOp2 = TinyBalanceFinder.balanceId().eq(8765);
        tinyBalanceOp2 = tinyBalanceOp2.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp2 = tinyBalanceOp2.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp2 = tinyBalanceOp2.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        TinyBalanceList tinyBalanceList2 = new TinyBalanceList(tinyBalanceOp2);
        assertTrue(tinyBalanceList2.size() > 0);
    }

    public void testPurgeBeforeBusinessDateCachingValuesBeforePurging() throws ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2007-06-29 18:30:00.0").getTime());

        //find everything for balance id 8765 (everything will be cached at the moment of purging)
        Operation tinyBalanceOp = TinyBalanceFinder.balanceId().eq(8765);
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        TinyBalanceList tinyBalanceList = new TinyBalanceList(tinyBalanceOp);
        assertEquals(36, tinyBalanceList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation tinyBalanceOp = TinyBalanceFinder.balanceId().eq(8765);
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
                tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate));
                TinyBalanceList tinyBalanceList = new TinyBalanceList(tinyBalanceOp);
                tinyBalanceList.purgeAll();
                return null;
            }
        });

        Timestamp before = new Timestamp(timestampFormat.parse("2006-03-15 18:30:00.0").getTime());
        Operation op = TinyBalanceFinder.balanceId().eq(8765);
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));
        op = op.and(TinyBalanceFinder.businessDate().eq(before));
        TinyBalance balance = TinyBalanceFinder.findOne(op);
        assertNull(balance);

        Timestamp before2 = new Timestamp(timestampFormat.parse("2007-01-15 18:30:00.0").getTime());
        Operation op2 = TinyBalanceFinder.balanceId().eq(8765);
        op2 = op2.and(TinyBalanceFinder.acmapCode().eq("A"));
        op2 = op2.and(TinyBalanceFinder.businessDate().eq(before2));
        TinyBalance balance2 = TinyBalanceFinder.findOne(op2);
        assertNull(balance2);

        Operation op3 = TinyBalanceFinder.balanceId().eq(8765);
        op3 = op3.and(TinyBalanceFinder.acmapCode().eq("A"));
        op3 = op3.and(TinyBalanceFinder.businessDate().eq(businessDate));
        TinyBalance balance3 = TinyBalanceFinder.findOne(op3);
        assertNotNull(balance3);

        Timestamp after = new Timestamp(timestampFormat.parse("2007-06-30 18:30:00.0").getTime());
        Operation op4 = TinyBalanceFinder.balanceId().eq(8765);
        op4 = op4.and(TinyBalanceFinder.acmapCode().eq("A"));
        op4 = op4.and(TinyBalanceFinder.businessDate().eq(after));
        TinyBalance balance4 = TinyBalanceFinder.findOne(op4);
        assertNotNull(balance4);

        Operation tinyBalanceOp1 = TinyBalanceFinder.balanceId().eq(8765);
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate));
        TinyBalanceList tinyBalanceList1 = new TinyBalanceList(tinyBalanceOp1);
        assertEquals(0, tinyBalanceList1.size());

        TinyBalanceList tinyBalanceList2 = new TinyBalanceList(tinyBalanceOp);
        assertTrue(tinyBalanceList2.size() < tinyBalanceList.size());
    }


    public void testPurgeAllBeforeBusinessDate() throws ParseException
    {
       final Timestamp businessDate = new Timestamp(timestampFormat.parse("2007-06-29 18:30:00.0").getTime());

       IntHashSet IntHashSet = new IntHashSet();
       IntHashSet.add(8765);
       IntHashSet.add(8764);
       Operation tinyBalanceOp = TinyBalanceFinder.balanceId().in(IntHashSet);
       tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
       tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
       tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
       TinyBalanceList tinyBalanceList = new TinyBalanceList(tinyBalanceOp);
       tinyBalanceList.forceResolve();
       MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
       {
           public Object executeTransaction(MithraTransaction tx) throws Throwable
           {
               Operation tinyBalanceOp = TinyBalanceFinder.all();
               tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
               tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
               tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
               tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate));
               TinyBalanceList tinyBalanceList = new TinyBalanceList(tinyBalanceOp);
               tinyBalanceList.purgeAll();
               return null;
           }
       });

        Timestamp before = new Timestamp(timestampFormat.parse("2006-03-15 18:30:00.0").getTime());
        Operation op = TinyBalanceFinder.balanceId().in(IntHashSet);
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));
        op = op.and(TinyBalanceFinder.businessDate().eq(before));
        TinyBalanceList list = new TinyBalanceList(op);
        assertTrue(list.isEmpty());

        Timestamp before2 = new Timestamp(timestampFormat.parse("2007-01-15 18:30:00.0").getTime());
        Operation op2 = TinyBalanceFinder.balanceId().in(IntHashSet);
        op2 = op2.and(TinyBalanceFinder.acmapCode().eq("A"));
        op2 = op2.and(TinyBalanceFinder.businessDate().eq(before2));
        TinyBalanceList list2 = new TinyBalanceList(op2);
        assertTrue(list2.isEmpty());

        Operation op3 = TinyBalanceFinder.balanceId().eq(8765);
        op3 = op3.and(TinyBalanceFinder.acmapCode().eq("A"));
        op3 = op3.and(TinyBalanceFinder.businessDate().eq(businessDate));
        TinyBalance balance3 = TinyBalanceFinder.findOne(op3);
        assertNotNull(balance3);

        Timestamp after = new Timestamp(timestampFormat.parse("2007-06-30 18:30:00.0").getTime());
        Operation op4 = TinyBalanceFinder.balanceId().eq(8765);
        op4 = op4.and(TinyBalanceFinder.acmapCode().eq("A"));
        op4 = op4.and(TinyBalanceFinder.businessDate().eq(after));
        TinyBalance balance4 = TinyBalanceFinder.findOne(op4);
        assertNotNull(balance4);

        Operation tinyBalanceOp1 = TinyBalanceFinder.balanceId().in(IntHashSet);
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate));
        TinyBalanceList tinyBalanceList1 = new TinyBalanceList(tinyBalanceOp1);
        assertEquals(0, tinyBalanceList1.size());

        TinyBalanceList tinyBalanceList2 = new TinyBalanceList(tinyBalanceOp);
        assertTrue(tinyBalanceList2.size() < tinyBalanceList.size());

    }

    public void testPurgeBeforeAuditOnly() throws ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2007-06-29 18:30:00.0").getTime());
        Operation op = AuditedOrderFinder.orderId().greaterThan(9998);
        op = op.and(AuditedOrderFinder.processingDate().equalsEdgePoint());
        AuditedOrderList orderList = new AuditedOrderList(op);
        assertEquals(20, orderList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = AuditedOrderFinder.processingDate().equalsEdgePoint();
                op = op.and(AuditedOrderFinder.orderId().greaterThan(9998));
                op = op.and(AuditedOrderFinder.processingDateTo().lessThanEquals(businessDate));
                AuditedOrderList listToPurge = new AuditedOrderList(op);
                listToPurge.purgeAll();
                return null;
            }
        });

        AuditedOrderList orderList2 = new AuditedOrderList(op);
        assertTrue(orderList2.size() < orderList.size());
        assertEquals(14, orderList2.size());


        Timestamp before = new Timestamp(timestampFormat.parse("2006-03-15 18:30:00.0").getTime());
        Operation op3 = AuditedOrderFinder.orderId().greaterThan(9998);
        op3 = op3.and(AuditedOrderFinder.processingDate().eq(before));
        AuditedOrderList orderList3 = new AuditedOrderList(op3);
        assertTrue(orderList3.isEmpty());

        Timestamp before2 = new Timestamp(timestampFormat.parse("2006-04-15 18:30:00.0").getTime());
        Operation op4 = AuditedOrderFinder.orderId().greaterThan(9998);
        op4 = op4.and(AuditedOrderFinder.processingDate().eq(before2));
        AuditedOrderList orderList4 = new AuditedOrderList(op4);
        assertTrue(orderList4.isEmpty());

        Timestamp after = new Timestamp(timestampFormat.parse("2007-10-01 00:00:00.0").getTime());
        Operation op5 = AuditedOrderFinder.orderId().greaterThan(9998);
        op5 = op5.and(AuditedOrderFinder.processingDate().eq(after));
        AuditedOrder order = AuditedOrderFinder.findOne(op5);
        assertNotNull(order);

    }

    public void testGsCollectionsList()
    {
        int start = 0xAE4927BE;
        OrderList list = OrderFinder.findMany(OrderFinder.all());
        int hash = 0xAE4927BE;
        for(int i = 0;i<list.size();i++)
        {
            hash = HashUtil.combineHashes(hash, list.get(i).getOrderId());
        }
        checkForEach(start, OrderFinder.findMany(OrderFinder.all()).asGscList(), hash);
        checkForEach(start, list.asGscList(), hash);
        OrderList adhocList = new OrderList(list);
        checkForEach(start, adhocList.asGscList(), hash);
        checkForEach(start, list.asGscList(), hash);
        checkForEach(start, adhocList.asGscList(), hash);

        // non MutableList extras:
        assertEquals(!adhocList.isEmpty(), adhocList.notEmpty());
        assertEquals(!list.isEmpty(), list.notEmpty());
        assertEquals(!list.asGscList().isEmpty(), list.asGscList().notEmpty());
        assertEquals(!adhocList.asGscList().isEmpty(), adhocList.asGscList().notEmpty());
    }

    private void checkForEach(int start, MutableList list, int hash)
    {
        final int[] hashArray = new int[1];
        hashArray[0] = start;
        list.forEach(new Procedure<Order>()
        {
            public void value(Order order)
            {
                hashArray[0] = HashUtil.combineHashes(hashArray[0], order.getOrderId());
            }
        });
        assertEquals(hash, hashArray[0]);
        hashArray[0] = start;
        list.forEachWith(new Procedure2<Order, int[]>()
        {
            public void value(Order order, int[] argument2)
            {
                argument2[0] = HashUtil.combineHashes(argument2[0], order.getOrderId());
            }
        }, hashArray);
        assertEquals(hash, hashArray[0]);
        hashArray[0] = start;
        list.forEachWithIndex(new ObjectIntProcedure<Order>()
        {
            public void value(Order order, int index)
            {
                hashArray[0] = HashUtil.combineHashes(hashArray[0], order.getOrderId());
            }
        });
        assertEquals(hash, hashArray[0]);
    }

    public void testEclipseCollectionsList()
    {
        int start = 0xAE4927BE;
        OrderList list = OrderFinder.findMany(OrderFinder.all());
        int hash = 0xAE4927BE;
        for(int i = 0;i<list.size();i++)
        {
            hash = HashUtil.combineHashes(hash, list.get(i).getOrderId());
        }
        checkForEach(start, OrderFinder.findMany(OrderFinder.all()).asEcList(), hash);
        checkForEach(start, list.asEcList(), hash);
        OrderList adhocList = new OrderList(list);
        checkForEach(start, adhocList.asEcList(), hash);
        checkForEach(start, list.asEcList(), hash);
        checkForEach(start, adhocList.asEcList(), hash);

        // non MutableList extras:
        assertEquals(!adhocList.isEmpty(), adhocList.notEmpty());
        assertEquals(!list.isEmpty(), list.notEmpty());
        assertEquals(!list.asGscList().isEmpty(), list.asGscList().notEmpty());
        assertEquals(!adhocList.asGscList().isEmpty(), adhocList.asGscList().notEmpty());
    }

    private void checkForEach(int start, org.eclipse.collections.api.list.MutableList list, int hash)
    {
        final int[] hashArray = new int[1];
        hashArray[0] = start;
        list.forEach(new org.eclipse.collections.api.block.procedure.Procedure<Order>()
        {
            public void value(Order order)
            {
                hashArray[0] = HashUtil.combineHashes(hashArray[0], order.getOrderId());
            }
        });
        assertEquals(hash, hashArray[0]);
        hashArray[0] = start;
        list.forEachWith(new org.eclipse.collections.api.block.procedure.Procedure2<Order, int[]>()
        {
            public void value(Order order, int[] argument2)
            {
                argument2[0] = HashUtil.combineHashes(argument2[0], order.getOrderId());
            }
        }, hashArray);
        assertEquals(hash, hashArray[0]);
        hashArray[0] = start;
        list.forEachWithIndex(new org.eclipse.collections.api.block.procedure.ObjectIntProcedure<Order>()
        {
            public void value(Order order, int index)
            {
                hashArray[0] = HashUtil.combineHashes(hashArray[0], order.getOrderId());
            }
        });
        assertEquals(hash, hashArray[0]);
    }

    public void testEqualsAndHashCode()
    {
        OrderList orderList = OrderFinder.findMany(OrderFinder.all());
        OrderList orderList2 = OrderFinder.findMany(OrderFinder.all());
        assertEquals(orderList, orderList2);
        assertEquals(orderList.hashCode(), orderList2.hashCode());
        OrderList orderList3 = new OrderList(orderList);
        assertEquals(orderList, orderList3);
        assertEquals(orderList.hashCode(), orderList3.hashCode());
        ArrayList orderList4 = new ArrayList(orderList);
        assertEquals(orderList, orderList3);
        assertEquals(orderList.hashCode(), orderList3.hashCode());
    }

    public void testRenewDatedObjectsFromDB() throws SQLException
    {
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            Timestamp time1 = Timestamp.valueOf("2010-11-03 02:51:00.0");
            Timestamp date1 = Timestamp.valueOf("2010-10-30 23:59:00");
            Timestamp date2 = Timestamp.valueOf("2005-01-14 18:30:00");
            Timestamp date3 = Timestamp.valueOf("2005-01-24 18:30:00");
            TinyBalanceList cachedList = new TinyBalanceList(this.getTinyBalanceDateOperation(date1).and(TinyBalanceFinder.balanceId().eq(10)));
            assertEquals(cachedList.size(), 2);

            // SQL to modify without going through mithra
            int insertedRows = this.insertOneRowToTinyBalance(60, date1, time1, 30.0);
            assertEquals(insertedRows, 1);

            int updatedRows = this.updateTinyBalanceForBalanceId(10, 233.0, date2, date3);
            assertEquals(updatedRows, 1);

            int deletedRows = this.deleteTinyBalanceForBalanceId(10, date2, date3);
            assertEquals(deletedRows, 1);

            RenewedCacheStats renewResult = new MithraRuntimeCacheController(TinyBalanceFinder.class).renewCacheForOperation(this.getTinyBalanceDateOperation(InfinityTimestamp.getParaInfinity()).and(TinyBalanceFinder.balanceId().eq(10)));
            assertEquals(renewResult.getInserted().size() + renewResult.getUpdated().size(), 1);

            RenewedCacheStats renewResult2 = new MithraRuntimeCacheController(TinyBalanceFinder.class).renewCacheForOperation(this.getTinyBalanceDateOperation(InfinityTimestamp.getParaInfinity()).and(TinyBalanceFinder.balanceId().eq(60)));
            assertEquals(renewResult2.getInserted().size() + renewResult2.getUpdated().size(), 1);

            // assert mithra objects on insert/update/delete result
            TinyBalanceList cachedList2 = new TinyBalanceList(this.getTinyBalanceDateOperation(date1).and(TinyBalanceFinder.balanceId().eq(10)));
            assertEquals(cachedList2.size(), 1);

            TinyBalance balance1 = TinyBalanceFinder.findOne(
                    this.getTinyBalanceDateOperation(InfinityTimestamp.getParaInfinity()).and(
                            TinyBalanceFinder.balanceId().eq(60))
                    );
            assertEquals(balance1.getQuantity(), 30.0);

            TinyBalance balance2 = TinyBalanceFinder.findOne(
                    this.getTinyBalanceDateOperation(date3).and(
                            TinyBalanceFinder.balanceId().eq(10)));
            assertEquals(balance2.getQuantity(), 233.0);

        }
    }

    private Operation getTinyBalanceDateOperation(Timestamp businessDateTo)
    {
        Operation tinyBalanceOp1 = TinyBalanceFinder.businessDateTo().lessThanEquals(businessDateTo);
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp1 = tinyBalanceOp1.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        return tinyBalanceOp1;
    }
    private int insertOneRowToTinyBalance(int balanceId, Timestamp businessDateFrom, Timestamp processDateFrom, double qty) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "insert into TINY_BALANCE(BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values (?,?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(3, businessDateFrom);
        ps.setDouble(2, qty);
        ps.setTimestamp(4, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(5, processDateFrom);
        ps.setTimestamp(6, InfinityTimestamp.getParaInfinity());
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    private int updateTinyBalanceForBalanceId(int balanceId, double qty, Timestamp businessDateFrom, Timestamp businessDateTo) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "update TINY_BALANCE set POS_QUANTITY_M = ? where BALANCE_ID = ? and FROM_Z = ? and THRU_Z = ? and OUT_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setDouble(1, qty);
        ps.setInt(2, balanceId);
        ps.setTimestamp(3, businessDateFrom);
        ps.setTimestamp(4, businessDateTo);
        ps.setTimestamp(5, InfinityTimestamp.getParaInfinity());
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    private int deleteTinyBalanceForBalanceId(int balanceId, Timestamp businessDateFrom, Timestamp businessDateTo) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "delete from TINY_BALANCE where BALANCE_ID = ? and FROM_Z = ? and OUT_Z < ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, businessDateFrom);
        ps.setTimestamp(3, businessDateTo);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    public void testRenewNonDatedObjectsFromDB() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            OrderList cachedList = new OrderList(OrderFinder.all());
            assertEquals(cachedList.size(), 7);

            // SQL to modify without going through mithra
            Order cachedOrder1000 = OrderFinder.findOne(OrderFinder.orderId().eq(1000));
            assertTrue(cachedOrder1000 == null);
            int insertedRows = this.insertOneRowToOrder(1000, new Timestamp(System.currentTimeMillis()), 3333, "test desc", "teststate", "testid");
            assertEquals(insertedRows, 1);

            int updatedRows = this.updateOrderForUserId(1, 9999);
            assertEquals(updatedRows, 1);

            int deletedRows = this.deleteOrdersForUserId(2);
            assertEquals(deletedRows, 1);

            int deletedRows2 = this.deleteOrderForOrderId(55);
            assertEquals(deletedRows2, 1);

            // assert no change on mithra objects in cache
            OrderList cachedList2 = new OrderList(OrderFinder.all());
            assertEquals(cachedList2.size(), 7);

            RenewedCacheStats renewResult = new MithraRuntimeCacheController(OrderFinder.class).renewCacheForOperation(OrderFinder.all());
            assertEquals(renewResult.getInserted().size() + renewResult.getUpdated().size(), 2);

            // assert mithra objects on insert/update/delete result
            OrderList cachedList3 = new OrderList(OrderFinder.all());
            assertEquals(cachedList3.size(), 6);

            Order cachedOrder55 = OrderFinder.findOne(OrderFinder.orderId().eq(55));
            assertTrue(cachedOrder55 == null);

            Order cachedOrder1 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            assertFalse(cachedOrder1 == null);
            assertEquals(cachedOrder1.getUserId(), 9999);

            cachedOrder1000 = OrderFinder.findOne(OrderFinder.orderId().eq(1000));
            assertTrue(cachedOrder1000 != null);
            assertEquals(cachedOrder1000.getUserId(), 3333);
        }
    }

    private int insertOneRowToOrder(int orderId, Timestamp orderDate, int userId, String desc, String state, String trackingId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "insert into APP.ORDERS (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) values (?,?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ps.setTimestamp(2, orderDate);
        ps.setInt(3, userId);
        ps.setString(4, desc);
        ps.setString(5, state);
        ps.setString(6, trackingId);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    private int updateOrderForUserId(int orderId, int newUserId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "update APP.ORDERS set USER_ID = ? where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, newUserId);
        ps.setInt(2, orderId);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    private int deleteOrderForOrderId(int orderId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "delete from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    private int deleteOrdersForUserId(int userId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "delete from APP.ORDERS where USER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, userId);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        return updatedRows;
    }

    public void testMutableList()
    {
        OrderList orders = OrderFinder.findMany(OrderFinder.all());
        assertFalse(orders.isEmpty());

        MutableList<Order> mutableList = orders.asGscList();

        try
        {
            mutableList.add(new Order());
            fail();
        }
        catch (Throwable t)
        {
            assertEquals(MithraBusinessException.class, t.getClass());
            assertEquals("Can't add to an operation based list. Make a copy of the list first.", t.getMessage());
        }

        try
        {
            mutableList.remove(0);
            fail();
        }
        catch (Throwable t)
        {
            assertEquals(MithraBusinessException.class, t.getClass());
            assertEquals("An operation based list is not modifiable. To modify this list, make a copy first.", t.getMessage());
        }

        try
        {
            mutableList.set(1, new Order());
            fail();
        }
        catch (Throwable t)
        {
            assertEquals(MithraBusinessException.class, t.getClass());
            assertEquals("Can't modify an operation based list.", t.getMessage());
        }
    }
}
