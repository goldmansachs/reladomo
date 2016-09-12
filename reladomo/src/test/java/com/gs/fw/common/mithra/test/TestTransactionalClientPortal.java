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

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.TimeZone;

import com.gs.collections.api.iterator.IntIterator;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.test.aggregate.TestSum;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;
import com.gs.fw.common.mithra.test.inherited.TestTxInherited;



public class TestTransactionalClientPortal extends RemoteMithraServerTestCase
        implements TestDatedBitemporalDatabaseChecker, TestDatedNonAuditedDatabaseChecker, TestDatabaseTimeoutSetter, TestDatedAuditOnlyDatabaseChecker
{

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private TestParaDatedBitemporal testParaDatedBitemporal;
    private TestDatedBitemporal testDatedBitemporal;
    private TestDatedBitemporalNull testDatedBitemporalNull;
    private TestDatedBitemporalOptimisticLocking testDatedBitemporalOptimisticLocking;
    private TestDatedAuditOnly testDatedAuditOnly;
    private TestDatedNonAudited testDatedNonAudited;
    private TestDatedNonAuditedNull testDatedNonAuditedNull;
    private TestConcurrentTransactions testConcurrentTransactions;
    private TestByteArray testByteArray;
    private TestReadOnlyTransactionParticipation testReadOnlyTransactionParticipation;
    private TestNullPrimaryKeyColumn testNullPrimaryKeyColumn;
    private TestOptimisticTransactionParticipation testOptimisticTransactionParticipation;
    private TestTxInherited testTxInherited;
    private TestTransactionalObject testTransactionalObject;
    private TestPureTransactionalObject testPureTransactionalObject;
    private TestInplaceUpdate testInplaceUpdate;
    private TestBigDecimal testBigDecimal;
    private TestUpdateListener testUpdateListener;
    public static final TimeZone TIME_ZONE = TimeZone.getTimeZone("Asia/Tokyo");
    private static long INITIAL_TIME;

    protected Class[] getRestrictedClassList()
    {
        HashSet result = new HashSet();
        addTestClassesFromOther(new TestMaxFromTable(), result);
        addTestClassesFromOther(new TestTxInherited(), result);
        addTestClassesFromOther(new TestUpdateListener(), result);

        result.add(TinyBalanceNull.class);
        result.add(TinyBalanceWithSmallDateNull.class);
        result.add(TestPositionIncomeExpenseNull.class);
        result.add(BitemporalOrderNull.class);
        result.add(BitemporalOrderItemNull.class);
        result.add(BitemporalOrderStatusNull.class);
        result.add(DatedAllTypesNull.class);


        result.add(NonAuditedBalanceNull.class);
        result.add(TestAgeBalanceSheetRunRateNull.class);

        result.add(AuditedOrder.class);
        result.add(AuditedOrderStatus.class);
        result.add(AuditedOrderStatusTwo.class);
        result.add(BitemporalOrder.class);
        result.add(Order.class);
        result.add(OrderParentToChildren.class);
        result.add(OrderItem.class);
        result.add(OrderStatus.class);
        result.add(OrderWi.class);
        result.add(OrderItemWi.class);
        result.add(OrderStatusWi.class);
        result.add(OrderItemStatus.class);
        result.add(TinyBalance.class);
        result.add(NonAuditedBalance.class);
        result.add(TestAgeBalanceSheetRunRate.class);
        result.add(TestPositionPrice.class);
        result.add(TestBinaryArray.class);
        result.add(AccountTransactionException.class);
        result.add(SpecialAccountTransactionException.class);
        result.add(MithraTestSequence.class);
        result.add(NotDatedWithNullablePK.class);
        result.add(DatedWithNullablePK.class);
        result.add(AuditOnlyBalance.class);
        result.add(OptimisticOrder.class);
        result.add(OptimisticOrderWithTimestamp.class);
        result.add(SalesLineItem.class);
        result.add(Sale.class);
        result.add(Seller.class);
        result.add(ProductSpecification.class);
        result.add(SalesLineItem.class);
        result.add(SpecialAccount.class);
        result.add(WallCrossImpl.class);
        result.add(PureOrder.class);
        result.add(PureOrderItem.class);
        result.add(BigOrder.class);
        result.add(BigOrderItem.class);
        result.add(BitemporalBigOrder.class);
        result.add(BitemporalBigOrderItem.class);
        result.add(ParaBalance.class);
        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    protected void setDefaultServerTimezone()
    {
        TimeZone.setDefault(TIME_ZONE);
    }

    public void slaveVmSetUp()
    {
        super.slaveVmSetUp();
        setInitialTime();
    }

    private void setInitialTime()
    {
        try
        {
            // timezone math is strange. don't change this by refactoring it.
            INITIAL_TIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2005-12-20 09:55:00").getTime();
        }
        catch (ParseException e)
        {
            throw new RuntimeException("will never get here");
        }
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        testDatedBitemporal = new TestDatedBitemporal();
        testDatedBitemporal.setChecker(this);
        testDatedBitemporalNull = new TestDatedBitemporalNull();
        testDatedBitemporalNull.setChecker(new NullDatedBitemporalDatabaseChecker(this));
        testDatedBitemporalOptimisticLocking = new TestDatedBitemporalOptimisticLocking();
        testDatedBitemporalOptimisticLocking.setChecker(this);
        testDatedAuditOnly = new TestDatedAuditOnly();
        testDatedAuditOnly.setChecker(this);
        testParaDatedBitemporal = new TestParaDatedBitemporal();
        this.testDatedNonAudited = new TestDatedNonAudited();
        this.testDatedNonAudited.setChecker(this);
        this.testDatedNonAuditedNull = new TestDatedNonAuditedNull();
        this.testDatedNonAuditedNull.setChecker(new NullDatedNonAuditedDatabaseChecker(this));
        this.testConcurrentTransactions = new TestConcurrentTransactions();
        this.testConcurrentTransactions.setDatabaseTimeoutSetter(this);
        this.testByteArray = new TestByteArray();
        this.testNullPrimaryKeyColumn = new TestNullPrimaryKeyColumn();
        this.testReadOnlyTransactionParticipation = new TestReadOnlyTransactionParticipation();
        this.testReadOnlyTransactionParticipation.setDatabaseTimeoutSetter(this);
        this.testOptimisticTransactionParticipation = new TestOptimisticTransactionParticipation();
        this.testTxInherited = new TestTxInherited();
        this.testTransactionalObject = new TestTransactionalObject();
        this.testPureTransactionalObject = new TestPureTransactionalObject();
        this.testInplaceUpdate = new TestInplaceUpdate();
        this.testBigDecimal = new TestBigDecimal();
        this.testUpdateListener = new TestUpdateListener();
        setInitialTime();
    }

    
    public void testAuditOnlyOptimisticTerminate()
    {
        final AuditedOrder order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1));
        assertNotNull(order);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                order.terminate();
                return null;
            }
        });

        assertNull(AuditedOrderFinder.findOneBypassCache(AuditedOrderFinder.orderId().eq(1)));
    }

    public void testBigDecimalInsert()
    {
       this.testBigDecimal.testInsert();
    }

    public void testBigDecimalUpdate()
    {
        this.testBigDecimal.testUpdate();
    }

    public void testBigDeicmalBitemporalUpdate()
    {
        this.testBigDecimal.testBitemporalUpdate();
    }

    public void testBigDecimalUpdateUntil()
            throws ParseException
    {
        this.testBigDecimal.testUpdateUntil();
    }

    public void testDatedIncrement() throws ParseException
    {
        this.testBigDecimal.testDatedIncrement();
    }

    public void testBigDecimalIncrementUntil()
            throws SQLException, ParseException
    {
        this.testBigDecimal.testIncrementUntil();
    }

    public void testAuditedWithRegularUpdate()
    {
        this.testInplaceUpdate.testAuditedWithRegularUpdate();
    }

    public void testBitemporalWithInplaceUpdate()
    {
        this.testInplaceUpdate.testBitemporalWithInplaceUpdate();
    }

    public void testBitemporalWithRegularUpdate()
    {
        this.testInplaceUpdate.testBitemporalWithRegularUpdate();
    }

    public void testBitemporalWithMixedUpdates()
    {
        this.testInplaceUpdate.testBitemporalWithMixedUpdates();
    }

    public void testAuditedWithInplaceUpdate()
    {
        this.testInplaceUpdate.testAuditedWithInplaceUpdate();
    }

    public void testAuditedWithInplaceUpdateAndDelete()
    {
        this.testInplaceUpdate.testAuditedWithInplaceUpdateAndDelete();
    }


    public void testAllOperationAndRegularAggregateAttribute()
    {
        TestSum testSum = new TestSum();
        testSum.testAllOperationAndRegularAggregateAttribute();
    }

    public void testRegularOperationAndAggregateAttribute()
    {
        TestSum testSum = new TestSum();
        testSum.testRegularOperationAndAggregateAttribute();
    }

    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        TestSum testSum = new TestSum();
        testSum.testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testUseMultiUpdate()
    {
        assertFalse(AccountTransactionExceptionFinder.getMithraObjectPortal().useMultiUpdate());
        assertTrue(OrderFinder.getMithraObjectPortal().useMultiUpdate());
    }

    public void testReadCacheUpdateRefreshesWithExistingReference()
    {
        testReadOnlyTransactionParticipation.testReadCacheUpdateRefreshesWithExistingReference();
    }

    public void testSimpleSequence()
    {
        AccountTransactionException transactionException = new AccountTransactionException();
        transactionException.setDeskId("A");
        transactionException.setExceptionDate(new Timestamp(System.currentTimeMillis()));
        transactionException.setExceptionDescription("Transaction failure");
        long exceptionId = transactionException.generateAndSetExceptionId();

        assertEquals(1005, exceptionId);
        transactionException.insert();
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertNotNull(AccountTransactionExceptionFinder.deskId().eq("A").and(AccountTransactionExceptionFinder.exceptionId().eq(1005)));
    }

    public void testDeleteAll()
    {
        TestTransactionalList test = new TestTransactionalList();
        test.testDeleteAll();
    }

    public void testDeleteAllAfterInsert()
    {
        TestTransactionalList test = new TestTransactionalList();
        test.testDeleteAllAfterInsert();
    }

    public void testPurgeAll()
    {
        TestTransactionalList test = new TestTransactionalList();
        test.testPurgeAll();
    }

    public void testPurgeAllAdHocList()
    {
        TestTransactionalAdhocFastList test = new TestTransactionalAdhocFastList();
        test.testPurgeAllListWithMultipleItems();
    }

    public void testPurgeAllAfterInsert() throws Exception
    {
        TestTransactionalList test = new TestTransactionalList();
        test.testPurgeAllAfterInsert();
    }

    public void serverCheckOrderDoesNotExist(int orderId)
            throws SQLException
    {
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(orderId)));
        Connection con = this.getServerSideConnection();
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

    public void testDeleteAllPreCached()
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));

        IntHashSet deletedIds = new IntHashSet();
        for(int i=0;i<list.size();i++)
        {
            deletedIds.add(list.getOrderAt(i).getOrderId());
        }

        list.deleteAll();
        checkServerSideDeleted(deletedIds);
        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
    }

    private void checkServerSideDeleted(IntHashSet deletedIds)
    {
        IntIterator it = deletedIds.intIterator();
        while(it.hasNext())
        {
            this.getRemoteSlaveVm().executeMethod("serverCheckOrderDoesNotExist", new Class[] { int.class} , new Object[] { new Integer(it.next())});
        }
    }

    public void setDatabaseLockTimeout(int timeout)
    {
        this.getRemoteSlaveVm().executeMethod("serverSetTimeout", new Class[] { int.class} , new Object[] { new Integer(timeout)});
    }

    public void serverSetTimeout(int timeout) throws SQLException
    {
        Connection con = null;
        Connection con2 = null;
        try
        {
            con = setTimeoutOnConnection(timeout);
            con2 = setTimeoutOnConnection(timeout);
        }
        finally
        {
            if (con != null) con.close();
            if (con2 != null) con2.close();
        }
    }

    private Connection setTimeoutOnConnection(int timeoutInMillis)
            throws SQLException
    {
        Connection con;
        con = getServerSideConnection();
        Statement stm = con.createStatement();
        stm.execute("SET LOCK_TIMEOUT "+timeoutInMillis);
        stm.close();
        return con;
    }

    public void testDeleteAllInBatches()
    {
        this.getRemoteSlaveVm().executeMethod("serverInsertOrderTestData", new Class[]{int.class, int.class}, new Object[]{Integer.valueOf(5000), Integer.valueOf(1000)});
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(500);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());
    }


    public void testDeleteAllInBatchesOneByOne()
    {
        OrderList firstList = createList(5000, 1000);
        firstList.insertAll();
        firstList.deleteAllInBatches(500);
        OrderList secondList = new OrderList(OrderFinder.userId().eq(999));
        assertEquals(0, secondList.size());
    }

    private OrderList createList(int count, int initialOrderId)
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

    public  void serverInsertOrderTestData(int count, int initialOrderId )
    {
        OrderList list = this.createList(count, initialOrderId);
        MithraTestResource mtr = this.getMithraTestResource();
        mtr.insertTestData(list);
    }

    public void testDeleteAllPreCachedInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
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
            assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(it.next())));
        }
        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
        tx.commit();
        list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
        checkServerSideDeleted(deletedIds);
    }

    public void testInsertingOneObjectWithinTx()
    {
        TestMaxFromTable testMaxFromTable = new TestMaxFromTable();
        testMaxFromTable.testInsertingOneObjectWithinTx();
    }

    public void testCount()
    {
        OrderList list = new OrderList(OrderFinder.state().eq("In-Progress"));
        int count = list.count();
        assertEquals(count, list.size());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(INITIAL_TIME);
        order.setOrderDate(orderDate);
        order.setUserIdNull();
        String description = "new order description";
        order.setDescription(description);
        order.setTrackingId("T1");
        order.setState("In-Progress");
        order.insert();
        list = new OrderList(OrderFinder.state().eq("In-Progress"));
        assertEquals(count+1, list.count());
        tx.commit();
    }

    public void testBatchUpdate()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderList list = new OrderList(OrderFinder.state().eq("In-Progress"));
        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        assertTrue(list.size() > 1);
        for(int i=0;i<list.size();i++)
        {
            Order order = list.getOrderAt(i);
            order.setTrackingId("T"+i);
            order.setUserId(1000+i);
        }
        tx.commit();

        list = new OrderList(OrderFinder.state().eq("In-Progress"));
        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        for(int i=0;i<list.size();i++)
        {
            Order order = list.getOrderAt(i);
            assertEquals("T"+i, order.getTrackingId());
            assertEquals(1000+i, order.getUserId());
        }

        this.getRemoteSlaveVm().executeMethod("serverTestBatchUpdateInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestBatchUpdateInDatabase");
    }

    public void serverTestBatchUpdateInCache()
    {
        OrderList list = new OrderList(OrderFinder.state().eq("In-Progress"));
        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        for(int i=0;i<list.size();i++)
        {
            Order order = list.getOrderAt(i);
            assertEquals("T"+i, order.getTrackingId());
            assertEquals(1000+i, order.getUserId());
        }
    }

    public void serverTestBatchUpdateInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select TRACKING_ID, USER_ID from APP.ORDERS where STATE = ? order by ORDER_ID";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, "In-Progress");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while(rs.next())
        {
            assertEquals("T"+count, rs.getString(1));
            assertEquals(1000+count, rs.getInt(2));
            count++;
        }
        rs.close();
        ps.close();
        con.close();
    }

    public void testBatchDelete()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        assertTrue(list.size() > 1);
        Order[] elements = list.elements();
        for(int i=0;i<elements.length;i++)
        {
            elements[i].delete();
        }
        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
        tx.commit();
        list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());

        this.getRemoteSlaveVm().executeMethod("serverTestBatchDeleteInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestBatchDeleteInDatabase");
    }

    public void serverTestBatchDeleteInCache()
    {
        assertNull(OrderFinder.findOne(OrderFinder.userId().eq(1)));
    }

    public void serverTestBatchDeleteInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select * from APP.ORDERS where USER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1017);
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testBatchInsertWithDatabaseRollback() throws SQLException
    {
        final int orderId = 1017;
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                            {
                                Order order = new Order();
                                order.setOrderId(orderId);
                                Timestamp orderDate = new Timestamp(System.currentTimeMillis());
                                order.setOrderDate(orderDate);
                                String description = "new order description";
                                order.setDescription(description);
                                order.insert();
                                Order order2 = new Order();
                                order2.setOrderId(orderId);
                                order2.setOrderDate(orderDate);
                                order2.setDescription(description);
                                order2.insert();
                                return null;
                            }
                        });
            fail("should not get here.");
        }
        catch(MithraDatabaseException e)
        {
            // ok
        }
        this.getRemoteSlaveVm().executeMethod("serverTestBatchInsertWithDatabaseRollbackInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestBatchInsertWithDatabaseRollbackInDatabase");
    }

    public void serverTestBatchInsertWithDatabaseRollbackInCache()
    {
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1017)));
    }

    public void serverTestBatchInsertWithDatabaseRollbackInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select * from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1017);
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testDelete()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        order.delete();
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1)));

        this.getRemoteSlaveVm().executeMethod("serverTestDeleteInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestDeleteInDatabase");
    }

    public void testDeleteInTransaction()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        order.delete();
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1)));
        tx.commit();

        this.getRemoteSlaveVm().executeMethod("serverTestDeleteInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestDeleteInDatabase");
    }

    public void serverTestDeleteInCache()
    {
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1)));
    }

    public void serverTestDeleteInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select * from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testBatchInsert()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        for(int i=1017;i<2000;i++)
        {
            Order order = new Order();
            int orderId = i;
            order.setOrderId(orderId);
            Timestamp orderDate = new Timestamp(INITIAL_TIME+i);
            order.setOrderDate(orderDate);
            order.setUserIdNull();
            String description = "description "+i;
            order.setDescription(description);
            order.setTrackingId("T"+i);
            order.insert();
        }
        tx.commit();

        this.getRemoteSlaveVm().executeMethod("serverTestBatchInsertInCache");

        this.getRemoteSlaveVm().executeMethod("serverTestBatchInsertInDatabase");
    }

    public void testQueryInsideTransaction() throws SQLException
    {
        testTransactionalObject.testQueryInsideTransaction();
    }

    public void testIsInMemory()
    {
        testTransactionalObject.testIsInMemory();
    }

    public void testConsecutiveTransactions()
    {
        testTransactionalObject.testConsecutiveTransactions();
    }

    public void testIsDeletedOrMarkForDeletion()
    {
        testTransactionalObject.testIsDeletedOrMarkForDeletion();
    }

    public void testIsDeletedOrMarkForDeletionWithSeparateThreads()
    {
        testTransactionalObject.testIsDeletedWithSeparateThreads();
    }

    public void testInsertWithException()
    {
        testTransactionalObject.testInsertWithException();
    }

    public void serverTestRefreshUpdateUserId()
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "update APP.ORDERS set USER_ID = ? where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        int newUserId = 1111;
        ps.setInt(1, newUserId);
        ps.setInt(2, 1);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        assertEquals(updatedRows, 1);
    }

    public void testRefresh() throws SQLException
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        this.getRemoteSlaveVm().executeMethod("serverTestRefreshUpdateUserId");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertEquals(1111, order.getUserId());
        tx.commit();
    }

    public void testRollback() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        tx.rollback();
    }

    public void testUpdateOneRow()
            throws SQLException
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        int oldValue = order.getUserId();
        OrderList firstList = new OrderList(OrderFinder.userId().eq(oldValue));
        int oldSize = firstList.size();
        int newValue = oldValue+10000;
        order.setUserId(newValue);
        assertEquals(order.getUserId(), newValue);
        assertEquals(newValue, ((Integer)this.getRemoteSlaveVm().executeMethod("serverTestUpdateOneRowDatabase")).intValue());
        assertEquals(newValue, ((Integer)this.getRemoteSlaveVm().executeMethod("serverTestUpdateOneRowCache")).intValue());

        OrderList list = new OrderList(OrderFinder.userId().eq(oldValue));
        assertEquals(oldSize, list.size()+1);
    }

    public Integer serverTestUpdateOneRowDatabase()
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select USER_ID from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int newValue = rs.getInt(1);
        rs.close();
        ps.close();
        con.close();
        return new Integer(newValue);
    }

    public Integer serverTestUpdateOneRowCache()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        return new Integer(order.getUserId());
    }

    public void testMultipleSet() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 1;
        int newValue = 7;
        String description = "new long description";
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.setUserId(newValue);
        order.setDescription(description);
        assertEquals(order.getUserId(), newValue);
        assertEquals(order.getDescription(), description);

        Order otherOrder = OrderFinder.findOne(OrderFinder.orderId().lessThan(orderId + 1));
        assertSame(otherOrder, order);
        assertEquals(order.getUserId(), newValue);
        assertEquals(order.getDescription(), description);

        tx.commit();

        assertEquals(order.getUserId(), newValue);
        assertEquals(order.getDescription(), description);

        assertEquals(newValue, ((Integer)this.getRemoteSlaveVm().executeMethod("serverTestUpdateOneRowDatabase")).intValue());
        assertEquals(newValue, ((Integer)this.getRemoteSlaveVm().executeMethod("serverTestUpdateOneRowCache")).intValue());

        assertEquals(description, this.getRemoteSlaveVm().executeMethod("serverTestMultipleSetDatabase"));
        assertEquals(description, this.getRemoteSlaveVm().executeMethod("serverTestMultipleSetCache"));

    }

    public String serverTestMultipleSetDatabase()
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select DESCRIPTION from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        rs.next();
        String result = rs.getString(1);
        rs.close();
        ps.close();
        con.close();
        return result;
    }

    public String serverTestMultipleSetCache()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        return order.getDescription();
    }

    public void testInsert() throws ParseException
    {
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(INITIAL_TIME);
        order.setOrderDate(orderDate);
        order.setUserIdNull();
        String description = "new order description";
        order.setDescription(description);
        order.setTrackingId("T1");
        order.insert();
        int retrieveCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertSame(order, order2);
        assertEquals(retrieveCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());

        OrderList list = new OrderList(OrderFinder.orderId().greaterThanEquals(orderId).and(OrderFinder.orderId().lessThan(1018)));
        assertEquals(1, list.size());
        assertSame(order, list.getOrderAt(0));

        String serverdesc = (String) this.getRemoteSlaveVm().executeMethod("serverTestInsert");
        assertEquals("new order description", serverdesc);

        this.getRemoteSlaveVm().executeMethod("serverTestInsertInDatabase");
    }

    public void testInsertInTransaction()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(INITIAL_TIME);
        order.setOrderDate(orderDate);
        order.setUserIdNull();
        String description = "new order description";
        order.setDescription(description);
        order.setTrackingId("T1");
        order.insert();
        int retrieveCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertSame(order, order2);
        assertEquals(retrieveCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());

        OrderList list = new OrderList(OrderFinder.orderId().greaterThanEquals(orderId).and(OrderFinder.orderId().lessThan(1018)));
        assertEquals(1, list.size());
        assertSame(order, list.getOrderAt(0));
        tx.commit();

        String serverdesc = (String) this.getRemoteSlaveVm().executeMethod("serverTestInsert");
        assertEquals("new order description", serverdesc);

        this.getRemoteSlaveVm().executeMethod("serverTestInsertInDatabase");
    }

    public void testInsertRollback()
            throws SQLException
    {
        testDatedBitemporalOptimisticLocking.testInsertRollback();
    }

    public void testInsertThenTerminateLaterBusinessDate()
            throws SQLException
    {
        testDatedBitemporalOptimisticLocking.testInsertThenTerminateLaterBusinessDate();
    }

    public void testInsertThenUpdateLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testInsertThenUpdateLaterBusinesDay();
    }

    public void testInsertWithIncrementOneSegment()
            throws Exception
    {
        testDatedBitemporalOptimisticLocking.testInsertWithIncrementOneSegment();
    }

    public void testInsertWithIncrementUntilOneSegment()
            throws Exception
    {
        testDatedBitemporalOptimisticLocking.testInsertWithIncrementUntilOneSegment();
    }

    public void testInsertWithIncrementUntilZeroSegments()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testInsertWithIncrementUntilZeroSegments();
    }

    public void testInsertWithIncrementZeroSegments()
            throws SQLException
    {
        testDatedBitemporalOptimisticLocking.testInsertWithIncrementZeroSegments();
    }

    public void testMultiSegmentIncrementDifferentBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementDifferentBusinesDay();
    }

    public void testMultiSegmentIncrementDifferentBusinesDayOnTwoDays()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementDifferentBusinesDayOnTwoDays();
    }

    public void testMultiSegmentIncrementSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementSameBusinesDay();
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment();
    }

    public void testMultiSegmentResetDifferentBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentResetDifferentBusinesDay();
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testMultiSegmentSetUntilSameBusinesDayUntilNextSegment()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentSetUntilSameBusinesDayUntilNextSegment();
    }

    public void testMultiSegmentTerminateLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentTerminateLaterBusinesDay();
    }

    public void testMultiSegmentTerminateSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentTerminateSameBusinesDay();
    }

    public void testMultiSegmentTransactionParticipation()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentTransactionParticipation();
    }

    public void testMultiSegmentUpdateDifferentBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentUpdateDifferentBusinesDay();
    }

    public void testMultiSegmentUpdateSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testMultiSegmentUpdateSameBusinesDay();
    }

    public void testNonPeristentCopy()
            throws Exception
    {
        testDatedBitemporalOptimisticLocking.testNonPeristentCopy();
    }

    public void testSetNullAndBack()
            throws Exception
    {
        testDatedBitemporalOptimisticLocking.testSetNullAndBack();
    }

    public void testSetOnMultipleObjects()
    {
        testDatedBitemporalOptimisticLocking.testSetOnMultipleObjects();
    }

    public void testTerminateLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testTerminateLaterBusinesDay();
    }

    public void testTerminateSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testTerminateSameBusinesDay();
    }

    public void testTerminateSameBusinesDayForAlreadyTerminated()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testTerminateSameBusinesDayForAlreadyTerminated();
    }

    public void testTripleIncrement()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testTripleIncrement();
    }

    public void testTripleSegmentIncrementDifferentBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testTripleSegmentIncrementDifferentBusinesDay();
    }

    public void testUpdateLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateLaterBusinesDay();
    }

    public void testUpdateSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateSameBusinesDay();
    }

    public void testUpdateSameBusinesDayRollback()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateSameBusinesDayRollback();
    }

    public void testUpdateSameBusinesDayTwice()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateSameBusinesDayTwice();
    }

    public void testUpdateUntilForLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateUntilForLaterBusinesDay();
    }

    public void testUpdateUntilForTwoLaterBusinesDays()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateUntilForTwoLaterBusinesDays();
    }

    public void testUpdateUntilSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testUpdateUntilSameBusinesDay();
    }

    public void testOptimisticInsertInTransaction()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderFinder.setTransactionModeReadCacheUpdateCausesRefreshAndLock(tx);
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(INITIAL_TIME);
        order.setOrderDate(orderDate);
        order.setUserIdNull();
        String description = "new order description";
        order.setDescription(description);
        order.setTrackingId("T1");
        order.insert();
        order = null;
        tx.commit();

        String serverdesc = (String) this.getRemoteSlaveVm().executeMethod("serverTestInsert");
        assertEquals("new order description", serverdesc);

        this.getRemoteSlaveVm().executeMethod("serverTestInsertInDatabase");
    }

    public void serverTestInsertInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select ORDER_DATE, USER_ID, DESCRIPTION, STATE, TRACKING_ID from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1017);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(INITIAL_TIME, rs.getTimestamp(1).getTime());
        rs.getInt(2);
        assertTrue(rs.wasNull());
        assertEquals("new order description", rs.getString(3));
        assertNull(rs.getString(4));
        assertEquals("T1", rs.getString(5));
        rs.close();
        ps.close();
        con.close();
    }

    public void serverTestBatchInsertInCache() throws SQLException
    {
        for(int i=1017;i<2000;i++)
        {
            Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(i));
            assertNotNull(order2);
            assertEquals(INITIAL_TIME+i, order2.getOrderDate().getTime());
            assertTrue(order2.isUserIdNull());
            assertEquals("description "+i, order2.getDescription());
            assertNull(order2.getState());
            assertEquals("T"+i, order2.getTrackingId());
        }
    }

    public void serverTestBatchInsertInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select ORDER_ID, ORDER_DATE, USER_ID, DESCRIPTION, STATE, TRACKING_ID from APP.ORDERS where ORDER_ID >= ? and ORDER_ID < ? ORDER BY ORDER_ID";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1017);
        ps.setInt(2, 2000);
        ResultSet rs = ps.executeQuery();
        for(int i=1017;i<2000;i++)
        {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals(INITIAL_TIME + i, rs.getTimestamp(2).getTime());
            rs.getInt(3);
            assertTrue(rs.wasNull());
            assertEquals("description "+i, rs.getString(4));
            assertNull(rs.getString(5));
            assertEquals("T"+i, rs.getString(6));
        }
        rs.close();
        ps.close();
        con.close();
    }

    public String serverTestInsert()
    {
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1017));
        return order2.getDescription();
    }

    public void serverCheckBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE where BALANCE_ID = ? and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(3, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public void serverCheckBitemporalInfinityRowNull(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE_NULL where BALANCE_ID = ? and " +
                "OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public int serverCheckDatedBitemporalRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return counts;
    }

    public int serverCheckDatedBitemporalRowCountsNull(int balanceId) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from TINY_BALANCE_NULL where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return counts;
    }

    public void serverCheckBitemporalTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID = ? and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(3, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void serverCheckBitemporalTerminatedNull(int balanceId)
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID = ? and " +
                " OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    protected Timestamp convertToServerTimeZone(Timestamp businessDate)
    {
        long time = businessDate.getTime();
        time += TimeZone.getDefault().getOffset(time) - TIME_ZONE.getOffset(time);
        return new Timestamp(time);
    }

    public void checkDatedBitemporalTerminated(int balance) throws SQLException
    {
        this.getRemoteSlaveVm().executeMethod("serverCheckBitemporalTerminated", new Class[] { int.class} , new Object[] { new Integer(balance) } );
    }

    public void checkDatedBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        businessDate = convertToServerTimeZone(businessDate);
        this.getRemoteSlaveVm().executeMethod("serverCheckBitemporalInfinityRow", new Class[] { int.class, double.class, Timestamp.class} ,
                new Object[] { new Integer(balanceId), new Double(quantity), businessDate } );
    }

    public void checkDatedBitemporalTimestampRow(int balanceId, double quantity, Timestamp businessDate, Timestamp processingDate) throws SQLException
    {
        businessDate = convertToServerTimeZone(businessDate);
        processingDate = convertToServerTimeZone(processingDate);
        this.getRemoteSlaveVm().executeMethod("serverCheckDatedBitemporalTimestampRow", new Class[] { int.class, double.class, Timestamp.class, Timestamp.class } ,
                new Object[] { new Integer(balanceId), new Double(quantity), businessDate, processingDate });
    }

    public int checkDatedBitemporalRowCounts(int balanceId) throws SQLException
    {
        return ((Integer) this.getRemoteSlaveVm().executeMethod("serverCheckDatedBitemporalRowCounts", new Class[] { int.class } ,
                new Object[] { new Integer(balanceId) } )).intValue();
    }

    public void serverCheckNonAuditedTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE where BALANCE_ID = ? and " +
                "THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void serverCheckNonAuditedTerminatedNull(int balanceId)
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE_NULL where BALANCE_ID = ? and " +
                "THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void serverCheckNonAuditedInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from NON_AUDITED_BALANCE where BALANCE_ID = ? and " +
                "THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public void serverCheckNonAuditedInfinityRowNull(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from NON_AUDITED_BALANCE_NULL where BALANCE_ID = ? and " +
                "THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public void serverCheckDatedBitemporalTimestampRow(int balanceId, double quantity, Timestamp businessDate, Timestamp processingDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE where BALANCE_ID = ? and " +
                "IN_Z < ? and OUT_Z >= ? and FROM_Z <= ? and THRU_Z > ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, processingDate);
        ps.setTimestamp(3, processingDate);
        ps.setTimestamp(4, businessDate);
        ps.setTimestamp(5, businessDate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertFalse(hasMoreResults);
    }

    public void serverCheckDatedBitemporalTimestampRowNull(int balanceId, double quantity, Timestamp businessDate, Timestamp processingDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE_NULL where BALANCE_ID = ? and " +
                "IN_Z < ? and (OUT_Z >= ? or OUT_Z is null) and FROM_Z <= ? and (THRU_Z > ? or THRU_Z is null)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, processingDate);
        ps.setTimestamp(3, processingDate);
        ps.setTimestamp(4, businessDate);
        ps.setTimestamp(5, businessDate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertFalse(hasMoreResults);
    }

    public Integer serverCheckNonAuditedRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return new Integer(counts);
    }

    public Integer serverCheckNonAuditedRowCountsNull(int balanceId) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE_NULL where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return new Integer(counts);
    }

    public void checkDatedAuditOnlyTerminated(int balance) throws SQLException
    {
        this.getRemoteSlaveVm().executeMethod("serverCheckDatedAuditOnlyTerminated", new Class[] { int.class} , new Object[] { new Integer(balance) } );
    }

    public void checkDatedAuditOnlyInfinityRow(int balanceId, double quantity, double interest) throws SQLException
    {
        this.getRemoteSlaveVm().executeMethod("serverCheckDatedAuditOnlyInfinityRow", new Class[] { int.class, double.class, double.class } ,
                new Object[] { new Integer(balanceId), new Double(quantity), new Double(interest) } );
    }

    public void checkDatedAuditOnlyTimestampRow(int balanceId, double quantity, double interest, Timestamp processingDate) throws SQLException
    {
        processingDate = convertToServerTimeZone(processingDate);
        this.getRemoteSlaveVm().executeMethod("serverCheckDatedAuditOnlyTimestampRow", new Class[] { int.class, double.class, double.class, Timestamp.class } ,
                new Object[] { new Integer(balanceId), new Double(quantity), new Double(interest), processingDate} );
    }

    public int checkDatedAuditOnlyRowCounts(int balanceId) throws SQLException
    {
        return ((Integer)this.getRemoteSlaveVm().executeMethod("serverCheckDatedAuditOnlyRowCounts", new Class[] { int.class} ,
                new Object[] { new Integer(balanceId) } )).intValue();
    }

    public void updateDatedAuditOnlyTimestamp(int balanceId, Timestamp timestamp) throws SQLException
    {
        timestamp = convertToServerTimeZone(timestamp);
        this.getRemoteSlaveVm().executeMethod("serverUpdateDatedAuditOnlyTimestamp", new Class[] { int.class, Timestamp.class } ,
                new Object[] { new Integer(balanceId), timestamp } );
    }

    public void insertDatedAuditOnly(int balanceId, double quantity, double interest, Timestamp timestamp) throws SQLException
    {
        timestamp = convertToServerTimeZone(timestamp);
        this.getRemoteSlaveVm().executeMethod("serverInsertDatedAuditOnly", new Class[] { int.class, double.class, double.class, Timestamp.class } ,
                new Object[] { new Integer(balanceId), new Double(quantity), new Double(interest), timestamp } );
    }

    public void serverCheckDatedAuditOnlyInfinityRow(int balanceId, double quantity, double interest) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, POS_INTEREST_M from AUDIT_ONLY_BALANCE where BALANCE_ID = ? and " +
                "OUT_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(quantity, rs.getDouble(1), 0);
        assertEquals(interest, rs.getDouble(2), 0);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void serverCheckDatedAuditOnlyTimestampRow(int balanceId, double quantity, double interest, Timestamp processingDate) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select POS_QUANTITY_M, POS_INTEREST_M from AUDIT_ONLY_BALANCE where BALANCE_ID = ? and " +
                "IN_Z <= ? and OUT_Z > ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, processingDate);
        ps.setTimestamp(3, processingDate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(quantity, rs.getDouble(1), 0);
        assertEquals(interest, rs.getDouble(2), 0);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void serverInsertDatedAuditOnly(int balanceId, double quantity, double interest, Timestamp timestamp) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "insert into AUDIT_ONLY_BALANCE (BALANCE_ID, POS_QUANTITY_M, POS_INTEREST_M, IN_Z, OUT_Z) values (?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setDouble(2, quantity);
        ps.setDouble(3, interest);
        ps.setTimestamp(4, timestamp);
        ps.setTimestamp(5, InfinityTimestamp.getParaInfinity());
        ps.executeUpdate();
        ps.close();
        con.close();
    }

    public void serverUpdateDatedAuditOnlyTimestamp(int balanceId, Timestamp timestamp) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "update AUDIT_ONLY_BALANCE set OUT_Z = ? where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, timestamp);
        ps.setInt(2, balanceId);
        ps.executeUpdate();
        ps.close();
        con.close();
    }

    public void serverCheckDatedAuditOnlyTerminated(int balanceId) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from AUDIT_ONLY_BALANCE where BALANCE_ID = ? and " +
                "OUT_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public Integer serverCheckDatedAuditOnlyRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select count(*) from AUDIT_ONLY_BALANCE where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return new Integer(counts);
    }

    public void checkDatedNonAuditedTerminated(int balance) throws SQLException
    {
        this.getRemoteSlaveVm().executeMethod("serverCheckNonAuditedTerminated", new Class[] { int.class} , new Object[] { new Integer(balance) } );
    }

    public void checkDatedNonAuditedInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        businessDate = convertToServerTimeZone(businessDate);
        this.getRemoteSlaveVm().executeMethod("serverCheckNonAuditedInfinityRow", new Class[] { int.class, double.class, Timestamp.class} ,
                new Object[] { new Integer(balanceId), new Double(quantity), businessDate } );
    }

    public int checkDatedNonAuditedRowCounts(int balanceId) throws SQLException
    {
        return ((Integer)this.getRemoteSlaveVm().executeMethod("serverCheckNonAuditedRowCounts", new Class[] { int.class} ,
                new Object[] { new Integer(balanceId) } )).intValue();
    }

    public void testDatedBitemporalDatedBitemporalInsert() throws SQLException
    {
        testDatedBitemporal.testInsertInTransaction();
    }

    public void testDatedBitemporalInsertInTransactionCustomInz() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertInTransactionCustomInz();
    }

    public void testDatedBitemporalInsertThenUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertThenUpdateLaterBusinesDay();
    }

    public void testDatedBitemporalTripleIncrement() throws SQLException, ParseException
    {
        testDatedBitemporal.testTripleIncrement();
    }

    public void testDatedBitemporalDatedBitemporalInsertRollback() throws SQLException
    {
        testDatedBitemporal.testInsertRollback();
    }

    public void testDatedBitemporalUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateSameBusinesDay();
    }

    public void testDatedBitemporalUpdateSameBusinesDayRollback() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateSameBusinesDayRollback();
    }

    public void testDatedBitemporalUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateSameBusinesDayTwice();
    }

    public void testDatedBitemporalUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateLaterBusinesDay();
    }

    public void testDatedBitemporalUpdateLaterBusinesDayCustomInz() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateLaterBusinesDayCustomInz();
    }

    public void testDatedBitemporalIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementSameBusinesDay();
    }

    public void testDatedBitemporalIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementSameBusinesDayRollback();
    }

    public void testDatedBitemporalIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementSameBusinesDayTwice();
    }

    public void testDatedBitemporalIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementLaterBusinesDay();
    }

    public void testDatedBitemporalTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testTerminateSameBusinesDay();
    }

    public void testDatedBitemporalTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testTerminateLaterBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentUpdateSameBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentUpdateDifferentBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentTransactionParticipation() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentTransactionParticipation();
    }

    public void testDatedBitemporalMultiSegmentResetDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentResetDifferentBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentTerminateSameBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentTerminateLaterBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementSameBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedBitemporalTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testTripleSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedBitemporalTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        testDatedBitemporal.testTerminateSameBusinesDayForAlreadyTerminated();
    }

    public void testDatedBitemporalIncrementUntilSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementUntilSameBusinesDay();
    }

    public void testDatedBitemporalIncrementUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementUntilForLaterBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testDatedBitemporalMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testDatedBitemporalMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testDatedBitemporalMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testDatedBitemporalUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateUntilSameBusinesDay();
    }

    public void testDatedBitemporalUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateUntilForLaterBusinesDay();
    }

    public void testDatedBitemporalMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentSetUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testDatedBitemporalMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testDatedBitemporalMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testDatedBitemporalMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testDatedBitemporalInsertWithIncrementZeroSegments() throws SQLException
    {
        testDatedBitemporal.testInsertWithIncrementZeroSegments();
    }

    public void testDatedBitemporalIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementLaterBusinesDayOnTwoDays();
    }

    public void testDatedBitemporalInsertWithIncrementOneSegment() throws Exception
    {
        testDatedBitemporal.testInsertWithIncrementOneSegment();
    }

    public void testDatedBitemporalMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedBitemporal.testMultiSegmentIncrementDifferentBusinesDayOnTwoDays();
    }

    public void testDatedBitemporalInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertWithIncrementUntilZeroSegments();
    }

    public void testDatedBitemporalInsertWithIncrementUntilOneSegment() throws Exception
    {
        testDatedBitemporal.testInsertWithIncrementUntilOneSegment();
    }

    public void testDatedBitemporalInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        testDatedBitemporal.testInsertThenTerminateLaterBusinessDate();
    }

    public void testDatedBitemporalInsertThenTerminateSameBusinessDate() throws SQLException
    {
        testDatedBitemporal.testInsertThenTerminateSameBusinessDate();
    }

    public void testDatedBitemporalUpdateUntilForTwoLaterBusinesDays() throws SQLException, ParseException
    {
        testDatedBitemporal.testUpdateUntilForTwoLaterBusinesDays();
    }

    public void testDatedBitemporalSetOnMultipleObjects()
    {
        testDatedBitemporal.testSetOnMultipleObjects();
    }

    public void testDatedBitemporalIncrementTwiceSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporal.testIncrementTwiceSameBusinesDay();
    }

    public void testDatedBitemporalPurge() throws SQLException, ParseException
    {
        testDatedBitemporal.testPurge();
    }

    public void testDatedBitemporalPurgeThenInsert() throws SQLException, ParseException
    {
        testDatedBitemporal.testPurgeThenInsert();
    }

    public void testDatedBitemporalPurgeAfterMultipleUpdateInsertOperations() throws SQLException, ParseException
    {
        testDatedBitemporal.testPurgeAfterMultipleUpdateInsertOperations();
    }

    public void testDatedBitemporalBatchPurge() throws SQLException, ParseException
    {
        testDatedBitemporal.testBatchPurge();
    }

    public void testDatedBitemporalInsertForRecovery() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertForRecovery();
    }

    public void testDatedBitemporalPurgeThenRollback() throws SQLException, ParseException
    {
        testDatedBitemporal.testPurgeThenRollback();
    }

    public void testDatedBitemporalInsertForRecoveryMultipleTimes() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertForRecoveryMultipleTimes();
    }

    public void testDatedBitemporalInsertForRecoveryThenInsert() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertForRecoveryThenInsert();
    }

    public void testDatedBitemporalInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        testDatedBitemporal.testInsertForRecoveryThenPurge();
    }

    public void testDatedBitemporalEqualsEdgePoint() throws SQLException, ParseException
    {
        testDatedBitemporal.testEqualsEdgePoint();
    }

    public void testDatedBitemporalEqualsEdgePointInTransaction() throws SQLException, ParseException
    {
        testDatedBitemporal.testEqualsEdgePointInTransaction();
    }

    public void testDatedNonAuditedInsertInTransaction() throws SQLException
    {
        testDatedNonAudited.testInsertInTransaction();
    }

    public void testDatedNonAuditedInsertThenUpdate() throws SQLException
    {
        testDatedNonAudited.testInsertThenUpdate();
    }

    public void testPositionPriceMultipleUpdatesForCBD()
    throws Exception
    {
        testParaDatedBitemporal.testPositionPriceMultipleUpdatesForCBD();
    }

    public void testPositionPriceMultipleUpdatesForLBD()
    throws Exception
    {
        testParaDatedBitemporal.testPositionPriceMultipleUpdatesForLBD();
    }

    public void testDatedNonAuditedInsertRollback() throws SQLException
    {
        testDatedNonAudited.testInsertRollback();
    }

    public void testDatedNonAuditedUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testUpdateSameBusinesDay();
    }

    public void testDatedNonAuditedUpdateSameBusinesDayNoLocking() throws SQLException, ParseException
    {
        testDatedNonAudited.testUpdateSameBusinesDayWithNoLocking();
    }

    public void testDatedNonAuditedMultipleUpdate() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultipleUpdate();
    }

    public void testDatedNonAuditedMultiKeyUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiKeyUpdateSameBusinesDay();
        testDatedNonAudited.testMultiKeyUpdateSameBusinesDay();
        testDatedNonAudited.testMultiKeyUpdateSameBusinesDay();
    }

    public void testDatedNonAuditedUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedNonAudited.testUpdateSameBusinesDayTwice();
    }

    public void testDatedNonAuditedUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testUpdateLaterBusinesDay();
    }

    public void testDatedNonAuditedTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testTerminateSameBusinesDay();
    }

    public void testDatedNonAuditedTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testTerminateLaterBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentUpdateSameBusinesDay();
        testDatedNonAudited.testMultiSegmentUpdateSameBusinesDay();
        testDatedNonAudited.testMultiSegmentUpdateSameBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentUpdateDifferentBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentTerminateSameBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentTerminateLaterBusinesDay();
    }

    public void testDatedNonAuditedTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        testDatedNonAudited.testTerminateSameBusinesDayForAlreadyTerminated();
    }

    public void testDatedNonAuditedUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testUpdateUntilSameBusinesDay();
    }

    public void testDatedNonAuditedUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testUpdateUntilForLaterBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentSetUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedNonAuditedMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testDatedNonAuditedMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testDatedNonAuditedMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testDatedNonAuditedMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testDatedNonAuditedMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testDatedNonAuditedIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testIncrementSameBusinesDay();
    }

    public void testDatedNonAuditedIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        testDatedNonAudited.testIncrementSameBusinesDayRollback();
    }

    public void testDatedNonAuditedIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedNonAudited.testIncrementSameBusinesDayTwice();
    }

    public void testDatedNonAuditedIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testIncrementLaterBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentIncrementSameBusinesDay();
    }

    public void testDatedNonAuditedMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedNonAuditedTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAudited.testTripleSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedNonAuditedInsertWithIncrementZeroSegments() throws SQLException
    {
        testDatedNonAudited.testInsertWithIncrementZeroSegments();
    }

    public void testDatedNonAuditedInsertWithIncrementOneSegment() throws Exception
    {
        testDatedNonAudited.testInsertWithIncrementOneSegment();
    }

    public void testDatedNonAuditedIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedNonAudited.testIncrementLaterBusinesDayOnTwoDays();
    }

    public void testDatedNonAuditedMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentIncrementDifferentBusinesDayOnTwoDays();
    }

    public void testDatedNonAuditedInsertThenIncrement() throws SQLException
    {
        testDatedNonAudited.testInsertThenIncrement();
    }

    public void testDatedNonAuditedInsertThenIncrementInOneTransaction() throws SQLException
    {
        testDatedNonAudited.testInsertThenIncrementInOneTransaction();
    }

    public void testDatedNonAuditedInsertWithIncrementTwiceTodayYesterday() throws SQLException
    {
        testDatedNonAudited.testInsertWithIncrementTwiceTodayYesterday();
    }

    public void testDatedNonAuditedInsertThenIncrementLaterDay() throws SQLException
    {
        testDatedNonAudited.testInsertThenIncrementLaterDay();
    }

    public void testDatedNonAuditedInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        testDatedNonAudited.testInsertWithIncrementUntilZeroSegments();
    }

    public void testDatedNonAuditedInsertWithIncrementUntilOneSegment() throws Exception
    {
        testDatedNonAudited.testInsertWithIncrementUntilOneSegment();
    }

    public void testDatedNonAuditedMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedNonAudited.testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedNonAuditedInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        testDatedNonAudited.testInsertThenTerminateLaterBusinessDate();
    }

    public void testDatedNonAuditedInsertThenTerminateSameBusinessDate() throws SQLException
    {
        testDatedNonAudited.testInsertThenTerminateSameBusinessDate();
    }

    public void testDatedNonAuditedPurge() throws SQLException, ParseException
    {
        testDatedNonAudited.testPurge();
    }

    public void testDatedNonAuditedPurgeThenInsert() throws SQLException, ParseException
    {
        testDatedNonAudited.testPurgeThenInsert();
    }

    public void testDatedNonAuditedPurgeAfterInsert() throws SQLException, ParseException
    {
        testDatedNonAudited.testPurgeAfterInsert();
    }

    public void testDatedNonAuditedBatchPurge() throws SQLException, ParseException
    {
        testDatedNonAudited.testBatchPurge();
    }

    public void testDatedNonAuditedPurgeThenRollback() throws SQLException, ParseException
    {
        testDatedNonAudited.testPurgeThenRollback();
    }

    public void testDatedNonAuditedInsertForRecovery() throws SQLException, ParseException
    {
        testDatedNonAudited.testInsertForRecovery();
    }

    public void testDatedNonAuditedInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        testDatedNonAudited.testInsertForRecoveryThenPurge();
    }

    public void testRollbackEffectivenessInDifferentTransaction()
    {
        testConcurrentTransactions.testRollbackEffectivenessInDifferentTransaction();
    }

    public void testRollbackEffectivenessInDifferentThread()
    {
        testConcurrentTransactions.testRollbackEffectivenessInDifferentThread();
    }

    public void testUniqueIndexVisibility()
    {
        testConcurrentTransactions.testUniqueIndexVisibility();
    }

    public void testPrimaryKeyVisibility()
    {
        testConcurrentTransactions.testPrimaryKeyVisibility();
    }

    public void testNonUniqueIndexVisibility()
    {
        testConcurrentTransactions.testNonUniqueIndexVisibility();
    }

    public void testDeletedObjectVisibility()
    {
        testConcurrentTransactions.testDeletedObjectVisibility();
    }

    public void testDeletedObjectBlockingInDifferentTransaction()
    {
        testConcurrentTransactions.testDeletedObjectBlockingInDifferentTransaction();
    }

    public void testImpactOfMultipleFindInSameTransaction()
    {
        testConcurrentTransactions.testImpactOfMultipleFindInSameTransaction();
    }

    public void testInMemoryObjectVisibilityInDifferentThread()
    {
        testConcurrentTransactions.testInMemoryObjectVisibilityInDifferentThread();
    }

    public void testInMemoryObjectVisibilityInDifferentTransaction()
    {
        testConcurrentTransactions.testInMemoryObjectVisibilityInDifferentTransaction();
    }

    public void testThreadNoTxObjectNoTxRead()
    {
        testConcurrentTransactions.testThreadNoTxObjectNoTxRead();
    }

    public void testThreadNoTxObjectNoTxWrite()
    {
        testConcurrentTransactions.testThreadNoTxObjectNoTxWrite();
    }

    public void testThreadNotInTxAndObjectInTxRead()
    {
        testConcurrentTransactions.testThreadNotInTxAndObjectInTxRead();
    }

    public void testSerializable()
    {
        testConcurrentTransactions.testSerializable();
    }

    public void testDifferentTransactionFindWaitsAndProceeds()
    {
        testConcurrentTransactions.testDifferentTransactionFindWaitsAndProceeds();
    }

    public void testDifferentTransactionGetterWaitsAndProceeds()
    {
        testConcurrentTransactions.testDifferentTransactionGetterWaitsAndProceeds();
    }

    public void testDifferentTransactionMustWaitAndGetException()
    {
        testConcurrentTransactions.testDifferentTransactionMustWaitAndGetException();
    }

    public void testFindConsistencyAfterCommit()
    {
        testConcurrentTransactions.testFindConsistencyAfterCommit();
    }

    public void testFindConsistencyBeforeCommit()
    {
        testConcurrentTransactions.testFindConsistencyBeforeCommit();
    }

    public void testDeadLockDetectionWithFoundObjects()
    {
        testConcurrentTransactions.testDeadLockDetectionWithFoundObjects();
    }

    public void testDatedOneThreadTxOneThreadNoTx()
    {
        testConcurrentTransactions.testDatedOneThreadTxOneThreadNoTx();
    }

    public void testDatedTwoThreadTx()
            throws Exception
    {
        testConcurrentTransactions.testDatedTwoThreadTxWritersBlock();
    }

    public void disabled_testDeadlockDetection() throws SQLException
    {
        //todo:uncomment this line and enable test case after testConcurrentTransactions.testDeadlockDetection() is enabled.
        //testConcurrentTransactions.testDeadlockDetection();
    }

    public void testByteArrayRetrieve()
    {
        testByteArray.testRetrieve();
    }

    public void testByteArrayUpdate()
    {
        testByteArray.testUpdate();
    }

    public void testByteArrayInsert()
    {
        testByteArray.testInsert();
    }

    public void testByteArrayFind()
    {
        testByteArray.testFind();
    }

    public void testByteArrayIn()
    {
        testByteArray.testIn();
    }

    public void testByteArrayNotIn()
    {
        testByteArray.testNotIn();
    }

    public void testUpdateObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testUpdateObjectWithNullablePrimaryKey();
    }

    public void testMultiUpdateObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testMultiUpdateObjectWithNullablePrimaryKey();
    }

    public void testMultiDeleteObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testMultiDeleteObjectWithNullablePrimaryKey();
    }

    public void testMixedMultiUpdateObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testMixedMultiUpdateObjectWithNullablePrimaryKey();
    }

    public void testMixedMultiDeleteObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testMixedMultiDeleteObjectWithNullablePrimaryKey();
    }

    public void testDeleteObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testDeleteObjectWithNullablePrimaryKey();
    }

    public void testRefreshObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testRefreshObjectWithNullablePrimaryKey();
    }

    public void testUpdateDatedObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testUpdateDatedObjectWithNullablePrimaryKey();
    }

    public void testMultiUpdateDatedObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testMultiUpdateDatedObjectWithNullablePrimaryKey();
    }

    public void testMixedMultiUpdateDatedObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testMixedMultiUpdateDatedObjectWithNullablePrimaryKey();
    }

    public void testGetForDateRangeWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testGetForDateRangeWithNullablePrimaryKey();
    }

    public void testTerminateDatedObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testTerminateDatedObjectWithNullablePrimaryKey();
    }

    public void testRefreshDatedObjectWithNullablePrimaryKey()
    {
        testNullPrimaryKeyColumn.testRefreshDatedObjectWithNullablePrimaryKey();
    }

    public void testDatedAuditOnlyInsertInTransaction() throws SQLException
    {
        testDatedAuditOnly.testInsertInTransaction();
    }

    public void testDatedAuditOnlyInsertInTransactionCustomInz() throws SQLException, ParseException
    {
        testDatedAuditOnly.testInsertInTransactionCustomInz();
    }

    public void testDatedAuditOnlyInsertRollback() throws SQLException
    {
        testDatedAuditOnly.testInsertRollback();
    }

    public void testDatedAuditOnlyUpdate() throws SQLException, ParseException
    {
        testDatedAuditOnly.testUpdate();
    }

    public void testDatedAuditOnlyUpdateCustomInz() throws SQLException, ParseException
    {
        testDatedAuditOnly.testUpdateCustomInz();
    }

    public void testDatedAuditOnlyUpdateWithFlush() throws SQLException, ParseException
    {
        testDatedAuditOnly.testUpdateWithFlush();
    }

    public void testDatedAuditOnlyTerminate() throws SQLException, ParseException
    {
        testDatedAuditOnly.testTerminate();
    }

    public void testDatedAuditOnlyTerminateCustomInz() throws SQLException, ParseException
    {
        testDatedAuditOnly.testTerminateCustomInz();
    }

    public void testDatedAuditOnlyUpdateTerminate() throws SQLException, ParseException
    {
        testDatedAuditOnly.testUpdateTerminate();
    }

    public void testDatedAuditOnlyInsertAll() throws SQLException
    {
        testDatedAuditOnly.testInsertAll();
    }

    public void testDatedAuditOnlyTransactionalRefresh() throws SQLException
    {
        testDatedAuditOnly.testTransactionalRefresh();
    }

    public void testDatedAuditOnlyPurge() throws SQLException, ParseException
    {
        testDatedAuditOnly.testPurge();
    }

    public void testDatedAuditOnlyPurgeAfterUpdate() throws SQLException, ParseException
    {
        testDatedAuditOnly.testPurgeAfterUpdate();
    }

    public void testDatedAuditOnlyPurgeMultipleTimes() throws SQLException, ParseException
    {
        testDatedAuditOnly.testPurgeMultipleTimes();
    }

    public void testDatedAuditOnlyPurgeThenRollback() throws SQLException, ParseException
    {
        testDatedAuditOnly.testPurgeThenRollback();
    }

    public void testDatedAuditOnlyPurgeCombineBatchPurge() throws SQLException, ParseException
    {
        testDatedAuditOnly.testPurgeCombineBatchPurge();
    }

    public void testDatedAuditOnlyPurgeForNestedTransaction() throws SQLException, ParseException
    {
        testDatedAuditOnly.testPurgeForNestedTransaction();
    }

    public void testDatedAuditOnlyInsertForRecovery() throws SQLException, ParseException
    {
        testDatedAuditOnly.testInsertForRecovery();
    }

    public void testDatedAuditOnlyInsertForRecoveryMultipleTimes() throws SQLException, ParseException
    {
        testDatedAuditOnly.testInsertForRecoveryMultipleTimes();
    }

    public void testDatedAuditOnlyInsertForRecoveryThenInsert() throws SQLException, ParseException
    {
        testDatedAuditOnly.testInsertForRecoveryThenInsert();
    }

    public void testDatedAuditOnlyInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        testDatedAuditOnly.testInsertForRecoveryThenPurge();
    }

    public void testDatedAuditOnlyLastModifiedQuerying() throws SQLException, ParseException
    {
        testDatedAuditOnly.testLastModifiedQuerying();
    }

    public void testDatedAuditOnlyInactivateForArchive() throws SQLException, ParseException
    {
        testDatedAuditOnly.testInactivateForArchive();
    }

    public void testDatedAuditOnlyEqualsEdgePoint() throws SQLException, ParseException
    {
        testDatedAuditOnly.testEqualsEdgePoint();
    }

    public void testDatedAuditOnlyEqualsEdgePointAfterChange() throws SQLException, ParseException
    {
        testDatedAuditOnly.testEqualsEdgePointAfterChange();
    }

    public void testDatedAuditOnlyEqualsEdgePointAfterChangeInTransaction() throws SQLException, ParseException
    {
        testDatedAuditOnly.testEqualsEdgePointAfterChangeInTransaction();
    }

    public void testDatedAuditOnlyEqualsEdgePointInTransaction() throws SQLException, ParseException
    {
        testDatedAuditOnly.testEqualsEdgePointInTransaction();
    }

    public void testUpdate()
    {
        testOptimisticTransactionParticipation.testUpdate();
    }

    public void testUpdateTwoAttributes()
    {
        testOptimisticTransactionParticipation.testUpdateTwoAttributes();
    }

    public void testSetVersionNotAllowed()
    {
        testOptimisticTransactionParticipation.testSetVersionNotAllowed();
    }

    public void testUpdateWithExecuteBufferedOperations()
    {
        testOptimisticTransactionParticipation.testUpdateWithExecuteBufferedOperations();
    }

    public void testBatchUpdateTwoAttributes()
    {
        testOptimisticTransactionParticipation.testBatchUpdateTwoAttributes();
    }

    public void testBatchUpdateTwoAttributesAndExecuteBufferedOperations()
    {
        testOptimisticTransactionParticipation.testBatchUpdateTwoAttributesAndExecuteBufferedOperations();
    }

    public void testMultiUpdateTwoAttributes()
    {
        testOptimisticTransactionParticipation.testMultiUpdateTwoAttributes();
    }

    public void testMultiUpdateTwoAttributesWithExecuteBuffered()
    {
        testOptimisticTransactionParticipation.testMultiUpdateTwoAttributesWithExecuteBuffered();
    }

    public void testOptimisticDelete()
    {
        testOptimisticTransactionParticipation.testOptimisticDelete();
    }

    public void testOptimisticDeleteAfterUpdate()
    {
        testOptimisticTransactionParticipation.testOptimisticDeleteAfterUpdate();
    }

    public void testOptimisticBatchDelete()
    {
        testOptimisticTransactionParticipation.testOptimisticBatchDelete();
    }

    public void testOptimisticDeleteAfterUpdateWithExecuteBuffer()
    {
        testOptimisticTransactionParticipation.testOptimisticDeleteAfterUpdateWithExecuteBuffer();
    }

    public void testUpdateWithTimestamp()
    {
        testOptimisticTransactionParticipation.testUpdateWithTimestamp();
    }

    public void testUpdateTwoAttributesWithTimestamp()
    {
        testOptimisticTransactionParticipation.testUpdateTwoAttributesWithTimestamp();
    }

    public void testSetVersionNotAllowedWithTimestamp()
    {
        testOptimisticTransactionParticipation.testSetVersionNotAllowedWithTimestamp();
    }

    public void testUpdateWithExecuteBufferedOperationsWithTimestamp()
    {
        testOptimisticTransactionParticipation.testUpdateWithExecuteBufferedOperationsWithTimestamp();
    }

    public void testBatchUpdateTwoAttributesWithTimestamp()
    {
        testOptimisticTransactionParticipation.testBatchUpdateTwoAttributesWithTimestamp();
    }

    public void testBatchUpdateTwoAttributesAndExecuteBufferedOperationsWithTimestamp()
    {
        testOptimisticTransactionParticipation.testBatchUpdateTwoAttributesAndExecuteBufferedOperationsWithTimestamp();
    }

    public void testMultiUpdateTwoAttributesWithTimestamp()
    {
        testOptimisticTransactionParticipation.testMultiUpdateTwoAttributesWithTimestamp();
    }

    public void testMultiUpdateTwoAttributesWithExecuteBufferedWithTimestamp()
    {
        testOptimisticTransactionParticipation.testMultiUpdateTwoAttributesWithExecuteBufferedWithTimestamp();
    }

    public void testOptimisticDeleteWithTimestamp()
    {
        testOptimisticTransactionParticipation.testOptimisticDeleteWithTimestamp();
    }

    public void testOptimisticDeleteAfterUpdateWithTimestamp()
    {
        testOptimisticTransactionParticipation.testOptimisticDeleteAfterUpdateWithTimestamp();
    }

    public void testOptimisticBatchDeleteWithTimestamp()
    {
        testOptimisticTransactionParticipation.testOptimisticBatchDeleteWithTimestamp();
    }

    public void testOptimisticDeleteAfterUpdateWithExecuteBufferWithTimestamp()
    {
        testOptimisticTransactionParticipation.testOptimisticDeleteAfterUpdateWithExecuteBufferWithTimestamp();
    }

    public void testOptimisticInsertWithTimestamp()
    {
        testOptimisticTransactionParticipation.testOptimisticInsertWithTimestamp();
    }

    public void testReadMonkeyById()
    {
        testTxInherited.testReadMonkeyById();
    }

    public void testReadMonkeyByName()
    {
        testTxInherited.testReadMonkeyByName();
    }

    public void testReadMonkeyByBodyTemp()
    {
        testTxInherited.testReadMonkeyByBodyTemp();
    }

    public void testReadMonkeyByBodyTailLength()
    {
        testTxInherited.testReadMonkeyByBodyTailLength();
    }

    public void testPolymorphicAnimalById()
    {
        testTxInherited.testPolymorphicAnimalById();
    }

    public void testPolymorphicAnimal()
    {
        testTxInherited.testPolymorphicAnimal();
    }

    public void testPolymorphicMammalById()
    {
        testTxInherited.testPolymorphicMammalById();
    }

    public void testInsertAnimal()
    {
        testTxInherited.testInsertAnimal();
    }

    public void testInsertMammal()
    {
        testTxInherited.testInsertMammal();
    }

    public void testInsertMonkey()
    {
        testTxInherited.testInsertMonkey();
    }

    public void testInsertTwelveMonkies()
    {
        testTxInherited.testInsertTwelveMonkies();
    }

    public void testDeleteAnimal()
    {
        testTxInherited.testDeleteAnimal();
    }

    public void testDeleteMammal()
    {
        testTxInherited.testDeleteMammal();
    }

    public void testDeleteMonkey()
    {
        testTxInherited.testDeleteMonkey();
    }

    public void testInheritedBatchDelete()
    {
        testTxInherited.testBatchDelete();
    }

    public void testUpdateAnimal()
    {
        testTxInherited.testUpdateAnimal();
    }

    public void testUpdateMammal()
    {
        testTxInherited.testUpdateMammal();
    }

    public void testUpdateMonkey()
    {
        testTxInherited.testUpdateMonkey();
    }

    public void testMultiUpdateMonkey()
    {
        testTxInherited.testMultiUpdateMonkey();
    }

    public void testBatchUpdateMonkey()
    {
        testTxInherited.testBatchUpdateMonkey();
    }

    public void testUniquenessTopFirst()
    {
        testTxInherited.testUniquenessTopFirst();
    }

    public void testUniquenessBottomFirst()
    {
        testTxInherited.testUniquenessBottomFirst();
    }

    public void testBitemporalOptimisticInClauseWithBusinessDate()
            throws Exception
    {
        testDatedBitemporalOptimisticLocking.testInClauseWithBusinessDate();
    }

    public void testBitemporalOptimisticIncrementLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementLaterBusinesDay();
    }

    public void testBitemporalOptimisticIncrementLaterBusinesDayOnTwoDays()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementLaterBusinesDayOnTwoDays();
    }

    public void testBitemporalOptimisticIncrementSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementSameBusinesDay();
    }

    public void testBitemporalOptimisticIncrementSameBusinesDayRollback()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementSameBusinesDayRollback();
    }

    public void testBitemporalOptimisticIncrementSameBusinesDayTwice()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementSameBusinesDayTwice();
    }

    public void testBitemporalOptimisticIncrementTwiceSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementTwiceSameBusinesDay();
    }

    public void testBitemporalOptimisticIncrementUntilForLaterBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementUntilForLaterBusinesDay();
    }

    public void testBitemporalOptimisticIncrementUntilSameBusinesDay()
            throws SQLException, ParseException
    {
        testDatedBitemporalOptimisticLocking.testIncrementUntilSameBusinesDay();
    }

    private TinyBalance findTinyBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return this.findTinyBalanceByDates(balanceId, businessDate, InfinityTimestamp.getParaInfinity());
    }

    private TinyBalance findTinyBalanceByDates(int balanceId, Timestamp businessDate, Timestamp processingDate)
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceFinder.businessDate().eq(businessDate))
                            .and(TinyBalanceFinder.processingDate().eq(processingDate)));
    }

    public void serverTerminateTinyBalance() throws ParseException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalance tb = findTinyBalanceForBusinessDate(1, new Timestamp(timestampFormat.parse("2005-01-18 00:00:00").getTime()));
                tb.terminate();
                return null;
            }
        });
    }

    public void testServerSideTermination() throws Exception
    {
        final TinyBalance tb = findTinyBalanceForBusinessDate(1, new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime()));
        this.getRemoteSlaveVm().executeMethod("serverTerminateTinyBalance", new Class[] {} , null);
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    tb.setQuantity(12.4);
                    fail("should not get here");
                    return null;
                }
            });
        }
        catch (MithraDeletedException e)
        {
            // ignore
        }
    }

    public void testServerSideTerminationWithOptimisticLocking() throws Exception
    {
        final TinyBalance tb = findTinyBalanceForBusinessDate(1, new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime()));
        this.getRemoteSlaveVm().executeMethod("serverTerminateTinyBalance", new Class[] {} , null);
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                    tb.setQuantity(12.4);
                    return null;
                }
            });
            fail("should not get here");
        }
        catch (MithraOptimisticLockException e)
        {
            // ignore
        }
    }

    public void serverModifyPureOrder()
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        order.setDescription("foo");
    }

    public void testPureBypassCache()
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        assertNotNull(order);
        this.getRemoteSlaveVm().executeMethod("serverModifyPureOrder");
        order = PureOrderFinder.findOneBypassCache(PureOrderFinder.orderId().eq(1));
        assertNotNull(order);
        assertEquals("foo", order.getDescription());
    }

    public void testPureBypassCacheInTransaction()
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        assertNotNull(order);
        this.getRemoteSlaveVm().executeMethod("serverModifyPureOrder");

        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        tx.commit();
        assertNotNull(order);
        assertEquals("foo", order.getDescription());
    }

    public void testPureAttributeCounterOnUpdate()
    {
        testPureTransactionalObject.testPureAttributeCounterOnUpdate();
    }

    public void testPureBatchInsertWithDatabaseRollback()
            throws SQLException
    {
        testPureTransactionalObject.testPureBatchInsertWithDatabaseRollback();
    }

    public void testPureCommitDelete()
            throws SQLException
    {
        testPureTransactionalObject.testPureCommitDelete();
    }

    public void testPureCommitFailure()
    {
        testPureTransactionalObject.testPureCommitFailure();
    }

    public void testPureCommitFailure2()
    {
        testPureTransactionalObject.testPureCommitFailure2();
    }

    public void testPureCommitFailureSingleResource()
    {
        testPureTransactionalObject.testPureCommitFailureSingleResource();
    }

    public void testPureCommitInsertDelete()
            throws SQLException
    {
        testPureTransactionalObject.testPureCommitInsertDelete();
    }

    public void testPureDelete()
            throws SQLException
    {
        testPureTransactionalObject.testPureDelete();
    }

    public void testPureDeleteAddSameFind()
    {
        testPureTransactionalObject.testPureDeleteAddSameFind();
    }

    public void testPureDeleteMultiple()
    {
        testPureTransactionalObject.testPureDeleteMultiple();
    }

    public void testPureDetachedIsDeletedWithSeparateThreads()
    {
        testPureTransactionalObject.testPureDetachedIsDeletedWithSeparateThreads();
    }

    public void testPureGetNonPersistentCopyInTransaction()
    {
        testPureTransactionalObject.testPureGetNonPersistentCopyInTransaction();
    }

    public void testPureGlobalCounterOnDelete()
    {
        testPureTransactionalObject.testPureGlobalCounterOnDelete();
    }

    public void testPureGlobalCounterOnInsert()
    {
        testPureTransactionalObject.testPureGlobalCounterOnInsert();
    }

    public void testPureInMemoryTransactionalObject()
    {
        testPureTransactionalObject.testPureInMemoryTransactionalObject();
    }

    public void testPureInnerTransactionRetriable()
    {
        testPureTransactionalObject.testPureInnerTransactionRetriable();
    }

    public void testPureInsert()
            throws SQLException
    {
        testPureTransactionalObject.testPureInsert();
    }

    public void testPureInsertAndFindInTransaction()
            throws SQLException
    {
        testPureTransactionalObject.testPureInsertAndFindInTransaction();
    }

    public void testPureInsertAndUpdate()
            throws SQLException
    {
        testPureTransactionalObject.testPureInsertAndUpdate();
    }

    public void testPureInsertAndUpdateAndInsertInTransaction()
            throws SQLException
    {
        testPureTransactionalObject.testPureInsertAndUpdateAndInsertInTransaction();
    }

    public void testPureInsertAndUpdateInTransaction()
            throws SQLException
    {
        testPureTransactionalObject.testPureInsertAndUpdateInTransaction();
    }

    public void testPureInsertForRecovery()
            throws SQLException
    {
        testPureTransactionalObject.testPureInsertForRecovery();
    }

    public void testPureInsertUpdateReordering()
    {
        testPureTransactionalObject.testPureInsertUpdateReordering();
    }

    public void testPureIsDeletedOrMarkForDeletion()
    {
        testPureTransactionalObject.testPureIsDeletedOrMarkForDeletion();
    }

    public void testPureIsDeletedWithSeparateThreads()
    {
        testPureTransactionalObject.testPureIsDeletedWithSeparateThreads();
    }

    public void testPureIsInMemory()
    {
        testPureTransactionalObject.testPureIsInMemory();
    }

    public void testPureMaximumStringLength()
            throws Exception
    {
        testPureTransactionalObject.testPureMaximumStringLength();
    }

    public void testPureMaxLengthWithTruncate()
    {
        testPureTransactionalObject.testPureMaxLengthWithTruncate();
    }

    public void testPureMultipleSet()
            throws SQLException
    {
        testPureTransactionalObject.testPureMultipleSet();
    }

    public void testPureQueryExpiration()
    {
        testPureTransactionalObject.testPureQueryExpiration();
    }

    public void testPureRollbackInsert()
            throws SQLException
    {
        testPureTransactionalObject.testPureRollbackInsert();
    }

    public void testPureRollbackInsertDelete()
            throws SQLException
    {
        testPureTransactionalObject.testPureRollbackInsertDelete();
    }

    public void testPureRollbackUpdate()
            throws SQLException
    {
        testPureTransactionalObject.testPureRollbackUpdate();
    }

    public void testPureSetPrimitiveAttributesToNullToInMemoryObject()
    {
        testPureTransactionalObject.testPureSetPrimitiveAttributesToNullToInMemoryObject();
    }

    public void testPureSetPrimitiveAttributesToNullToPersistedObjectWithReadOnlyAttribute()
    {
        testPureTransactionalObject.testPureSetPrimitiveAttributesToNullToPersistedObjectWithReadOnlyAttribute();
    }

    public void testPureSetter()
    {
        testPureTransactionalObject.testPureSetter();
    }

    public void testPureSingleSet()
            throws SQLException
    {
        testPureTransactionalObject.testPureSingleSet();
    }

    public void testPureTransactionalMethod()
            throws SQLException
    {
        testPureTransactionalObject.testPureTransactionalMethod();
    }

    public void testPureTransactionalMethodWithException()
            throws SQLException
    {
        testPureTransactionalObject.testPureTransactionalMethodWithException();
    }

    public void testPureTransactionalRetrieve()
    {
        testPureTransactionalObject.testPureTransactionalRetrieve();
    }

    public void testPureUpdateOneRow()
            throws SQLException
    {
        testPureTransactionalObject.testPureUpdateOneRow();
    }

    private TinyBalanceList getPurgeAllTinyBalanceList(int balanceId)
    {
        Operation tinyBalanceOp = TinyBalanceFinder.balanceId().eq(balanceId);
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.acmapCode().eq("A"));
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        tinyBalanceOp = tinyBalanceOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        TinyBalanceList tinyBalanceList = new TinyBalanceList(tinyBalanceOp);

        return tinyBalanceList;
    }

    private AuditOnlyBalanceList getPurgeAllAuditOnlyList(int balanceId)
    {
        Operation auditOnlyOp = AuditOnlyBalanceFinder.balanceId().eq(balanceId);
        auditOnlyOp = auditOnlyOp.and(AuditOnlyBalanceFinder.acmapCode().eq("A"));
        auditOnlyOp = auditOnlyOp.and(AuditOnlyBalanceFinder.processingDate().equalsEdgePoint());
        AuditOnlyBalanceList auditOnlyList = new AuditOnlyBalanceList(auditOnlyOp);

        return auditOnlyList;
    }

    private NonAuditedBalanceList getPurgeAllNonAuditedList(int balanceId)
    {
        Operation nonAuditBalanceOp = NonAuditedBalanceFinder.balanceId().eq(balanceId);
        nonAuditBalanceOp = nonAuditBalanceOp.and(NonAuditedBalanceFinder.acmapCode().eq("A"));
        nonAuditBalanceOp = nonAuditBalanceOp.and(NonAuditedBalanceFinder.businessDate().equalsEdgePoint());
        NonAuditedBalanceList nonAuditBalanceList = new NonAuditedBalanceList(nonAuditBalanceOp);

        return nonAuditBalanceList;
    }



    public class NullDatedNonAuditedDatabaseChecker implements TestDatedNonAuditedDatabaseChecker
    {
        private TestTransactionalClientPortal portal;

        public NullDatedNonAuditedDatabaseChecker(TestTransactionalClientPortal portal)
        {
            this.portal = portal;
        }

        public void checkDatedNonAuditedTerminated(int balance) throws SQLException
        {
            portal.getRemoteSlaveVm().executeMethod("serverCheckNonAuditedTerminatedNull", new Class[] { int.class} , new Object[] { new Integer(balance) } );
        }

        public void checkDatedNonAuditedInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
        {
            businessDate = convertToServerTimeZone(businessDate);
            portal.getRemoteSlaveVm().executeMethod("serverCheckNonAuditedInfinityRowNull", new Class[] { int.class, double.class, Timestamp.class} ,
                    new Object[] { new Integer(balanceId), new Double(quantity), businessDate } );
        }

        public int checkDatedNonAuditedRowCounts(int balanceId) throws SQLException
        {
            return ((Integer)portal.getRemoteSlaveVm().executeMethod("serverCheckNonAuditedRowCountsNull", new Class[] { int.class} ,
                    new Object[] { new Integer(balanceId) } )).intValue();
        }
    }

    public class NullDatedBitemporalDatabaseChecker implements TestDatedBitemporalDatabaseChecker
    {
        private TestTransactionalClientPortal portal;

        public NullDatedBitemporalDatabaseChecker(TestTransactionalClientPortal portal)
        {
            this.portal = portal;
        }

        public void checkDatedBitemporalTerminated(int balance) throws SQLException
        {
            portal.getRemoteSlaveVm().executeMethod("serverCheckBitemporalTerminatedNull", new Class[] { int.class} , new Object[] { new Integer(balance) } );
        }

        public void checkDatedBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
        {
            businessDate = convertToServerTimeZone(businessDate);
            portal.getRemoteSlaveVm().executeMethod("serverCheckBitemporalInfinityRowNull", new Class[] { int.class, double.class, Timestamp.class} ,
                    new Object[] { new Integer(balanceId), new Double(quantity), businessDate } );
        }

        public void checkDatedBitemporalTimestampRow(int balanceId, double quantity, Timestamp businessDate, Timestamp processingDate) throws SQLException
        {
            businessDate = convertToServerTimeZone(businessDate);
            processingDate = convertToServerTimeZone(processingDate);
            portal.getRemoteSlaveVm().executeMethod("serverCheckDatedBitemporalTimestampRowNull", new Class[] { int.class, double.class, Timestamp.class, Timestamp.class } ,
                    new Object[] { new Integer(balanceId), new Double(quantity), businessDate, processingDate });
        }

        public int checkDatedBitemporalRowCounts(int balanceId) throws SQLException
        {
            return ((Integer) portal.getRemoteSlaveVm().executeMethod("serverCheckDatedBitemporalRowCountsNull", new Class[] { int.class } ,
                    new Object[] { new Integer(balanceId) } )).intValue();
        }
    }


    public void testDatedBitemporalNullDatedBitemporalInsert() throws SQLException
    {
        testDatedBitemporalNull.testInsertInTransaction();
    }

    public void testDatedBitemporalNullInsertInTransactionCustomInz() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertInTransactionCustomInz();
    }

    public void testDatedBitemporalNullInsertThenUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertThenUpdateLaterBusinesDay();
    }

    public void testDatedBitemporalNullTripleIncrement() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testTripleIncrement();
    }

    public void testDatedBitemporalNullDatedBitemporalInsertRollback() throws SQLException
    {
        testDatedBitemporalNull.testInsertRollback();
    }

    public void testDatedBitemporalNullUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateSameBusinesDay();
    }

    public void testDatedBitemporalNullUpdateSameBusinesDayRollback() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateSameBusinesDayRollback();
    }

    public void testDatedBitemporalNullUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateSameBusinesDayTwice();
    }

    public void testDatedBitemporalNullUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateLaterBusinesDay();
    }

    public void testDatedBitemporalNullUpdateLaterBusinesDayCustomInz() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateLaterBusinesDayCustomInz();
    }

    public void testDatedBitemporalNullIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementSameBusinesDay();
    }

    public void testDatedBitemporalNullIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementSameBusinesDayRollback();
    }

    public void testDatedBitemporalNullIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementSameBusinesDayTwice();
    }

    public void testDatedBitemporalNullIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementLaterBusinesDay();
    }

    public void testDatedBitemporalNullTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testTerminateSameBusinesDay();
    }

    public void testDatedBitemporalNullTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testTerminateLaterBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentUpdateSameBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentUpdateDifferentBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentTransactionParticipation() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentTransactionParticipation();
    }

    public void testDatedBitemporalNullMultiSegmentResetDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentResetDifferentBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentTerminateSameBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentTerminateLaterBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementSameBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedBitemporalNullTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testTripleSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedBitemporalNullTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testTerminateSameBusinesDayForAlreadyTerminated();
    }

    public void testDatedBitemporalNullIncrementUntilSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementUntilSameBusinesDay();
    }

    public void testDatedBitemporalNullIncrementUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementUntilForLaterBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testDatedBitemporalNullUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateUntilSameBusinesDay();
    }

    public void testDatedBitemporalNullUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateUntilForLaterBusinesDay();
    }

    public void testDatedBitemporalNullMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentSetUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalNullMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testDatedBitemporalNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testDatedBitemporalNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testDatedBitemporalNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testDatedBitemporalNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testDatedBitemporalNullInsertWithIncrementZeroSegments() throws SQLException
    {
        testDatedBitemporalNull.testInsertWithIncrementZeroSegments();
    }

    public void testDatedBitemporalNullIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementLaterBusinesDayOnTwoDays();
    }

    public void testDatedBitemporalNullInsertWithIncrementOneSegment() throws Exception
    {
        testDatedBitemporalNull.testInsertWithIncrementOneSegment();
    }

    public void testDatedBitemporalNullMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testMultiSegmentIncrementDifferentBusinesDayOnTwoDays();
    }

    public void testDatedBitemporalNullInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertWithIncrementUntilZeroSegments();
    }

    public void testDatedBitemporalNullInsertWithIncrementUntilOneSegment() throws Exception
    {
        testDatedBitemporalNull.testInsertWithIncrementUntilOneSegment();
    }

    public void testDatedBitemporalNullInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        testDatedBitemporalNull.testInsertThenTerminateLaterBusinessDate();
    }

    public void testDatedBitemporalNullInsertThenTerminateSameBusinessDate() throws SQLException
    {
        testDatedBitemporalNull.testInsertThenTerminateSameBusinessDate();
    }

    public void testDatedBitemporalNullUpdateUntilForTwoLaterBusinesDays() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testUpdateUntilForTwoLaterBusinesDays();
    }

    public void testDatedBitemporalNullSetOnMultipleObjects()
    {
        testDatedBitemporalNull.testSetOnMultipleObjects();
    }

    public void testDatedBitemporalNullIncrementTwiceSameBusinesDay() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testIncrementTwiceSameBusinesDay();
    }

    public void testDatedBitemporalNullPurge() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testPurge();
    }

    public void testDatedBitemporalNullPurgeThenInsert() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testPurgeThenInsert();
    }

    public void testDatedBitemporalNullPurgeAfterMultipleUpdateInsertOperations() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testPurgeAfterMultipleUpdateInsertOperations();
    }

    public void testDatedBitemporalNullBatchPurge() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testBatchPurge();
    }

    public void testDatedBitemporalNullInsertForRecovery() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertForRecovery();
    }

    public void testDatedBitemporalNullPurgeThenRollback() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testPurgeThenRollback();
    }

    public void testDatedBitemporalNullInsertForRecoveryMultipleTimes() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertForRecoveryMultipleTimes();
    }

    public void testDatedBitemporalNullInsertForRecoveryThenInsert() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertForRecoveryThenInsert();
    }

    public void testDatedBitemporalNullInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        testDatedBitemporalNull.testInsertForRecoveryThenPurge();
    }

    public void testDatedNonAuditedNullInsertInTransaction() throws SQLException
    {
        testDatedNonAuditedNull.testInsertInTransaction();
    }

    public void testDatedNonAuditedNullInsertThenUpdate() throws SQLException
    {
        testDatedNonAuditedNull.testInsertThenUpdate();
    }

    public void testDatedNonAuditedNullInsertRollback() throws SQLException
    {
        testDatedNonAuditedNull.testInsertRollback();
    }

    public void testDatedNonAuditedNullUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testUpdateSameBusinesDay();
    }

    public void testDatedNonAuditedNullUpdateSameBusinesDayNoLocking() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testUpdateSameBusinesDayWithNoLocking();
    }

    public void testDatedNonAuditedNullMultipleUpdate() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultipleUpdate();
    }

    public void testDatedNonAuditedNullMultiKeyUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiKeyUpdateSameBusinesDay();
        testDatedNonAuditedNull.testMultiKeyUpdateSameBusinesDay();
        testDatedNonAuditedNull.testMultiKeyUpdateSameBusinesDay();
    }

    public void testDatedNonAuditedNullUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testUpdateSameBusinesDayTwice();
    }

    public void testDatedNonAuditedNullUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testUpdateLaterBusinesDay();
    }

    public void testDatedNonAuditedNullTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testTerminateSameBusinesDay();
    }

    public void testDatedNonAuditedNullTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testTerminateLaterBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentUpdateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentUpdateSameBusinesDay();
        testDatedNonAuditedNull.testMultiSegmentUpdateSameBusinesDay();
        testDatedNonAuditedNull.testMultiSegmentUpdateSameBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentUpdateDifferentBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentTerminateSameBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentTerminateLaterBusinesDay();
    }

    public void testDatedNonAuditedNullTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testTerminateSameBusinesDayForAlreadyTerminated();
    }

    public void testDatedNonAuditedNullUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testUpdateUntilSameBusinesDay();
    }

    public void testDatedNonAuditedNullUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testUpdateUntilForLaterBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentSetUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedNonAuditedNullMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment();
    }

    public void testDatedNonAuditedNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment();
    }

    public void testDatedNonAuditedNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment();
    }

    public void testDatedNonAuditedNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice();
    }

    public void testDatedNonAuditedNullMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush();
    }

    public void testDatedNonAuditedNullIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testIncrementSameBusinesDay();
    }

    public void testDatedNonAuditedNullIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testIncrementSameBusinesDayRollback();
    }

    public void testDatedNonAuditedNullIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testIncrementSameBusinesDayTwice();
    }

    public void testDatedNonAuditedNullIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testIncrementLaterBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentIncrementSameBusinesDay();
    }

    public void testDatedNonAuditedNullMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedNonAuditedNullTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testTripleSegmentIncrementDifferentBusinesDay();
    }

    public void testDatedNonAuditedNullInsertWithIncrementZeroSegments() throws SQLException
    {
        testDatedNonAuditedNull.testInsertWithIncrementZeroSegments();
    }

    public void testDatedNonAuditedNullInsertWithIncrementOneSegment() throws Exception
    {
        testDatedNonAuditedNull.testInsertWithIncrementOneSegment();
    }

    public void testDatedNonAuditedNullIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testIncrementLaterBusinesDayOnTwoDays();
    }

    public void testDatedNonAuditedNullMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentIncrementDifferentBusinesDayOnTwoDays();
    }

    public void testDatedNonAuditedNullInsertThenIncrement() throws SQLException
    {
        testDatedNonAuditedNull.testInsertThenIncrement();
    }

    public void testDatedNonAuditedNullInsertThenIncrementInOneTransaction() throws SQLException
    {
        testDatedNonAuditedNull.testInsertThenIncrementInOneTransaction();
    }

    public void testDatedNonAuditedNullInsertWithIncrementTwiceTodayYesterday() throws SQLException
    {
        testDatedNonAuditedNull.testInsertWithIncrementTwiceTodayYesterday();
    }

    public void testDatedNonAuditedNullInsertThenIncrementLaterDay() throws SQLException
    {
        testDatedNonAuditedNull.testInsertThenIncrementLaterDay();
    }

    public void testDatedNonAuditedNullInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testInsertWithIncrementUntilZeroSegments();
    }

    public void testDatedNonAuditedNullInsertWithIncrementUntilOneSegment() throws Exception
    {
        testDatedNonAuditedNull.testInsertWithIncrementUntilOneSegment();
    }

    public void testDatedNonAuditedNullMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment();
    }

    public void testDatedNonAuditedNullInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        testDatedNonAuditedNull.testInsertThenTerminateLaterBusinessDate();
    }

    public void testDatedNonAuditedNullInsertThenTerminateSameBusinessDate() throws SQLException
    {
        testDatedNonAuditedNull.testInsertThenTerminateSameBusinessDate();
    }

    public void testDatedNonAuditedNullPurge() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testPurge();
    }

    public void testDatedNonAuditedNullPurgeThenInsert() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testPurgeThenInsert();
    }

    public void testDatedNonAuditedNullPurgeAfterInsert() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testPurgeAfterInsert();
    }

    public void testDatedNonAuditedNullBatchPurge() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testBatchPurge();
    }

    public void testDatedNonAuditedNullPurgeThenRollback() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testPurgeThenRollback();
    }

    public void testDatedNonAuditedNullInsertForRecovery() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testInsertForRecovery();
    }

    public void testDatedNonAuditedNullInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        testDatedNonAuditedNull.testInsertForRecoveryThenPurge();
    }

    public void testUpdateListener()
    {
        testUpdateListener.testUpdateListener();
    }

    public void testUpdateListenerViaDetached()
    {
        testUpdateListener.testUpdateListenerViaDetached();
    }

    public void testUpdateListenerViaCopy()
    {
        testUpdateListener.testUpdateListenerViaCopy();
    }

    public void testDatedUpdateListener()
    {
        testUpdateListener.testDatedUpdateListener();
    }

    public void testDatedUpdateListenerViaDetached()
    {
        testUpdateListener.testDatedUpdateListenerViaDetached();
    }

    public void testDatedUpdateListenerViaCopy()
    {
        testUpdateListener.testDatedUpdateListenerViaCopy();
    }

    public void testLotsOfTransactions()
    {
        for(int i=0;i<1000;i++)
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Order o = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                    o.setUserId(o.getUserId() + 1);
                    return null;
                }
            });
        }
    }
}
