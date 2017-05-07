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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.joda.time.DateTime;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;



public class TestTransactionalObject extends MithraTestAbstract
{
    static private Logger logger = LoggerFactory.getLogger(TestTransactionalObject.class.getName());
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

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
            OrderParentToChildren.class,
            OrderStatus.class,
            OrderItem.class,
            OrderItemStatus.class,
            OrderWi.class,
            OrderItemWi.class,
            OrderStatusWi.class,
            FileDirectory.class,
            Employee.class,
            Book.class,
            TestAgeBalanceSheetRunRate.class,
            TinyBalance.class,
            Sale.class,
            TestCheckGsDesk.class,
            GsDesk.class,
            Seller.class,
            StringDatedOrder.class
        };
    }

    // this test can only be run manually, because if something fails, it's not guaranteed that it will finish
    // also, it runs forever.
    public void xtestSimultaneousReadsAndInsert() throws Exception
    {
        int threadCount = 8;
        CyclicBarrier endBarrier = new CyclicBarrier(threadCount, new Runnable() {
            public void run()
            {
                OrderFinder.findOne(OrderFinder.orderId().eq(1017)).delete();
            }
        });
        CyclicBarrier startBarrier = new CyclicBarrier(threadCount);
        Thread[] threads = new Thread[threadCount];
        RunnableWithBarrierAndException[] runnables = new RunnableWithBarrierAndException[threadCount];
        runnables[0] = new InsertRunnable(startBarrier, endBarrier, threads);
        threads[0] = new Thread(runnables[0]);
        threads[0].start();
        for(int i= 1; i< threadCount; i++)
        {
            runnables[i] = new ReaderRunnable(startBarrier, endBarrier, threads);
            threads[i] = new Thread(runnables[i]);
            threads[i].start();
        }
        threads[0].join();
        threads[1].join();
        if (runnables[1].diedWithException())
        {
            fail("died with exception. see the logs.");
        }
    }

    private abstract static class RunnableWithBarrierAndException implements Runnable
    {
        protected CyclicBarrier startBarrier;
        protected CyclicBarrier endBarrier;
        protected Thread[] allThreads;
        protected Throwable exception;

        protected RunnableWithBarrierAndException(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Thread[] allThreads)
        {
            this.startBarrier = startBarrier;
            this.endBarrier = endBarrier;
            this.allThreads = allThreads;
        }

        public boolean diedWithException()
        {
            return this.exception != null;
        }

        public void interruptAll()
        {
            for(Thread t: allThreads)
            {
                t.interrupt();
            }
        }
    }

    private static class ReaderRunnable extends RunnableWithBarrierAndException
    {

        private ReaderRunnable(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Thread[] allThreads)
        {
            super(startBarrier, endBarrier, allThreads);
        }

        public void run()
        {
            Random random = new Random(this.hashCode()+System.currentTimeMillis());
            long start = 0;
            while(true)
            {
                try
                {
                    this.startBarrier.await();
                    boolean inTx = (random.nextInt() & 127) == 0;

                    if (inTx)
                    {
                        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                return OrderFinder.findOne(OrderFinder.orderId().eq(1017));
                            }
                        });
                    }
                    else
                    {
                        OrderFinder.findOne(OrderFinder.orderId().eq(1017));
                    }
                    this.endBarrier.await();
                    if (System.currentTimeMillis() - start > 30000)
                    {
                        System.out.println("Thread "+Thread.currentThread().getName());
                        start = System.currentTimeMillis();
                    }
                }
                catch (Throwable e)
                {
                    logger.error("unexpected", e);
                    this.exception = e;
                    interruptAll();
                    break;
                }
            }
        }
    }

    private class InsertRunnable extends RunnableWithBarrierAndException
    {
        private InsertRunnable(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Thread[] allThreads)
        {
            super(startBarrier, endBarrier, allThreads);
        }

        public void run()
        {
            long start = 0;
            while(true)
            {
                try
                {
                    this.startBarrier.await();
                    MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    constructNewOrder().insert();
                    tx.commit();
                    this.endBarrier.await();
                    if (System.currentTimeMillis() - start > 30000)
                    {
                        System.out.println("Thread "+Thread.currentThread().getName());
                        start = System.currentTimeMillis();
                    }
                }
                catch (Throwable e)
                {
                    logger.error("unexpected", e);
                    this.exception = e;
                    interruptAll();
                    break;
                }
            }
        }
    }

    public void setNullAndBack()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        int oldUserId = order.getUserId();
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();

        order.setUserIdNull();
        order.setUserId(oldUserId);
        tx.commit();
        assertEquals(oldUserId, order.getUserId());
    }

    public void testInnerTransactionRetriable()
    {
        final int[] count = new int[1];
        count[0] = 0;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = new Order();
                order.setNullablePrimitiveAttributesToNull();
                order.setOrderId(999);
                order.insert();
                try
                {
                    MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            count[0]++;
                            if (count[0] == 1)
                            {
                                MithraBusinessException excp = new MithraBusinessException("for testing");
                                excp.setRetriable(true);
                                throw excp;
                            }
                            return null;
                        }
                    });
                }
                catch (Exception e)
                {
                    assertTrue(count[0] == 1);
                }
                return null;
            }
        });
        OrderFinder.clearQueryCache();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(999));
        assertNotNull(order2);
    }

    public void testInnerTransactionDies()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Order order = new Order();
                    order.setNullablePrimitiveAttributesToNull();
                    order.setOrderId(999);
                    order.insert();
                    MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            throw new Exception("expected for testing");
                        }
                    });
                    return null;
                }
            });
        }
        catch (MithraBusinessException e)
        {
            assertEquals("expected for testing", e.getCause().getMessage());
        }
        OrderFinder.clearQueryCache();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(999));
        assertNull(order2);
    }

    public void testSetPrimitiveAttributesToNullToInMemoryObject()
    {
        Order order = new Order();
        order.setNullablePrimitiveAttributesToNull();
        order.setOrderId(999);
        order.insert();
        OrderFinder.clearQueryCache();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(999));
        assertNotNull(order2);
        assertTrue(order2.isUserIdNull());
    }

    public void testSetPrimitiveAttributesToNullToPersistedObject()
    {
        Book book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
        assertNotNull(book);
        book.setNullablePrimitiveAttributesToNull();
        BookFinder.clearQueryCache();
        book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
        assertTrue(book.isInventoryLevelNull());
        assertTrue(book.isNumberOfPagesNull());
        assertTrue(book.isManufacturerIdNull());
    }

    public void testSetPrimitiveAttributesToNullToPersistedObjectWithReadOnlyAttribute()
    {
        try
        {
            OrderItem item = OrderItemFinder.findOne(OrderItemFinder.id().eq(1));
            assertNotNull(item);
            item.setNullablePrimitiveAttributesToNull();
            fail("Should not get here");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected Exception. OrderItem has a readOnly attribute and cannot be updated when is already persisted");
        }
    }

    public void testSetPrimitiveAttributeToNullToDatedObject()
    {
        final Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TestAgeBalanceSheetRunRate rate = new TestAgeBalanceSheetRunRate(businessDate);
                rate.setTradingdeskLevelTypeId(11);
                rate.setTradingDeskorDeskHeadId(1001);
                rate.setNullablePrimitiveAttributesToNull();
                rate.insert();
                return null;
            }
        });

        TestAgeBalanceSheetRunRateFinder.clearQueryCache();
        Operation op = TestAgeBalanceSheetRunRateFinder.businessDate().eq(businessDate);
        op = op.and(TestAgeBalanceSheetRunRateFinder.tradingdeskLevelTypeId().eq(11));
        op = op.and(TestAgeBalanceSheetRunRateFinder.tradingDeskorDeskHeadId().eq(1001));
        TestAgeBalanceSheetRunRate rate2 = TestAgeBalanceSheetRunRateFinder.findOne(op);
        assertNotNull(rate2);
        assertTrue(rate2.isPriceNull());
        assertTrue(rate2.isValueNull());
    }

    public void testSetPrimitiveAttributeToNullToPersistedDatedObject()
    throws Exception
    {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = TestAgeBalanceSheetRunRateFinder.businessDate().eq(new Timestamp(format.parse("2005-12-01 00:00:00.0").getTime()));
                op = op.and(TestAgeBalanceSheetRunRateFinder.tradingdeskLevelTypeId().eq(10));
                op = op.and(TestAgeBalanceSheetRunRateFinder.tradingDeskorDeskHeadId().eq(1000));
                TestAgeBalanceSheetRunRate rate = TestAgeBalanceSheetRunRateFinder.findOne(op);
                assertNotNull(rate);
                assertFalse(rate.isPriceNull());
                assertFalse(rate.isValueNull());

                rate.setNullablePrimitiveAttributesToNull();
                assertTrue(rate.isPriceNull());
                assertTrue(rate.isValueNull());
                return null;
            }
        });
    }

    public void testChainedSetPrimitiveAttributeToNullToPersistedDatedObject()
    throws Exception
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
        Timestamp businessDate =  new Timestamp(format.parse("2005-01-25 00:00:00.0").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = TestAgeBalanceSheetRunRateFinder.businessDate().eq(businessDate);
        op = op.and(TestAgeBalanceSheetRunRateFinder.tradingdeskLevelTypeId().eq(10));
        op = op.and(TestAgeBalanceSheetRunRateFinder.tradingDeskorDeskHeadId().eq(1000));
        TestAgeBalanceSheetRunRate rate = TestAgeBalanceSheetRunRateFinder.findOne(op);
        assertNotNull(rate);
        assertFalse(rate.isPriceNull());
        assertFalse(rate.isValueNull());

        rate.setNullablePrimitiveAttributesToNull();

        Operation op2 = TestAgeBalanceSheetRunRateFinder.businessDate().equalsEdgePoint();
        op2 = op2.and(TestAgeBalanceSheetRunRateFinder.tradingdeskLevelTypeId().eq(10));
        op2 = op2.and(TestAgeBalanceSheetRunRateFinder.tradingDeskorDeskHeadId().eq(1000));

        TestAgeBalanceSheetRunRateList rateList = new TestAgeBalanceSheetRunRateList(op2);

        for(int i = 0; i < rateList.size(); i++ )
        {
            TestAgeBalanceSheetRunRate obj = rateList.getTestAgeBalanceSheetRunRateAt(i);
            assertTrue(obj.isPriceNull());
            assertTrue(obj.isValueNull());
        }
        tx.commit();
    }

    public void testQueryInsideTransaction()
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertTrue(order.zIsParticipatingInTransaction(tx));
        assertEquals(1, order.getOrderId());
        tx.commit();
        assertNotNull(order);
        assertEquals(1, order.getOrderId());
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getDataForFind().getTotalObjects() > 0);
    }
    /* this is not a trivial test; just look at the implementation of get/setOrderId() */
    public void testSetter()
    {
        Order order = new Order();
        order.setOrderId(17);
        assertEquals(order.getOrderId(), 17);
    }

    public void testMaxLengthWithTruncate()
    {
        String longDescription="0123456789A123456789B123456789C123456789D123456789E123456789F";
        String truncatedDescription="0123456789A123456789B123456789C123456789D123456789";
        Order order = new Order();
        order.setDescription(longDescription);
        assertEquals(truncatedDescription, order.getDescription());
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
        checkUserId(newValue, 1);

        OrderList list = new OrderList(OrderFinder.userId().eq(oldValue));
        assertEquals(oldSize, list.size()+1);
    }

    public void testUpdateOneRowManyTimes()
            throws SQLException
    {
        final Order orderOne = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(orderOne);
        final Order orderTwo = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        assertNotNull(orderTwo);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (int i = 0; i < 1000; i++)
                {
                    orderOne.setUserId(0);
                    orderTwo.setUserId(7);
                    orderOne.setUserId(1);
                    orderTwo.setUserId(6);
                }
                return null;
            }
        });
        assertEquals(orderOne.getUserId(), 1);
        checkUserId(1, 1);
    }

    public void testUpdateObjectWithSqlDateAsString()
            throws Exception
    {
        Timestamp ts =  new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime());
        java.sql.Date oldDate = new java.sql.Date(dateFormat.parse("2004-01-12").getTime());
        java.sql.Date newDate = new java.sql.Date(System.currentTimeMillis());
        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(ts));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        assertEquals(oldDate, order.getOrderDate());
        order.setOrderDate(newDate);
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertEquals(newDate, order2.getOrderDate());
    }

    public void testUpdateObjectWithUtilDateAsString2()
        throws Exception
    {
        java.util.Date newDate = dateFormat.parse("2008-03-29");
        java.util.Date oldDate = dateFormat.parse("2004-01-12");
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime());

        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(ts));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        assertEquals(oldDate, order.getOrderDate());
        order.setOrderDate(newDate);
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertEquals(newDate, order2.getOrderDate());
    }

    private void checkUserId(int newValue, int orderId)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select USER_ID from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(newValue, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();
    }

    private void checkOrderExists(int orderId)
            throws SQLException
    {
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(orderId)));
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();
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

    public void testInsert() throws SQLException
    {
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();
        int dbCount = this.getRetrievalCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertSame(order, order2);
        assertEquals(dbCount, this.getRetrievalCount());
        String sql = "select * from APP.ORDERS where ORDER_ID = " + orderId;

        OrderList list = new OrderList(OrderFinder.orderId().eq(orderId));

        this.genericRetrievalTest(sql, list);
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getDataForInsert().getTotalObjects() > 0);
    }

    public void testInsertWithDateAsString()
    throws Exception
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2009-03-02 00:00:00.0").getTime());
        Date dt = new Date(dateFormat.parse("2009-03-02").getTime());
        Operation op  = StringDatedOrderFinder.orderDate().eq(dt).and(StringDatedOrderFinder.processingDate().eq(ts));
        StringDatedOrder order1 = StringDatedOrderFinder.findOne(op);
        assertNull(order1);
        StringDatedOrder order2 = new StringDatedOrder();
        int orderId = 9876;
        order2.setOrderId(orderId);
        order2.setDescription("Order "+orderId);
        order2.setUserId(1);
        order2.setOrderDate(dt);
        order2.setProcessingDate(ts);
        order2.insert();

        StringDatedOrder order3 = StringDatedOrderFinder.findOne(op);
        assertNotNull(order3);
    }


    public void testInsertWithException()
    {
        Order order = new Order();
        final int orderId = 1;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        try
        {
            order.insert();
        }
        catch (MithraBusinessException e)
        {
            assertTrue(e instanceof MithraUniqueIndexViolationException);
        }
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                return OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(orderId));
            }
        });
    }

    public void testInsertForRecovery() throws SQLException
    {
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insertForRecovery();
        int dbCount = this.getRetrievalCount();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertSame(order, order2);
        assertEquals(dbCount, this.getRetrievalCount());
        String sql = "select * from APP.ORDERS where ORDER_ID = " + orderId;

        OrderList list = new OrderList(OrderFinder.orderId().eq(orderId));

        this.genericRetrievalTest(sql, list);
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
        checkOrderDoesNotExist(orderId);
    }

    public void testDelete()
            throws SQLException
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        order.delete();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNull(order2);

        checkOrderDoesNotExist(1);
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getDataForDelete().getTotalObjects() > 0);
    }

    public void testDeleteForObjectWithCompositeKey()
            throws SQLException
    {
        FileDirectory directory = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1));
        assertNotNull(directory);
        directory.delete();
        FileDirectory directory2 = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1));
        assertNull(directory2);

    }

    public void testDeleteMultiple()
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        assertTrue(list.size() > 0);
        Order[] elements = list.elements();
        for(int i=0;i<elements.length;i++)
        {
            elements[i].delete();
        }
        OrderList list2 = new OrderList(OrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getDataForDelete().getTotalObjects() > 0);
    }

    public void testDeleteObjectWithDateAsString()
            throws Exception
    {
        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime())));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        order.delete();
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertNull(order2);
    }

    public void testBatchDeleteWithDateAsString()
    {
        Operation op = StringDatedOrderFinder.processingDate().lessThan(new Timestamp(System.currentTimeMillis()));
        op = op.and(StringDatedOrderFinder.orderDate().lessThan(new java.util.Date()));

        StringDatedOrderList orderList = StringDatedOrderFinder.findMany(op);
        assertEquals(4, orderList.size());
        StringDatedOrderFinder.clearQueryCache();
        orderList.deleteAll();
        StringDatedOrderList orderList2 = StringDatedOrderFinder.findMany(op);
        assertEquals(0, orderList2.size());
    }

    public void testRefresh() throws SQLException
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        int retrievalCount = getRetrievalCount();
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        int refreshBefore = data.getDataForRefresh().getTotalObjects();
        int newUserId = jdbcUpdateUserId(order);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertEquals(order.getUserId(), newUserId);
        order.setUserId(newUserId+1000);
        tx.commit();

        assertEquals(retrievalCount + 1, getRetrievalCount());
        data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertEquals(refreshBefore + 1, data.getDataForRefresh().getTotalObjects());

        this.checkUserId(newUserId+1000, 1);
    }

    public void testDeleteJdbcInsertRefresh() throws SQLException
    {
        final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);

        Order copy = order.getNonPersistentCopy();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.delete();
                return null;
            }
        });
        jdbcInsertOrder(copy);
        Order newOrder = (Order) MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order fresh = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                return fresh;
            }
        });
        assertEquals(1, newOrder.getOrderId());
    }

    private int jdbcUpdateUserId(Order order)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "update APP.ORDERS set USER_ID = ? where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        int newUserId = order.getUserId()+1000;
        ps.setInt(1, newUserId);
        ps.setInt(2, 1);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        assertEquals(updatedRows, 1);
        return newUserId;
    }

    private void jdbcInsertOrder(Order order)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "insert into APP.ORDERS (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) values (?,?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, order.getOrderId());
        ps.setTimestamp(2, order.getOrderDate());
        ps.setInt(3, order.getUserId());
        ps.setString(4, order.getDescription());
        ps.setString(5, order.getState());
        ps.setString(6, order.getTrackingId());
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        assertEquals(updatedRows, 1);
    }
    public void testQueryExpiration()
    {
        OrderList firstList = new OrderList(OrderFinder.userId().eq(1));
        firstList.forceResolve();
        assertTrue(firstList.size() > 0);
        assertFalse(firstList.isStale());
        Order order = firstList.getOrderAt(0);
        order.setUserId(order.getUserId()+1000);
        assertTrue(firstList.isStale());
        OrderList secondList = new OrderList(OrderFinder.userId().eq(1));
        secondList.forceResolve();
        assertTrue(secondList.size() < firstList.size());
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
        tx.commit();

        assertEquals(order.getUserId(), newValue);
        assertEquals(order.getDescription(), description);
        Connection con = this.getConnection();
        String sql = "select USER_ID, DESCRIPTION from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(newValue, rs.getInt(1));
        assertEquals(description, rs.getString(2));
        rs.close();
        ps.close();
        con.close();
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getDataForUpdate().getTotalObjects() > 0);
    }

    public void testMaximumStringLength() throws Exception
    {
        MithraTransaction tx = null;
        boolean setDescription = false;
        boolean rolledBack = false;
        try
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            int orderId = 1;
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));

            String description = "123456789012345678901234567890123456789012345678901234567890112345678901234567890123456789011234567890123456789012345678901";
            order.setDescription(description); // should not fail, as it trims.
            setDescription = true;
            String state = "1234567890123456789012345678901";
            // this set will cause an exception due to state.length longer then maxLength defined.
            order.setState(state);
            tx.commit();
        }
        catch (MithraBusinessException e)
        {
            rolledBack = true;
            // expected
            if (tx != null)
            {
                tx.rollback();
            }
        }
        assertTrue(setDescription);
        assertTrue(rolledBack);
    }

    public void testSingleSet() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 1;
        int newValue = 7;
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.setUserId(newValue);
        tx.commit();

        assertEquals(order.getUserId(), newValue);
        Connection con = this.getConnection();
        String sql = "select USER_ID, DESCRIPTION from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(newValue, rs.getInt(1));
        rs.close();
        ps.close();
        con.close();
    }

    public void testTransactionalMethod() throws SQLException
    {
        int orderId = 1;
        int newValue = 7;
        String description = "new long description";
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.setUserIdAndDescription(newValue,description);

        Connection con = this.getConnection();
        String sql = "select USER_ID, DESCRIPTION from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(newValue, rs.getInt(1));
        assertEquals(description, rs.getString(2));
        rs.close();
        ps.close();
        con.close();
    }


    public void testTransactionalMethodWithException() throws SQLException
    {
        int orderId = 1;
        int newValue = 7;
        String description = "";
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        assertTrue(order.getDescription().length() > 0);
        int oldUserId = order.getUserId();
        try
        {
            order.setUserIdAndDescription(newValue,description);
            fail("should've thrown an exception");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        this.checkUserId(oldUserId, orderId);
    }

    public void testDeleteAddSameFind()
    {
        MithraTransaction tx = null;
        try
        {
            int orderId = 1;
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
            order.delete();
            assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(orderId)));
            Order order2 = new Order();
            order2.setOrderId(orderId);
            order2.setState("test state");
            order2.setDescription("test description");
            order2.setUserId(2);
            order2.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order2.insert();
            Order order3 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
            assertSame(order3, order2);
        }
        finally
        {
            if (tx != null)
            {
                tx.commit();
            }
        }
    }

    public void testRollbackInsertDelete() throws SQLException
    {
        int orderId = 11111;
        checkOrderDoesNotExist(orderId);
        Order order = new Order();
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        order.delete();
        tx.rollback();
        assertTrue(order.isInMemory());
        checkOrderDoesNotExist(orderId);
    }

    public void testRollbackUpdate() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 1;
        int newUserId = 7;
        String description = "new long description";
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        int oldUserId = order.getUserId();
        String oldDescription = order.getDescription();
        order.setUserId(newUserId);
        order.setDescription(description);
        tx.rollback();
        assertEquals(oldUserId, order.getUserId());
        assertEquals(oldDescription, order.getDescription());
        checkUserId(oldUserId, orderId);
    }


    public void testRollbackDelete() throws SQLException
    {
        int orderId = 1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.delete();
        tx.rollback();
        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.getUserId(); // make sure it doesn't throw an exception here.
        assertSame(order2, order);

        checkOrderExists(orderId);
    }

    public void testRollbackInsert() throws SQLException
    {
        int orderId = 11111;
        Order order = new Order();
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        tx.rollback();
        assertTrue(order.isInMemory());

        checkOrderDoesNotExist(orderId);
    }

    public void testCommitInsertDelete() throws SQLException
    {
        int orderId = 11111;
        Order order = new Order();
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        order.delete();
        tx.commit();
        checkOrderDoesNotExist(orderId);
    }

    public void testCommitDelete() throws SQLException
    {
        int orderId = 1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.delete();
        tx.commit();

        checkOrderDoesNotExist(orderId);
        MithraPerformanceData data = OrderFinder.getMithraObjectPortal().getPerformanceData();
        assertTrue(data.getDataForDelete().getTotalObjects() > 0);
    }

    public void testInsertAndUpdate() throws SQLException
    {
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();

        order.setUserId(17);
        checkUserId(17, orderId);
    }

    public void testInsertAndFindInTransaction() throws SQLException
    {
        int userId = 17111;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        OrderFinder.findOne(OrderFinder.userId().eq(userId)); // causes the tx to flush after we've enrolled this object
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.setUserId(userId);
        order.insert();

        assertSame(order, OrderFinder.findOne(OrderFinder.userId().eq(userId)));

        tx.commit();
        assertNotNull(order);
        checkUserId(userId, orderId);
    }

    public void testInsertAndUpdateInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();

        order.setUserId(17);
        tx.commit();
        checkUserId(17, orderId);
    }

    public void testInsertAndUpdateAndInsertInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = new Order();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();

        order.setUserId(17);
        order = new Order();
        order.setOrderId(orderId+1);
        order.setOrderDate(orderDate);
        order.insert();
        tx.commit();
        checkUserId(17, orderId);
    }

    public void testGlobalCounterOnInsert()
    {
        int startingUpdateCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = constructNewOrder();
        order.insert();
        tx.commit();
        int endingUpdateCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }

    public void testAttributeCounterOnUpdate()
    {
        int startingUpdateCount = OrderFinder.state().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        order.setState("void");
        tx.commit();
        int endingUpdateCount = OrderFinder.state().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }

    public void testGlobalCounterOnDelete()
    {
        int startingUpdateCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        order.delete();
        tx.commit();
        int endingUpdateCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }

    public void testNestedRollbackOfInnerMethodWhenOuterMethodFails() throws SQLException
    {
        int orderId = 1;
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        try
        {
            order.cancelAndFail();
            fail("transaction should have failed");
        }
        catch (IllegalArgumentException e)
        {
            //we set tracking id to null to get this
        }
        Connection con = this.getConnection();
        try
        {
            String sql = "select DESCRIPTION from APP.ORDERS where ORDER_ID = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertNotSame(rs.getString(1), Order.CANCELLED);
            assertNotSame(order.getState(), Order.CANCELLED);
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }

    }

    public void testConstructObjectInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = new Order();
                assertEquals(0, order.getUserId());
                TinyBalance balance = new TinyBalance(new Timestamp(1234083443));
                assertEquals(0, balance.getQuantity(), 0.0);
                return null;
            }
        });
    }

    public void testNestedRollbackOfOuterMethodWhenInnerMethodFails() throws SQLException
    {
        int orderId = 1;
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        int userId = -99999;
        try
        {
            order.setUserIdCancelAndFail(userId);
            fail("transaction should have failed");
        }
        catch (IllegalArgumentException e)
        {
            //we are purposefully creating the exception
        }
        Connection con = this.getConnection();
        try
        {
            String sql = "select USER_ID from APP.ORDERS where ORDER_ID = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertTrue(rs.getInt(1) != userId);
            assertTrue(order.getUserId() != userId);
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }

    }

    public void testRollbackBecauseOfDbTimeout() throws SQLException
    {
        int orderId = 1;
        int newUserId = -999999;
        String description = "new long description";
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        int oldUserId = order.getUserId();
        String oldDescription = order.getDescription();

        Connection con = this.getConnection();
        con.setAutoCommit(false);
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        String sql = "select * from APP.ORDERS for UPDATE";
        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        rs.next();
        try
        {
            try
            {
                order.setUserIdAndDescription(newUserId, description);
                fail("commit should have failed");
            }
            catch (MithraDatabaseException e)
            {
                //must roll back because we have shutdown the db
            }
            assertEquals(oldUserId, order.getUserId());
            assertEquals(oldDescription, order.getDescription());
        }
        finally
        {
            ps.close();
            if (con != null)
            {
                con.close();
            }
        }
    }

    public void testNestedRollbackBecauseOfDbTimeout() throws SQLException
    {
        int orderId = 1;
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        OrderItemFinder.getMithraObjectPortal();
        Connection con = this.getConnection();
        con.setAutoCommit(false);
        String sql = "select * from APP.ORDER_ITEM for UPDATE";
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        PreparedStatement ps = con.prepareStatement(sql);
        ps.execute();
        try
        {
            try
            {
                order.cancelOrderAndOrderItems();
                fail("commint should have failed");
            }
            catch (MithraDatabaseException e)
            {
                //must roll back because we have shutdown the db
            }
            assertNotSame(order.getState(), Order.CANCELLED);
        }
        finally
        {
            ps.close();
            if (con != null)
            {
                con.close();
            }
        }
    }

    public void testTransactonalObjectWithSourceId()
    {
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.id().eq(1).and(EmployeeFinder.sourceId().eq(1)));
        assertTrue(employee != null);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        employee.setPhone("222-222-2222");
        tx.commit();
    }

    public void testTransactionInSeparateThreadMainThreadRollback()
    {
        final String newPhone = "222-222-2222";
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = constructNewOrder();

        MithraManagerProvider.getMithraManager().executeTransactionalCommandInSeparateThread(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Employee employee = EmployeeFinder.findOne(EmployeeFinder.id().eq(1).and(EmployeeFinder.sourceId().eq(1)));
                assertTrue(employee != null);
                assertFalse(newPhone.equals(employee.getPhone()));
                employee.setPhone(newPhone);
                return null;
            }
        });

        order.insert();
        tx.rollback();
        assertTrue(order.isInMemory());
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1017)));
        Employee employee = EmployeeFinder.findOneBypassCache(EmployeeFinder.id().eq(1).and(EmployeeFinder.sourceId().eq(1)));
        assertTrue(employee != null);
        assertEquals(newPhone, employee.getPhone());
    }

    public void testTransactionInSeparateThreadMainThreadCommit()
    {
        final String newPhone = "222-222-2222";
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = constructNewOrder();

        MithraManagerProvider.getMithraManager().executeTransactionalCommandInSeparateThread(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Employee employee = EmployeeFinder.findOne(EmployeeFinder.id().eq(1).and(EmployeeFinder.sourceId().eq(1)));
                assertTrue(employee != null);
                assertFalse(newPhone.equals(employee.getPhone()));
                employee.setPhone(newPhone);
                return null;
            }
        });

        order.insert();
        tx.commit();
        assertFalse(order.isInMemory());
        order = null;
        OrderFinder.clearQueryCache();
        assertNotNull(OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1017)));
        Employee employee = EmployeeFinder.findOneBypassCache(EmployeeFinder.id().eq(1).and(EmployeeFinder.sourceId().eq(1)));
        assertTrue(employee != null);
        assertEquals(newPhone, employee.getPhone());
    }

    public void testTransactionInSeparateThreadWithException()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = constructNewOrder();

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommandInSeparateThread(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Employee employee = new Employee();
                    employee.setId(1);
                    employee.setSourceId(1);
                    employee.insert(); // this should throw an exception, as this employee exists.
                    return null;
                }
            });
            fail("we should not get here");
        }
        catch (MithraBusinessException e)
        {
            // ok
        }

        order.insert();
        tx.commit();
        assertFalse(order.isInMemory());
        order = null;
        OrderFinder.clearQueryCache();
        assertNotNull(OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1017)));
    }

    private Order constructNewOrder()
    {
        Order order = new Order();
        order.setOrderId(1017);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        order.setDescription("testing");
        order.setUserId(17);
        return order;
    }

    public void testInMemoryTransactionalObject()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = constructNewOrder();
        tx.commit();
        assertNull("in memory object should not be in cache", OrderFinder.findOne(OrderFinder.orderId().eq(1017)));
    }

    public void testClearCache() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            assertNotNull(order);
            int userId = order.getUserId();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            OrderFinder.clearQueryCache();
            jdbcUpdateUserId(order);
            assertEquals(userId, order.getUserId());
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            assertEquals(userId+1000, order.getUserId());
            assertTrue(count < MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testReloadInTxCacheUpdate() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            assertNotNull(order);
            int userId = order.getUserId();
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            jdbcUpdateUserId(order);
            reloadInTx();
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            assertEquals(userId+1000, order.getUserId());
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    private void reloadInTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderFinder.reloadCache();
                return null;
            }
        });
    }

    public void testReloadInTxCacheInsert() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            int orderId = 5555;
            Timestamp orderDate = new Timestamp(System.currentTimeMillis());
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
            assertNull(order);

            jdbcInsertNewOrder(orderDate);

            reloadInTx();
            order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
            assertNotNull(order);
            assertEquals(orderId, order.getOrderId());
            assertEquals(orderDate, order.getOrderDate());
            assertEquals(2222, order.getUserId());
            assertEquals("test desc", order.getDescription());
            assertEquals("teststate", order.getState());
            assertEquals("testid", order.getTrackingId());

            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }

    }

    private void jdbcInsertNewOrder(Timestamp orderDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "insert into APP.ORDERS (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) values (?,?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 5555);
        ps.setTimestamp(2, orderDate);
        ps.setInt(3, 2222);
        ps.setString(4, "test desc");
        ps.setString(5, "teststate");
        ps.setString(6, "testid");
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        assertEquals(updatedRows, 1);
    }

    public void testReloadInTxCacheDelete() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            assertNotNull(order);
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            jdbcDeleteOrder();

            reloadInTx();

            assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1)));

            try
            {
                order.getOrderId();
                fail("deleted object must not be accessible");
            }
            catch(MithraDeletedException e)
            {
                // ok
            }

            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testReloadCacheUpdate() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            assertNotNull(order);
            int userId = order.getUserId();
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            jdbcUpdateUserId(order);
            OrderFinder.reloadCache();
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            assertEquals(userId+1000, order.getUserId());
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testReloadCacheInsert() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            int orderId = 5555;
            Timestamp orderDate = new Timestamp(System.currentTimeMillis());
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
            assertNull(order);

            jdbcInsertNewOrder(orderDate);

            OrderFinder.reloadCache();
            order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
            assertNotNull(order);
            assertEquals(orderId, order.getOrderId());
            assertEquals(orderDate, order.getOrderDate());
            assertEquals(2222, order.getUserId());
            assertEquals("test desc", order.getDescription());
            assertEquals("teststate", order.getState());
            assertEquals("testid", order.getTrackingId());

            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testInsertUpdateReordering()
    {
        final Order order1 = new Order();
        final Order order2 = new Order();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order1.setOrderId(2001);
                order1.insert();

                OrderItem item = new OrderItem();
                item.setId(2500);
                item.setOrderId(1);
                item.insert();

                order1.setDescription("foo");

                order2.setOrderId(2005);
                order2.insert();
                return null;
            }
        });
    }

    public void testReloadCacheDelete() throws SQLException
    {
        if (OrderFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            assertNotNull(order);
            assertSame(order, OrderFinder.findOne(OrderFinder.orderId().eq(1)));
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            jdbcDeleteOrder();

            OrderFinder.reloadCache();

            assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(1)));

            try
            {
                order.getOrderId();
                fail("deleted object must not be accessible");
            }
            catch(MithraDeletedException e)
            {
                // ok
            }

            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    private void jdbcDeleteOrder() throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "delete from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, 1);
        int updatedRows = ps.executeUpdate();
        ps.close();
        con.close();
        assertEquals(updatedRows, 1);
    }

    public void testCommitFailure()
    {
        final int[] status = new int[1];
        status[0] = -90000; // random  non-zero value
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                    order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                    tx.registerSynchronization(new Synchronization() {
                        public void afterCompletion(int i)
                        {
                            status[0] = i;
                        }

                        public void beforeCompletion()
                        {
                            throw new RuntimeException("for testing");
                        }
                    });
                    return null;
                }
            });
        }
        catch (MithraBusinessException e)
        {
            this.logger.info("Expected exception", e);
        }
        assertEquals(Status.STATUS_ROLLEDBACK, status[0]);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                return null;
            }
        });
    }

    public void testCommitFailure2()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    tx.enlistResource(new ExceptionThrowingXaResource(true));
                    Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                    order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                    return null;
                }
            });
        }
        catch (MithraBusinessException e)
        {
            this.logger.info("Expected exception", e);
        }

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                return null;
            }
        });
    }

    public void testCommitFailureSingleResource()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    tx.enlistResource(new ExceptionThrowingXaResource(true));
                    return null;
                }
            });
        }
        catch (MithraBusinessException e)
        {
            this.logger.info("Expected exception", e);
        }

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                return null;
            }
        });
    }

    public void testGetNonPersistentCopyInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tran) throws Throwable
                    {
                        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                        Order copy = order.getNonPersistentCopy();
                        assertEquals(order.getOrderId(), copy.getOrderId());
                        return null;
                    }
                });
    }

    public void testInClauseCausesFlush()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tran) throws Throwable
                    {
                        Order order = new Order();
                        order.setOrderId(1000);
                        order.setState("x");
                        order.setUserId(1);
                        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                        order.setDescription("t");
                        order.insert();
                        IntHashSet set = new IntHashSet();
                        set.add(1000);
                        set.add(1001);
                        assertEquals(1, OrderFinder.findMany(OrderFinder.orderId().in(set)).size());
                        return null;
                    }
                });
    }

    public void testMultiEqualityWithInClauseCausesFlush()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tran) throws Throwable
                    {
                        Order order = new Order();
                        order.setOrderId(1000);
                        order.setState("x");
                        order.setUserId(1);
                        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                        order.setDescription("t");
                        order.insert();
                        IntHashSet set = new IntHashSet();
                        set.add(1000);
                        set.add(1001);
                        assertEquals(1, OrderFinder.findMany(OrderFinder.description().eq("t").and(OrderFinder.orderId().in(set))).size());
                        return null;
                    }
                });
    }

    public void testOrClauseCausesFlush()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tran) throws Throwable
                    {
                        Order order = new Order();
                        order.setOrderId(1000);
                        order.setState("x");
                        order.setUserId(1);
                        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                        order.setDescription("t");
                        order.insert();
                        assertEquals(1, OrderFinder.findMany(OrderFinder.orderId().eq(1000).or(OrderFinder.orderId().eq(1001))).size());
                        return null;
                    }
                });
    }

    public void testIsInMemory()
    {
        Order order1 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertFalse(order1.isInMemoryAndNotInserted());

        Order detachedOrder = order1.getDetachedCopy();
        assertTrue(detachedOrder.isInMemoryAndNotInserted());

        Order order2 = new Order();
        order2.setOrderId(987);
        order2.setState("Created");
        order2.setUserId(123);
        order2.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertTrue(order2.isInMemoryAndNotInserted());

        order2.insert();
        assertFalse(order2.isInMemoryAndNotInserted());
    }

    public void testIsDeletedOrMarkForDeletion()
    {
        Order order1 = new Order();
        order1.setOrderId(987);
        order1.setState("Created");
        order1.setUserId(123);
        order1.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertFalse(order1.isDeletedOrMarkForDeletion());

        order1.insert();
        assertFalse(order1.isDeletedOrMarkForDeletion());

        order1.delete();
        assertTrue(order1.isDeletedOrMarkForDeletion());

        Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        Order detachedOrder = order2.getDetachedCopy();
        assertFalse(order2.isDeletedOrMarkForDeletion());

        detachedOrder.delete();
        assertFalse(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedOrder.isDeletedOrMarkForDeletion());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        assertTrue(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedOrder.isDeletedOrMarkForDeletion());
    }

    public void testIsDeletedWithSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                Order order1 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertFalse(order1.isDeletedOrMarkForDeletion());
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertFalse(order1.isDeletedOrMarkForDeletion());
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertTrue(order1.isDeletedOrMarkForDeletion());
            }


        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                        new TransactionalCommand()
                        {

                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                waitForOtherThread(rendezvous);
                                order2.delete();
                                waitForOtherThread(rendezvous);
                                assertTrue(order2.isDeletedOrMarkForDeletion());
                                waitForOtherThread(rendezvous);
                                return null;
                            }
                        });
                waitForOtherThread(rendezvous);
            }
        };


        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDetachedIsDeletedWithSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                Order order1 = OrderFinder.findOne(OrderFinder.orderId().eq(1));

                assertFalse(order1.isDeletedOrMarkForDeletion());

                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertFalse(order1.isDeletedOrMarkForDeletion());

                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertFalse(order1.isDeletedOrMarkForDeletion());

                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertTrue(order1.isDeletedOrMarkForDeletion());

            }


        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                        new TransactionalCommand()
                        {

                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                                Order detachedOrder2 = order2.getDetachedCopy();
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                assertFalse(detachedOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                detachedOrder2.delete();

                                waitForOtherThread(rendezvous);
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                assertTrue(detachedOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                detachedOrder2.copyDetachedValuesToOriginalOrInsertIfNew();

                                waitForOtherThread(rendezvous);
                                assertTrue(order2.isDeletedOrMarkForDeletion());
                                assertTrue(detachedOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                return null;
                            }
                        });
                waitForOtherThread(rendezvous);
            }
        };


        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDatedTransactionalCache() throws Exception
    {
        doInsertAndChain();
        emptyTable();
        doInsertAndChain();
    }

    public void testInactiveRecordIncludesCurrentBusinessDate()
    {
        DateTime today = new DateTime().withTime(23, 59, 0, 0);
        DateTime priorDay = today.minusDays(1);

        final Timestamp businessDate = new Timestamp(today.getMillis());
        final Timestamp priorBusinessDate = new Timestamp(priorDay.getMillis());

        final int balanceId = 101;
        final String acmap = "A";

        this.insertTinyBalance(priorBusinessDate, acmap, balanceId, 100.0);
        this.sleep(20);
        final Timestamp processingDate = new Timestamp(System.currentTimeMillis());
        this.sleep(20);
        this.updateTinyBalance(acmap, balanceId, businessDate, 200.0);
        /**
         * Since the processingDate is prior to the update time, only the original (now inactive row) will be returned.
         * Successfully finding a record for our businessDate (which is today as of midnight) asserts that the GenericBiTemporalDirector
         * correctly updated the businessDateTo to be after the current business date (which is today as of now, i.e. prior to midnight)
         */
        TinyBalance inactiveRow = this.findTinyBalance(acmap, balanceId, businessDate, processingDate);
        assertNotNull(inactiveRow);
        assertEquals(100.0, inactiveRow.getQuantity());
    }

    private TinyBalance findTinyBalance(final String acmap, final int balanceId, final Timestamp businessDate, final Timestamp startProcessingDate)
    {
        return (TinyBalance) MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(acmap)
                        .and(TinyBalanceFinder.balanceId().eq(balanceId))
                        .and(TinyBalanceFinder.businessDate().eq(businessDate))
                        .and(TinyBalanceFinder.processingDate().eq(startProcessingDate)));
            }
        });
    }

    private void insertTinyBalance(final Timestamp priorBusinessDate, final String acmap, final int balanceId, final double quantity)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                TinyBalance o = new TinyBalance(priorBusinessDate, InfinityTimestamp.getParaInfinity());
                o.setAcmapCode(acmap);
                o.setBalanceId(balanceId);
                o.setQuantity(quantity);
                o.insert();
                return null;
            }
        });
    }

    private void updateTinyBalance(final String acmap, final int balanceId, final Timestamp businessDate, final double quantity)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                TinyBalance o = TinyBalanceFinder.findOne(
                        TinyBalanceFinder.acmapCode().eq(acmap)
                                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                );
                o.setQuantity(quantity);
                return null;
            }
        });
    }

    public void testDeadThreadReleasesObjects() throws Exception
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        ExceptionCatchingThread catchingThread = new ExceptionCatchingThread(new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().startOrContinueTransaction(new TransactionStyle(1));
                Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
            }
        });
        catchingThread.start();
        catchingThread.joinWithExceptionHandling();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order2.setDescription("test123");
                return null;
            }
        }, new TransactionStyle(10));
        assertEquals("test123", order.getDescription());
    }

    public void testHungThreadReleasesObjects() throws Exception
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        final CountDownLatch latch = new CountDownLatch(1);
        ExceptionCatchingThread catchingThread = new ExceptionCatchingThread(new Runnable()
        {
            public void run()
            {
                MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction(new TransactionStyle(1));
                Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                latch.countDown();
                sleep(10000);
                try
                {
                    tx.commit();
                }
                catch(MithraException e)
                {
                    // ok, this is expected as it was asynchronously rolled back
                    tx.rollback();
                }
                // now make sure we can start a brand new transaction here.
                tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction(new TransactionStyle(1));
                tx.commit();
            }
        });
        try
        {
            catchingThread.start();
            latch.await();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Order order2 = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                    order2.setDescription("test123");
                    return null;
                }
            }, new TransactionStyle(10));
            assertEquals("test123", order.getDescription());
        }
        finally
        {
            catchingThread.joinWithExceptionHandling();
        }
    }

    public void testFlushTransactionForRelatedObject()
    {

        Order order = new Order();
        order.setOrderId(9876);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setUserId(1);
        order.setDescription("Order 9876");
        order.setState("In-Process");
        order.setTrackingId("1234");
        order.insert();

        Operation op = OrderFinder.items().originalPrice().greaterThanEquals(1000000.00);
                OrderList orderList = new OrderList(op);

        assertEquals(0, orderList.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {


            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {

                OrderItem item = new OrderItem();
                item.setId(9999);
                item.setOrderId(9876);
                item.setProductId(1);
                item.setQuantity(10);
                item.setOriginalPrice(1000000.00);
                item.setDiscountPrice(975000.00);
                item.setState("Test");
                item.insert();

                Operation op = OrderFinder.items().originalPrice().greaterThanEquals(1000000.00);
                OrderList orderList = new OrderList(op);

                assertEquals(1, orderList.size());
                return null;
            }
        });

    }

    public void testFlushTransactionForLooslyRelatedObject()
    {

        Sale sale = new Sale();
        sale.setSaleId(9876);
        sale.setSaleDate(new Timestamp(System.currentTimeMillis()));
        sale.setSellerId(111);
        sale.setDescription("Order 9876");
        sale.setActiveBoolean(true);
        sale.setDiscountPercentage(1);
        sale.insert();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {


            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {

                SellerList list = new SellerList(SellerFinder.sellerId().eq(111));
                list.deleteAll();
                Seller seller = new Seller();
                seller.setName("John Doe");
                seller.setActive(false);
                seller.setZipCode("10001");
                seller.setSellerId(111);
                seller.insert();

                Operation op = SaleFinder.saleId().eq(9876);
                op= op.and(SaleFinder.seller().exists());

                SaleList saleList = new SaleList(op);
                assertEquals(1, saleList.size());
                return null;
            }
        });
    }

    public void testNoneCacheDeepFetchHitsDatabase() throws Exception
    {
        GsDeskList list = new GsDeskList(GsDeskFinder.all());
        list.deepFetch(GsDeskFinder.checkGsDesk());
        list.setOrderBy(GsDeskFinder.id().ascendingOrderBy());

        GsDesk gsDesk = list.get(0);
        assertEquals(1, gsDesk.getId());
        assertEquals("foo", gsDesk.getCheckGsDesk().get(0).getName());
        updateCheckDesk();

        list = new GsDeskList(GsDeskFinder.all());
        list.deepFetch(GsDeskFinder.checkGsDesk());
        list.setOrderBy(GsDeskFinder.id().ascendingOrderBy());

        gsDesk = list.get(0);
        assertEquals("newName", gsDesk.getCheckGsDesk().get(0).getName());
    }

    public void testNoneCacheHitsDatabase() throws Exception
    {
        TestCheckGsDesk desk = TestCheckGsDeskFinder.findOne(TestCheckGsDeskFinder.checkId().eq(4));
        assertEquals("foo", desk.getName());
        updateCheckDesk();
        desk = TestCheckGsDeskFinder.findOne(TestCheckGsDeskFinder.checkId().eq(4));
        assertEquals("newName", desk.getName());
    }

    public void testTransactionalResourceRegistration()
    {
        final TransactionalResourceForTests resource = TransactionalResourceForTests.getInstance();
        resource.setValue("Foo", "Bar");
        resource.setValue("XYZ", "ABC");

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                
                assertEquals("ABC", resource.getValue("XYZ"));
                assertEquals("Bar", resource.getValue("Foo"));
                assertNull(resource.getValue("gonzra"));
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertEquals("ABC", resource.getValue("XYZ"));
                assertEquals("Bar", resource.getValue("Foo"));
                assertNull(resource.getValue("gonzra"));
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                assertEquals("ABC2", resource.getValue("XYZ"));
                assertEquals("Bar2", resource.getValue("Foo"));
                assertEquals("Rafael", resource.getValue("gonzra"));
            }

        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                 MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         waitForOtherThread(rendezvous);
                         resource.setValue("Foo", "Bar2");
                         resource.setValue("XYZ", "ABC2");
                         resource.setValue("gonzra", "Rafael");
                         waitForOtherThread(rendezvous);
                         assertEquals("ABC2", resource.getValue("XYZ"));
                         assertEquals("Bar2", resource.getValue("Foo"));
                         assertEquals("Rafael", resource.getValue("gonzra"));
                         waitForOtherThread(rendezvous);
                         return null;
                     }
                 });
                waitForOtherThread(rendezvous);
            }
        };

        assertTrue(runMultithreadedTest(runnable1, runnable2));

    }

    public void testUpdateCombo()
    {
        final long now = System.currentTimeMillis()/10*10;
        // this test queues thes operations: upd A, upd B, ins C, upd B, upd A, updB, updA
        // the tricky thing is the insert in the middle that allows the reversal of update order
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order orderA = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                Order orderB = OrderFinder.findOne(OrderFinder.orderId().eq(2));

                orderA.setDescription("foobar");
                orderB.setOrderDate(new Timestamp(now));

                Order order = new Order();
                order.setOrderId(9876);
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                order.setUserId(1);
                order.setDescription("Order 9876");
                order.setState("In-Process");
                order.setTrackingId("1234");
                order.insert();

                orderB.setState("barfoo");
                orderA.setOrderDate(new Timestamp(now + 100));
                orderB.setDescription("foobar2");
                orderA.setState("barfoo2");
                return null;
            }
        });
        Order orderA = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(1));
        Order orderB = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(2));
        assertEquals("foobar", orderA.getDescription());
        assertEquals("foobar2", orderB.getDescription());
        assertEquals(new Timestamp(now), orderB.getOrderDate());
        assertEquals(new Timestamp(now + 100), orderA.getOrderDate());
        assertEquals("barfoo", orderB.getState());
        assertEquals("barfoo2", orderA.getState());
    }

    public void testConsecutiveTransactions()
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        readLockAndRelease(order);
        readLockAndRelease(order);
        writeLockAndRelease(order, 2);
        writeLockAndRelease(order, 3);
        readLockAndRelease(order);
        readLockAndRelease(order);
        assertEquals(3, order.getUserId());
        writeLockAndRelease(order, 4);
        readLockAndRelease(order);
    }

    public void testBatchCombineWithDelete()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order one = new Order();
                one.setOrderId(1000);
                one.insert();
                Order two = new Order();
                two.setOrderId(1001);
                two.insert();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setUserId(2345);
                Order four = new Order();
                four.setOrderId(1004);
                four.insert();
                one.delete();
                Order five = new Order();
                five.setOrderId(1005);
                five.insert();
                return null;
            }
        });
    }

    private void writeLockAndRelease(final Order order, final int newUserID)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.setUserId(newUserID);
                return null;
            }
        });
    }

    private void readLockAndRelease(final Order order)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.getUserId();
                return null;
            }
        });
    }

    private void updateCheckDesk() throws Exception
    {
        Connection conn = ConnectionManagerForTests.getInstance().getConnection("A");
        Statement stmt = null;
        try
        {
            stmt = conn.createStatement();
            stmt.execute("update CHECK_GS_DESK set NAME='newName' where CHECK_ID=4");
            conn.commit();
        }
        finally
        {
            if(stmt != null)
            {
                stmt.close();
            }
            conn.close();
        }
    }

    private void emptyTable() throws Exception
    {
        Connection conn = ConnectionManagerForTests.getInstance().getConnection("A");
        Statement stmt = null;
        try
        {
            stmt = conn.createStatement();
            stmt.execute("delete TINY_BALANCE");
            conn.commit();
        }
        finally
        {
            if(stmt != null)
            {
                stmt.close();
            }
            conn.close();
        }
    }

    private void doInsertAndChain()
    {
        final Timestamp sep28 = Timestamp.valueOf("2004-09-28 18:30:00.000");
        final Timestamp sep29 = Timestamp.valueOf("2004-09-29 18:30:00.000");
        final Timestamp sep30 = Timestamp.valueOf("2004-09-30 18:30:00.000");
        final Timestamp oct1  = Timestamp.valueOf("2004-10-01 18:30:00.000");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                TinyBalance o = new TinyBalance(sep28, InfinityTimestamp.getParaInfinity());
                o.setAcmapCode("A");
                o.setBalanceId(9876);
                o.setQuantity(100.0);
                o.insert();
                return null;
            }
        });

        TinyBalanceList tinyBalanceList = checkResults("A", 9876, sep29);
        assertNotNull(tinyBalanceList);
        assertEquals(100, tinyBalanceList.get(0).getQuantity(), 0);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                TinyBalance o = TinyBalanceFinder.findOne(
                        TinyBalanceFinder.acmapCode().eq("A")
                        .and(TinyBalanceFinder.balanceId().eq(9876))
                        .and(TinyBalanceFinder.businessDate().eq(sep29))
                );
                o.setQuantity(110.0);
                return null;
            }
        });
        tinyBalanceList = checkResults("A", 9876, sep30);
        assertNotNull(tinyBalanceList);
        assertEquals(110, tinyBalanceList.get(0).getQuantity(), 0);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                TinyBalance o = TinyBalanceFinder.findOne(
                        TinyBalanceFinder.acmapCode().eq("A")
                        .and(TinyBalanceFinder.balanceId().eq(9876))
                        .and(TinyBalanceFinder.businessDate().eq(sep30))
                );
                o.setQuantity(120.0);
                return null;
            }
        });
        tinyBalanceList = checkResults("A", 9876, oct1);
        assertNotNull(tinyBalanceList);
        assertEquals(120, tinyBalanceList.get(0).getQuantity(), 0);
    }

    private TinyBalanceList checkResults(final String sourceId, final int balanceId, final Timestamp businessDate )
    {
        final TinyBalanceList[] list = new TinyBalanceList[]{null};
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
            {
                Operation op = TinyBalanceFinder.acmapCode().eq(sourceId)
                        .and(TinyBalanceFinder.balanceId().eq(balanceId))
                        .and(TinyBalanceFinder.businessDate().eq(businessDate));
                list[0] = new TinyBalanceList(op);

                return null;
            }
        });
        return list[0];
    }

private static class TransactionalResourceForTests implements Synchronization
{
    private static TransactionalResourceForTests instance = new TransactionalResourceForTests();

    private Map<String, String> commitedMap = new HashMap<String, String>();
    private Map<String, String> transactionalMap = new HashMap<String, String>();
    private MithraTransaction currentTx = null;

    public static TransactionalResourceForTests getInstance()
    {
        return instance;
    }


    private void checkTx()
    {
        if (currentTx == null)
        {
            currentTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            if (currentTx != null)
            {
                currentTx.registerSynchronization(this);
            }
        }
    }

    public void setValue(String key, String value)
    {
        checkTx();
        if (currentTx != null)
        {
            this.transactionalMap.put(key, value);
        }
        else
        {
            this.commitedMap.put(key, value);
        }
    }

    public String getValue(String key)
    {
        checkTx();

        String value = null;
        if(MithraManagerProvider.getMithraManager().getCurrentTransaction() != null)
        {
            value = this.transactionalMap.get(key);
        }

        if (value == null)
        {
            value = this.commitedMap.get(key);
        }

        return value;
    }

    public void afterCompletion(int status)
    {
        this.currentTx = null;
        if (status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING)
        {
            //Load the commited map with the transactional map
            this.commitedMap.putAll(this.transactionalMap);
            this.transactionalMap.clear();
        }
        else if (status == Status.STATUS_ROLLEDBACK || status == Status.STATUS_ROLLING_BACK)
        {
            this.transactionalMap.clear();
        }
        else
        {
            throw new RuntimeException("Unhandled transaction status: " + status);
        }
    }

    public void beforeCompletion()
    {
        //Doesn't need to do anything
    }
}
    private static class ExceptionThrowingXaResource implements XAResource
    {
        private boolean throwExceptionOnCommit = false;

        public ExceptionThrowingXaResource(boolean throwExceptionOnCommit)
        {
            this.throwExceptionOnCommit = throwExceptionOnCommit;
        }

        public int getTransactionTimeout() throws XAException
        {
            return 0;
        }

        public boolean setTransactionTimeout(int i) throws XAException
        {
            return true;
        }

        public boolean isSameRM(XAResource xaResource) throws XAException
        {
            return xaResource == this;
        }

        public Xid[] recover(int i) throws XAException
        {
            return new Xid[0];
        }

        public int prepare(Xid xid) throws XAException
        {
            return 0;
        }

        public void forget(Xid xid) throws XAException
        {
        }

        public void rollback(Xid xid) throws XAException
        {
        }

        public void end(Xid xid, int i) throws XAException
        {
        }

        public void start(Xid xid, int i) throws XAException
        {
        }

        public void commit(Xid xid, boolean b) throws XAException
        {
            if (this.throwExceptionOnCommit)
            {
                throw new XAException("for testing.");
            }
        }
    }
}