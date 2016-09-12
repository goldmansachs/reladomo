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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.transaction.TransactionStyle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.Exchanger;
import java.text.SimpleDateFormat;


public class TestConcurrentTransactions extends MithraTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderItem.class,
            TinyBalance.class
        };
    }

    public void testDatedOneThreadTxOneThreadNoTx()
    {
        getTinyBalance(); // make sure it's in the cache, so the non-tx thread doesn't hit the db
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                TinyBalance tb = getTinyBalance();
                assertNotNull(tb);
                waitForOtherThread(rendezvous);
                assertEquals(200, tb.getQuantity(),0);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                Timestamp businessDate = new Timestamp(System.currentTimeMillis());
                Operation op = TinyBalanceFinder.acmapCode().eq("A");
                op = op.and(TinyBalanceFinder.balanceId().eq(50));
                op = op.and(TinyBalanceFinder.businessDate().eq(businessDate));

                MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                TinyBalance tb = TinyBalanceFinder.findOne(op);
                assertNotNull(tb);
                waitForOtherThread(rendezvous);

                assertEquals(200, tb.getQuantity(),0);
                tx.commit();
            }
        };

        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDatedTwoThreadTxWritersBlock() throws Exception
    {
        final Timestamp busDate = new Timestamp(timestampFormat.parse("2008-03-03 00:00:00").getTime());
        final Exchanger rendezvous = new Exchanger();
        final int[] runCountOne = new int[1];
        final int[] runCountTwo = new int[1];
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runCountOne[0]++;
                        TinyBalance tb = findTinyBalance(busDate);
                        assertNotNull(tb);
                        if (runCountOne[0] == 1) waitForOtherThread(rendezvous);

                        tb.incrementQuantity(100);
                        return null;
                    }
                });
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
                        runCountTwo[0]++;
                        TinyBalance tb = findTinyBalance(busDate);
                        assertNotNull(tb);
                        if (runCountTwo[0] == 1) waitForOtherThread(rendezvous);

                        tb.incrementQuantity(200);
                        return null;
                    }
                });
            }
        };

        assertTrue(runMultithreadedTest(runnable1, runnable2));
        assertEquals(500.0, findTinyBalance(busDate).getQuantity(), 0.0);
    }

    private TinyBalance getTinyBalance()
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        return findTinyBalance(businessDate);
    }

    private TinyBalance findTinyBalance(Timestamp businessDate)
    {
        Operation op = TinyBalanceFinder.acmapCode().eq("A");
        op = op.and(TinyBalanceFinder.balanceId().eq(50));
        op = op.and(TinyBalanceFinder.businessDate().eq(businessDate));

        return TinyBalanceFinder.findOne(op);
    }

    public void testDeleteJdbcInsertRefreshWithClear() throws SQLException
    {
        final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(order);
        Order copy = order.getNonPersistentCopy();

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        Order toDelete = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                        waitForOtherThread(rendezvous);
                        waitForOtherThread(rendezvous);
                        toDelete.delete();
                        return null;
                    }
                });

            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                OrderFinder.clearQueryCache();
                waitForOtherThread(rendezvous);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));

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

    public void testRollbackEffectivenessInDifferentTransaction()
    {

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setState("Unknown");
                tx.rollback();
                waitForOtherThread(rendezvous);
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                String previousState = order.getState();
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                waitForOtherThread(rendezvous);
                String newState = order.getState();
                assertSame(previousState, newState);
                tx.rollback();
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testRollbackEffectivenessInDifferentThread()
    {

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setState("Unknown");
                tx.rollback();
                waitForOtherThread(rendezvous);
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                String previousState = order.getState();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                String newState = order.getState();
                assertSame(previousState, newState);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testTableChangeWhileObjectInTransaction()
    {

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                try
                {
                    order.getState();
                }
                catch (Exception e)
                {
                    assertTrue(e instanceof MithraDeletedException);
                }
                finally
                {
                    tx.rollback();
                }
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                try
                {
                    executeSql("delete from APP.ORDERS where ORDER_ID = 1");
                }
                finally
                {
                    waitForOtherThread(rendezvous);
                }

            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    private void executeSql(String sql)
    {
        Connection conn  = getConnection();
        try
        {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if(conn != null)
            {
                try
                {
                    conn.close();
                }
                catch (SQLException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void testUniqueIndexVisibility()
    {

        final Exchanger rendezvous = new Exchanger();
        final String startTrackingId = "123";
        final String trackingId = "Test Tracking";

        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setTrackingId(trackingId);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                sleep(500);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.trackingId().eq(startTrackingId));
                assertNotNull(order);
                waitForOtherThread(rendezvous);
                order = OrderFinder.findOne(OrderFinder.trackingId().eq(trackingId));
                assertNull(order);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testPrimaryKeyVisibility()
    {

        final Exchanger rendezvous = new Exchanger();
        final String trackingId = "Test Tracking";

        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setTrackingId(trackingId);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testNonUniqueIndexVisibility()
    {

        final Exchanger rendezvous = new Exchanger();

        Runnable runnable1 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setState("Unknown");
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                OrderList orderList = new OrderList(OrderFinder.state().eq("In-Progress"));
                orderList.forceResolve();
                int previousSize = orderList.size();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                orderList = new OrderList(OrderFinder.state().eq("In-Progress"));
                orderList.forceResolve();
                int newSize = orderList.size();
                waitForOtherThread(rendezvous);
                assertEquals(newSize, previousSize);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDeletedObjectVisibility()
    {

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.delete();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertTrue(order != null);
                waitForOtherThread(rendezvous);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDeletedObjectBlockingInDifferentTransaction()
    {

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                waitForOtherThread(rendezvous);
                order.delete();
                waitForOtherThread(rendezvous);
                sleep(1000);
                tx.commit();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                waitForOtherThread(rendezvous);
                long startTime = System.currentTimeMillis();
                try
                {
                    order.getUserId();
                }
                catch (Exception e)
                {
                    getLogger().info("waited for: " + (System.currentTimeMillis() - startTime) + " ms");
                    assertTrue(e instanceof MithraDeletedException);
                    assertTrue(System.currentTimeMillis() - startTime >= 1000);
                }
                finally
                {
                    tx.rollback();
                }

            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testImpactOfMultipleFindInSameTransaction()
    {

        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setUserId(-999);
                OrderList orders = new OrderList(OrderFinder.state().eq("In-Progress"));
                orders.forceResolve();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                int prevUserId = order.getUserId();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                int newUserId = order.getUserId();
                waitForOtherThread(rendezvous);
                assertEquals(prevUserId, newUserId);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testInMemoryObjectVisibilityInDifferentThread()
    {
        final Exchanger rendezvous = new Exchanger();
        final String descr = "visibility testing";
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = new Order();
                order.setOrderId(10);
                order.setUserId(1);
                order.setDescription(descr);
                order.insert();
                waitForOtherThread(rendezvous);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.description().eq(descr));
                assertTrue(order == null);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testInMemoryObjectVisibilityInDifferentTransaction()
    {
        final Exchanger rendezvous = new Exchanger();
        final String descr = "visibility testing";
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = new Order();
                order.setOrderId(10);
                order.setUserId(1);
                order.setDescription(descr);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                tx.rollback();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.description().eq(descr));
                waitForOtherThread(rendezvous);
                assertTrue(order == null);
                tx.rollback();
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testThreadNoTxObjectNoTxRead()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.getUserId();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.getUserId();
                waitForOtherThread(rendezvous);
                assertTrue(true);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testThreadNoTxObjectNoTxWrite()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.getUserId();
                order.setUserId(50);
                waitForOtherThread(rendezvous);
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertEquals(50, order.getUserId());
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testThreadNotInTxAndObjectInTxRead()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.getUserId();
                order.setUserId(20);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                sleep(1000);
                tx.commit();
            }
        };

        Runnable runnable2 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                waitForOtherThread(rendezvous);
                long startTime = System.currentTimeMillis();
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertTrue(System.currentTimeMillis() - startTime >= 1000);
                assertEquals(20, order.getUserId());
                tx.commit();
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));

    }

    public void testSerializable()
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertTrue(order.zIsParticipatingInTransaction(tx));
        order.getUserId();
        order.setUserId(20);
        tx.commit();

    }

    public void testDifferentTransactionFindWaitsAndProceeds()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.getUserId();
                order.setUserId(20);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                sleep(1000);
                tx.commit();
            }
        };
        Runnable runnable2 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                waitForOtherThread(rendezvous);
                long startTime = System.currentTimeMillis();
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                assertTrue(System.currentTimeMillis() - startTime >= 1000);
                assertEquals(20, order.getUserId());
                tx.commit();
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDifferentTransactionGetterWaitsAndProceeds()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                order.getUserId();
                order.setUserId(20);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                sleep(1000);
                tx.commit();
            }
        };
        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                waitForOtherThread(rendezvous);
                long startTime = System.currentTimeMillis();
                waitForOtherThread(rendezvous);
                assertTrue(order.getUserId() == 20);
                assertTrue(System.currentTimeMillis() - startTime >= 1000);
                tx.commit();
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDifferentTransactionMustWaitAndGetException()
    {
        this.getLogger().info("Running a timeout test... this will take about 15 seconds...");
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction(new TransactionStyle(5));
                waitForOtherThread(rendezvous);
                try
                {
                    Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                    tx.commit();
                    fail("should not be able to get the order, as it is locked by other thread");
                }
                catch (Exception e)
                {
                    tx.rollback();
                    waitForOtherThread(rendezvous);
                    assertEquals(e.getMessage(), "waited too long for transaction to finish!");
                }
            }
        };
        Runnable runnable2 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction(new TransactionStyle(15));
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                order.setUserId(order.getUserId() + 100);
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                tx.commit();
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testFindConsistencyAfterCommit()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                waitForOtherThread(rendezvous);
                tx.commit();
                Object obj = waitForOtherThread(rendezvous);
                assertTrue(obj == order);
            }
        };
        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                waitForOtherThreadAndPassObject(rendezvous, order);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testFindConsistencyBeforeCommit()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                waitForOtherThread(rendezvous);
                Object obj = waitForOtherThread(rendezvous);
                assertTrue(obj == order);
                tx.commit();
            }
        };
        Runnable runnable2 = new Runnable(){
            public void run()
            {
                waitForOtherThread(rendezvous);
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                waitForOtherThreadAndPassObject(rendezvous, order);
            }
        };
        assertTrue(runMultithreadedTest(runnable1, runnable2));
    }

    public void testDeadLockDetectionWithFoundObjects()
    {
        for(int i=0;i<1;i++)
        {
            this.innerTestDeadlockDetectionWithFoundObjects();
        }
    }

    public void innerTestDeadlockDetectionWithFoundObjects()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable orderCancellingThread = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                OrderItem orderItem = order.getItems().getOrderItemAt(0);
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                try
                {
                    order.setUserId(7);
                    waitForOtherThread(rendezvous);
                    orderItem.setState("Cancelled");
                    order.setState("Cancelled");
                    tx.commit();
                }
                catch(MithraBusinessException mbe)
                {
                    if (!mbe.isRetriable())
                    {
                        tx.rollback();
                        throw mbe; // this will fail the test
                    }
                    tx.rollback();
                    mbe.waitBeforeRetrying();
                }
                catch(Throwable t)
                {
                    tx.rollback();
                    throw new RuntimeException(t);
                }
            }
        };
        Runnable orderFillingThread = new Runnable(){
            public void run()
            {
                OrderItem item = OrderItemFinder.findOne(OrderItemFinder.orderId().eq(1));
                Order order = item.getOrder();
                waitForOtherThread(rendezvous);
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                try
                {
                    item.setState("Shipped");
                    waitForOtherThread(rendezvous);
                    order.setState("Filled");
                    tx.commit();
                }
                catch(MithraBusinessException mbe)
                {
                    if (!mbe.isRetriable())
                    {
                        tx.rollback();
                        throw mbe; // this will fail the test
                    }
                    tx.rollback();
                    mbe.waitBeforeRetrying();
                }
                catch(Throwable t)
                {
                    tx.rollback();
                    throw new RuntimeException(t);
                }
            }
        };
        assertTrue(runMultithreadedTest(orderCancellingThread, orderFillingThread));
    }

    public void disabled_testDeadlockDetection() throws SQLException
    {
        for(int i=0;i<1;i++)
        {
            this.innerTestDeadlockDetection();
        }
    }

    public void innerTestDeadlockDetection() throws SQLException
    {
        MithraManagerProvider.getMithraManager().setTransactionTimeout(45);
        setLockTimeout(60000);
        final Exchanger rendezvous = new Exchanger();
        Runnable orderCancellingThread = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                try
                {
                    Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                    order.setUserId(7);
                    waitForOtherThread(rendezvous);
                    order.getItems().getOrderItemAt(0).setState("Cancelled");
                    order.setState("eCancelled");
                    tx.commit();
                }
                catch(MithraBusinessException mbe)
                {
                    if (!mbe.isRetriable())
                    {
                        tx.rollback();
                        throw mbe; // this will fail the test
                    }
                    tx.rollback();
                    mbe.waitBeforeRetrying();
                }
                catch(Throwable t)
                {
                    tx.rollback();
                    throw new RuntimeException(t);
                }
            }
        };
        Runnable orderFillingThread = new Runnable(){
            public void run()
            {
                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                try
                {
                    OrderItem item = OrderItemFinder.findOne(OrderItemFinder.orderId().eq(1));
                    item.setState("Shipped");
                    waitForOtherThread(rendezvous);
                    item.getOrder().setState("Filled");
                    tx.commit();
                }
                catch(MithraBusinessException mbe)
                {
                    if (!mbe.isRetriable())
                    {
                        tx.rollback();
                        throw mbe; // this will fail the test
                    }
                    tx.rollback();
                    mbe.waitBeforeRetrying();
                }
                catch(Throwable t)
                {
                    tx.rollback();
                    throw new RuntimeException(t);
                }
            }
        };
        assertTrue(runMultithreadedTest(orderCancellingThread, orderFillingThread));
    }

}
