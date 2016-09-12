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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;



public class TestClientPortalTimeoutDuringDatabaseOperation extends RemoteMithraServerTestCase
{
    private static Logger logger = LoggerFactory.getLogger(TestClientPortalTimeoutDuringDatabaseOperation.class.getName());

    private static final int TRANSACTION_TIMEOUT_SECONDS = 2;
    public static final TimeZone TIME_ZONE = TimeZone.getTimeZone("Asia/Tokyo");

    private static final int EXISTING_ORDER_ID = 1;
    private static final int EXISTING_ORDER_ITEM_ID = 1;
    private static final int NEW_ORDER_ID = 1017;
    private static final int NEW_ORDER_ITEM_ID = 1017;

    private Connection connectionForLock;
    private Thread lockReleaserThread;

    protected Class[] getRestrictedClassList()
    {
        HashSet result = new HashSet();

        // This test uses these classes
        result.add(Order.class);
        result.add(OrderItem.class);

        // This test does not use these classes but they are full cached by the server and generate errors if not added
        result.add(FullyCachedTinyBalance.class);
        result.add(SpecialAccount.class);

        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    @Override
    public void slaveVmSetUp()
    {
        MithraManagerProvider.getMithraManager().setTransactionTimeout(TRANSACTION_TIMEOUT_SECONDS);
        super.slaveVmSetUp();
    }

    @Override
    protected void setUp() throws Exception
    {
        MithraManagerProvider.getMithraManager().setTransactionTimeout(TRANSACTION_TIMEOUT_SECONDS);
        super.setUp();
        setDatabaseLockTimeout(TRANSACTION_TIMEOUT_SECONDS * 100 * 1000);
    }

    protected void setDefaultServerTimezone()
    {
        TimeZone.setDefault(TIME_ZONE);
    }

    public void setDatabaseLockTimeout(int timeout)
    {
        this.getRemoteSlaveVm().executeMethod("serverSetTimeout", new Class[]{int.class}, new Object[]{new Integer(timeout)});
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

    public void serverTakeLockOnOrder() throws SQLException
    {
        this.connectionForLock = this.getServerSideConnection();
        this.connectionForLock.setAutoCommit(false);
        String sql = "update APP.ORDERS set DESCRIPTION = 'my new description'";
        PreparedStatement ps = this.connectionForLock.prepareStatement(sql);
        assertEquals(7, ps.executeUpdate());
        ps.close();
    }

    public void serverReleaseLockOnOrder() throws SQLException
    {
        this.connectionForLock.rollback();
        this.connectionForLock.close();
        this.connectionForLock = null;
    }

    // This test reproduces the same issue we observed in production.
    // i.e. an insert is blocked in the database for a long time and by the time it finally goes through
    // the Mithra transaction timeout has already been reached.
    public void testLocalTimeoutDuringInitialInsert() throws SQLException, InterruptedException
    {
        assertPreConditionOnExistingDatabaseRecords();

        // At the pre-commit stage, the SQL insert statement will run and get blocked on the database side.
        // When lockReleaser thread releases the database lock, the SQL insert statement will complete.
        // At this point the Mithra transaction will have already exceeded the Mithra tx timeout and will need to
        // be rolled back on both the local (client) side and the remote (server) side as well as in the database.
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                            {
                                holdTableLockLongEnoughToExceedTxTimeoutAndReleaseConcurrently();

                                Order order = new Order();
                                order.setOrderId(NEW_ORDER_ID);
                                order.insert(); // the SQL insert is actually deferred/buffered until the pre-commit stage
                                return null; // Mithra timeout should happen in pre-commit stage
                            }
                        });
            fail("should not get here.");
        }
        catch(MithraDatabaseException e)
        {
            assertExceptionIsCausedByTimeout(e);
        }
        waitToEnsureLockReleaseHasCompleted(); // should already be done by now but make sure

        this.setDatabaseLockTimeout(0); // use zero lock timeout to detect any remaining database locks as any statement which gets blocked will fail
        this.getRemoteSlaveVm().executeMethod("serverTestOrderInsertRollbackInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestOrderInsertRollbackInDatabase");
        this.getRemoteSlaveVm().executeMethod("serverTestForLocksOfAnyKindOnOrderTable");
    }

    private void assertExceptionIsCausedByTimeout(MithraDatabaseException e)
    {
        assertTrue("Unexpected type of exception: " + e, e.isTimedOut() || e.getMessage().contains("timeout") || (e.getCause() != null && e.getCause().getMessage().contains("timeout")));
    }

    public void testLocalTimeoutDuringInitialSelect() throws SQLException, InterruptedException
    {
        assertPreConditionOnExistingDatabaseRecords();

        // The SQL select statement triggered by findOne() will block in the database.
        // When competing database locks are released, the select will return but the findOne()
        // will throw an exception as the Mithra timeout has been reached.
        // We just need to ensure that the database locks held by the select statement
        // get released in a timely fashion.
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                            {
                                holdTableLockLongEnoughToExceedTxTimeoutAndReleaseConcurrently();

                                OrderFinder.findOne(OrderFinder.orderId().eq(EXISTING_ORDER_ID)); // note select within a transaction takes a lock
                                return null;
                            }
                        });
            fail("should not get here as we expect Mithra transaction timeout.");
        }
        catch(MithraDatabaseException e)
        {
            assertExceptionIsCausedByTimeout(e);
        }
        waitToEnsureLockReleaseHasCompleted(); // should already be done by now but make sure

        this.setDatabaseLockTimeout(0); // use zero lock timeout to detect any remaining database locks as any statement which gets blocked will fail
        this.getRemoteSlaveVm().executeMethod("serverTestForLocksOfAnyKindOnOrderTable");
    }

    // In this test the first insert has a chance to define a ClientTransactionContext under normal conditions.
    // The second insert is blocked for a long time and finishes only after the Mithra tx timeout is exceeded.
    public void testLocalTimeoutDuringSecondInsert() throws SQLException, InterruptedException
    {
        assertPreConditionOnExistingDatabaseRecords();

        // At the pre-commit stage, the SQL insert statement will run and get blocked on the database side.
        // When lockReleaser thread releases the database lock, the SQL insert statement will complete.
        // At this point the Mithra transaction will have already exceeded the Mithra tx timeout and will need to
        // be rolled back on both the local (client) side and the remote (server) side as well as in the database.
        final AtomicBoolean firstOperationCompleted = new AtomicBoolean(false);
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                            {
                                OrderItem orderItem = new OrderItem();
                                orderItem.setId(NEW_ORDER_ITEM_ID);
                                orderItem.insert();

                                // Force SQL insert statement to be executed right now
                                mithraTransaction.setImmediateOperations(true);
                                firstOperationCompleted.set(true);

                                holdTableLockLongEnoughToExceedTxTimeoutAndReleaseConcurrently();

                                Order order = new Order();
                                order.setOrderId(NEW_ORDER_ID);
                                order.insert(); // will block in the database. By the time it completes, it will time out.
                                return null;
                            }
                        });
            fail("should not get here.");
        }
        catch(MithraDatabaseException e)
        {
            assertExceptionIsCausedByTimeout(e);
        }
        assertTrue(firstOperationCompleted.get());
        waitToEnsureLockReleaseHasCompleted(); // should already be done by now but make sure

        this.setDatabaseLockTimeout(0); // use zero lock timeout to detect any remaining database locks as any statement which gets blocked will fail
        this.getRemoteSlaveVm().executeMethod("serverTestOrderItemInsertRollbackInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestOrderItemInsertRollbackInDatabase");
        this.getRemoteSlaveVm().executeMethod("serverTestOrderInsertRollbackInCache");
        this.getRemoteSlaveVm().executeMethod("serverTestOrderInsertRollbackInDatabase");
        this.getRemoteSlaveVm().executeMethod("serverTestForLocksOfAnyKindOnOrderTable");
        this.getRemoteSlaveVm().executeMethod("serverTestForLocksOfAnyKindOnOrderItemTable");
    }

    // In this test the first select has a chance to define a ClientTransactionContext under normal conditions.
    // The second insert is blocked for a long time and finishes only after the Mithra tx timeout is exceeded.
    public void testLocalTimeoutDuringSecondSelect() throws SQLException, InterruptedException
    {
        assertPreConditionOnExistingDatabaseRecords();

        holdTableLockLongEnoughToExceedTxTimeoutAndReleaseConcurrently();

        // The SQL select statement triggered by findOne() will block in the database.
        // When competing database locks are released, the select will return but the findOne()
        // will throw an exception as the Mithra timeout has been reached.
        // We just need to ensure that the database locks held by the select statement
        // get released in a timely fashion.
        final AtomicBoolean firstOperationCompleted = new AtomicBoolean(false);
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                            {
                                // First select operation in this test must be on a different table so as not to be blocked
                                OrderItem orderItem = OrderItemFinder.findOne(OrderItemFinder.id().eq(EXISTING_ORDER_ITEM_ID)); // note select within a transaction takes a lock
                                assertNotNull(orderItem);
                                firstOperationCompleted.set(true);

                                OrderFinder.findOne(OrderFinder.orderId().eq(EXISTING_ORDER_ID)); // note select within a transaction takes a lock
                                // Mithra transaction has timed out by this point but an exception is not triggered until we try to commit
                                return null;
                            }
                        });
            fail("should not get here as we expect transaction commit to fail due to timeout.");
        }
        catch(MithraDatabaseException e)
        {
            assertExceptionIsCausedByTimeout(e);
        }
        assertTrue(firstOperationCompleted.get());
        waitToEnsureLockReleaseHasCompleted(); // should already be done by now but make sure

        this.setDatabaseLockTimeout(0); // use zero lock timeout to detect any remaining database locks as any statement which gets blocked will fail
        this.getRemoteSlaveVm().executeMethod("serverTestForLocksOfAnyKindOnOrderTable");
        this.getRemoteSlaveVm().executeMethod("serverTestForLocksOfAnyKindOnOrderItemTable");
    }

    private void holdTableLockLongEnoughToExceedTxTimeoutAndReleaseConcurrently()
    {
        TestClientPortalTimeoutDuringDatabaseOperation.this.getRemoteSlaveVm().executeMethod("serverTakeLockOnOrder");
        releaseLockAtFuturePointInTime(TRANSACTION_TIMEOUT_SECONDS + 1); // wait just long enough to exceed Mithra tx timeout
    }

    private void releaseLockAtFuturePointInTime(final int secondsToWait)
    {
        this.lockReleaserThread = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    long startTime = System.currentTimeMillis();

                    while (System.currentTimeMillis() - startTime < secondsToWait)
                    {
                        try
                        {
                            long millisLeftToWait = (secondsToWait - (System.currentTimeMillis() - startTime)) * 1000L;
                            Thread.sleep(millisLeftToWait);
                        }
                        catch (InterruptedException e)
                        {
                            // Ignore
                        }
                    }

                    logger.warn("Releasing lock on database table");
                    TestClientPortalTimeoutDuringDatabaseOperation.this.getRemoteSlaveVm().executeMethod("serverReleaseLockOnOrder");
                }
            });
        this.lockReleaserThread.start();
    }

    private void waitToEnsureLockReleaseHasCompleted() throws InterruptedException
    {
        this.lockReleaserThread.join();
    }

    private void assertPreConditionOnExistingDatabaseRecords()
    {
        Order unwantedOrder = OrderFinder.findOne(OrderFinder.orderId().eq(NEW_ORDER_ID));
        assertNull("There is a conflicting existing Order in the database with orderId=" + NEW_ORDER_ID + ". Please remove it!", unwantedOrder);

        OrderItem unwantedOrderItem = OrderItemFinder.findOne(OrderItemFinder.id().eq(NEW_ORDER_ITEM_ID));
        assertNull("There is a conflicting existing OrderItem in the database with id=" + NEW_ORDER_ITEM_ID + ". Please remove it!", unwantedOrderItem);

        Order requiredOrder = OrderFinder.findOne(OrderFinder.orderId().eq(EXISTING_ORDER_ID));
        assertNotNull("This test requires the existence of an Order with orderId=" + EXISTING_ORDER_ID + " from the test data file. Please put it back!", requiredOrder);

        OrderItem requiredOrderItem = OrderItemFinder.findOne(OrderItemFinder.id().eq(EXISTING_ORDER_ITEM_ID));
        assertNotNull("This test requires the existence of an OrderItem with id=" + EXISTING_ORDER_ITEM_ID + " from the test data file. Please put it back!", requiredOrderItem);
    }

    public void serverTestOrderInsertRollbackInCache()
    {
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(NEW_ORDER_ID)));
    }

    public void serverTestOrderInsertRollbackInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select * from APP.ORDERS where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, NEW_ORDER_ID);
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void serverTestOrderItemInsertRollbackInCache()
    {
        assertNull(OrderItemFinder.findOne(OrderItemFinder.orderId().eq(NEW_ORDER_ITEM_ID)));
    }

    public void serverTestOrderItemInsertRollbackInDatabase() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "select * from APP.ORDER_ITEM where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, NEW_ORDER_ITEM_ID);
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void serverTestForLocksOfAnyKindOnOrderTable() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("SET LOCK_TIMEOUT 0"); // this is to ensure any existing table locks will cause the truncate table to fail immediately
        stmt.executeUpdate("truncate table APP.ORDERS");
        stmt.close();
        con.close();
    }

    public void serverTestForLocksOfAnyKindOnOrderItemTable() throws SQLException
    {
        Connection con = this.getServerSideConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("SET LOCK_TIMEOUT 0"); // this is to ensure any existing table locks will cause the truncate table to fail immediately
        stmt.executeUpdate("truncate table APP.ORDER_ITEM");
        stmt.close();
        con.close();
    }
}
