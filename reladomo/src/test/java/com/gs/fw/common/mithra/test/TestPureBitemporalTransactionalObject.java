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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;

import javax.transaction.Synchronization;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.Exchanger;



public class TestPureBitemporalTransactionalObject extends MithraTestAbstract
{
    static private Logger logger = LoggerFactory.getLogger(TestTransactionalObject.class.getName());

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final Timestamp businessDate = new Timestamp(System.currentTimeMillis());


    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            PureBitemporalOrder.class,
            PureBitemporalOrderItem.class,
        };
    }

    public void setNullAndBack()
    {
        PureBitemporalOrder order = PureBitemporalOrderFinder.findOne(PureBitemporalOrderFinder.orderId().eq(1));
        int oldUserId = order.getUserId();
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();

        order.setUserIdNull();
        order.setUserId(oldUserId);
        tx.commit();
        assertEquals(oldUserId, order.getUserId());
    }

    public void testComplexQueryInsideTransaction()
    {
        Operation op = PureBitemporalOrderFinder.items().order().description().contains("order");
        op = op.and(PureBitemporalOrderFinder.businessDate().eq(businessDate));
        assertEquals(2, PureBitemporalOrderFinder.findMany(op).size());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = PureBitemporalOrderFinder.items().order().description().contains("order");
                op = op.and(PureBitemporalOrderFinder.businessDate().eq(businessDate));
                assertEquals(2, PureBitemporalOrderFinder.findMany(op).size());
                return null;
            }
        });

    }

    public void testInnerTransactionRetriable()
    {
        final int[] count = new int[1];
        count[0] = 0;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
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
        PureBitemporalOrderFinder.clearQueryCache();
        PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(999));
        assertNotNull(order2);
    }

    private PureBitemporalOrder findOrderNow(Operation operation)
    {
        return PureBitemporalOrderFinder.findOne(operation.and(PureBitemporalOrderFinder.businessDate().eq(businessDate)));
    }

    public void testSetPrimitiveAttributesToNullToInMemoryObject()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
                order.setNullablePrimitiveAttributesToNull();
                order.setOrderId(999);
                order.insert();
                return null;
            }
        });
        PureBitemporalOrderFinder.clearQueryCache();
        PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(999));
        assertNotNull(order2);
        assertTrue(order2.isUserIdNull());
    }

    public void testSetPrimitiveAttributesToNullToPersistedObjectWithReadOnlyAttribute()
    {
        try
        {
            PureBitemporalOrderItem item = PureBitemporalOrderItemFinder.findOne(PureBitemporalOrderItemFinder.id().eq(1));
            assertNotNull(item);
            item.setNullablePrimitiveAttributesToNull();
            fail("Should not get here");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected Exception. OrderItem has a readOnly attribute and cannot be updated when is already persisted");
        }
    }

    public void testQueryInsideTransaction()
    {
        MithraPerformanceData data = PureBitemporalOrderFinder.getMithraObjectPortal().getPerformanceData();
        int cacheHits = data.getObjectCacheHits();
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
        tx.commit();
        assertNotNull(order);
        assertTrue(data.getObjectCacheHits() > cacheHits);
    }
    /* this is not a trivial test; just look at the implementation of get/setOrderId() */
    public void testSetter()
    {
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        order.setOrderId(17);
        assertEquals(order.getOrderId(), 17);
    }

    public void testMaxLengthWithTruncate()
    {
        String longDescription="0123456789A123456789B123456789C123456789D123456789E123456789F";
        String truncatedDescription="0123456789A123456789B123456789C123456789D123456789";
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        order.setDescription(longDescription);
        assertEquals(truncatedDescription, order.getDescription());
    }


    public void testTransactionalRetrieve()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                assertNotNull(order);
                assertTrue(order.zIsParticipatingInTransaction(tx));
                return null;
            }
        });
    }

    public void testUpdateOneRow()
            throws SQLException
    {
        final PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
        assertNotNull(order);
        final int oldValue = order.getUserId();
        final PureBitemporalOrderList firstList = findForUserId(oldValue);
        int oldSize = firstList.size();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                int newValue = oldValue+10000;
                order.setUserId(newValue);
                assertEquals(order.getUserId(), newValue);
                checkUserId(newValue, 1);
                return null;
            }
        });

        PureBitemporalOrderList list = findForUserId(oldValue);
        assertEquals(oldSize, list.size()+1);
    }

    private PureBitemporalOrderList findForUserId(int userId)
    {
        return new PureBitemporalOrderList(PureBitemporalOrderFinder.userId().eq(userId).and(PureBitemporalOrderFinder.businessDate().eq(businessDate)));
    }

    private void checkUserId(int newValue, int orderId)
            throws SQLException
    {
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
        assertNotNull(order);
        assertEquals(newValue, order.getUserId());
    }

    private void checkPureBitemporalOrderExists(int orderId)
            throws SQLException
    {
        assertNotNull(findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId)));
    }

    private void checkPureBitemporalOrderDoesNotExist(int orderId)
            throws SQLException
    {
        assertNull(findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId)));
    }

    public void testInsert() throws SQLException
    {
        final int orderId = 1017;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
                order.setOrderId(orderId);
                Timestamp orderDate = new Timestamp(System.currentTimeMillis());
                order.setOrderDate(orderDate);
                String description = "new order description";
                order.setDescription(description);
                order.insert();
                int dbCount = getRetrievalCount();
                PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
                assertSame(order, order2);
                assertEquals(dbCount, getRetrievalCount());
                return null;
            }
        });
    }

    public void testInsertForRecovery() throws SQLException
    {
        final int orderId = 1017;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
                order.setOrderId(orderId);
                Timestamp orderDate = new Timestamp(System.currentTimeMillis());
                order.setOrderDate(orderDate);
                order.setBusinessDateFrom(businessDate);
                order.setBusinessDateTo(DefaultInfinityTimestamp.getDefaultInfinity());
                order.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
                order.setProcessingDateTo(DefaultInfinityTimestamp.getDefaultInfinity());
                String description = "new order description";
                order.setDescription(description);
                order.insertForRecovery();
                int dbCount = getRetrievalCount();
                PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
                assertSame(order, order2);
                assertEquals(dbCount, getRetrievalCount());
                return null;
            }
        });
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
                                PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
                                order.setOrderId(orderId);
                                Timestamp orderDate = new Timestamp(System.currentTimeMillis());
                                order.setOrderDate(orderDate);
                                String description = "new order description";
                                order.setDescription(description);
                                order.insert();
                                PureBitemporalOrder order2 = new PureBitemporalOrder(businessDate);
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
        checkPureBitemporalOrderDoesNotExist(orderId);
    }

    public void testTerminate()
            throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                assertNotNull(order);
                order.terminate();
                PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                assertNull(order2);
                return null;
            }
        });

        checkPureBitemporalOrderDoesNotExist(1);
    }

    public void testTerminateMultiple()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrderList list = findForUserId(1);
                assertTrue(list.size() > 0);
                PureBitemporalOrder[] elements = list.elements();
                for(int i=0;i<elements.length;i++)
                {
                    elements[i].terminate();
                }
                return null;
            }
        });
        PureBitemporalOrderList list2 = findForUserId(1);
        assertEquals(0, list2.size());
    }

    public void testQueryExpiration()
    {
        final PureBitemporalOrderList firstList = findForUserId(1);
        firstList.setOrderBy(PureBitemporalOrderFinder.orderId().ascendingOrderBy());
        firstList.forceResolve();
        assertTrue(firstList.size() > 0);
        assertFalse(firstList.isStale());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = firstList.getPureBitemporalOrderAt(0);
                order.setUserId(order.getUserId()+1000);
                assertTrue(firstList.isStale());
                return null;
            }
        });
        PureBitemporalOrderList secondList = findForUserId(1);
        secondList.forceResolve();
        assertTrue(secondList.size() < firstList.size());
    }

    public void testMultipleSet() throws SQLException
    {
        final int orderId = 1;
        final int newValue = 7;
        final String description = "new long description";
        PureBitemporalOrder order = (PureBitemporalOrder) MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
                order.setUserId(newValue);
                order.setDescription(description);
                return order;
            }
        });

        assertEquals(order.getUserId(), newValue);
        assertEquals(order.getDescription(), description);
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
            PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));

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
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
        order.setUserId(newValue);
        tx.commit();

        assertEquals(order.getUserId(), newValue);
    }

    public void testTransactionalMethod() throws SQLException
    {
        int orderId = 1;
        int newValue = 7;
        String description = "new long description";
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
        order.setUserIdAndDescription(newValue,description);

        assertEquals(newValue, order.getUserId());
        assertEquals(description, order.getDescription());
    }


    public void testTransactionalMethodWithException() throws SQLException
    {
        int orderId = 1;
        int newValue = 7;
        String description = "";
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
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

    public void testTerminateAddSameFind()
    {
        MithraTransaction tx = null;
        try
        {
            int orderId = 1;
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
            order.terminate();
            assertNull(findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId)));
            PureBitemporalOrder order2 = new PureBitemporalOrder(businessDate);
            order2.setOrderId(orderId);
            order2.setState("test state");
            order2.setDescription("test description");
            order2.setUserId(2);
            order2.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order2.insert();
            PureBitemporalOrder order3 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
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
        checkPureBitemporalOrderDoesNotExist(orderId);
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        order.terminate();
        tx.rollback();
        assertTrue(order.isInMemoryAndNotInserted());
        checkPureBitemporalOrderDoesNotExist(orderId);
    }

    public void testRollbackUpdate() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 1;
        int newUserId = 7;
        String description = "new long description";
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
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
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
        order.terminate();
        tx.rollback();
        PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
        order.getUserId(); // make sure it doesn't throw an exception here.
        assertSame(order2, order);

        checkPureBitemporalOrderExists(orderId);
    }

    public void testRollbackInsert() throws SQLException
    {
        int orderId = 11111;
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        tx.rollback();
        assertTrue(order.isInMemoryAndNotInserted());

        checkPureBitemporalOrderDoesNotExist(orderId);
    }

    public void testCommitInsertTerminate() throws SQLException
    {
        int orderId = 11111;
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        order.terminate();
        tx.commit();
        checkPureBitemporalOrderDoesNotExist(orderId);
    }

    public void testCommitTerminate() throws SQLException
    {
        int orderId = 1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(orderId));
        order.terminate();
        tx.commit();

        checkPureBitemporalOrderDoesNotExist(orderId);
    }

    public void testInsertAndUpdate() throws SQLException
    {
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();

        order.setUserId(17);
        tx.commit();
        checkUserId(17, orderId);
    }

    public void testInsertAndFindInTransaction() throws SQLException
    {
        int userId = 17111;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        int orderId = 1017;
        order.setOrderId(orderId);
        findOrderNow(PureBitemporalOrderFinder.userId().eq(userId)); // causes the tx to flush after we've enrolled this object
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.setUserId(userId);
        order.insert();

        order = findOrderNow(PureBitemporalOrderFinder.userId().eq(userId));

        tx.commit();
        assertNotNull(order);
        checkUserId(userId, orderId);
    }

    public void testInsertAndUpdateInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
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
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();

        order.setUserId(17);
        order = new PureBitemporalOrder(businessDate);
        order.setOrderId(orderId+1);
        order.setOrderDate(orderDate);
        order.insert();
        tx.commit();
        checkUserId(17, orderId);
    }

    public void testGlobalCounterOnInsert()
    {
        int startingUpdateCount = PureBitemporalOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureBitemporalOrder order = constructNewPureBitemporalOrder();
        order.insert();
        tx.commit();
        int endingUpdateCount = PureBitemporalOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }

    public void testGlobalCounterOnUpdate()
    {
        int startingUpdateCount = PureBitemporalOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
        order.setState("void");
        tx.commit();
        int endingUpdateCount = PureBitemporalOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(1, endingUpdateCount - startingUpdateCount);
    }

    public void testGlobalCounterOnTerminate()
    {
        int startingUpdateCount = PureBitemporalOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
        order.terminate();
        tx.commit();
        int endingUpdateCount = PureBitemporalOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }


    private PureBitemporalOrder constructNewPureBitemporalOrder()
    {
        PureBitemporalOrder order = new PureBitemporalOrder(businessDate);
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
        PureBitemporalOrder order = constructNewPureBitemporalOrder();
        tx.commit();
        assertNull("in memory object should not be in cache", findOrderNow(PureBitemporalOrderFinder.orderId().eq(1017)));
    }

    public void testInsertUpdateReordering()
    {
        final PureBitemporalOrder order1 = new PureBitemporalOrder(businessDate);
        final PureBitemporalOrder order2 = new PureBitemporalOrder(businessDate);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order1.setOrderId(2001);
                order1.insert();

                PureBitemporalOrderItem item = new PureBitemporalOrderItem(businessDate);
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

    public void testCommitFailure()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                    order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                    tx.registerSynchronization(new Synchronization() {
                        public void afterCompletion(int i)
                        {
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

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
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
                    PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
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
                PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
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
                PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
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
                        PureBitemporalOrder order = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                        PureBitemporalOrder copy = order.getNonPersistentCopy();
                        assertEquals(order.getOrderId(), copy.getOrderId());
                        return null;
                    }
                });
    }

    public void testIsInMemory()
    {
        PureBitemporalOrder order1 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
        assertFalse(order1.isInMemoryAndNotInserted());

        PureBitemporalOrder detachedPureBitemporalOrder = order1.getDetachedCopy();
        assertTrue(detachedPureBitemporalOrder.isInMemoryAndNotInserted());

        PureBitemporalOrder order2 = new PureBitemporalOrder(businessDate);
        order2.setOrderId(987);
        order2.setState("Created");
        order2.setUserId(123);
        order2.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertTrue(order2.isInMemoryAndNotInserted());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order2.insert();
        tx.commit();
        assertFalse(order2.isInMemoryAndNotInserted());
    }

    public void testIsDeletedOrMarkForDeletion()
    {
        PureBitemporalOrder order1 = new PureBitemporalOrder(businessDate);
        order1.setOrderId(987);
        order1.setState("Created");
        order1.setUserId(123);
        order1.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertFalse(order1.isDeletedOrMarkForDeletion());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order1.insert();
        tx.commit();
        assertFalse(order1.isDeletedOrMarkForDeletion());

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order1.terminate();
        tx.commit();
        assertTrue(order1.isDeletedOrMarkForDeletion());

        PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
        PureBitemporalOrder detachedPureBitemporalOrder = order2.getDetachedCopy();
        assertFalse(order2.isDeletedOrMarkForDeletion());

        detachedPureBitemporalOrder.terminate();
        assertFalse(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedPureBitemporalOrder.isDeletedOrMarkForDeletion());

        detachedPureBitemporalOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        assertTrue(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedPureBitemporalOrder.isDeletedOrMarkForDeletion());
    }

    public void testIsTerminatedWithSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                PureBitemporalOrder order1 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
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
                                PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                waitForOtherThread(rendezvous);
                                order2.terminate();
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

    public void testDetachedIsTerminatedWithSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                PureBitemporalOrder order1 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));

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
                                PureBitemporalOrder order2 = findOrderNow(PureBitemporalOrderFinder.orderId().eq(1));
                                PureBitemporalOrder detachedPureBitemporalOrder2 = order2.getDetachedCopy();
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                assertFalse(detachedPureBitemporalOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                detachedPureBitemporalOrder2.terminate();

                                waitForOtherThread(rendezvous);
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                assertTrue(detachedPureBitemporalOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                detachedPureBitemporalOrder2.copyDetachedValuesToOriginalOrInsertIfNew();

                                waitForOtherThread(rendezvous);
                                assertTrue(order2.isDeletedOrMarkForDeletion());
                                assertTrue(detachedPureBitemporalOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                return null;
                            }
                        });
                waitForOtherThread(rendezvous);
            }
        };


        assertTrue(runMultithreadedTest(runnable1, runnable2));
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
