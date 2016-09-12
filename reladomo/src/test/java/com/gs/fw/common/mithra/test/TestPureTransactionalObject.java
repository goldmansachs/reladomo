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

import javax.transaction.Synchronization;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.Exchanger;



public class TestPureTransactionalObject extends MithraTestAbstract
{
    static private Logger logger = LoggerFactory.getLogger(TestTransactionalObject.class.getName());

    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            PureOrder.class,
            PureOrderItem.class,
        };
    }

    public void setNullAndBack()
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        int oldUserId = order.getUserId();
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();

        order.setUserIdNull();
        order.setUserId(oldUserId);
        tx.commit();
        assertEquals(oldUserId, order.getUserId());
    }

    public void testFactoryParameter()
    {
        PureOrderObjectFactory factory = (PureOrderObjectFactory) PureOrderFinder.getMithraObjectPortal().getMithraObjectDeserializer();
        assertEquals("testParameter", factory.getFactoryParameter());        
    }

    public void testPureInnerTransactionRetriable()
    {
        final int[] count = new int[1];
        count[0] = 0;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureOrder order = new PureOrder();
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
        PureOrderFinder.clearQueryCache();
        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(999));
        assertNotNull(order2);
    }

    public void testPureSetPrimitiveAttributesToNullToInMemoryObject()
    {
        PureOrder order = new PureOrder();
        order.setNullablePrimitiveAttributesToNull();
        order.setOrderId(999);
        order.insert();
        PureOrderFinder.clearQueryCache();
        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(999));
        assertNotNull(order2);
        assertTrue(order2.isUserIdNull());
    }

    public void testPureSetPrimitiveAttributesToNullToPersistedObjectWithReadOnlyAttribute()
    {
        try
        {
            PureOrderItem item = PureOrderItemFinder.findOne(PureOrderItemFinder.id().eq(1));
            assertNotNull(item);
            item.setNullablePrimitiveAttributesToNull();
            fail("Should not get here");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected Exception. OrderItem has a readOnly attribute and cannot be updated when is already persisted");
        }
    }

    public void testPureBypassCacheQuery()
    {
        MithraPerformanceData data = PureOrderFinder.getMithraObjectPortal().getPerformanceData();
        int cacheHits = data.getObjectCacheHits();
        PureOrder order = PureOrderFinder.findOneBypassCache(PureOrderFinder.orderId().eq(1));
        assertNotNull(order);
        assertTrue(data.getObjectCacheHits() > cacheHits);
    }

    public void testPureQueryInsideTransaction()
    {
        MithraPerformanceData data = PureOrderFinder.getMithraObjectPortal().getPerformanceData();
        int cacheHits = data.getObjectCacheHits();
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        tx.commit();
        assertNotNull(order);
        assertTrue(data.getObjectCacheHits() > cacheHits);
    }
    /* this is not a trivial test; just look at the implementation of get/setOrderId() */
    public void testPureSetter()
    {
        PureOrder order = new PureOrder();
        order.setOrderId(17);
        assertEquals(order.getOrderId(), 17);
    }

    public void testPureMaxLengthWithTruncate()
    {
        String longDescription="0123456789A123456789B123456789C123456789D123456789E123456789F";
        String truncatedDescription="0123456789A123456789B123456789C123456789D123456789";
        PureOrder order = new PureOrder();
        order.setDescription(longDescription);
        assertEquals(truncatedDescription, order.getDescription());
    }


    public void testPureTransactionalRetrieve()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
                assertNotNull(order);
                assertTrue(order.zIsParticipatingInTransaction(tx));
                return null;
            }
        });
    }

    public void testPureUpdateOneRow()
            throws SQLException
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        assertNotNull(order);
        int oldValue = order.getUserId();
        PureOrderList firstList = new PureOrderList(PureOrderFinder.userId().eq(oldValue));
        int oldSize = firstList.size();
        int newValue = oldValue+10000;
        order.setUserId(newValue);
        assertEquals(order.getUserId(), newValue);
        checkUserId(newValue, 1);

        PureOrderList list = new PureOrderList(PureOrderFinder.userId().eq(oldValue));
        assertEquals(oldSize, list.size()+1);
    }

    private void checkUserId(int newValue, int orderId)
            throws SQLException
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        assertNotNull(order);
        assertEquals(newValue, order.getUserId());
    }

    private void checkPureOrderExists(int orderId)
            throws SQLException
    {
        assertNotNull(PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId)));
    }

    private void checkPureOrderDoesNotExist(int orderId)
            throws SQLException
    {
        assertNull(PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId)));
    }

    public void testPureInsert() throws SQLException
    {
        PureOrder order = new PureOrder();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();
        int dbCount = this.getRetrievalCount();
        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        assertSame(order, order2);
        assertEquals(dbCount, this.getRetrievalCount());
    }

    public void testPureInsertForRecovery() throws SQLException
    {
        PureOrder order = new PureOrder();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insertForRecovery();
        int dbCount = this.getRetrievalCount();
        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        assertSame(order, order2);
        assertEquals(dbCount, this.getRetrievalCount());
    }

    public void testPureBatchInsertWithDatabaseRollback() throws SQLException
    {
        final int orderId = 1017;
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction mithraTransaction) throws Throwable
                            {
                                PureOrder order = new PureOrder();
                                order.setOrderId(orderId);
                                Timestamp orderDate = new Timestamp(System.currentTimeMillis());
                                order.setOrderDate(orderDate);
                                String description = "new order description";
                                order.setDescription(description);
                                order.insert();
                                PureOrder order2 = new PureOrder();
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
        checkPureOrderDoesNotExist(orderId);
    }

    public void testPureDelete()
            throws SQLException
    {
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        assertNotNull(order);
        order.delete();
        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        assertNull(order2);

        checkPureOrderDoesNotExist(1);
    }

    public void testPureDeleteMultiple()
    {
        PureOrderList list = new PureOrderList(PureOrderFinder.userId().eq(1));
        assertTrue(list.size() > 0);
        PureOrder[] elements = list.elements();
        for(int i=0;i<elements.length;i++)
        {
            elements[i].delete();
        }
        PureOrderList list2 = new PureOrderList(PureOrderFinder.userId().eq(1));
        assertEquals(0, list2.size());
    }

    public void testPureQueryExpiration()
    {
        PureOrderList firstList = new PureOrderList(PureOrderFinder.userId().eq(1));
        firstList.forceResolve();
        assertTrue(firstList.size() > 0);
        assertFalse(firstList.isStale());
        PureOrder order = firstList.getPureOrderAt(0);
        order.setUserId(order.getUserId()+1000);
        assertTrue(firstList.isStale());
        PureOrderList secondList = new PureOrderList(PureOrderFinder.userId().eq(1));
        secondList.forceResolve();
        assertTrue(secondList.size() < firstList.size());
    }

    public void testPureMultipleSet() throws SQLException
    {
        final int orderId = 1;
        final int newValue = 7;
        final String description = "new long description";
        PureOrder order = (PureOrder) MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
                order.setUserId(newValue);
                order.setDescription(description);
                return order;
            }
        });

        assertEquals(order.getUserId(), newValue);
        assertEquals(order.getDescription(), description);
    }

    public void testPureMaximumStringLength() throws Exception
    {
        MithraTransaction tx = null;
        boolean setDescription = false;
        boolean rolledBack = false;
        try
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            int orderId = 1;
            PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));

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

    public void testPureSingleSet() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 1;
        int newValue = 7;
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        order.setUserId(newValue);
        tx.commit();

        assertEquals(order.getUserId(), newValue);
    }

    public void testPureTransactionalMethod() throws SQLException
    {
        int orderId = 1;
        int newValue = 7;
        String description = "new long description";
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        order.setUserIdAndDescription(newValue,description);

        assertEquals(newValue, order.getUserId());
        assertEquals(description, order.getDescription());
    }


    public void testPureTransactionalMethodWithException() throws SQLException
    {
        int orderId = 1;
        int newValue = 7;
        String description = "";
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
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

    public void testPureDeleteAddSameFind()
    {
        MithraTransaction tx = null;
        try
        {
            int orderId = 1;
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
            PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
            order.delete();
            assertNull(PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId)));
            PureOrder order2 = new PureOrder();
            order2.setOrderId(orderId);
            order2.setState("test state");
            order2.setDescription("test description");
            order2.setUserId(2);
            order2.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order2.insert();
            PureOrder order3 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
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

    public void testPureRollbackInsertDelete() throws SQLException
    {
        int orderId = 11111;
        checkPureOrderDoesNotExist(orderId);
        PureOrder order = new PureOrder();
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        order.delete();
        tx.rollback();
        assertTrue(order.isInMemoryAndNotInserted());
        checkPureOrderDoesNotExist(orderId);
    }

    public void testPureRollbackUpdate() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 1;
        int newUserId = 7;
        String description = "new long description";
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        int oldUserId = order.getUserId();
        String oldDescription = order.getDescription();
        order.setUserId(newUserId);
        order.setDescription(description);
        tx.rollback();
        assertEquals(oldUserId, order.getUserId());
        assertEquals(oldDescription, order.getDescription());
        checkUserId(oldUserId, orderId);
    }


    public void testPureRollbackDelete() throws SQLException
    {
        int orderId = 1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        order.delete();
        tx.rollback();
        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        order.getUserId(); // make sure it doesn't throw an exception here.
        assertSame(order2, order);

        checkPureOrderExists(orderId);
    }

    public void testPureRollbackInsert() throws SQLException
    {
        int orderId = 11111;
        PureOrder order = new PureOrder();
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        tx.rollback();
        assertTrue(order.isInMemoryAndNotInserted());

        checkPureOrderDoesNotExist(orderId);
    }

    public void testPureCommitInsertDelete() throws SQLException
    {
        int orderId = 11111;
        PureOrder order = new PureOrder();
        order.setOrderId(orderId);
        order.setState("test state");
        order.setDescription("test description");
        order.setUserId(2);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.insert();
        order.delete();
        tx.commit();
        checkPureOrderDoesNotExist(orderId);
    }

    public void testPureCommitDelete() throws SQLException
    {
        int orderId = 1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(orderId));
        order.delete();
        tx.commit();

        checkPureOrderDoesNotExist(orderId);
    }

    public void testPureInsertAndUpdate() throws SQLException
    {
        PureOrder order = new PureOrder();
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

    public void testPureInsertAndFindInTransaction() throws SQLException
    {
        int userId = 17111;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = new PureOrder();
        int orderId = 1017;
        order.setOrderId(orderId);
        PureOrderFinder.findOne(PureOrderFinder.userId().eq(userId)); // causes the tx to flush after we've enrolled this object
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.setUserId(userId);
        order.insert();

        order = PureOrderFinder.findOne(PureOrderFinder.userId().eq(userId));

        tx.commit();
        assertNotNull(order);
        checkUserId(userId, orderId);
    }

    public void testPureInsertAndUpdateInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = new PureOrder();
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

    public void testPureInsertAndUpdateAndInsertInTransaction() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = new PureOrder();
        int orderId = 1017;
        order.setOrderId(orderId);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        String description = "new order description";
        order.setDescription(description);
        order.insert();

        order.setUserId(17);
        order = new PureOrder();
        order.setOrderId(orderId+1);
        order.setOrderDate(orderDate);
        order.insert();
        tx.commit();
        checkUserId(17, orderId);
    }

    public void testPureGlobalCounterOnInsert()
    {
        int startingUpdateCount = PureOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = constructNewPureOrder();
        order.insert();
        tx.commit();
        int endingUpdateCount = PureOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }

    public void testPureAttributeCounterOnUpdate()
    {
        int startingUpdateCount = PureOrderFinder.state().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        order.setState("void");
        tx.commit();
        int endingUpdateCount = PureOrderFinder.state().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }

    public void testPureGlobalCounterOnDelete()
    {
        int startingUpdateCount = PureOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        order.delete();
        tx.commit();
        int endingUpdateCount = PureOrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        assertEquals(endingUpdateCount - startingUpdateCount, 1);
    }


    private PureOrder constructNewPureOrder()
    {
        PureOrder order = new PureOrder();
        order.setOrderId(1017);
        Timestamp orderDate = new Timestamp(System.currentTimeMillis());
        order.setOrderDate(orderDate);
        order.setDescription("testing");
        order.setUserId(17);
        return order;
    }

    public void testPureInMemoryTransactionalObject()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        PureOrder order = constructNewPureOrder();
        tx.commit();
        assertNull("in memory object should not be in cache", PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1017)));
    }

    public void testPureInsertUpdateReordering()
    {
        final PureOrder order1 = new PureOrder();
        final PureOrder order2 = new PureOrder();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order1.setOrderId(2001);
                order1.insert();

                PureOrderItem item = new PureOrderItem();
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

    public void testPureCommitFailure()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
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
                PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                return null;
            }
        });
    }

    public void testPureCommitFailure2()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand() {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    tx.enlistResource(new ExceptionThrowingXaResource(true));
                    PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
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
                PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                return null;
            }
        });
    }

    public void testPureCommitFailureSingleResource()
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
                PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                return null;
            }
        });
    }

    public void testPureGetNonPersistentCopyInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tran) throws Throwable
                    {
                        PureOrder order = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
                        PureOrder copy = order.getNonPersistentCopy();
                        assertEquals(order.getOrderId(), copy.getOrderId());
                        return null;
                    }
                });
    }

    public void testPureIsInMemory()
    {
        PureOrder order1 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        assertFalse(order1.isInMemoryAndNotInserted());

        PureOrder detachedPureOrder = order1.getDetachedCopy();
        assertTrue(detachedPureOrder.isInMemoryAndNotInserted());

        PureOrder order2 = new PureOrder();
        order2.setOrderId(987);
        order2.setState("Created");
        order2.setUserId(123);
        order2.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertTrue(order2.isInMemoryAndNotInserted());

        order2.insert();
        assertFalse(order2.isInMemoryAndNotInserted());
    }

    public void testPureIsDeletedOrMarkForDeletion()
    {
        PureOrder order1 = new PureOrder();
        order1.setOrderId(987);
        order1.setState("Created");
        order1.setUserId(123);
        order1.setOrderDate(new Timestamp(System.currentTimeMillis()));
        assertFalse(order1.isDeletedOrMarkForDeletion());

        order1.insert();
        assertFalse(order1.isDeletedOrMarkForDeletion());

        order1.delete();
        assertTrue(order1.isDeletedOrMarkForDeletion());

        PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
        PureOrder detachedPureOrder = order2.getDetachedCopy();
        assertFalse(order2.isDeletedOrMarkForDeletion());

        detachedPureOrder.delete();
        assertFalse(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedPureOrder.isDeletedOrMarkForDeletion());

        detachedPureOrder.copyDetachedValuesToOriginalOrInsertIfNew();
        assertTrue(order2.isDeletedOrMarkForDeletion());
        assertTrue(detachedPureOrder.isDeletedOrMarkForDeletion());
    }

    public void testPureIsDeletedWithSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                PureOrder order1 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
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
                                PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
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

    public void testPureDetachedIsDeletedWithSeparateThreads()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {

                PureOrder order1 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));

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
                                PureOrder order2 = PureOrderFinder.findOne(PureOrderFinder.orderId().eq(1));
                                PureOrder detachedPureOrder2 = order2.getDetachedCopy();
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                assertFalse(detachedPureOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                detachedPureOrder2.delete();

                                waitForOtherThread(rendezvous);
                                assertFalse(order2.isDeletedOrMarkForDeletion());
                                assertTrue(detachedPureOrder2.isDeletedOrMarkForDeletion());

                                waitForOtherThread(rendezvous);
                                detachedPureOrder2.copyDetachedValuesToOriginalOrInsertIfNew();

                                waitForOtherThread(rendezvous);
                                assertTrue(order2.isDeletedOrMarkForDeletion());
                                assertTrue(detachedPureOrder2.isDeletedOrMarkForDeletion());

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
