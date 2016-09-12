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
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;

import java.sql.SQLException;
import java.sql.Timestamp;

public class TestDetachedAuditOnly extends MithraTestAbstract
{
    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AuditedOrder.class,
            AuditedOrderItem.class,
            OrderItem.class,
            OrderItemStatus.class,
            AuditedOrderStatus.class,
            AuditedOrderStatusTwo.class,
            AuditedOrderItemStatus.class
        };
    }

    public void testOneObjectDetachedInsert() throws SQLException
    {
        final int orderId = 1234000;
        AuditedOrder inserted = MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<AuditedOrder>()
        {
            public AuditedOrder executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrder order = new AuditedOrder(InfinityTimestamp.getParaInfinity());
                order.setDescription("test 1");
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                order.setOrderId(orderId);
                order.setState("test state");
                order.setTrackingId("t1234000");
                order.setUserId(17);
                return order.copyDetachedValuesToOriginalOrInsertIfNew();
            }
        });
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderList list = new AuditedOrderList(AuditedOrderFinder.orderId().eq(orderId));
        assertEquals(1, list.size());
        assertSame(inserted, list.get(0));
    }

    public void testOneObjectDetachedUpdate() throws SQLException
    {
        int orderId = 1;
        AuditedOrder originalAuditedOrder = findOrder(orderId);
        final AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        assertNotSame(order, originalAuditedOrder);
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        assertFalse(originalAuditedOrder.getDescription().equals(order.getDescription()));
        AuditedOrder inserted = MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<AuditedOrder>()
        {
            public AuditedOrder executeTransaction(MithraTransaction tx) throws Throwable
            {
                return order.copyDetachedValuesToOriginalOrInsertIfNew();
            }
        });
        assertEquals(order.getDescription(), originalAuditedOrder.getDescription());
        assertEquals(order.getState(), originalAuditedOrder.getState());
        assertEquals(order.getTrackingId(), originalAuditedOrder.getTrackingId());
        assertEquals(order.getOrderDate(), originalAuditedOrder.getOrderDate());
        assertEquals(order.getUserId(), originalAuditedOrder.getUserId());
        int dbCount = this.getRetrievalCount();
        AuditedOrder order2 = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(orderId));
        assertSame(inserted, order2);
        assertSame(originalAuditedOrder, order2);
        assertEquals(dbCount, this.getRetrievalCount());

        originalAuditedOrder = null;
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderList list = new AuditedOrderList(AuditedOrderFinder.orderId().eq(orderId));
        assertEquals(1, list.size());
        originalAuditedOrder = list.getAuditedOrderAt(0);

        assertEquals(order.getDescription(), originalAuditedOrder.getDescription());
        assertEquals(order.getState(), originalAuditedOrder.getState());
        assertEquals(order.getTrackingId(), originalAuditedOrder.getTrackingId());
        assertEquals(order.getOrderDate(), originalAuditedOrder.getOrderDate());
        assertEquals(order.getUserId(), originalAuditedOrder.getUserId());
    }

    public void testOneObjectDetachedTerminate() throws SQLException
    {
        int orderId = 1;
        AuditedOrder originalAuditedOrder = findOrder(1);
        final AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        assertTrue(order != originalAuditedOrder);
        order.terminate();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        originalAuditedOrder = null;
        AuditedOrderFinder.clearQueryCache();

        AuditedOrder order2 = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(orderId));
        assertNull(order2);
    }

    public void testDetachedList() throws SQLException
    {
        AuditedOrderList ol = new AuditedOrderList(AuditedOrderFinder.orderId().greaterThan(1));
        AuditedOrderList detached = ol.getDetachedCopy();
        AuditedOrder removed = detached.getAuditedOrderAt(0);
        int removedId = removed.getOrderId();
        detached.remove(0);
        AuditedOrder order = detached.getAuditedOrderAt(0);
        int changedId = order.getOrderId();
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        order = new AuditedOrder();
        order.setDescription("test 2");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()+1000));
        order.setOrderId(10001);
        order.setState("test state2");
        order.setTrackingId("t10001");
        order.setUserId(18);
        detached.add(order);
        detached.copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved();

        order = null;
        ol = null;
        AuditedOrderFinder.clearQueryCache();

        assertNotNull(findOrder(10001));
        assertNull(findOrder(removedId));
        AuditedOrder changedOrder = findOrder(changedId);
        assertNotNull(changedOrder);
        assertEquals("test 1", changedOrder.getDescription());
        assertEquals(17, changedOrder.getUserId());
    }

    public void testDetachedListInRetriedTransaction() throws SQLException
    {
        AuditedOrderList ol = new AuditedOrderList(AuditedOrderFinder.orderId().greaterThan(1));
        final AuditedOrderList detached = ol.getDetachedCopy();
        AuditedOrder removed = detached.getAuditedOrderAt(0);
        int removedId = removed.getOrderId();
        detached.remove(0);
        AuditedOrder order = detached.getAuditedOrderAt(0);
        int changedId = order.getOrderId();
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        order = new AuditedOrder();
        order.setDescription("test 2");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()+1000));
        order.setOrderId(10001);
        order.setState("test state2");
        order.setTrackingId("t10001");
        order.setUserId(18);
        detached.add(order);
        final int[] count = new int[1];
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                detached.copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved();
                if (count[0] == 0)
                {
                    count[0] = 1;
                    MithraBusinessException excp = new MithraBusinessException("for testing retry");
                    excp.setRetriable(true);
                    throw excp;
                }
                return null;
            }
        });


        order = null;
        ol = null;
        AuditedOrderFinder.clearQueryCache();

        assertNotNull(findOrder(10001));
        assertNull(findOrder(removedId));
        AuditedOrder changedOrder = findOrder(changedId);
        assertNotNull(changedOrder);
        assertEquals("test 1", changedOrder.getDescription());
        assertEquals(17, changedOrder.getUserId());
    }

    public void testDetachedListRemoveByObject() throws SQLException
    {
        AuditedOrderList ol = new AuditedOrderList(AuditedOrderFinder.orderId().greaterThan(1));
        AuditedOrderList detached = ol.getDetachedCopy();
        AuditedOrder removed = detached.getAuditedOrderAt(0);
        int removedId = removed.getOrderId();
        detached.remove(removed);
        AuditedOrder order = detached.getAuditedOrderAt(0);
        int changedId = order.getOrderId();
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        order = new AuditedOrder();
        order.setDescription("test 2");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()+1000));
        order.setOrderId(10001);
        order.setState("test state2");
        order.setTrackingId("t10001");
        order.setUserId(18);
        detached.add(order);
        detached.copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved();

        order = null;
        ol = null;
        AuditedOrderFinder.clearQueryCache();

        assertNotNull(findOrder(10001));
        assertNull(findOrder(removedId));
        AuditedOrder changedOrder = findOrder(changedId);
        assertNotNull(changedOrder);
        assertEquals("test 1", changedOrder.getDescription());
        assertEquals(17, changedOrder.getUserId());
    }


    public void testOriginalObjectTerminated() throws SQLException
    {
        final AuditedOrder originalAuditedOrder = findOrder(1);
        final AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                originalAuditedOrder.terminate();
                return null;
            }
        });

        order.setDescription("test 1");
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    order.copyDetachedValuesToOriginalOrInsertIfNew();
                    fail("erroneously updated a deleted object");
                    return null;
                }
            });
        }
        catch(MithraDeletedException e)
        {
            // good
        }
    }

    public void testDetachUpdateRollback()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("detached test");
        tx.rollback();
        assertEquals(originalAuditedOrder.getDescription(), order.getDescription());
    }

    public void testDetachTerminateRollback()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.terminate();
        tx.rollback();
        assertEquals(originalAuditedOrder.getDescription(), order.getDescription());
    }

    public void testDetachedReset()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        order.setDescription("test 1700");
        assertEquals(order.getDescription(), "test 1700");
        order.resetFromOriginalPersistentObject();
        assertEquals(originalAuditedOrder.getDescription(), order.getDescription());
    }

    public void testDetachedResetInTransactionWithCommit()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        order.setDescription("test 1700");
        assertEquals(order.getDescription(), "test 1700");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.commit();
        assertEquals(originalAuditedOrder.getDescription(), order.getDescription());

    }

    public void testDetachedResetInTransactionWithRollback()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        order.setDescription("test 1700");
        assertEquals(order.getDescription(), "test 1700");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.rollback();
        assertEquals(order.getDescription(), "test 1700");
    }

    public void testDetachTerminateReset()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        order.terminate();
        order.resetFromOriginalPersistentObject();
        assertEquals(originalAuditedOrder.getDescription(), order.getDescription());
    }

    public void testDetachTerminateResetInTransactionWithCommit()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        order.terminate();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.commit();
        assertEquals(originalAuditedOrder.getDescription(), order.getDescription());
    }

    public void testDetachTerminateResetInTransactionWithRollback() throws SQLException
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        order.terminate();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.resetFromOriginalPersistentObject();
        tx.rollback();
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        tx.commit();
        order = null;
        originalAuditedOrder = null;
        AuditedOrderFinder.clearQueryCache();
        assertNull(findOrder(1));
    }

    public void testIsModified()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment());
        order.setDescription("test 1800");
        assertTrue(order.isModifiedSinceDetachment());
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.terminate();
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment());
    }

    public void testIsModifiedInTransactionWithCommit()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        assertTrue(order.isModifiedSinceDetachment());
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment());
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.copyDetachedValuesToOriginalOrInsertIfNew();
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment());
    }

    public void testIsModifiedInTransactionWithRollback()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        assertTrue(order.isModifiedSinceDetachment());
        tx.rollback();
        assertFalse(order.isModifiedSinceDetachment());
    }

    public void testIsModifiedByAttribute()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        order.setDescription("test 1800");
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
        order.setUserIdNull();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
    }

    public void testIsModifiedByAttributeInTransactionWithCommit()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        order.setUserIdNull();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
    }

    public void testIsModifiedByAttributeInTransactionWithRollback()
    {
        AuditedOrder originalAuditedOrder = findOrder(1);
        AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        order.setDescription("test 1900");
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        order.setUserIdNull();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
        tx.rollback();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.description()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.userId()));
    }

    public void testIsModifiedByRelatedObjects()
    {
        AuditedOrder originalOrder = findOrder(1);
        AuditedOrder order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        AuditedOrderItemList orderItemList = order.getItems();
        AuditedOrderItem item = orderItemList.get(0);
        item.setQuantity(999999.99);
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
        item.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
        item.terminate();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
        order.getItems().remove(0);
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        order.getItems().add(new AuditedOrderItem());
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        assertNotNull(originalOrder.getOrderStatus());
        order.resetFromOriginalPersistentObject();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
        order.setOrderStatus(null);
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.orderStatus()));
    }

    public void testIsModifiedByRelatedObjectsInTransactionWithCommit()
    {
        AuditedOrder originalOrder = findOrder(1);
        AuditedOrder order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        AuditedOrderItemList orderItemList = order.getItems();
        AuditedOrderItem item = orderItemList.get(0);
        item.setQuantity(999999.99);
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        tx.commit();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
    }

    public void testIsModifiedByRelatedObjectsInTransactionWithRollback()
    {
        AuditedOrder originalOrder = findOrder(1);
        AuditedOrder order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        AuditedOrderItemList orderItemList = order.getItems();
        AuditedOrderItem item = orderItemList.get(0);
        item.terminate();
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        tx.rollback();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
    }

    public void testIsModifiedByDeeplyRelatedObjects()
    {
        AuditedOrder originalOrder = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1));
        AuditedOrder order = originalOrder.getDetachedCopy();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        AuditedOrderItemList orderItemList = order.getItems();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        AuditedOrderItem item = orderItemList.get(0);
        AuditedOrderItemStatus orderItemStatus = item.getAuditedOrderItemStatus();
        assertFalse(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        orderItemStatus.setLastUser("gonzra");
        assertTrue(order.isModifiedSinceDetachment(AuditedOrderFinder.items()));

        AuditedOrder originalOrder2 = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2));
        AuditedOrder order2 = originalOrder2.getDetachedCopy();
        assertFalse(order2.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        AuditedOrderItemList orderItemList2 = order.getItems();
        assertFalse(order2.isModifiedSinceDetachment(AuditedOrderFinder.items()));
        AuditedOrderItem item2 = orderItemList2.get(0);
        item2.setAuditedOrderItemStatus(null);
        tx.commit();
        assertFalse(order2.isModifiedSinceDetachment(AuditedOrderFinder.items()));
    }

    private AuditedOrder findOrder(int orderId)
    {
        return AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(orderId));
    }

    public void testDetachedRelationship()
    {
        AuditedOrder order = findOrder(2);
        assertEquals(3, order.getItems().size());
    }

    public void testAuditedInsertUpdateReordering()
    {
        final AuditedOrder order1 = new AuditedOrder();
        final AuditedOrder order2 = new AuditedOrder();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order1.setOrderId(2001);
                order1.insert();

                AuditedOrderItem item = new AuditedOrderItem();
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

    public void testOneObjectDetachedUpdateWithRelationships() throws SQLException
    {
        int orderId = 1;
        AuditedOrder originalAuditedOrder = findOrder(orderId);
        final AuditedOrder order = originalAuditedOrder.getDetachedCopy();
        assertTrue(order != originalAuditedOrder);
        order.setDescription("test 1");
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("test state");
        order.setTrackingId("t1234000");
        order.setUserId(17);
        assertFalse(originalAuditedOrder.getDescription().equals(order.getDescription()));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertTrue(order.getItems().size() > 0);
                order.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
    }

    public void testDetachedToManyMoveChild()
    {
        final AuditedOrder firstAuditedOrder = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1)).getDetachedCopy();
        int firstSize = firstAuditedOrder.getItems().size();
        final AuditedOrder secondAuditedOrder = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getDetachedCopy();

        secondAuditedOrder.getItems().setOrderBy(AuditedOrderItemFinder.id().ascendingOrderBy());
        int size = secondAuditedOrder.getItems().size();
        AuditedOrderItem toMove = secondAuditedOrder.getItems().get(0);
        toMove.setOrder(firstAuditedOrder);

        assertEquals(size - 1, secondAuditedOrder.getItems().size());
        assertEquals(firstAuditedOrder, toMove.getOrder());
        assertEquals(firstAuditedOrder.getOrderId(), toMove.getOrderId());

        assertFalse(toMove.isDeletedOrMarkForDeletion());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                firstAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                secondAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        assertEquals(1 + firstSize, AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1)).getItems().size());
        assertEquals(size - 1, AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getItems().size());
    }

    public void testDetachedToManyMoveChildToUninsertedParent()
    {
        final AuditedOrder firstAuditedOrder = new AuditedOrder();
        firstAuditedOrder.setOrderId(1000);
        final AuditedOrder secondAuditedOrder = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getDetachedCopy();

        secondAuditedOrder.getItems().setOrderBy(AuditedOrderItemFinder.id().ascendingOrderBy());
        int size = secondAuditedOrder.getItems().size();
        AuditedOrderItem toMove = secondAuditedOrder.getItems().get(0);
        toMove.setOrder(firstAuditedOrder);

        assertEquals(size - 1, secondAuditedOrder.getItems().size());
        assertEquals(firstAuditedOrder, toMove.getOrder());
        assertEquals(firstAuditedOrder.getOrderId(), toMove.getOrderId());

        assertFalse(toMove.isDeletedOrMarkForDeletion());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                firstAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                secondAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        assertEquals(1 , AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1000)).getItems().size());
        assertEquals(size - 1, AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getItems().size());
    }
}
