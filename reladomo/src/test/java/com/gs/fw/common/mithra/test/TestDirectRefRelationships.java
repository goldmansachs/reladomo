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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;


public class TestDirectRefRelationships extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            DirectRefOrder.class,
            DirectRefOrderItem.class,
            OrderItemStatus.class,
            Product.class,
            DirectRefOrderStatus.class,
        };
    }

    public void testDependentToOneRelationshipInMemory()
    {
        DirectRefOrder order = new DirectRefOrder();
        order.setOrderId(1);
        assertNull(order.getOrderStatus());
    }

    public void testToManyTwice()
    {
        DirectRefOrderList orders = new DirectRefOrderList(DirectRefOrderFinder.orderId().in(IntHashSet.newSetWith(new int[] { 1, 2, 3, 4})));
        orders.deepFetch(DirectRefOrderFinder.items());
        orders.forceResolve();
        int oldCount = dbCalls();
        orders = new DirectRefOrderList(DirectRefOrderFinder.orderId().in(IntHashSet.newSetWith(new int[] { 1, 4})));
        orders.deepFetch(DirectRefOrderFinder.items());
        orders.forceResolve();
        assertEquals(oldCount, dbCalls());
    }

    public void testRemoveDependentObjectByIndex()
    {
        DirectRefOrder order = DirectRefOrderFinder.findOne(DirectRefOrderFinder.orderId().eq(2));
        assertNotNull(order);
        DirectRefOrderItemList items = order.getItems();
        items.setOrderBy(DirectRefOrderItemFinder.id().ascendingOrderBy());
        int originalSize = items.size();
        assertTrue(originalSize > 1);
        DirectRefOrderItem item = items.getDirectRefOrderItemAt(0);
        int orderItemId = item.getId();
        assertSame(items.remove(0), item);
        assertEquals(originalSize - 1, items.size());
        assertEquals(originalSize - 1, order.getItems().size());
        assertNull(DirectRefOrderItemFinder.findOne(DirectRefOrderItemFinder.id().eq(orderItemId)));
    }

    public void testRemoveDependentObjectByObject()
    {
        DirectRefOrder order = DirectRefOrderFinder.findOne(DirectRefOrderFinder.orderId().eq(2));
        assertNotNull(order);
        DirectRefOrderItemList items = order.getItems();
        items.setOrderBy(DirectRefOrderItemFinder.id().ascendingOrderBy());
        int originalSize = items.size();
        assertTrue(originalSize > 2);
        DirectRefOrderItem item = items.getDirectRefOrderItemAt(1);
        int orderItemId = item.getId();
        assertTrue(items.remove(item));
        assertEquals(originalSize - 1, items.size());
        assertEquals(originalSize - 1, order.getItems().size());
        assertNull(DirectRefOrderItemFinder.findOne(DirectRefOrderItemFinder.id().eq(orderItemId)));
    }

    public void testCollapsedDeepFetch()
    {
        DirectRefOrderItemList list = new DirectRefOrderItemList();
        for(int i=0;i<2000;i++)
        {
            DirectRefOrderItem item = new DirectRefOrderItem();
            item.setDiscountPrice(i+1);
            item.setId(i+2000);
            item.setOrderId(1);
            list.add(item);
        }
        list.insertAll();
        DirectRefOrderItemList list2 = new DirectRefOrderItemList(DirectRefOrderItemFinder.discountPrice().lessThan(5000));
        list2.deepFetch(DirectRefOrderItemFinder.order());
        assertTrue(list2.size() > 1000);
        assertTrue(list2.getOrders().size() < 1000);
    }

//

    public void testInsertSettingDependentsAtOnceBeforeId()
    {
        DirectRefOrder order = new DirectRefOrder();
        order.setDescription("test");

        DirectRefOrderItemList itemList = new DirectRefOrderItemList();
        for(int i=0;i<10;i++)
        {
            DirectRefOrderItem item = new DirectRefOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());

        checkOrderOnItems(itemList, order);

        addItem(order);
        assertEquals(11, order.getItems().size());

        checkOrderOnItems(itemList, order);

        DirectRefOrderStatus status = createStatus();

        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            DirectRefOrderItem item = itemList.getDirectRefOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        DirectRefOrderFinder.clearQueryCache();
        DirectRefOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = DirectRefOrderFinder.findOne(DirectRefOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    private DirectRefOrderStatus createStatus()
    {
        DirectRefOrderStatus status = new DirectRefOrderStatus();
        status.setStatus(10);
        status.setLastUser("wilma");
        return status;
    }

    private void checkOrderOnItems(DirectRefOrderItemList itemList, DirectRefOrder order)
    {
        for(int i=0;i<itemList.size();i++)
        {
            DirectRefOrderItem item = itemList.getDirectRefOrderItemAt(i);
            assertSame(order, item.getOrder());
        }
    }

    private void addItem(DirectRefOrder order)
    {
        DirectRefOrderItem item = new DirectRefOrderItem();
        item.setDiscountPrice(15);
        item.setId(3000);
        order.getItems().add(item);
    }

    public void testInsertSettingDependentsAtOnceAfterId()
    {
        DirectRefOrder order = new DirectRefOrder();
        order.setDescription("test");
        order.setOrderId(2000);

        DirectRefOrderItemList itemList = new DirectRefOrderItemList();
        for(int i=0;i<10;i++)
        {
            DirectRefOrderItem item = new DirectRefOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());
        for(int i=0;i<itemList.size();i++)
        {
            DirectRefOrderItem item = itemList.getDirectRefOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        checkOrderOnItems(itemList, order);

        addItem(order);
        checkOrderOnItems(itemList, order);

        assertEquals(11, order.getItems().size());
        assertEquals(2000, itemList.getDirectRefOrderItemAt(10).getOrderId());

        DirectRefOrderStatus status = createStatus();
        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        DirectRefOrderFinder.clearQueryCache();
        DirectRefOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = DirectRefOrderFinder.findOne(DirectRefOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    public void testInsertSettingDependentsAtOnceAfterIdWithRetry()
    {
        DirectRefOrder order = new DirectRefOrder();
        order.setDescription("test");
        order.setOrderId(2000);

        DirectRefOrderItemList itemList = new DirectRefOrderItemList();
        for(int i=0;i<10;i++)
        {
            DirectRefOrderItem item = new DirectRefOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());
        for(int i=0;i<itemList.size();i++)
        {
            DirectRefOrderItem item = itemList.getDirectRefOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        checkOrderOnItems(itemList, order);

        addItem(order);
        checkOrderOnItems(itemList, order);

        assertEquals(11, order.getItems().size());
        assertEquals(2000, itemList.getDirectRefOrderItemAt(10).getOrderId());

        DirectRefOrderStatus status = createStatus();
        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());
        assertEquals(2000, status.getOrderId());

        copyOrder(order);
        itemList = order.getItems();
        assertEquals(11, itemList.size());
        for(int i=0;i<itemList.size();i++)
        {
            assertTrue(itemList.get(i).zIsDetached());
        }
        assertTrue(order.getOrderStatus().zIsDetached());
        order = null;
        itemList = null;
        DirectRefOrderFinder.clearQueryCache();
        DirectRefOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = DirectRefOrderFinder.findOne(DirectRefOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    private void copyOrder(final DirectRefOrder order)
    {
        final int[] count = new int[1];
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.copyDetachedValuesToOriginalOrInsertIfNew();
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
    }

    public void testInsertSettingDependentsWithOrder()
    {
        DirectRefOrder order = new DirectRefOrder();
        order.setDescription("test");

        for(int i=0;i<10;i++)
        {
            DirectRefOrderItem item = new DirectRefOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            item.setOrder(order);
        }
        DirectRefOrderItemList itemList = order.getItems();
        assertEquals(10, itemList.size());
        order.setOrderId(2000);

        addItem(order);
        assertEquals(11, itemList.size());
        checkOrderOnItems(itemList, order);

        for(int i=0;i<itemList.size();i++)
        {
            DirectRefOrderItem item = itemList.getDirectRefOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        DirectRefOrderFinder.clearQueryCache();
        DirectRefOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = DirectRefOrderFinder.findOne(DirectRefOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

}
