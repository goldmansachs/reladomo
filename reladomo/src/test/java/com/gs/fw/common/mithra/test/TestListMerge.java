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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Date;
import java.sql.Timestamp;

public class TestListMerge extends MithraTestAbstract
{
    public void testShallow()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                IntHashSet set = IntHashSet.newSetWith(1,2,3,4);
                OrderList list = OrderFinder.findMany(OrderFinder.orderId().in(set));
                OrderList nonPersistentCopy = list.getNonPersistentCopy();
                nonPersistentCopy.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
                for(int i=0;i<4;i++)
                {
                    Order order = nonPersistentCopy.get(i);
                    order.setUserId(order.getUserId()+1000);
                    order.setDescription("X"+order.getDescription());
                    order.setOrderId(0);
                }
                nonPersistentCopy.remove(2);
                Order newOrder = createOrder2000();
                nonPersistentCopy.add(newOrder);

                TopLevelMergeOptions mergeOptions = new TopLevelMergeOptions(OrderFinder.getFinderInstance());
                mergeOptions.matchOn(OrderFinder.trackingId());
                list.merge(nonPersistentCopy, mergeOptions);
                assertNull(OrderFinder.findByPrimaryKey(3));
                assertNotNull(OrderFinder.findByPrimaryKey(2000));
                assertTrue(OrderFinder.findByPrimaryKey(1).getDescription().startsWith("X"));
                assertTrue(OrderFinder.findByPrimaryKey(2).getUserId() > 1000);
                return null;
            }
        });
        assertNull(OrderFinder.findOne(OrderFinder.orderId().eq(3)));
        assertNotNull(OrderFinder.findByPrimaryKey(2000));
        assertTrue(OrderFinder.findByPrimaryKey(1).getDescription().startsWith("X"));
        assertTrue(OrderFinder.findByPrimaryKey(2).getUserId() > 1000);
    }

    private Order createOrder2000()
    {
        Order newOrder = new Order();
        newOrder.setOrderId(2000);
        newOrder.setUserId(50);
        newOrder.setTrackingId("newnew");
        newOrder.setDescription("new desc");
        return newOrder;
    }

    public void testDeep()
    {
        final TopLevelMergeOptions mergeOptions = new TopLevelMergeOptions(OrderFinder.getFinderInstance());
        mergeOptions.matchOn(OrderFinder.trackingId());
        mergeOptions.navigateTo(OrderFinder.items().orderItemStatus());
        mergeOptions.navigateTo(OrderFinder.orderStatus());

        mergeDeep(mergeOptions);
        assertDeep();
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertDeep();
    }

    public void testDeepFullDependents()
    {
        final TopLevelMergeOptions mergeOptions = new TopLevelMergeOptions(OrderFinder.getFinderInstance());
        mergeOptions.matchOn(OrderFinder.trackingId());
        mergeOptions.navigateToAllDeepDependents();

        mergeDeep(mergeOptions);
        assertDeep();
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        assertDeep();
    }

    private void mergeDeep(final TopLevelMergeOptions mergeOptions)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                IntHashSet set = IntHashSet.newSetWith(1,2,3,4);
                OrderList list = OrderFinder.findMany(OrderFinder.orderId().in(set));
                list.deepFetch(OrderFinder.items().orderItemStatus());
                list.deepFetch(OrderFinder.orderStatus());
                OrderList nonPersistentCopy = list.getNonPersistentCopy();
                nonPersistentCopy.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
                for(int i=0;i<4;i++)
                {
                    Order order = nonPersistentCopy.get(i);
                    order.setUserId(order.getUserId()+1000);
                    order.setDescription("X"+order.getDescription());
                    order.setOrderId(0);
                }
                // Order id 1 has 1 item. We're not populating that, expecting it to be removed. Also tests order item status removal
                OrderStatus orderStatus1 = list.get(0).getOrderStatus().getNonPersistentCopy();
                orderStatus1.setLastUser("peter");
                orderStatus1.setStatus(155);
                orderStatus1.setOrderId(-12);
                nonPersistentCopy.get(0).setOrderStatus(orderStatus1);
                // Order id 2 has 3 items. We'll remove 1, add 1 and modify 1
                OrderItemList sortedPersistedItems2 = list.get(1).getItems();
                sortedPersistedItems2.setOrderBy(OrderItemFinder.id().ascendingOrderBy());

                OrderItemList order2Items = sortedPersistedItems2.getNonPersistentCopy();
                order2Items.remove(0); // order item id 2
                OrderItem orderItem = order2Items.get(0); //order item id 3
                orderItem.setOriginalPrice(17.7);
                orderItem.setDiscountPrice(14.4);

                OrderItemStatus status3 = sortedPersistedItems2.get(1).getOrderItemStatus().getNonPersistentCopy();
                status3.setLastUser("Wilma");
                orderItem.setOrderItemStatus(status3);

                OrderItem newItem = new OrderItem();
                newItem.setId(1000);
                newItem.setDiscountPrice(13.3);
                newItem.setOriginalPrice(66);
                newItem.setProductId(1);
                newItem.setQuantity(7);
                newItem.setState("brand new");
                order2Items.add(newItem);

                OrderItemStatus status = new OrderItemStatus();
                status.setStatus(5);
                status.setLastUpdateTime(new Timestamp(12345678901234567L));
                status.setLastUser("foo");
                newItem.setOrderItemStatus(status);

                nonPersistentCopy.get(1).setItems(order2Items);

                //Order id 4 has no items, we'll add one.
                OrderItem newItem2 = new OrderItem();
                newItem2.setId(2000);
                newItem2.setDiscountPrice(13.4);
                newItem2.setOriginalPrice(67);
                newItem2.setProductId(2);
                newItem2.setQuantity(8);
                newItem2.setState("x brand new");

                nonPersistentCopy.get(3).getItems().add(newItem2);

                // remove order id 3.
                nonPersistentCopy.remove(2);
                Order newOrder = createOrder2000();
                OrderStatus orderStatus2000 = new OrderStatus();
                orderStatus2000.setExpectedDate(new Date(System.currentTimeMillis()));
                orderStatus2000.setLastUpdateTime(new Timestamp(12345678901234567L));
                orderStatus2000.setLastUser("george");
                orderStatus2000.setStatus(17);
                newOrder.setOrderStatus(orderStatus2000);


                nonPersistentCopy.add(newOrder);

                list.merge(nonPersistentCopy, mergeOptions);

                assertDeep();

                return null;
            }
        });
    }

    private void assertDeep()
    {
        assertNull(OrderFinder.findByPrimaryKey(3));
        assertNotNull(OrderFinder.findByPrimaryKey(2000));
        assertTrue(OrderFinder.findByPrimaryKey(1).getDescription().startsWith("X"));
        assertTrue(OrderFinder.findByPrimaryKey(2).getUserId() > 1000);

        assertEquals(0, OrderItemFinder.findMany(OrderItemFinder.orderId().eq(3)).size());
        assertEquals(1, OrderItemFinder.findMany(OrderItemFinder.orderId().eq(4)).size());
        assertEquals(0, OrderItemFinder.findMany(OrderItemFinder.orderId().eq(1)).size());
        OrderItemList itemsForOrder2 = OrderItemFinder.findMany(OrderItemFinder.orderId().eq(2));
        itemsForOrder2.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
        assertEquals(3, itemsForOrder2.size());
        assertEquals(14.4, itemsForOrder2.get(0).getDiscountPrice());
        assertEquals(17.7, itemsForOrder2.get(0).getOriginalPrice());
        assertEquals(1000, itemsForOrder2.get(2).getId());

        assertEquals("peter", OrderStatusFinder.findByPrimaryKey(1).getLastUser());
        assertEquals(155, OrderStatusFinder.findByPrimaryKey(1).getStatus());
        assertEquals("george", OrderStatusFinder.findByPrimaryKey(2000).getLastUser());
        assertEquals(17, OrderStatusFinder.findByPrimaryKey(2000).getStatus());

        assertEquals("Wilma", OrderItemFinder.findByPrimaryKey(3).getOrderItemStatus().getLastUser());
        assertEquals("foo", OrderItemFinder.findByPrimaryKey(1000).getOrderItemStatus().getLastUser());
    }
}
