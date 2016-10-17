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
package com.gs.fw.common.mithra.test.util;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.MultiThreadedBatchProcessor;
import com.gs.fw.finder.Navigation;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedBatchProcessorTest extends MithraTestAbstract
{

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        createOrdersAndItems();
    }

    public void testNoShards()
    {
        List deepFetches = FastList.newListWith(OrderFinder.items(), OrderFinder.orderStatus());

        OrderConsumer consumer = new OrderConsumer();

        MultiThreadedBatchProcessor<Order, OrderList> mtbp = new MultiThreadedBatchProcessor<Order, OrderList>(OrderFinder.getFinderInstance(),
                OrderFinder.orderId().greaterThanEquals(1000), (List<Navigation<Order>>) deepFetches, consumer, null);
        mtbp.setBatchSize(77);
        mtbp.process();
        assertEquals(1100, consumer.count.get());
    }

    private static class OrderConsumer implements MultiThreadedBatchProcessor.Consumer<Order, OrderList>
    {
        private AtomicInteger count = new AtomicInteger();

        @Override
        public void startConsumption(MultiThreadedBatchProcessor<Order, OrderList> processor)
        {

        }

        @Override
        public void consume(OrderList list) throws Exception
        {
            count.addAndGet(list.size());
            for(Order o: list)
            {
                assertEquals(2, o.getItems().size());
                assertNotNull(o.getOrderStatus());
            }
        }

        @Override
        public void endConsumption(MultiThreadedBatchProcessor<Order, OrderList> processor)
        {

        }
    }

    private IntHashSet createOrdersAndItems()
    {
        OrderList orderList = new OrderList();
        for (int i = 0; i < 1100; i++)
        {
            Order order = new Order();
            order.setOrderId(i+1000);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setTrackingId("T"+i+1000);
            orderList.add(order);
        }
        orderList.bulkInsertAll();
        OrderItemList items = new OrderItemList();
        IntHashSet itemIds = new IntHashSet();
        for (int i = 0; i < 1100; i++)
        {
            OrderItem item = new OrderItem();
            item.setOrderId(i+1000);
            item.setId(i + 1000);
            items.add(item);

            item = new OrderItem();
            item.setOrderId(i+1000);
            item.setId(i+3000);
            items.add(item);

            itemIds.add(i + 1000);
            itemIds.add(i+3000);
        }
        items.bulkInsertAll();
        OrderStatusList statusList = new OrderStatusList();
        for (int i = 0; i < 1100; i++)
        {
            OrderStatus status = new OrderStatus();
            status.setOrderId(i+1000);
            status.setLastUser(""+i);
            statusList.add(status);
        }
        statusList.bulkInsertAll();
        return itemIds;
    }
}
