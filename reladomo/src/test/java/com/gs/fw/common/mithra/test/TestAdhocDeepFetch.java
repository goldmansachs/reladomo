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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.DeepFetchTree;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.AuditedOrder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItem;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemList;
import com.gs.fw.common.mithra.test.domain.AuditedOrderList;
import com.gs.fw.common.mithra.test.domain.AuditedOrderStatus;
import com.gs.fw.common.mithra.test.domain.AuditedOrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderStatusList;
import com.gs.fw.common.mithra.test.domain.ExchangeRate;
import com.gs.fw.common.mithra.test.domain.ExchangeRateChild;
import com.gs.fw.common.mithra.test.domain.ExchangeRateChildFinder;
import com.gs.fw.common.mithra.test.domain.ExchangeRateChildList;
import com.gs.fw.common.mithra.test.domain.ExchangeRateList;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.OrderStatus;
import com.gs.fw.common.mithra.test.domain.OrderStatusList;
import com.gs.fw.common.mithra.test.domain.ProductWithSourceAttribute;
import com.gs.fw.common.mithra.test.domain.ProductWithSourceAttributeFinder;
import com.gs.fw.common.mithra.test.domain.ProductWithSourceAttributeList;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import com.gs.fw.common.mithra.test.domain.TinyBalanceList;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import com.gs.fw.common.mithra.test.domain.UserList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.junit.Assert;

import java.sql.Timestamp;
import java.util.List;


public class TestAdhocDeepFetch extends MithraTestAbstract
{

    protected void setUp() throws Exception
    {
        super.setUp();
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

    private IntHashSet createAuditedOrdersAndItems(int start, int count)
    {
        AuditedOrderList orderList = new AuditedOrderList();
        for (int i = 0; i < count; i++)
        {
            AuditedOrder order = new AuditedOrder();
            order.setOrderId(i+ start);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setTrackingId("T"+i+ start);
            orderList.add(order);
        }
        orderList.bulkInsertAll();
        AuditedOrderItemList items = new AuditedOrderItemList();
        IntHashSet itemIds = new IntHashSet();
        for (int i = 0; i < count; i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setOrderId(i+ start);
            item.setId(i + start);
            items.add(item);

            item = new AuditedOrderItem();
            item.setOrderId(i+ start);
            item.setId(i+start+ count);
            items.add(item);

            itemIds.add(i + start);
            itemIds.add(i+start+ count);
        }
        items.bulkInsertAll();
        AuditedOrderStatusList statusList = new AuditedOrderStatusList();
        for (int i = 0; i < count; i++)
        {
            AuditedOrderStatus status = new AuditedOrderStatus();
            status.setOrderId(i+ start);
            status.setLastUser(""+i);
            statusList.add(status);
        }
        statusList.bulkInsertAll();
        return itemIds;
    }

    public void testAdhocDeepFetchWithFromIsInclusiveTempTable()
    {
        int increment = 2;
        if (AuditedOrderFinder.getMithraObjectPortal().isFullyCached())
        {
            increment = 0;
        }
        createAuditedOrdersAndItems(1000, 2000);
        AuditedOrderList orderList = new AuditedOrderList();
        long now = System.currentTimeMillis()+1000;
        for (int i = 0; i < 500; i++)
        {
            orderList.add(AuditedOrderFinder.findByPrimaryKey(i+1000, new Timestamp(now + i*1000)));
        }
        for (int i = 500; i < 1100; i++)
        {
            orderList.add(AuditedOrderFinder.findByPrimaryKey(i+1000, AuditedOrderFinder.processingDate().getInfinityDate()));
        }
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        orderList.deepFetch(AuditedOrderFinder.items());
        orderList.deepFetch(AuditedOrderFinder.orderStatus());
        int count = this.getRetrievalCount();
        for(AuditedOrder order: orderList)
        {
            assertEquals(2, order.getItems().size());
            assertNotNull(order.getOrderStatus());
        }
        assertEquals(count+increment, this.getRetrievalCount());
    }

    public void testPartialDeepAdhocDeepFetchWithFromIsInclusiveTempTable()
    {
        int increment = 2;
        if (OrderFinder.getMithraObjectPortal().isFullyCached())
        {
            increment = 0;
        }
        createAuditedOrdersAndItems(10000, 10000);
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        AuditedOrderList adhoc = new AuditedOrderList(AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().greaterThanEquals(10000).and(AuditedOrderFinder.orderId().lessThan(16000))));
        adhoc.deepFetch(AuditedOrderFinder.items());
        adhoc.deepFetch(AuditedOrderFinder.orderStatus());
        int count = this.getRetrievalCount();
        for(AuditedOrder order: adhoc)
        {
            assertTrue(order.getItems().size() != 0);
            assertNotNull(order.getOrderStatus());
        }
        assertEquals(count + increment, this.getRetrievalCount());

        adhoc.addAll(AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().greaterThanEquals(16000).and(AuditedOrderFinder.orderId().lessThan(17010))));
        count = this.getRetrievalCount();
        for(AuditedOrder order: adhoc)
        {
            assertTrue(order.getItems().size() != 0);
            assertNotNull(order.getOrderStatus());
        }
        assertEquals(count + increment, this.getRetrievalCount());
        assertNotNull(AuditedOrderStatusFinder.findOne(AuditedOrderStatusFinder.orderId().eq(10010)));
        assertNotNull(AuditedOrderStatusFinder.findOne(AuditedOrderStatusFinder.orderId().eq(16010)));
    }

    public void testPartialDeepAdhocDeepFetch()
    {
        int increment = 2;
        if (OrderFinder.getMithraObjectPortal().isFullyCached())
        {
            increment = 0;
        }
        createOrdersAndItems();
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        OrderList adhoc = new OrderList(OrderFinder.findMany(OrderFinder.orderId().greaterThanEquals(100).and(OrderFinder.orderId().lessThan(1500))));
        adhoc.deepFetch(OrderFinder.items());
        adhoc.deepFetch(OrderFinder.orderStatus());
        int count = this.getRetrievalCount();
        for(Order order: adhoc)
        {
            order.getItems().size();
            assertNotNull(order.getOrderStatus());
        }
        assertEquals(count+increment, this.getRetrievalCount());

        adhoc.addAll(OrderFinder.findMany(OrderFinder.orderId().greaterThanEquals(1500).and(OrderFinder.orderId().lessThan(1520))));
        count = this.getRetrievalCount();
        for(Order order: adhoc)
        {
            order.getItems().size();
            assertNotNull(order.getOrderStatus());
        }
        assertEquals(count + increment, this.getRetrievalCount());
    }

    public void testPartialDeepAdhocDeepFetchMultiAttribute()
    {
        int increment = 1;
        if (ExchangeRateChildFinder.getMithraObjectPortal().isFullyCached())
        {
            increment = 0;
        }
        createExchangeRates();
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        Operation op = ExchangeRateChildFinder.acmapCode().eq("A").and(ExchangeRateChildFinder.source().lessThan(50));
        ExchangeRateChildList adhoc = new ExchangeRateChildList(ExchangeRateChildFinder.findMany(op));
        adhoc.deepFetch(ExchangeRateChildFinder.parent());
        int count = this.getRetrievalCount();
        for(ExchangeRateChild rate: adhoc)
        {
            assertNotNull(rate.getParent());
        }
        assertEquals(count+increment, this.getRetrievalCount());

        adhoc.addAll(ExchangeRateChildFinder.findMany(ExchangeRateChildFinder.acmapCode().eq("A").and(ExchangeRateChildFinder.source().greaterThanEquals(50).and(ExchangeRateChildFinder.source().lessThan(60)))));
        count = this.getRetrievalCount();
        for(ExchangeRateChild rate: adhoc)
        {
            assertNotNull(rate.getParent());
        }
        assertEquals(count + increment, this.getRetrievalCount());
    }

    public void createExchangeRates()
    {
        long now = System.currentTimeMillis();
        ExchangeRateList list = new ExchangeRateList();
        for(int i=0;i<100;i++)
        {
            ExchangeRate rate = new ExchangeRate();
            rate.setAcmapCode("A");
            rate.setCurrency(i+"X");
            rate.setDate(new Timestamp(now+i*1000));
            rate.setSource(i);

            list.add(rate);
        }
        list.bulkInsertAll();
        ExchangeRateChildList children = new ExchangeRateChildList();
        for(int i=0;i<100;i++)
        {
            ExchangeRateChild child = new ExchangeRateChild();
            child.setChildNum(i+1000);
            child.setAcmapCode("A");
            child.setCurrency(i+"X");
            child.setDt(new Timestamp(now + i * 1000));
            child.setSource(i);

            children.add(child);
        }
        children.bulkInsertAll();
    }

    public void testOneToManyOneToOneSameAttribute()
    {
        OrderList orderList = new OrderList(OrderFinder.findMany(OrderFinder.orderId().lessThan(5)));
        orderList.deepFetch(OrderFinder.items());
        orderList.deepFetch(OrderFinder.items().productInfo());
        orderList.deepFetch(OrderFinder.orderStatus());

        DeepFetchTree deepFetchTree = orderList.getDeepFetchTree();
        assertNotNull(deepFetchTree);
        List<DeepFetchTree> children = deepFetchTree.getChildren();
        assertEquals(2, children.size());
        for(int i=0;i<children.size();i++)
        {
            DeepFetchTree child = children.get(i);
            String relationshipName = child.getRelationshipAttribute().getRelationshipName();
            if (relationshipName.equals("items"))
            {
                List<DeepFetchTree> deepChildren = child.getChildren();
                assertEquals(1, deepChildren.size());
                assertEquals("productInfo", deepChildren.get(0).getRelationshipAttribute().getRelationshipName());
            }
            else if (relationshipName.equals("orderStatus"))
            {
                assertEquals(0, child.getChildren().size());
            }
            else
            {
                fail("unexpected relationship "+ relationshipName);
            }
        }

        assertTrue(orderList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        int prodCount = 0;
        for(int i=0;i<orderList.size();i++)
        {
            OrderItemList items = orderList.get(i).getItems();
            itemCount += items.size();

            for(OrderItem item: items)
            {
                if (item.getProductInfo() != null) prodCount++;
            }
            orderList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertTrue(prodCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToManyOneToOneSameAttributeJustOne()
    {
        OrderList orderList = new OrderList(OrderFinder.findMany(OrderFinder.orderId().eq(1)));
        orderList.deepFetch(OrderFinder.items());
        orderList.deepFetch(OrderFinder.items().productInfo());
        orderList.deepFetch(OrderFinder.orderStatus());

        assertTrue(orderList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        int prodCount = 0;
        for(int i=0;i<orderList.size();i++)
        {
            OrderItemList items = orderList.get(i).getItems();
            itemCount += items.size();

            for(OrderItem item: items)
            {
                if (item.getProductInfo() != null) prodCount++;
            }
            orderList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertTrue(prodCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToManyOneToOneSameAttributeDated()
    {
        AuditedOrderList auditedorderList = new AuditedOrderList(AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().lessThan(5)));
        auditedorderList.deepFetch(AuditedOrderFinder.items());
        auditedorderList.deepFetch(AuditedOrderFinder.items().productInfo());
        auditedorderList.deepFetch(AuditedOrderFinder.orderStatus());

        assertTrue(auditedorderList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        int prodCount = 0;
        for(int i=0;i<auditedorderList.size();i++)
        {
            AuditedOrderItemList items = auditedorderList.get(i).getItems();
            itemCount += items.size();

            for(AuditedOrderItem item: items)
            {
                if (item.getProductInfo() != null) prodCount++;
            }
            auditedorderList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertTrue(prodCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToManyOneToOneSameAttributeDatedMultiTime()
    {
        Operation op = AuditedOrderFinder.orderId().lessThan(5);
        AuditedOrderList auditedorderList = new AuditedOrderList(AuditedOrderFinder.findMany(op));
        Timestamp now = new Timestamp(System.currentTimeMillis());
        auditedorderList.addAll(AuditedOrderFinder.findMany(op.and(AuditedOrderFinder.processingDate().eq(now))));
        auditedorderList.deepFetch(AuditedOrderFinder.items());
        auditedorderList.deepFetch(AuditedOrderFinder.items().productInfo());
        auditedorderList.deepFetch(AuditedOrderFinder.orderStatus());

        assertTrue(auditedorderList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        int prodCount = 0;
        for(int i=0;i<auditedorderList.size();i++)
        {
            AuditedOrderItemList items = auditedorderList.get(i).getItems();
            itemCount += items.size();

            for(AuditedOrderItem item: items)
            {
                if (item.getProductInfo() != null) prodCount++;
            }
            auditedorderList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertTrue(prodCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToOneWithSource()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Operation op  = TinyBalanceFinder.balanceId().lessThan(100);
        op = op.and(TinyBalanceFinder.businessDate().eq(now));
        op = op.and(TinyBalanceFinder.acmapCode().eq(SOURCE_A));
        TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.findMany(op));
        list.deepFetch(TinyBalanceFinder.testRelationship());
        assertTrue(list.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int related = 0;
        for(int i=0;i<list.size();i++)
        {
            if (list.get(i).getTestRelationship() != null) related++;
        }
        assertTrue(related > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToOneWithSourceWithTemp()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Operation op  = TinyBalanceFinder.balanceId().lessThan(100);
        op = op.and(TinyBalanceFinder.businessDate().eq(now));
        op = op.and(TinyBalanceFinder.acmapCode().eq(SOURCE_A));
        TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.findMany(op));
        for(int i=0;i<1000;i++)
        {
            TinyBalance tb = new TinyBalance(now);
            tb.setAcmapCode(SOURCE_A);
            tb.setBalanceId(i+10000);
            tb.setQuantity(i*7);
            list.add(tb);
        }
        list.deepFetch(TinyBalanceFinder.testRelationship());
        assertTrue(list.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int related = 0;
        for(int i=0;i<list.size();i++)
        {
            if (list.get(i).getTestRelationship() != null) related++;
        }
        assertTrue(related > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testChained()
    {
        Operation op = UserFinder.id().lessThan(10);
        op = op.and(UserFinder.sourceId().eq(0));
        UserList list = new UserList(UserFinder.findMany(op));
        list.deepFetch(UserFinder.groups());
        list.deepFetch(UserFinder.groupsWithManagers());

        assertTrue(list.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int groups = 0;
        int managedGroups = 0;
        for(int i=0;i<list.size();i++)
        {
            groups += list.get(i).getGroups().size();
            managedGroups += list.get(i).getGroupsWithManagers().size();
        }
        assertTrue(groups > 0);
        assertTrue(managedGroups > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }
    
    public void testMissingDoubleDeep()
    {
        OrderList list = new OrderList();
        list.add(OrderFinder.findOne(OrderFinder.orderId().eq(3)));
        list.add(OrderFinder.findOne(OrderFinder.orderId().eq(4)));
        list.deepFetch(OrderFinder.items().productInfo());
        assertEquals(2, list.size());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for(int i=0;i<list.size();i++)
        {
            list.get(i).getItems();
        }
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testMissingDoubleDeepAlreadyFetched()
    {
        OrderList toFetch = OrderFinder.findMany(OrderFinder.orderId().eq(1).or(OrderFinder.orderId().eq(2)));
        toFetch.deepFetch(OrderFinder.items().productInfo());
        toFetch.forceResolve();
        OrderItemList list = new OrderItemList(toFetch.getItems());
        list.deepFetch(OrderItemFinder.order().orderStatus());
        list.forceResolve();
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for(int i=0;i<list.size();i++)
        {
            list.get(i).getOrder().getOrderStatus();
        }
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToManyOneToOneSameAttributeDatedMultiTimeInTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                AuditedOrderItemFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                AuditedOrderStatusFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                testOneToManyOneToOneSameAttributeDatedMultiTime();
                return null;
            }
        });
    }

    public void testManyToOneManyToManySameAttribute()
    {
        ProductWithSourceAttributeList adHocList = new ProductWithSourceAttributeList();
        adHocList.addAll(ProductWithSourceAttributeFinder.findMany(ProductWithSourceAttributeFinder.acmapCode().eq(SOURCE_A)));

        adHocList.deepFetch(ProductWithSourceAttributeFinder.profileType().productCategories());
        adHocList.forceResolve();

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        for(ProductWithSourceAttribute prod : adHocList)
        {
            prod.getProfileType().getProductCategories();
        }

        Assert.assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testManyToOneManyToManySameAttributeInTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testManyToOneManyToManySameAttribute();
                return null;
            }
        });
    }

    public void testOneToOneManyToOneManyToManySameAttribute()
    {
        ProductWithSourceAttributeList adHocList = new ProductWithSourceAttributeList();
        adHocList.addAll(ProductWithSourceAttributeFinder.findMany(ProductWithSourceAttributeFinder.acmapCode().eq(SOURCE_A)));

        adHocList.deepFetch(ProductWithSourceAttributeFinder.profileType().productCategories());
        adHocList.deepFetch(ProductWithSourceAttributeFinder.parentProduct().profileType().productCategories());
        adHocList.forceResolve();

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        for(ProductWithSourceAttribute prod : adHocList)
        {
            prod.getProfileType().getProductCategories();
            ProductWithSourceAttributeFinder.parentProduct().profileType().productCategories().valueOf(prod);
        }

        Assert.assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToOneManyToOneManyToManySameAttributeInTx()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testOneToOneManyToOneManyToManySameAttribute();
                return null;
            }
        });
    }

    public void testOneToOneAcrossMultipleSources()
    {
        TinyBalanceList allItemsList = new TinyBalanceList();
        List<String> sourceAttributes = FastList.newListWith(SOURCE_A, SOURCE_B);

        for (String sourceAttributeValue : sourceAttributes)
        {
            Operation op = TinyBalanceFinder.acmapCode().eq(sourceAttributeValue);
            op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
            allItemsList.addAll(TinyBalanceFinder.findMany(op));
        }

        allItemsList.deepFetch(TinyBalanceFinder.testRelationship());
        allItemsList.forceResolve();
    }

    public void testManyToOneAllNulls()
    {
        OrderItemList items = new OrderItemList();
        for(int i=0;i<1000;i++)
        {
            OrderItem item = new OrderItem();
            item.setId(i+10000);
            item.setOrderIdNull();
            items.add(item);
        }
        items.bulkInsertAll();
        items.deepFetch(OrderItemFinder.order());
        int count = getRetrievalCount();
        for(int i=0;i<1000;i++)
        {
            assertNull(items.get(i).getOrder());
        }
        assertEquals(count, getRetrievalCount());
    }

    public void testManyToOneAllNullsDated()
    {
        AuditedOrderItemList insertItems = new AuditedOrderItemList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setId(i+10000);
            item.setOrderIdNull();
            insertItems.add(item);
        }
        insertItems.bulkInsertAll();
        AuditedOrderItemList queryItems = new AuditedOrderItemList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrderItem item = AuditedOrderItemFinder.findByPrimaryKey(i + 10000, new Timestamp(System.currentTimeMillis() + i));
            queryItems.add(item);
        }

        queryItems.deepFetch(AuditedOrderItemFinder.order().items());
        int count = getRetrievalCount();
        for(int i=0;i<1000;i++)
        {
            assertNull(queryItems.get(i).getOrder());
        }
        assertEquals(count, getRetrievalCount());
    }
    
    public void testManyToOneTwoDeepWithNullsDated()
    {
        AuditedOrderItemList insertItems = new AuditedOrderItemList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setId(i+10000);
            item.setOrderId(i + 10000);
            insertItems.add(item);
        }
        insertItems.bulkInsertAll();
        
        AuditedOrderStatusList insertStatuss = new AuditedOrderStatusList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrderStatus status = new AuditedOrderStatus();
            status.setOrderId(i + 10000);
            insertStatuss.add(status);
        }
        insertStatuss.bulkInsertAll();
        
        AuditedOrderList inserts = new AuditedOrderList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrder order = new AuditedOrder();
            order.setOrderId(i + 10000);
            if ((i & 1) == 1)
            {
                order.setUserIdNull();
            }
            inserts.add(order);
        }
        inserts.bulkInsertAll();
        
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();

        AuditedOrderItemList queryItems = new AuditedOrderItemList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrderItem item = AuditedOrderItemFinder.findByPrimaryKey(i + 10000, new Timestamp(System.currentTimeMillis() + i));
            queryItems.add(item);
        }

        queryItems.deepFetch(AuditedOrderItemFinder.order().leftNullFilteredItems());
        queryItems.deepFetch(AuditedOrderItemFinder.order().leftNullFilteredStatus());
        queryItems.forceResolve();

        int count = getRetrievalCount();
        for(int i=0;i<1000;i++)
        {
            if ((i & 1) == 1)
            {
                assertNull(queryItems.get(i).getOrder().getLeftNullFilteredStatus());
                assertEquals(0, queryItems.get(i).getOrder().getLeftNullFilteredItems().size());
            }
            else
            {
                assertNotNull(queryItems.get(i).getOrder().getLeftNullFilteredStatus());
                assertEquals(1, queryItems.get(i).getOrder().getLeftNullFilteredItems().size());
            }
        }
        assertEquals(count, getRetrievalCount());
    }

    public void testOneToManyNullFiltered()
    {
        AuditedOrderList inserts = new AuditedOrderList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrder order = new AuditedOrder();
            order.setOrderId(i + 10000);
            if ((i & 1) == 1)
            {
                order.setUserIdNull();
            }
            inserts.add(order);
        }
        inserts.bulkInsertAll();

        AuditedOrderItemList insertItems = new AuditedOrderItemList();
        for(int i=0;i<1000;i+=8)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setId(i+10000);
            item.setOrderId(i + 10000);
            insertItems.add(item);
        }
        insertItems.bulkInsertAll();

        AuditedOrderList querys = new AuditedOrderList();
        for(int i=0;i<1000;i++)
        {
            AuditedOrder order  = AuditedOrderFinder.findByPrimaryKey(i + 10000, new Timestamp(System.currentTimeMillis() + i));
            querys.add(order);
        }

        querys.deepFetch(AuditedOrderFinder.items());
        querys.deepFetch(AuditedOrderFinder.leftNullFilteredItems());
        querys.forceResolve();

        int count = getRetrievalCount();
        for(int i=0;i<1000;i++)
        {
            if (i % 8 == 0)
            {
                assertEquals(1, querys.get(i).getItems().size());
                assertEquals(1, querys.get(i).getLeftNullFilteredItems().size());
            }
            else
            {
                assertTrue(querys.get(i).getItems().isEmpty());
                assertTrue(querys.get(i).getLeftNullFilteredItems().isEmpty());
            }
        }
        assertEquals(count, getRetrievalCount());
    }
}
