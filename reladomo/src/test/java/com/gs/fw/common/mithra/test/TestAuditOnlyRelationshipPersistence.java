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


import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;

import java.sql.SQLException;
import java.sql.Timestamp;

public class TestAuditOnlyRelationshipPersistence
extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[] { OrderItem.class, OrderItemStatus.class, AuditedOrder.class, AuditedOrderItem.class, AuditedOrderItemStatus.class, AuditedOrderStatus.class, AuditedOrderStatusTwo.class, Product.class, Stock.class, StockPrice.class };
    }

    private void checkOrderOnItems(AuditedOrderItemList itemList, AuditedOrder order)
    {
        for(int i=0;i<itemList.size();i++)
        {
            AuditedOrderItem item = itemList.getAuditedOrderItemAt(i);
            assertSame(order, item.getOrder());
        }
    }

    private AuditedOrderStatus createStatus()
    {
        AuditedOrderStatus status = new AuditedOrderStatus();
        status.setStatus(10);
        status.setLastUser("wilma");
        return status;
    }

    public void testInsertSettingDependentsAtOnceBeforeId()
    {
        AuditedOrder order = new AuditedOrder();
        order.setDescription("test");

        AuditedOrderItemList itemList = new AuditedOrderItemList();
        for(int i=0;i<10;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
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

        AuditedOrderStatus status = createStatus();

        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            AuditedOrderItem item = itemList.getAuditedOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    private void addItem(AuditedOrder order)
    {
        AuditedOrderItem item = new AuditedOrderItem();
        item.setDiscountPrice(15);
        item.setId(3000);
        order.getItems().add(item);
    }

    public void testInsertSettingDependentsAtOnceAfterId()
    {
        AuditedOrder order = new AuditedOrder();
        order.setDescription("test");
        order.setOrderId(2000);

        AuditedOrderItemList itemList = new AuditedOrderItemList();
        for(int i=0;i<10;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);

        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());
        checkOrderOnItems(itemList, order);
        for(int i=0;i<itemList.size();i++)
        {
            AuditedOrderItem item = itemList.getAuditedOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }

        addItem(order);

        assertEquals(11, order.getItems().size());
        assertEquals(2000, itemList.getAuditedOrderItemAt(10).getOrderId());
        checkOrderOnItems(itemList, order);

        AuditedOrderStatus status = createStatus();
        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    public void testInsertSettingDependentsWithAuditedOrder()
    {
        AuditedOrder order = new AuditedOrder();
        order.setDescription("test");

        for(int i=0;i<10;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            item.setOrder(order);
        }
        AuditedOrderItemList itemList = order.getItems();
        assertEquals(10, itemList.size());
        order.setOrderId(2000);
        checkOrderOnItems(itemList, order);

        addItem(order);
        assertEquals(11, itemList.size());
        checkOrderOnItems(itemList, order);

        for(int i=0;i<itemList.size();i++)
        {
            AuditedOrderItem item = itemList.getAuditedOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    public void testInsertAddingDependentsBeforeId()
    {
        AuditedOrder order = new AuditedOrder();
        order.setDescription("test");

        AuditedOrderItemList itemList = order.getItems();
        for(int i=0;i<10;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        assertEquals(10, order.getItems().size());
        checkOrderOnItems(itemList, order);
        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            AuditedOrderItem item = itemList.getAuditedOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        order.cascadeInsert();

        assertEquals(10, order.getItems().size());
        order = null;
        itemList = null;
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2000));
        assertEquals(10, order.getItems().size());
    }

    public void testInsertAddingDependentsAfterId()
    {
        AuditedOrder order = new AuditedOrder();
        order.setDescription("test");
        order.setOrderId(2000);

        AuditedOrderItemList itemList = order.getItems();
        for(int i=0;i<10;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        assertEquals(10, order.getItems().size());
        checkOrderOnItems(itemList, order);
        for(int i=0;i<itemList.size();i++)
        {
            AuditedOrderItem item = itemList.getAuditedOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        order.cascadeInsert();

        assertEquals(10, order.getItems().size());
        order = null;
        itemList = null;
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2000));
        assertEquals(10, order.getItems().size());
    }

    private AuditedOrder findOne(int orderId)
    {
        return AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(orderId));
    }

    public void testPersistedAuditedOrderAddItem()
    {
        final AuditedOrder order = findOne(1);

        final AuditedOrderItem item = new AuditedOrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.getItems().add(item);
                return null;
            }
        });

        assertEquals(order.getOrderId(), item.getOrderId());
        assertSame(item, AuditedOrderItemFinder.findOneBypassCache(AuditedOrderItemFinder.id().eq(1234)));

    }

    public void testPersistedOrderSetStatus()
    {
        final AuditedOrder order = findOne(2);

        final AuditedOrderStatus status = createStatus();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.setOrderStatus(status);
                return null;
            }
        });

        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        assertEquals(order.getOrderId(), status.getOrderId());
        assertSame(status, AuditedOrderStatusFinder.findOneBypassCache(AuditedOrderStatusFinder.orderId().eq(2)));
    }

    public void testPersistedOrderSetStatusForExisting()
    {
        final AuditedOrder order = findOne(1);

        final AuditedOrderStatus status = createStatus();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.setOrderStatus(status);
                return null;
            }
        });

        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        assertEquals(order.getOrderId(), status.getOrderId());
        assertSame(status, AuditedOrderStatusFinder.findOneBypassCache(AuditedOrderStatusFinder.orderId().eq(1)));
    }

    public void testPersistedOrderSetStatusNull()
    {
        final AuditedOrder order = findOne(1);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.setOrderStatus(null);
                return null;
            }
        });

        assertNull(order.getOrderStatus());

        assertNull(AuditedOrderStatusFinder.findOneBypassCache(AuditedOrderStatusFinder.orderId().eq(1)));
    }

    public void testPersistedAuditedOrderRemoveItem()
    {
        AuditedOrder order = findOne(1);

        final AuditedOrderItemList items = order.getItems();
        int oldSize = items.size();
        int oldId = items.getAuditedOrderItemAt(0).getId();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                items.remove(0);
                return null;
            }
        });

        assertEquals(oldSize - 1, order.getItems().size());
        assertNull(AuditedOrderItemFinder.findOneBypassCache(AuditedOrderItemFinder.id().eq(oldId)));

    }

    public void testPersistedAuditedOrderCascadeTerminate()
    {
        final AuditedOrder order = findOne(1);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.cascadeTerminate();
                return null;
            }
        });

        AuditedOrderItemList list = new AuditedOrderItemList(AuditedOrderItemFinder.orderId().eq(1));
        list.setBypassCache(true);

        assertEquals(0, list.size());
        assertNull(AuditedOrderStatusFinder.findOne(AuditedOrderStatusFinder.orderId().eq(1)));

    }

    public void testPersistedAuditedOrderCascadeTerminateDetached()
    {
        final AuditedOrder order = findOne(1).getDetachedCopy();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.cascadeTerminate();
                order.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });

        AuditedOrderItemList list = new AuditedOrderItemList(AuditedOrderItemFinder.orderId().eq(1));
        list.setBypassCache(true);

        assertEquals(0, list.size());
        assertNull(AuditedOrderStatusFinder.findOne(AuditedOrderStatusFinder.orderId().eq(1)));

    }

    public void testPersistedAuditedOrderSetAuditedOrderOnNewItem()
    {
        final AuditedOrder order = findOne(1);

        AuditedOrderItem item = (AuditedOrderItem)
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderItem item = new AuditedOrderItem();
                item.setDiscountPrice(17);
                item.setId(1234);
                item.setOrder(order);
                return item;
            }
        });

        assertEquals(order.getOrderId(), item.getOrderId());
        assertSame(item, AuditedOrderItemFinder.findOneBypassCache(AuditedOrderItemFinder.id().eq(1234)));

    }

    public void testPersistedAuditedOrderResetAuditedOrderItems()
    {
        final AuditedOrder order = findOne(2);

        AuditedOrderItemList items = order.getItems();
        int originalSize = items.size();
        assertTrue(originalSize > 2);
        final AuditedOrderItemList newItems = new AuditedOrderItemList();

        AuditedOrderItem oldItem1 = (AuditedOrderItem) items.get(0);
        newItems.add(oldItem1);
        AuditedOrderItem oldItem2 = (AuditedOrderItem) items.get(1);
        newItems.add(oldItem2);

        AuditedOrderItem item = new AuditedOrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);

        newItems.add(item);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.setItems(newItems);
                return null;
            }
        });

        AuditedOrderItemList persistedItems = order.getItems();
        assertEquals(3, persistedItems.size());
        assertTrue(persistedItems.contains(oldItem1));
        assertTrue(persistedItems.contains(oldItem2));
        assertTrue(persistedItems.contains(item));

        final AuditedOrderItem item2 = new AuditedOrderItem();
        item2.setDiscountPrice(17);
        item2.setId(1235);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                newItems.add(item2);
                order.setItems(newItems);
                return null;
            }
        });

        persistedItems = order.getItems();
        assertEquals(4, persistedItems.size());
        assertTrue(persistedItems.contains(item2));
    }

    public void testNonDependentRelationshipSetsAttributes()
    {
        final AuditedOrderItem item = new AuditedOrderItem();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                item.setProductInfo(ProductFinder.findOne(ProductFinder.productId().eq(1)));
                return null;
            }
        });
        assertEquals(1, item.getProductId());
    }

    public void testNonDependentRelationshipSetsAttributesToNull()
    {
        final AuditedOrderItem item = new AuditedOrderItem();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                item.setProductInfo(null);
                return null;
            }
        });
        assertTrue(item.isProductIdNull());
        assertNotNull(item.getProcessingDate());

        final AuditedOrderItem item2 = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                item2.setProductInfo(null);
                return null;
            }
        });
        assertTrue(item2.isProductIdNull());
        assertNotNull(item2.getProcessingDate());

    }

    public void testNonDependentRelationshipSetsAttributesToNullOnDatedObject()
    {
        final Stock stock = new Stock(new Timestamp(System.currentTimeMillis()));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                stock.setStockPrice(null);
                return null;
            }
        });
        assertTrue(stock.isStockPriceIdNull());
        assertNotNull(stock.getBusinessDate());
    }

    public void testNonDependentRelationshipSetsAttributesForPersisted()
    {
        final AuditedOrderItem item = findOne(1).getItems().getAuditedOrderItemAt(0);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                item.setProductInfo(ProductFinder.findOne(ProductFinder.productId().eq(1)));
                return null;
            }
        });
        AuditedOrderItem item2 = findOne(1).getItems().getAuditedOrderItemAt(0);
        assertEquals(1, item2.getProductId());
    }

    public void testOneObjectDetachedInsert() throws SQLException
    {
        final int orderId = 1234000;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrder order = new AuditedOrder(InfinityTimestamp.getParaInfinity());
                order.setDescription("test 1");
                order.setOrderDate(new Timestamp(System.currentTimeMillis()));
                order.setOrderId(orderId);
                order.setState("test state");
                order.setTrackingId("t1234000");
                order.setUserId(17);
                AuditedOrderItemList items = new AuditedOrderItemList();
                AuditedOrderItem item = new AuditedOrderItem();
                item.setDiscountPrice(17);
                item.setId(1234);
                items.add(item);
                order.setItems(items);
                order.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        AuditedOrderFinder.clearQueryCache();
        AuditedOrderList list = new AuditedOrderList(AuditedOrderFinder.orderId().eq(orderId));
        assertEquals(1, list.size());
        assertEquals(1, list.getAuditedOrderAt(0).getItems().size());
    }

    public void testDetachedAddItem()
    {
        AuditedOrder order = findOne(1);

        AuditedOrder detachedAuditedOrder = order.getDetachedCopy();

        detachedAuditedOrder.setDescription("new desc");
        AuditedOrderItemList items = detachedAuditedOrder.getItems();
        checkOrderOnItems(items, detachedAuditedOrder);
        AuditedOrderItem item = new AuditedOrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);
        checkOrderOnItems(items, detachedAuditedOrder);
        // add an item
        // remove an exisiting item
        // update an item

        detachedAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(2, order.getItems().size());
    }

    public void testDetachedRemoveItem()
    {
        AuditedOrder order = findOne(2);
        int sizeBefore = order.getItems().size();
        AuditedOrder detachedAuditedOrder = order.getDetachedCopy();

        detachedAuditedOrder.setDescription("new desc");
        detachedAuditedOrder.getItems().remove(0);
        assertEquals(sizeBefore - 1, detachedAuditedOrder.getItems().size());

        detachedAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(sizeBefore - 1, order.getItems().size());
    }

    public void testDetachedUpdateItem()
    {
        AuditedOrder order = findOne(1);
        AuditedOrder detachedAuditedOrder = order.getDetachedCopy();

        detachedAuditedOrder.getItems().getAuditedOrderItemAt(0).setDiscountPrice(1234.56);

        detachedAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(1234.56, order.getItems().getAuditedOrderItemAt(0).getDiscountPrice(), 0);
    }

    public void testDetachedMixed()
    {
        AuditedOrder order = findOne(2);
        int sizeBefore = order.getItems().size();
        AuditedOrder detachedAuditedOrder = order.getDetachedCopy();

        detachedAuditedOrder.setDescription("new desc");
        AuditedOrderItemList items = detachedAuditedOrder.getItems();
        items.remove(0);
        AuditedOrderItem modifiedItem = items.getAuditedOrderItemAt(0);
        modifiedItem.setDiscountPrice(1234.56);
        int modifiedItemId = modifiedItem.getId();

        for(int i=0;i<5;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }
        checkOrderOnItems(items, detachedAuditedOrder);

        for(int i=0;i<5;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(170+i);
            item.setId(12340+i);
            item.setOrder(detachedAuditedOrder);
        }

        checkOrderOnItems(items, detachedAuditedOrder);
        detachedAuditedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        AuditedOrderItemList persistedItems = order.getItems();
        assertEquals(sizeBefore-1+5+5, persistedItems.size());
        for(int i=0;i<persistedItems.size();i++)
        {
            if (persistedItems.getAuditedOrderItemAt(i).getId() == modifiedItemId)
            {
                assertEquals(1234.56, persistedItems.getAuditedOrderItemAt(i).getDiscountPrice(), 0);
            }
        }
    }

    public void testDetachedReplaceList()
    {
        AuditedOrder order = findOne(2);
        AuditedOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        AuditedOrderItemList items = new AuditedOrderItemList();

        for(int i=0;i<5;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        AuditedOrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());
    }

    public void testDetachedReplaceMixDetachedIntoNewList()
    {
        AuditedOrder order = findOne(2);
        AuditedOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        AuditedOrderItemList items = new AuditedOrderItemList();

        for(int i=0;i<5;i++)
        {
            AuditedOrderItem item = new AuditedOrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        AuditedOrderItemList detachedList = detachedOrder.getItems();
        items.add(detachedList.get(0));
        items.add(detachedList.get(1));

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        AuditedOrderItemList persistedItems = order.getItems();
        assertEquals(7, persistedItems.size());
    }

    public void testDetachedSetStatusForExisting()
    {
        AuditedOrder order = findOne(1);

        AuditedOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        AuditedOrderStatus status = createStatus();
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());
        assertEquals(1, status.getOrderId());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusNullForExisting()
    {
        AuditedOrder order = findOne(1);

        AuditedOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertNull(order.getOrderStatus());
    }

    public void testDetachedSetStatusNullAndBackForExisting()
    {
        AuditedOrder order = findOne(1);

        AuditedOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        AuditedOrderStatus status = createStatus();
        detachedOrder.setOrderStatus(status);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusForNonExisting()
    {
        AuditedOrder order = findOne(2);

        AuditedOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        AuditedOrderStatus status = createStatus();
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertSame(status, order.getOrderStatus());
    }

    public void testSettingRelationship()
    {
        AuditedOrderItem item = new AuditedOrderItem();
        Product product = new Product();
        item.setProductInfo(product);
        assertSame(product, item.getProductInfo());
    }
}
