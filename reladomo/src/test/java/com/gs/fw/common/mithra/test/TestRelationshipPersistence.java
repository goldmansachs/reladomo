
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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.SQLException;

public class TestRelationshipPersistence
extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[] { Order.class, OrderParentToChildren.class, OrderItem.class, OrderStatus.class, OrderItemStatus.class, Book.class, Manufacturer.class, Product.class,
            OrderItemWi.class, OrderStatusWi.class};
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        createOrderItemForeignKey();
    }

    private void createOrderItemForeignKey() throws SQLException
    {
        executeStatement("ALTER TABLE app.ORDER_ITEM ADD CONSTRAINT ITEM_FK {FOREIGN KEY (ORDER_ID) REFERENCES app.ORDERS(ORDER_ID)}");
    }

    private void createOrderItemProductForeignKey() throws SQLException
    {
        executeStatement("ALTER TABLE app.ORDER_ITEM ADD CONSTRAINT ITEM_PROD_FK {FOREIGN KEY (PRODUCT_ID) REFERENCES app.PRODUCT(PROD_ID)}");
    }

    protected void tearDown() throws Exception
    {
        dropOrderItemForeignKey();
        super.tearDown();
    }

    protected void dropOrderItemForeignKey()
            throws SQLException
    {
        executeStatement("ALTER TABLE app.ORDER_ITEM DROP CONSTRAINT ITEM_FK");
    }

    protected void dropOrderItemProductForeignKey()
            throws SQLException
    {
        executeStatement("ALTER TABLE app.ORDER_ITEM DROP CONSTRAINT ITEM_PROD_FK");
    }

    public void testInsertSettingDependentsAtOnceBeforeId()
    {
        Order order = new Order();
        order.setDescription("test");

        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
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

        OrderStatus status = createStatus();

        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    public void testInsertSettingDependentsAtOnceBeforeId2()
    {
        Order order = new Order();
        order.setDescription("test");

        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
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

        OrderStatus status = createStatus();

        status.setOrder(order);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    private OrderStatus createStatus()
    {
        OrderStatus status = new OrderStatus();
        status.setStatus(10);
        status.setLastUser("wilma");
        return status;
    }

    private void checkOrderOnItems(OrderItemList itemList, Order order)
    {
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertSame(order, item.getOrder());
        }
    }

    private void addItem(Order order)
    {
        OrderItem item = new OrderItem();
        item.setDiscountPrice(15);
        item.setId(3000);
        order.getItems().add(item);
    }

    public void testInsertSettingDependentsAtOnceAfterId()
    {
        Order order = new Order();
        order.setDescription("test");
        order.setOrderId(2000);

        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        checkOrderOnItems(itemList, order);

        addItem(order);
        checkOrderOnItems(itemList, order);

        assertEquals(11, order.getItems().size());
        assertEquals(2000, itemList.getOrderItemAt(10).getOrderId());

        OrderStatus status = createStatus();
        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());
        assertEquals(2000, status.getOrderId());

        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    public void testInsertSettingDependentsAtOnceAfterIdWithRetry()
    {
        Order order = new Order();
        order.setDescription("test");
        order.setOrderId(2000);

        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        checkOrderOnItems(itemList, order);

        addItem(order);
        checkOrderOnItems(itemList, order);

        assertEquals(11, order.getItems().size());
        assertEquals(2000, itemList.getOrderItemAt(10).getOrderId());

        OrderStatus status = createStatus();
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
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    private void copyOrder(final Order order)
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
        Order order = new Order();
        order.setDescription("test");

        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            item.setOrder(order);
        }
        OrderItemList itemList = order.getItems();
        assertEquals(10, itemList.size());
        order.setOrderId(2000);

        addItem(order);
        assertEquals(11, itemList.size());
        checkOrderOnItems(itemList, order);

        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        order.cascadeInsert();
        assertEquals(11, order.getItems().size());
        order = null;
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(11, order.getItems().size());

    }

    public void testInsertAddingDependentsBeforeId()
    {
        Order order = new Order();
        order.setDescription("test");

        OrderItemList itemList = order.getItems();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        assertEquals(10, order.getItems().size());
        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        checkOrderOnItems(itemList, order);

        order.cascadeInsert();

        assertEquals(10, order.getItems().size());
        order = null;
        itemList = null;
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(10, order.getItems().size());
    }

    public void testInsertAddingDependentsAfterId()
    {
        Order order = new Order();
        order.setDescription("test");
        order.setOrderId(2000);

        OrderItemList itemList = order.getItems();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        assertEquals(10, order.getItems().size());
        for(int i=0;i<itemList.size();i++)
        {
            OrderItem item = itemList.getOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        checkOrderOnItems(itemList, order);

        order.cascadeInsert();

        assertEquals(10, order.getItems().size());
        order = null;
        itemList = null;
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = OrderFinder.findOne(OrderFinder.orderId().eq(2000));
        assertEquals(10, order.getItems().size());
    }

    private Order findOne(int orderId)
    {
        return OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
    }

    public void testPersistedOrderAddItem()
    {
        Order order = findOne(1);

        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        order.getItems().add(item);

        assertEquals(order.getOrderId(), item.getOrderId());
        assertSame(item, OrderItemFinder.findOneBypassCache(OrderItemFinder.id().eq(1234)));
    }

    public void testPersistedOrderSetStatus()
    {
        Order order = findOne(2);

        OrderStatus status = createStatus();
        order.setOrderStatus(status);

        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        assertEquals(order.getOrderId(), status.getOrderId());
        assertSame(status, OrderStatusFinder.findOneBypassCache(OrderStatusFinder.orderId().eq(2)));
    }

    public void testPersistedOrderSetStatusForExisting()
    {
        Order order = findOne(1);

        OrderStatus status = createStatus();
        order.setOrderStatus(status);

        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        assertEquals(order.getOrderId(), status.getOrderId());
        assertSame(status, OrderStatusFinder.findOneBypassCache(OrderStatusFinder.orderId().eq(1)));
    }

    public void testPersistedOrderSetStatusNull()
    {
        Order order = findOne(1);

        order.setOrderStatus(null);

        assertNull(order.getOrderStatus());

        assertNull(OrderStatusFinder.findOneBypassCache(OrderStatusFinder.orderId().eq(1)));
    }

    public void testPersistedOrderRemoveItem()
    {
        Order order = findOne(1);

        OrderItemList items = order.getItems();
        int oldSize = items.size();
        int oldId = items.getOrderItemAt(0).getId();
        items.remove(0);

        assertEquals(oldSize - 1, order.getItems().size());
        assertNull(OrderItemFinder.findOneBypassCache(OrderItemFinder.id().eq(oldId)));
    }

    public void testPersistedOrderSetOrderOnNewItem()
    {
        Order order = findOne(1);

        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        item.setOrder(order);

        assertEquals(order.getOrderId(), item.getOrderId());
        assertSame(item, OrderItemFinder.findOneBypassCache(OrderItemFinder.id().eq(1234)));

    }

    public void testPersistedOrderResetOrderItems()
    {
        Order order = findOne(2);

        OrderItemList items = order.getItems();
        int originalSize = items.size();
        assertTrue(originalSize > 2);
        OrderItemList newItems = new OrderItemList();

        OrderItem oldItem1 = (OrderItem)items.get(0);
        newItems.add(oldItem1);
        OrderItem oldItem2 = (OrderItem)items.get(1);
        newItems.add(oldItem2);

        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);

        newItems.add(item);

        order.setItems(newItems);

        OrderItemList persistedItems = order.getItems();
        assertEquals(3, persistedItems.size());
        assertTrue(persistedItems.contains(oldItem1));
        assertTrue(persistedItems.contains(oldItem2));
        assertTrue(persistedItems.contains(item));

        item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1235);

        newItems.add(item);

        persistedItems = order.getItems();
        assertEquals(4, persistedItems.size());
        assertTrue(persistedItems.contains(item));
    }

    public void testNonDependentRelationshipSetsAttributes()
    {
        OrderItem item = new OrderItem();
        item.setProductInfo(ProductFinder.findOne(ProductFinder.productId().eq(1)));
        assertEquals(1, item.getProductId());
    }

    public void testNonDependentRelationshipSetsAttributesForNull()
    {
        Book book = new Book();
        book.setManufacturer(null);
        assertTrue(book.isManufacturerIdNull());

        book = BookFinder.findOne(BookFinder.inventoryId().eq(1));
        assertTrue(book != null);
        book.setManufacturer(null);
        assertTrue(book.isManufacturerIdNull());
    }

    public void testNonDependentRelationshipSetsAttributesForPersisted()
    {
        OrderItem item = findOne(1).getItems().getOrderItemAt(0);
        item.setProductInfo(ProductFinder.findOne(ProductFinder.productId().eq(1)));
        item = findOne(1).getItems().getOrderItemAt(0);
        assertEquals(1, item.getProductId());
    }

    public void testNonDependentRelationshipIsRetrieved()
    {
        Book book = new Book();
        book.setManufacturerId(1);
        assertNotNull(book.getManufacturer());
        assertEquals(1, book.getManufacturer().getManufacturerId());
    }

    public void testDetachedAddItem()
    {
        Order order = findOne(1);

        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderItemList items = detachedOrder.getItems();
        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(2, order.getItems().size());
    }

    public void testDetachedAddItemThenRemoveIt()
    {
        Order order = findOne(1);

        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderItemList items = detachedOrder.getItems();
        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);

        item = new OrderItem();
        item.setDiscountPrice(18);
        item.setId(1235);
        items.add(item);
        items.remove(item);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(2, order.getItems().size());
    }

    public void testDetachedAddItemWithTransactionRetry()
    {
        Order order = findOne(1);

        final Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderItemList items = detachedOrder.getItems();
        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);

        copyOrder(detachedOrder);
        assertEquals(2, order.getItems().size());
    }

    public void testDetachedAddItemInTransaction()
    {
        Order order = findOne(1);

        final Order detachedOrder = order.getDetachedCopy();

        OrderItemList items = detachedOrder.getItems();
        OrderItem item = new OrderItem();
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);
        checkOrderOnItems(items, detachedOrder);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                detachedOrder.setDescription("new desc");
                detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
                return null;
            }
        });
        assertEquals(2, order.getItems().size());
    }

    public void testMultipleDetachedAddItemInTransaction() throws SQLException
    {
        final Order order = new Order();
        order.setDescription("test");
        order.setOrderId(2000);

        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);

        final Order order2 = new Order();
        order2.setDescription("test");
        order2.setOrderId(3000);

        OrderItemList itemList2 = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+3000);
            itemList2.add(item);
        }
        order2.setItems(itemList2);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.copyDetachedValuesToOriginalOrInsertIfNew();
                order2.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
    }

    public void testMultipleDetachedAddItemInTransaction2() throws SQLException
    {
        final Order order = new Order();
        order.setDescription("test");
        order.setOrderId(2000);

        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);
        }
        order.setItems(itemList);

        final Order order2 = new Order();
        order2.setDescription("test");
        order2.setOrderId(3000);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.copyDetachedValuesToOriginalOrInsertIfNew();
                order2.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
    }

    public void testDetachedRemoveItem()
    {
        Order order = findOne(2);
        int sizeBefore = order.getItems().size();
        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.getItems().remove(0);
        assertEquals(sizeBefore - 1, detachedOrder.getItems().size());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(sizeBefore - 1, order.getItems().size());
    }

    public void testDetachedUpdateItem()
    {
        Order order = findOne(1);
        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.getItems().getOrderItemAt(0).setDiscountPrice(1234.56);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(1234.56, order.getItems().getOrderItemAt(0).getDiscountPrice(), 0);
    }

    public void testDetachedMixed()
    {
        Order order = findOne(2);
        int sizeBefore = order.getItems().size();
        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderItemList items = detachedOrder.getItems();
        items.remove(0);
        OrderItem modifiedItem = items.getOrderItemAt(0);
        modifiedItem.setDiscountPrice(1234.56);
        int modifiedItemId = modifiedItem.getId();

        for(int i=0;i<5;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        for(int i=0;i<5;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(170+i);
            item.setId(12340+i);
            item.setOrder(detachedOrder);
        }
        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        OrderItemList persistedItems = order.getItems();
        assertEquals(sizeBefore-1+5+5, persistedItems.size());
        for(int i=0;i<persistedItems.size();i++)
        {
            if (persistedItems.getOrderItemAt(i).getId() == modifiedItemId)
            {
                assertEquals(1234.56, persistedItems.getOrderItemAt(i).getDiscountPrice(), 0);
            }
        }
    }

    public void testDetachedReplaceList()
    {
        Order order = findOne(2);
        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderItemList items = new OrderItemList();

        for(int i=0;i<5;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        OrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());
    }

    public void testDetachedReplaceListWithNewItem()
    {
        Order order = findOne(2);
        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");

        OrderItem newItem = new OrderItem();
        newItem.setDiscountPrice(17000);
        newItem.setId(1234000);
        OrderItemList detachedItems = detachedOrder.getItems();
        detachedItems.add(newItem);

        OrderItemList items = new OrderItemList();

        for(int i=0;i<5;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        OrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());
    }

    public void testDetachedReplaceMixDetachedIntoNewList()
    {
        Order order = findOne(2);
        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderItemList items = new OrderItemList();

        for(int i=0;i<5;i++)
        {
            OrderItem item = new OrderItem();
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        OrderItemList detachedList = detachedOrder.getItems();
        items.add(detachedList.get(0));
        items.add(detachedList.get(1));

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        OrderItemList persistedItems = order.getItems();
        assertEquals(7, persistedItems.size());
    }

    public void testDetachedSetStatusForExisting()
    {
        Order order = findOne(1);

        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderStatus status = createStatus();
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());
        assertEquals(1, status.getOrderId());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusNullForExisting()
    {
        Order order = findOne(1);

        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertNull(order.getOrderStatus());
    }

    public void testDetachedSetStatusNullAndBackForExisting()
    {
        Order order = findOne(1);

        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        OrderStatus status = createStatus();
        detachedOrder.setOrderStatus(status);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusForNonExisting()
    {
        Order order = findOne(2);

        Order detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        OrderStatus status = createStatus();
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertSame(status, detachedOrder.getOrderStatus());
        assertTrue(status.zIsDetached());
    }

    public void testSettingNonDependentRelationship()
    {
        OrderItem item = new OrderItem();
        Product product = new Product();
        item.setProductInfo(product);
        assertSame(product, item.getProductInfo());
    }

    public void testMultipleDeleteInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderFinder.findOne(OrderFinder.orderId().eq(1)).cascadeDelete();
                OrderFinder.findOne(OrderFinder.orderId().eq(4)).cascadeDelete();
                return null;
            }
        });
    }

    public void testMultipleDeleteInTransaction2()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderFinder.findOne(OrderFinder.orderId().eq(4)).cascadeDelete();
                OrderFinder.findOne(OrderFinder.orderId().eq(1)).cascadeDelete();
                return null;
            }
        });
    }

    public void testMultipleDeleteInTransaction3()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderFinder.findOne(OrderFinder.orderId().eq(1)).cascadeDelete();
                OrderFinder.findOne(OrderFinder.orderId().eq(2)).cascadeDelete();
                OrderFinder.findOne(OrderFinder.orderId().eq(3)).cascadeDelete();
                OrderFinder.findOne(OrderFinder.orderId().eq(4)).cascadeDelete();
                return null;
            }
        });
    }

    public void testNonDependentForeignKey() throws SQLException
    {
        this.createOrderItemProductForeignKey();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderItem item1 = new OrderItem();
                item1.setId(1000);
                item1.setOrderId(1);
                item1.setProductId(1);
                item1.insert();

                Product prod = new Product();
                prod.setProductId(2000);
                prod.insert();

                OrderItem item2 = new OrderItem();
                item2.setId(1001);
                item2.setOrderId(1);
                item2.setProductId(2000);
                item2.insert();

                return null;
            }
        });
        this.dropOrderItemProductForeignKey();
    }

    public void testNonDependentForeignKeyProdFirst() throws SQLException
    {
        this.createOrderItemProductForeignKey();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Product prod = new Product();
                prod.setProductId(2000);
                prod.insert();

                OrderItem item1 = new OrderItem();
                item1.setId(1000);
                item1.setOrderId(1);
                item1.setProductId(2000);
                item1.insert();

                prod = new Product();
                prod.setProductId(2001);
                prod.insert();

                return null;
            }
        });
        this.dropOrderItemProductForeignKey();
    }
}
