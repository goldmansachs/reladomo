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

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class TestDatedDetachedRelationshipPersistence extends MithraTestAbstract
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Timestamp now = null;
    private Timestamp previousBusinessDate = null;
    private Timestamp businessDate = null;
    private Timestamp exclusiveUntil = null;

    public Class[] getRestrictedClassList()
    {
        return new Class[]{
                BitemporalOrder.class,
                BitemporalOrderItem.class,
                BitemporalOrderItemStatus.class,
                BitemporalOrderStatus.class,
                OrderWithAuditedStatus.class,
                AuditedOrderStatusWithNonDatedOrder.class,
                AuditedOrder.class,
                AuditedOrderItem.class,
                OrderItem.class,
                OrderItemStatus.class,
                AuditedOrderItemStatus.class,
                AuditedOrderStatus.class,
                AuditedOrderStatusTwo.class,
                EntityRestrictedItem.class,
                RestrictedEntity.class,
                MithraTestSequence.class
        };
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.initializeDates();
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
        now = null;
        previousBusinessDate = null;
        businessDate = null;
        exclusiveUntil = null;
    }

    private void initializeDates()
            throws Exception
    {
        now = new Timestamp(System.currentTimeMillis());
        previousBusinessDate = new Timestamp(timestampFormat.parse("2005-12-31 00:00:00").getTime());
        businessDate = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        exclusiveUntil = new Timestamp(timestampFormat.parse("2006-07-01 00:00:00").getTime());
    }

    public void testInsertSettingDependentsAtOnceBeforeId()
    {
        BitemporalOrder order = new BitemporalOrder(businessDate);
        order.setDescription("test");

        BitemporalOrderItemList itemList = new BitemporalOrderItemList();
        for(int i=0;i<10;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
            item.setDiscountPrice(i);
            item.setId(i+2000);
            itemList.add(item);

        }
        order.setItems(itemList);
        assertSame(itemList, order.getItems());
        assertEquals(10, order.getItems().size());
        checkOrderOnItems(itemList, order);

        BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
        item.setDiscountPrice(12);
        item.setId(3000);
        order.getItems().add(item);
        assertEquals(11, order.getItems().size());
        checkOrderOnItems(itemList, order);

        BitemporalOrderStatus status = createStatus();

        order.setOrderStatus(status);
        assertSame(order, status.getOrder());
        assertSame(status, order.getOrderStatus());

        order.setOrderId(2000);
        for(int i=0;i<itemList.size();i++)
        {
            item = itemList.getBitemporalOrderItemAt(i);
            assertEquals(2000, item.getOrderId());
        }
        assertEquals(2000, status.getOrderId());

        order.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil);
        assertEquals(11, order.getItems().size());
        order = null;
        itemList = null;
        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        order = BitemporalOrderFinder.findOne(BitemporalOrderFinder.orderId().eq(2000).and(BitemporalOrderFinder.businessDate().eq(businessDate)));
        assertEquals(11, order.getItems().size());
        BitemporalOrderItemList bitemporalOrderItemList = new BitemporalOrderItemList(
                BitemporalOrderItemFinder.orderId().eq(2000).and(BitemporalOrderItemFinder.businessDate().eq(businessDate)));
        bitemporalOrderItemList.setBypassCache(true);
        assertEquals(11, bitemporalOrderItemList.size());
        assertNotNull(BitemporalOrderStatusFinder.findOneBypassCache(
                BitemporalOrderStatusFinder.orderId().eq(2000).and(BitemporalOrderStatusFinder.businessDate().eq(businessDate))));
    }

    private BitemporalOrderStatus createStatus()
    {
        BitemporalOrderStatus status = new BitemporalOrderStatus(businessDate);
        status.setStatus(10);
        status.setLastUser("wilma");
        return status;
    }

    public void testDatedDetachedAddItem()
    {
        BitemporalOrder order = findOrder(1, now);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = detachedOrder.getItems();
        BitemporalOrderItem item = new BitemporalOrderItem(now);
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(2, order.getItems().size());
    }

    public void testDatedDetachedAddItemInTransaction()
    {
        BitemporalOrder order = findOrder(1, now);

        final BitemporalOrder detachedOrder = order.getDetachedCopy();

        BitemporalOrderItemList items = detachedOrder.getItems();
        BitemporalOrderItem item = new BitemporalOrderItem(now);
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

    public void testDatedDetachedRemoveItem()
    {
        BitemporalOrder order = findOrder(2, now);
        int sizeBefore = order.getItems().size();
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.getItems().remove(0);
        assertEquals(sizeBefore - 1, detachedOrder.getItems().size());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(sizeBefore - 1, order.getItems().size());
    }

    public void testDatedDetachedUpdateItem()
    {
        BitemporalOrder order = findOrder(1, now);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.getItems().getBitemporalOrderItemAt(0).setDiscountPrice(1234.56);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals(1234.56, order.getItems().getBitemporalOrderItemAt(0).getDiscountPrice(), 0);
    }

    public void testDatedDetachedMixed()
    {
        BitemporalOrder order = findOrder(2, now);
        int sizeBefore = order.getItems().size();
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = detachedOrder.getItems();
        items.remove(0);
        BitemporalOrderItem modifiedItem = items.getBitemporalOrderItemAt(0);
        modifiedItem.setDiscountPrice(1234.56);
        int modifiedItemId = modifiedItem.getId();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(now);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(now);
            item.setDiscountPrice(170+i);
            item.setId(12340+i);
            item.setOrder(detachedOrder);
        }
        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(sizeBefore-1+5+5, persistedItems.size());
        for(int i=0;i<persistedItems.size();i++)
        {
            if (persistedItems.getBitemporalOrderItemAt(i).getId() == modifiedItemId)
            {
                assertEquals(1234.56, persistedItems.getBitemporalOrderItemAt(i).getDiscountPrice(), 0);
            }
        }
    }

    public void testDetachedReplaceList()
    {
        BitemporalOrder order = findOrder(2, now);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = new BitemporalOrderItemList();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(now);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());
    }

    public void testDetachedReplaceListWithNewItem()
    {
        BitemporalOrder order = findOrder(2, now);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");

        BitemporalOrderItem newItem = new BitemporalOrderItem(now);
        newItem.setDiscountPrice(17000);
        newItem.setId(1234000);
        BitemporalOrderItemList detachedItems = detachedOrder.getItems();
        detachedItems.add(newItem);

        BitemporalOrderItemList items = new BitemporalOrderItemList();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(now);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());
    }

    public void testDetachedReplaceMixDetachedIntoNewList()
    {
        BitemporalOrder order = findOrder(2, now);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = new BitemporalOrderItemList();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(now);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        BitemporalOrderItemList detachedList = detachedOrder.getItems();
        items.add(detachedList.get(0));
        items.add(detachedList.get(1));

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(7, persistedItems.size());
    }

    public void testDetachedSetStatusForExisting()
            throws Exception
    {
        BitemporalOrder order = findOrder(1, now);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderStatus status = createStatus(now);
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());
        assertEquals(1, status.getOrderId());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusNullForExisting()
    {
        BitemporalOrder order = findOrder(1, now);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertNull(order.getOrderStatus());
    }

    public void testDetachedSetStatusNullAndBackForExisting()
    {
        BitemporalOrder order = findOrder(1, now);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        BitemporalOrderStatus status = createStatus(now);
        detachedOrder.setOrderStatus(status);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusForNonExisting()
    {
        BitemporalOrder order = findOrder(2, now);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderStatus status = createStatus(now);
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew(); // should deal with all dependent relationships
        assertSame(status, detachedOrder.getOrderStatus());
        assertTrue(status.zIsDetached());
        assertNotSame(status, order.getOrderStatus());
    }

    //exclusiveUntil
    public void testDetachedAddItemUntil() throws ParseException
    {
        BitemporalOrder order = findOrder(1, businessDate);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = detachedOrder.getItems();
        BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertEquals(2, order.getItems().size());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        BitemporalOrder order1 = findOrder(1, businessDate);
        assertEquals(2,order1.getItems().size());

        BitemporalOrder order2 = findOrder(1, previousBusinessDate);
        assertEquals(1,order2.getItems().size());

        BitemporalOrder order3 = findOrder(1, now);
        assertEquals(1,order3.getItems().size());

    }

    public void testDetachedAddItemInTransactionUntil() throws Exception
    {
        BitemporalOrder order = findOrder(1, businessDate);

        final BitemporalOrder detachedOrder = order.getDetachedCopy();

        BitemporalOrderItemList items = detachedOrder.getItems();
        BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
        item.setDiscountPrice(17);
        item.setId(1234);
        items.add(item);
        checkOrderOnItems(items, detachedOrder);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                detachedOrder.setDescription("new desc");
                detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
                return null;
            }
        });
        assertEquals(2, order.getItems().size());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        BitemporalOrder order1 = findOrder(1, businessDate);
        assertEquals(2,order1.getItems().size());

        BitemporalOrder order2 = findOrder(1, previousBusinessDate);
        assertEquals(1,order2.getItems().size());

        BitemporalOrder order3 = findOrder(1, now);
        assertEquals(1,order3.getItems().size());
    }

    public void testDetachedRemoveItemUntil()
            throws Exception
    {
        BitemporalOrder order = findOrder(2, businessDate);
        int sizeBefore = order.getItems().size();
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.getItems().remove(0);
        assertEquals(sizeBefore - 1, detachedOrder.getItems().size());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertEquals(sizeBefore - 1, order.getItems().size());
        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        assertEquals(sizeBefore - 1, findOrder(2, businessDate).getItems().size());
        assertEquals(sizeBefore - 1, findOrder(2, now).getItems().size());
        assertEquals(sizeBefore, findOrder(2, previousBusinessDate).getItems().size());
    }

    public void testDetachedUpdateItemUntil()
            throws Exception
    {
        BitemporalOrder order = findOrder(1, businessDate);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.getItems().getBitemporalOrderItemAt(0).setDiscountPrice(1234.56);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertEquals(1234.56, order.getItems().getBitemporalOrderItemAt(0).getDiscountPrice(), 0);

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        assertEquals(1234.56, findOrder(1, businessDate).getItems().getBitemporalOrderItemAt(0).getDiscountPrice(), 0);
        assertEquals(10.5, findOrder(1, previousBusinessDate).getItems().getBitemporalOrderItemAt(0).getDiscountPrice(), 0);
        assertEquals(10.5, findOrder(1, now).getItems().getBitemporalOrderItemAt(0).getDiscountPrice(), 0);
    }

    public void testDetachedMixedUntil()
            throws Exception
    {

        BitemporalOrder order = findOrder(2, businessDate);
        int sizeBefore = order.getItems().size();
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = detachedOrder.getItems();
        items.remove(0);
        BitemporalOrderItem modifiedItem = items.getBitemporalOrderItemAt(0);
        double originalDiscountPrice = modifiedItem.getDiscountPrice();
        modifiedItem.setDiscountPrice(1234.56);
        int modifiedItemId = modifiedItem.getId();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
            item.setDiscountPrice(170+i);
            item.setId(12340+i);
            item.setOrder(detachedOrder);
        }
        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        //Original List
        BitemporalOrderItemList persistedItems1 = findOrder(2, previousBusinessDate).getItems();
        assertEquals(sizeBefore, persistedItems1.size());
        for(int i=0;i<persistedItems1.size();i++)
        {
            if (persistedItems1.getBitemporalOrderItemAt(i).getId() == modifiedItemId)
            {
                assertEquals(originalDiscountPrice, persistedItems1.getBitemporalOrderItemAt(i).getDiscountPrice(), 0);
            }
        }

        //Original List - 1 terminated + 10 inserted
        BitemporalOrderItemList persistedItems0 = findOrder(2, businessDate).getItems();
        assertEquals(sizeBefore-1+5+5, persistedItems0.size());
        for(int i=0;i<persistedItems0.size();i++)
        {
            if (persistedItems0.getBitemporalOrderItemAt(i).getId() == modifiedItemId)
            {
                assertEquals(1234.56, persistedItems0.getBitemporalOrderItemAt(i).getDiscountPrice(), 0);
            }
        }
        //OriginalList - 1 terminated
        BitemporalOrderItemList persistedItems2 = findOrder(2, now).getItems();
        assertEquals(sizeBefore - 1, persistedItems2.size());
        for(int i=0;i<persistedItems2.size();i++)
        {
            if (persistedItems2.getBitemporalOrderItemAt(i).getId() == modifiedItemId)
            {
                assertEquals(originalDiscountPrice, persistedItems2.getBitemporalOrderItemAt(i).getDiscountPrice(), 0);
            }
        }
    }

    public void testDetachedReplaceListUntil()
    {
        BitemporalOrder order = findOrder(2, businessDate);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = new BitemporalOrderItemList();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        assertEquals(5, findOrder(2, businessDate).getItems().size());
        assertEquals(3, findOrder(2, previousBusinessDate).getItems().size());
        assertEquals(0, findOrder(2, now).getItems().size());

    }

    public void testDetachedReplaceListWithNewItemUntil()
    {
        BitemporalOrder order = findOrder(2, businessDate);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");

        BitemporalOrderItem newItem = new BitemporalOrderItem(businessDate);
        newItem.setDiscountPrice(17000);
        newItem.setId(1234000);
        BitemporalOrderItemList detachedItems = detachedOrder.getItems();
        detachedItems.add(newItem);

        BitemporalOrderItemList items = new BitemporalOrderItemList();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(5, persistedItems.size());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        assertEquals(3, findOrder(2, previousBusinessDate).getItems().size());
        assertEquals(5, findOrder(2, businessDate).getItems().size());
        assertEquals(0, findOrder(2, now).getItems().size());


    }

    public void testDetachedReplaceMixDetachedIntoNewListUntil()
    {
        BitemporalOrder order = findOrder(2, businessDate);
        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderItemList items = new BitemporalOrderItemList();

        for(int i=0;i<5;i++)
        {
            BitemporalOrderItem item = new BitemporalOrderItem(businessDate);
            item.setDiscountPrice(17+i);
            item.setId(1234+i);
            items.add(item);
        }

        BitemporalOrderItemList detachedList = detachedOrder.getItems();
        items.add(detachedList.get(0));
        items.add(detachedList.get(1));

        detachedOrder.setItems(items);

        checkOrderOnItems(items, detachedOrder);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        BitemporalOrderItemList persistedItems = order.getItems();
        assertEquals(7, persistedItems.size());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderItemFinder.clearQueryCache();

        assertEquals(3, findOrder(2, previousBusinessDate).getItems().size());
        assertEquals(7, findOrder(2, businessDate).getItems().size());
        assertEquals(2, findOrder(2, now).getItems().size());
    }

    public void testDetachedSetStatusForExistingUntil()
    {
        BitemporalOrder order = findOrder(1, businessDate);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderStatus status = createStatus(businessDate);
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());
        assertEquals(1, status.getOrderId());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderStatusFinder.clearQueryCache();

        assertEquals("First order", findOrder(1, previousBusinessDate).getDescription());
        assertEquals("Fred", findOrder(1, previousBusinessDate).getOrderStatus().getLastUser());

        assertEquals("new desc", findOrder(1, businessDate).getDescription());
        assertEquals("wilma", findOrder(1, businessDate).getOrderStatus().getLastUser());

        assertEquals("First order", findOrder(1, now).getDescription());
        assertEquals("Fred", findOrder(1, now).getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusNullForExistingUntil()
    {
        BitemporalOrder order = findOrder(1, businessDate);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertNull(order.getOrderStatus());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderStatusFinder.clearQueryCache();

        assertEquals("First order", findOrder(1, previousBusinessDate).getDescription());
        assertEquals("Fred", findOrder(1, previousBusinessDate).getOrderStatus().getLastUser());

        assertEquals("new desc", findOrder(1, businessDate).getDescription());
        assertNull(findOrder(1, businessDate).getOrderStatus());

        assertEquals("First order", findOrder(1, now).getDescription());
        assertNull(findOrder(1, now).getOrderStatus());
    }

    public void testDetachedSetStatusNullAndBackForExistingUntil()
    {
        BitemporalOrder order = findOrder(1, businessDate);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        detachedOrder.setOrderStatus(null);

        assertNull(detachedOrder.getOrderStatus());

        BitemporalOrderStatus status = createStatus(businessDate);
        detachedOrder.setOrderStatus(status);

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertEquals("wilma", order.getOrderStatus().getLastUser());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderStatusFinder.clearQueryCache();

        assertEquals("First order", findOrder(1, previousBusinessDate).getDescription());
        assertEquals("Fred", findOrder(1, previousBusinessDate).getOrderStatus().getLastUser());

        assertEquals("new desc", findOrder(1, businessDate).getDescription());
        assertEquals("wilma", findOrder(1, businessDate).getOrderStatus().getLastUser());

        assertEquals("First order", findOrder(1, now).getDescription());
        assertEquals("Fred", findOrder(1, now).getOrderStatus().getLastUser());
    }

    public void testDetachedSetStatusForNonExistingUntil()
    {
        BitemporalOrder order = findOrder(2, businessDate);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescription("new desc");
        BitemporalOrderStatus status = createStatus(businessDate);
        detachedOrder.setOrderStatus(status);

        assertSame(detachedOrder, status.getOrder());
        assertSame(status, detachedOrder.getOrderStatus());

        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNewUntil(exclusiveUntil); // should deal with all dependent relationships
        assertSame(status, order.getOrderStatus());

        BitemporalOrderFinder.clearQueryCache();
        BitemporalOrderStatusFinder.clearQueryCache();

        assertEquals("Second order", findOrder(2, previousBusinessDate).getDescription());
        assertNull(findOrder(2, previousBusinessDate).getOrderStatus());

        assertEquals("new desc", findOrder(2, businessDate).getDescription());
        assertEquals("wilma", findOrder(2, businessDate).getOrderStatus().getLastUser());

        assertEquals("Second order", findOrder(2, now).getDescription());
        assertNull(findOrder(2, now).getOrderStatus());

    }

    public void testGeneratedSequenceInRelationship()
    {
        EntityRestrictedItem entityRestrictedItem = new EntityRestrictedItem(businessDate);
        entityRestrictedItem.setUpdateBy("moh");
        RestrictedEntity restrictedEntity = new RestrictedEntity(businessDate);
        restrictedEntity.setGsClientFlg("x");
        entityRestrictedItem.setRestrictedEntity(restrictedEntity);
        entityRestrictedItem.cascadeInsert();
        assertEquals(entityRestrictedItem.getRestrictedItemId(), restrictedEntity.getRestrictedItemId());
    }

    private BitemporalOrder findOrder(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderFinder.
                findOne(BitemporalOrderFinder.orderId().eq(orderId).
                        and(BitemporalOrderFinder.businessDate().eq(businessDate)));
    }

    private void checkOrderOnItems(BitemporalOrderItemList itemList, BitemporalOrder order)
    {
        for(int i=0;i<itemList.size();i++)
        {
            BitemporalOrderItem item = itemList.getBitemporalOrderItemAt(i);
            assertSame(order, item.getOrder());
        }
    }

    private BitemporalOrderStatus createStatus(Timestamp busisnessDate)
    {
        BitemporalOrderStatus status = new BitemporalOrderStatus(busisnessDate);
        status.setStatus(10);
        status.setLastUser("wilma");
        return status;
    }

    public void testInPlaceUpdateAndTerminateTopLevel()
    {
        BitemporalOrder order = findOrder(2, businessDate);

        BitemporalOrder detachedOrder = order.getDetachedCopy();

        detachedOrder.setDescriptionUsingInPlaceUpdate("before terminate");
        detachedOrder.terminate();
        detachedOrder.copyDetachedValuesToOriginalOrInsertIfNew();

        BitemporalOrder oldOrder = BitemporalOrderFinder.
                findOneBypassCache(BitemporalOrderFinder.orderId().eq(2).
                        and(BitemporalOrderFinder.businessDate().eq(businessDate)).and(BitemporalOrderFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000))));

        assertEquals("before terminate", oldOrder.getDescription());
    }

    public void testCascadeInPlaceUpdateAndTerminate()
    {
        AuditedOrder order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1)).getDetachedCopy();

        AuditedOrderItem orderItem = order.getItems().get(0);
        int firstOrderItemId = orderItem.getId();
        orderItem.setDiscountPriceUsingInPlaceUpdate(17.8);

        order.getOrderStatus().setLastUpdateTimeUsingInPlaceUpdate(new Timestamp(1234));

        order.terminate();
        order.copyDetachedValuesToOriginalOrInsertIfNew();

        AuditedOrderItem oldItem = AuditedOrderItemFinder.findOneBypassCache(AuditedOrderItemFinder.id().eq(firstOrderItemId).and(AuditedOrderItemFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 10000))));
        assertEquals(17.8, oldItem.getDiscountPrice(), 0.0);

        AuditedOrderStatus oldStatus = AuditedOrderStatusFinder.findOneBypassCache(AuditedOrderStatusFinder.orderId().eq(1).and(AuditedOrderStatusFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 10000))));
        assertEquals(new Timestamp(1234), oldStatus.getLastUpdateTime());
    }

    public void testInPlaceUpdateAndTerminateOfRelated()
    {
        AuditedOrder order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(2)).getDetachedCopy();

        AuditedOrderItem orderItem = order.getItems().get(0);
        int firstOrderItemId = orderItem.getId();
        orderItem.setDiscountPriceUsingInPlaceUpdate(17.8);
        order.getItems().remove(0);

        order.copyDetachedValuesToOriginalOrInsertIfNew();

        AuditedOrderItem oldItem = AuditedOrderItemFinder.findOneBypassCache(AuditedOrderItemFinder.id().eq(firstOrderItemId).and(AuditedOrderItemFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 10000))));
        assertEquals(17.8, oldItem.getDiscountPrice(), 0.0);
    }

    public void testInPlaceUpdateAndTerminateOfRelatedToOne()
    {
        AuditedOrder order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1)).getDetachedCopy();

        order.getOrderStatus().setLastUpdateTimeUsingInPlaceUpdate(new Timestamp(1234));
        order.setOrderStatus(null);

        order.copyDetachedValuesToOriginalOrInsertIfNew();

        AuditedOrderStatus oldStatus = AuditedOrderStatusFinder.findOneBypassCache(AuditedOrderStatusFinder.orderId().eq(1).and(AuditedOrderStatusFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 10000))));
        assertEquals(new Timestamp(1234), oldStatus.getLastUpdateTime());
    }

    public void testInPlaceUpdateAndTerminateOfRelatedToOneFromNonDated()
    {
        OrderWithAuditedStatus order = OrderWithAuditedStatusFinder.findOne(OrderWithAuditedStatusFinder.orderId().eq(1)).getDetachedCopy();

        order.getOrderStatus().setLastUpdateTimeUsingInPlaceUpdate(new Timestamp(1234));
        order.setOrderStatus(null);

        order.copyDetachedValuesToOriginalOrInsertIfNew();

        AuditedOrderStatusWithNonDatedOrder oldStatus = AuditedOrderStatusWithNonDatedOrderFinder.findOneBypassCache(AuditedOrderStatusWithNonDatedOrderFinder.orderId().eq(1).and(AuditedOrderStatusWithNonDatedOrderFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 10000))));
        assertEquals(new Timestamp(1234), oldStatus.getLastUpdateTime());
    }
}
