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

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.UpdateCountHolder;
import com.gs.fw.common.mithra.finder.UpdateCountHolderImpl;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderItemStatus;
import com.gs.fw.common.mithra.test.domain.OrderItemWi;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.OrderParentToChildren;
import com.gs.fw.common.mithra.test.domain.OrderStatus;
import com.gs.fw.common.mithra.test.domain.OrderStatusWi;
import com.gs.fw.common.mithra.test.domain.OrderWi;
import com.gs.fw.common.mithra.util.StatisticCounter;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TestDetachedListUsesCache extends MithraTestAbstract
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestDetachedListUsesCache.class);


    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
    }


    @Override
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderParentToChildren.class,
            OrderItem.class,
            OrderStatus.class,
            OrderWi.class,
            OrderItemWi.class,
            OrderStatusWi.class,
            OrderItemStatus.class
        };
    }


    public void testDeletingAnItemDoesntInvalidateCache()
    {
        OrderList orders = new OrderList(OrderFinder.all());
        orders.deepFetch(OrderFinder.items().orderItemStatus());
        orders.setOrderBy(OrderFinder.orderId().ascendingOrderBy());

        final OrderList detachedOrders = orders.getDetachedCopy();
        for (Order detachedOrder : detachedOrders)
        {
            OrderItemList items = detachedOrder.getItems();
            if (detachedOrder.getOrderId() == 1)
            {
                items.remove(0);
            }
        }

        int dbCount = this.getRetrievalCount();
        this.saveDetachedOrdersInTransaction(detachedOrders);
        assertEquals(3, this.getRetrievalCount() - dbCount);
    }


    public void testChangingOneStatusDoesntInvalidateCache()
    {
        OrderList orders = new OrderList(OrderFinder.all());
        orders.deepFetch(OrderFinder.items().orderItemStatus());

        final OrderList detachedOrders = orders.getDetachedCopy();
        for (Order detachedOrder : detachedOrders)
        {
            OrderItemList items = detachedOrder.getItems();
            items.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
            if (detachedOrder.getOrderId() == 1)
            {
                detachedOrder.getItems().get(0).getOrderItemStatus().setLastUser("Sheena");
            }
            else
            if (detachedOrder.getOrderId() > 50 && items.notEmpty())
            {
                items.get(0).getOrderItemStatus().setLastUser("Jim");
            }
        }

        int dbCount = this.getRetrievalCount();
        this.saveDetachedOrdersInTransaction(detachedOrders);
        assertEquals(3, this.getRetrievalCount() - dbCount);
    }


    public void testDeletingAnItemStatusDoesntInvalidateCache()
    {
        OrderList orders = new OrderList(OrderFinder.all());
        orders.deepFetch(OrderFinder.items().orderItemStatus());
        orders.setOrderBy(OrderFinder.orderId().ascendingOrderBy());

        final OrderList detachedOrders = orders.getDetachedCopy();
        for (Order detachedOrder : detachedOrders)
        {
            OrderItemList items = detachedOrder.getItems();
            items.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
            if (detachedOrder.getOrderId() == 1)
            {
                items.get(0).setOrderItemStatus(null);
            }
            else
            if (detachedOrder.getOrderId() > 50 && items.notEmpty())
            {
                items.get(0).getOrderItemStatus().setLastUser("Jim");
            }
        }

        int dbCount = this.getRetrievalCount();
        this.saveDetachedOrdersInTransaction(detachedOrders);
        assertEquals(3, this.getRetrievalCount() - dbCount);
    }


    public void testAddingAnItemStatusDoesntInvalidateCache()
    {
        OrderList orders = new OrderList(OrderFinder.all());
        orders.deepFetch(OrderFinder.items().orderItemStatus());
        orders.setOrderBy(OrderFinder.orderId().ascendingOrderBy());

        final OrderList detachedOrders = orders.getDetachedCopy();
        for (Order detachedOrder : detachedOrders)
        {
            OrderItemList items = detachedOrder.getItems();
            items.setOrderBy(OrderItemFinder.id().ascendingOrderBy());
            if (detachedOrder.getOrderId() == 2)
            {
                OrderItemStatus status = new OrderItemStatus();
                status.setStatus(42);
                status.setLastUser("Kylie");
                status.setLastUpdateTime(new Timestamp(System.currentTimeMillis()));
                items.get(0).setOrderItemStatus(status);
            }
            else
            if (detachedOrder.getOrderId() > 50 && items.notEmpty())
            {
                items.get(0).getOrderItemStatus().setLastUser("Jim");
            }
        }

        int dbCount = this.getRetrievalCount();
        this.saveDetachedOrdersInTransaction(detachedOrders);
        assertEquals(3, this.getRetrievalCount() - dbCount);
    }


    private void saveDetachedOrdersInTransaction(final OrderList detachedOrders)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                LOGGER.debug("----- start of transaction -----");
                detachedOrders.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
                LOGGER.debug("----- end of transaction -----");
                return null;
            }
        });
    }



    public void testUpdateCountHolderDetachedMode()
    {
        final UpdateCountHolder counter = new UpdateCountHolderImpl();

        assertEquals(100000000, counter.getNonTxUpdateCount());
        assertEquals(100000000, counter.getUpdateCount());

        counter.incrementUpdateCount();

        assertEquals(100000001, counter.getNonTxUpdateCount());
        assertEquals(100000001, counter.getUpdateCount());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                assertEquals(0, counter.getUpdateCount());

                counter.incrementUpdateCount();

                assertEquals(100000001, counter.getNonTxUpdateCount());
                assertEquals(1, counter.getUpdateCount());

                counter.setUpdateCountDetachedMode(true);

                assertEquals(1, counter.getUpdateCount());

                counter.incrementUpdateCount();
                counter.incrementUpdateCount();
                counter.incrementUpdateCount();

                assertEquals(1, counter.getUpdateCount());

                counter.setUpdateCountDetachedMode(false);

                assertEquals(2, counter.getUpdateCount());

                counter.setUpdateCountDetachedMode(true);

                assertEquals(2, counter.getUpdateCount());

                counter.setUpdateCountDetachedMode(false);

                assertEquals(2, counter.getUpdateCount());
                assertEquals(100000001, counter.getNonTxUpdateCount());

                counter.setUpdateCountDetachedMode(true);
                counter.incrementUpdateCount();
                counter.setUpdateCountDetachedMode(true);

                assertEquals(2, counter.getUpdateCount());

                counter.commitUpdateCount();

                assertEquals(0, counter.getUpdateCount());
                assertEquals(100000002, counter.getNonTxUpdateCount());

                return null;
            }
        });

        assertEquals(100000002, counter.getNonTxUpdateCount());
        assertEquals(100000002, counter.getUpdateCount());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                counter.incrementUpdateCount();
                counter.setUpdateCountDetachedMode(true);
                counter.incrementUpdateCount();

                assertEquals(1, counter.getUpdateCount());

                counter.rollbackUpdateCount();

                assertEquals(100000002, counter.getNonTxUpdateCount());
                assertEquals(0, counter.getUpdateCount());

                return null;
            }
        });

        assertEquals(100000002, counter.getNonTxUpdateCount());
        assertEquals(100000002, counter.getUpdateCount());
    }

    public void testStatisticCounter()
    {
        StatisticCounter stat = new StatisticCounter();

        stat.registerHit(false);
        assertEquals("StatisticCounter[hits=0; total=1; hitRate=0.0]", stat.toString());
        stat.registerHit(true);
        assertEquals("StatisticCounter[hits=1; total=2; hitRate=0.5]", stat.toString());

        stat = new StatisticCounter();
        stat.registerHit(true);
        assertEquals("StatisticCounter[hits=1; total=1; hitRate=1.0]", stat.toString());
    }


    // ===== Verify methods copied from caramel-testutils-4.4.1 =======================================================

    private static final int MAX_DIFFERENCES = 5;

    public static void assertSetsEqual(String setName, Set<?> expectedSet, Set<?> actualSet)
    {
        try
        {
            if (null == expectedSet)
            {
                Assert.assertNull(setName + " should be null", actualSet);
                return;
            }

            assertObjectNotNull(setName, actualSet);
            assertSize(setName, expectedSet.size(), actualSet);

            if (!actualSet.equals(expectedSet))
            {
                Set<?> inExpectedOnlySet = UnifiedSet.newSet(expectedSet);
                inExpectedOnlySet.removeAll(actualSet);

                int numberDifferences = inExpectedOnlySet.size();
                String message =
                        setName + ": " + numberDifferences + "element(s)"
                                + " different.";

                if (numberDifferences > MAX_DIFFERENCES)
                {
                    Assert.fail(message);
                }

                Set<?> inActualOnlySet = UnifiedSet.newSet(actualSet);
                inActualOnlySet.removeAll(expectedSet);

                failNotEquals(message, inExpectedOnlySet, inActualOnlySet);
            }
        }
        catch (AssertionError e)
        {
            throwMangledException(e);
        }
    }

    public static void assertMapsEqual(Map<?, ?> expectedMap, Map<?, ?> actualMap)
    {
        try
        {
            assertMapsEqual("map", expectedMap, actualMap);
        }
        catch (AssertionError e)
        {
            throwMangledException(e);
        }
    }

    public static void assertMapsEqual(String mapName, Map<?, ?> expectedMap, Map<?, ?> actualMap)
    {
        try
        {
            if (null == expectedMap)
            {
                Assert.assertNull(mapName + " should be null", actualMap);
                return;
            }

            Assert.assertNotNull(mapName + " should not be null", actualMap);

            assertSetsEqual(mapName + " keys", expectedMap.keySet(), actualMap.keySet());
            assertSetsEqual(mapName + " entries", expectedMap.entrySet(), actualMap.entrySet());
        }
        catch (AssertionError e)
        {
            throwMangledException(e);
        }
    }

    /**
     * Assert the size of the given {@link Collection}.
     */
    public static void assertSize(
            String collectionName,
            int expectedSize,
            Collection<?> actualCollection)
    {
        try
        {
            assertObjectNotNull(collectionName, actualCollection);

            int actualSize = actualCollection.size();
            if (actualSize != expectedSize)
            {
                Assert.fail("Incorrect size for "
                        + collectionName
                        + "; expected:<"
                        + expectedSize
                        + "> but was:<"
                        + actualSize
                        + '>');
            }
        }
        catch (AssertionError e)
        {
            throwMangledException(e);
        }
    }

    public static void assertObjectNotNull(String objectName, Object actualObject)
    {
        try
        {
            Assert.assertNotNull(objectName + " should not be null", actualObject);
        }
        catch (AssertionError e)
        {
            throwMangledException(e);
        }
    }

    public static void failNotEquals(String message, Object expected, Object actual)
    {
        Assert.fail(format(message, expected, actual));
    }

    public static String format(String message, Object expected, Object actual)
    {
        String formatted = "";
        if (message != null)
        {
            formatted = message + ' ';
        }
        return formatted + "expected:<" + expected + "> but was:<" + actual + '>';
    }

    public static void throwMangledException(AssertionError e)
    {
        /*
         * Note that we actually remove 3 frames from the stack trace because
         * we wrap the real method doing the work: e.fillInStackTrace() will
         * include us in the exceptions stack frame.
         */
        throw e;
    }
}
