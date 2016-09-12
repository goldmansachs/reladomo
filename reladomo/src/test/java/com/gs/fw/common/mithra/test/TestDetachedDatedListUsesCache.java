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
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItem;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemStatus;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemStatusFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemWi;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatus;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatusWi;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderWi;
import com.gs.fw.common.mithra.util.StatisticCounter;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TestDetachedDatedListUsesCache extends MithraTestAbstract
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestDetachedDatedListUsesCache.class);

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
                BitemporalOrder.class,
                BitemporalOrderItem.class,
                BitemporalOrderItemStatus.class,
                BitemporalOrderStatus.class,
                BitemporalOrderWi.class,
                BitemporalOrderItemWi.class,
                BitemporalOrderStatusWi.class
        };
    }


    public void testDeletingAnItemDoesntInvalidateCache()
    {
        BitemporalOrderList orders = new BitemporalOrderList(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        orders.deepFetch(BitemporalOrderFinder.items().orderItemStatus());
        orders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());

        final BitemporalOrderList detachedOrders = orders.getDetachedCopy();
        for (BitemporalOrder detachedOrder : detachedOrders)
        {
            BitemporalOrderItemList items = detachedOrder.getItems();
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
        BitemporalOrderList orders = new BitemporalOrderList(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        orders.deepFetch(BitemporalOrderFinder.items().orderItemStatus());

        final BitemporalOrderList detachedOrders = orders.getDetachedCopy();
        for (BitemporalOrder detachedOrder : detachedOrders)
        {
            BitemporalOrderItemList items = detachedOrder.getItems();
            items.setOrderBy(BitemporalOrderItemFinder.id().ascendingOrderBy());
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
        BitemporalOrderList orders = new BitemporalOrderList(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        orders.deepFetch(BitemporalOrderFinder.items().orderItemStatus());
        orders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());

        final BitemporalOrderList detachedOrders = orders.getDetachedCopy();
        for (BitemporalOrder detachedOrder : detachedOrders)
        {
            BitemporalOrderItemList items = detachedOrder.getItems();
            items.setOrderBy(BitemporalOrderItemFinder.id().ascendingOrderBy());
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
        BitemporalOrderList orders = new BitemporalOrderList(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
        orders.deepFetch(BitemporalOrderFinder.items().orderItemStatus());
        orders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());

        final BitemporalOrderList detachedOrders = orders.getDetachedCopy();
        for (BitemporalOrder detachedOrder : detachedOrders)
        {
            BitemporalOrderItemList items = detachedOrder.getItems();
            items.setOrderBy(BitemporalOrderItemFinder.id().ascendingOrderBy());
            if (detachedOrder.getOrderId() == 2)
            {
                BitemporalOrderItemStatus status = new BitemporalOrderItemStatus(new Timestamp(System.currentTimeMillis()));
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


    private void saveDetachedOrdersInTransaction(final BitemporalOrderList detachedOrders)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            @Override
            public Object executeTransaction(final MithraTransaction tx)
            {
                detachedOrders.copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
                return null;
            }
        });
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
                        setName + ": " + numberDifferences + " element(s) different.";

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
     * Assert the size of the given {@link java.util.Collection}.
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

    private StatisticCounter newStat(int hits, int total)
    {
        return new TestStatCounter(hits, total);
    }

    private static class TestStatCounter extends StatisticCounter
    {
        private TestStatCounter(int hits, int total)
        {
            if (hits > total)
            {
                throw new IllegalArgumentException("hits (" + hits + ") cant be bigger than total (" + total + ")!");
            }
            this.hits = hits;
            this.total = total;
        }
    }


    public void testDeepFetchInTransactionWhenFullyCachedWithOptimisticLocking()
    {
        if (BitemporalOrderFinder.isFullCache()
                && BitemporalOrderItemFinder.isFullCache()
                && BitemporalOrderItemStatusFinder.isFullCache())
        {
            int dbCount = this.getRetrievalCount();

            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                @Override
                public Object executeTransaction(final MithraTransaction tx)
                {
                    BitemporalOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                    BitemporalOrderItemFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                    BitemporalOrderItemStatusFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

                    BitemporalOrderList orders = new BitemporalOrderList(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));
                    orders.deepFetch(BitemporalOrderFinder.items().orderItemStatus());
                    orders.addOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());
                    for (int i = 0, ordersSize = orders.size(); i < ordersSize; i++)
                    {
                        BitemporalOrder order = orders.get(i);
                        LOGGER.debug("Order: " + order.getOrderId());
                        BitemporalOrderItemList items = order.getItems();
                        items.addOrderBy(BitemporalOrderItemFinder.id().ascendingOrderBy());
                        for (int i1 = 0, itemsSize = items.size(); i1 < itemsSize; i1++)
                        {
                            BitemporalOrderItem item = items.get(i1);
                            LOGGER.debug("  \\--- item: " + item.getId());
                            if (item.getOrderItemStatus() != null)
                            {
                                LOGGER.debug("         \\--- status: " + item.getOrderItemStatus().getLastUser());
                            }
                        }
                    }
                    return null;
                }
            });

            assertEquals(3, this.getRetrievalCount() - dbCount);
        }
    }
}
