/*
 Copyright 2019 Goldman Sachs.
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

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.superclassimpl.MithraTransactionalObjectImpl;
import com.gs.fw.common.mithra.test.domain.*;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestNotificationDuringDeepFetch extends MithraTestAbstract
{
    private static final Logger logger = LoggerFactory.getLogger(TestNotificationDuringDeepFetch.class.getName());

    private MithraTestResource mithraTestResource;

    private enum TimingTestCase
    {
        WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH,
        WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE,
        WAIT_DURING_CHILD_CACHE_SIMPLIFIED_RESULT,
        WAIT_DURING_CHILD_CACHE_RESULTS_MAP,
        WAIT_BEFORE_CHILD_DEEP_FETCH
    }

    private Semaphore signalWaitingForUpdate;
    private Semaphore signalDoneUpdating;

    protected void setUp() throws Exception
    {
        // Necessary to call parent setUp() as the runtime requires connection managers to be registered for all classes in the runtime config.
        super.setUp();

        // Here we override the test data we care about for this test.
        // Modifying the original test data would have impacted a large number of existing tests.
        mithraTestResource = buildMithraTestResource();
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "A", MITHRA_TEST_DATA_FILE_PATH + "testNotificationDuringDeepFetch.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();

        resetTestHarnessModifications();
        signalWaitingForUpdate = new Semaphore(0);
        signalDoneUpdating = new Semaphore(0);
    }

    private void resetTestHarnessModifications()
    {
        resetDeepFetchRelationship(OrderFinder.items());
        resetDeepFetchRelationship(OrderFinder.orderStatus());
        DeepRelationshipUtility.resetMaxSimplifiedIn();
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        resetTestHarnessModifications();
    }

    public void testDeepFetch_OneToMany_NotificationBetweenChildFetchAndCache_SimplifiedOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, false, false);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenChildFetchAndCache_ComplexOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, true, false);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenChildFetchAndCache_BypassCacheInPriorQuery_SimplifiedOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, false, true);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenChildFetchAndCache_BypassCacheInPriorQuery_ComplexOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, true, true);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenParentFetchAndChildFetch_SimplifiedOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, false, false);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenParentFetchAndChildFetch_ComplexOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, true, false);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenParentFetchAndChildFetch_BypassCacheInPriorQuery_SimplifiedOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, false, true);
    }

    public void testDeepFetch_OneToMany_NotificationBetweenParentFetchAndChildFetch_BypassCacheInPriorQuery_ComplexOp() throws Exception
    {
        runDeepFetchOneToManyTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, true, true);
    }

    private void runDeepFetchOneToManyTest(TimingTestCase timingTestCase, boolean forceComplexOp, boolean bypassCacheInPriorQuery) throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        forceComplexOp(forceComplexOp);

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = OrderFinder.findMany(op);
        orderList.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        final Function<Order, Iterable<OrderItem>> getOrderItemsOfOrder = new Function<Order, Iterable<OrderItem>>()
        {
            @Override
            public Iterable<OrderItem> valueOf(Order order)
            {
                return order.getItems();
            }
        };

        final Predicate<OrderItem> orderItemStateEqualsInProgress = new Predicate<OrderItem>()
        {
            @Override
            public boolean accept(OrderItem each)
            {
                return "In-Progress".equals(each.getState());
            }
        };

        allowAllFetches();

        Assert.assertEquals(4, orderList.size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(4, orderList.getItems().size());
        Assert.assertEquals(4, orderList.getItems().asEcList().count(orderItemStateEqualsInProgress));

        // This update and notification gives the findMany a reason to have to hit the database, as it marks the cache as dirty.
        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
        simulateOrderUpdateNotification(55, "state");

        makeFetchesWaitForSignal();

        // Simulate a database update and notification message occurring during the deep fetch
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 56");
                executeAndAssertSqlUpdate(1, "update APP.ORDER_ITEM set STATE = 'In-Progress' where ID = 6 and ORDER_ID = 55");

                simulateOrderUpdateNotification(56, "state");
                simulateOrderItemUpdateNotification(6, "state");

                signalUpdateThreadDone(); // allow the deep fetch to complete
            }
        });
        thread.start();

        OrderList orderList2 = OrderFinder.findMany(op);
        orderList2.setBypassCache(bypassCacheInPriorQuery);
        orderList2.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        logger.info("Performing deep fetch retrieval with concurrent database update and notification");

        // The next call will resolve the deep fetch using the deep fetch strategy.
        // The harnessed version of the deep fetch strategy coordinates with the update thread,
        // to ensure the database updates are applied after the parent list is resolved but before the deep fetch was completed.
        Assert.assertEquals(5, orderList2.size());
        Assert.assertEquals(6, orderList2.asEcList().flatCollect(getOrderItemsOfOrder).size());

        // This result includes OrderItem id 6 because Order.getItems() does a new finder query which hits the database due to the stale query cache.
        // It gives the correct results but there is scope to optimise the performance if we could adopt a more sophisticated strategy.
        Assert.assertEquals(6, orderList2.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));

        // Do not query orderList2.getItems() here or else it will re-fetch from database and that will mean the next deep fetch will hit the cache instead of the DB.
        // The next part of the test is testing that the deep fetch hits the database to retrieve the updated Order 56, which triggers a re-fetch of OrderItem.

        thread.join();

        allowAllFetches();

        // Should now reflect all updates to date as this is a brand new query
        OrderList orderList3 = OrderFinder.findMany(op);
        orderList3.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        logger.info("Performing final deep fetch retrieval");

        final int retrievalCountBeforeFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        Assert.assertEquals(6, orderList3.size());

        Assert.assertEquals(8, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(7, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));
        Assert.assertEquals(8, orderList3.getItems().size());
        Assert.assertEquals(7, orderList3.getItems().asEcList().count(orderItemStateEqualsInProgress));

        final int retrievalCountAfterFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        final int retrievalCountDuringFinalFetch = retrievalCountAfterFinalFetch - retrievalCountBeforeFinalFetch;
        Assert.assertTrue("Executed too many database retrievals: " + retrievalCountDuringFinalFetch, retrievalCountDuringFinalFetch <= 2);
    }

    public void testDeepFetch_OneToMany_NotificationDuringChildCacheSimplifiedResult() throws Exception
    {
        runDeepFetchOneToManyTest_NotificationDuringChildCacheSimplifiedResult(false);
    }

    public void testDeepFetch_OneToMany_NotificationDuringChildCacheSimplifiedResult_BypassCacheInPriorQuery() throws Exception
    {
        runDeepFetchOneToManyTest_NotificationDuringChildCacheSimplifiedResult(true);
    }

    private void runDeepFetchOneToManyTest_NotificationDuringChildCacheSimplifiedResult(boolean bypassCacheInPriorQuery) throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        TimingTestCase timingTestCase = TimingTestCase.WAIT_DURING_CHILD_CACHE_SIMPLIFIED_RESULT;
        forceComplexOp(false);

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = OrderFinder.findMany(op);
        orderList.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        final Function<Order, Iterable<OrderItem>> getOrderItemsOfOrder = new Function<Order, Iterable<OrderItem>>()
        {
            @Override
            public Iterable<OrderItem> valueOf(Order order)
            {
                return order.getItems();
            }
        };

        final Predicate<OrderItem> orderItemStateEqualsInProgress = new Predicate<OrderItem>()
        {
            @Override
            public boolean accept(OrderItem each)
            {
                return "In-Progress".equals(each.getState());
            }
        };

        allowAllFetches();

        Assert.assertEquals(4, orderList.size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(4, orderList.getItems().size());
        Assert.assertEquals(4, orderList.getItems().asEcList().count(orderItemStateEqualsInProgress));

        // This update and notification gives the findMany a reason to have to hit the database for both tables, as it marks the cache as dirty.
        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
        simulateOrderUpdateNotification(55, "state");
        executeAndAssertSqlUpdate(1, "update APP.ORDER_ITEM set STATE = 'Something Else' where ID = 6 and ORDER_ID = 55");
        simulateOrderItemUpdateNotification(6, "state");

        makeFetchesWaitForSignal();

        // Simulate a database update and notification message occurring during the deep fetch - on the related table only
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                executeAndAssertSqlUpdate(1, "update APP.ORDER_ITEM set STATE = 'In-Progress' where ID = 6 and ORDER_ID = 55");
                simulateOrderItemUpdateNotification(6, "state");

                signalUpdateThreadDone(); // allow the deep fetch to complete
            }
        });
        thread.start();

        OrderList orderList2 = OrderFinder.findMany(op);
        orderList2.setBypassCache(bypassCacheInPriorQuery);
        orderList2.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        logger.info("Performing deep fetch retrieval with concurrent database update and notification");

        // The next call will resolve the deep fetch using the deep fetch strategy.
        // The harnessed version of the deep fetch strategy coordinates with the update thread,
        // to ensure the database updates are applied after the parent list is resolved but before the deep fetch was completed.
        Assert.assertEquals(5, orderList2.size());
        Assert.assertEquals(6, orderList2.getItems().size());

        Assert.assertEquals(6, orderList2.getItems().asEcList().count(orderItemStateEqualsInProgress));

        // The next part of the test is testing that the next deep fetch hits the database to retrieve the updated OrderItem 6.

        thread.join();

        allowAllFetches();

        // Should now reflect all updates to date as this is a brand new query
        OrderList orderList3 = OrderFinder.findMany(op);
        orderList3.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        logger.info("Performing final deep fetch retrieval");

        final int retrievalCountBeforeFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        Assert.assertEquals(5, orderList3.size());

        Assert.assertEquals(6, orderList3.getItems().size());
        Assert.assertEquals(6, orderList3.getItems().asEcList().count(orderItemStateEqualsInProgress));
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));

        final int retrievalCountAfterFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        final int retrievalCountDuringFinalFetch = retrievalCountAfterFinalFetch - retrievalCountBeforeFinalFetch;
        Assert.assertTrue("Executed too many database retrievals: " + retrievalCountDuringFinalFetch, retrievalCountDuringFinalFetch <= 2);
    }

    public void testDeepFetch_OneToMany_SingleItemRelationshipCaching_NotificationDuringCache_FetchInMemory() throws Exception
    {
        runDeepFetchOneToManyTest_SingleItemRelationshipCaching_NotificationDuringCache(true);
    }

    public void testDeepFetch_OneToMany_SingleItemRelationshipCaching_NotificationDuringCache_FetchFromServer() throws Exception
    {
        runDeepFetchOneToManyTest_SingleItemRelationshipCaching_NotificationDuringCache(false);
    }

    public void runDeepFetchOneToManyTest_SingleItemRelationshipCaching_NotificationDuringCache(boolean inMemory) throws Exception
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        TimingTestCase timingTestCase = TimingTestCase.WAIT_DURING_CHILD_CACHE_RESULTS_MAP;
        forceComplexOp(false);

        // Set up additional test data before the first deep fetch
        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
        simulateOrderUpdateNotification(55, "state");

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = OrderFinder.findMany(op);
        orderList.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        final Function<Order, Iterable<OrderItem>> getOrderItemsOfOrder = new Function<Order, Iterable<OrderItem>>()
        {
            @Override
            public Iterable<OrderItem> valueOf(Order order)
            {
                return order.getItems();
            }
        };

        final Predicate<OrderItem> orderItemStateEqualsInProgress = new Predicate<OrderItem>()
        {
            @Override
            public boolean accept(OrderItem each)
            {
                return "In-Progress".equals(each.getState());
            }
        };

        allowAllFetches();

        Assert.assertEquals(5, orderList.size());
        Assert.assertEquals(6, orderList.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(5, orderList.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));
        Assert.assertEquals(6, orderList.getItems().size());
        Assert.assertEquals(5, orderList.getItems().asEcList().count(orderItemStateEqualsInProgress));

        if (!inMemory)
        {
            // This update and notification gives the deep fetch a reason to use deepFetchToManyFromServer
            executeAndAssertSqlUpdate(1, "update APP.ORDER_ITEM set STATE = 'Something Else' where ID = 6 and ORDER_ID = 55");
            simulateOrderItemUpdateNotification(6, "state");
        } // else there's no update so deepFetchToManyInMemory will be used

        makeFetchesWaitForSignal();

        // Simulate a database update and notification message occurring when the deep fetch result map is cached.
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                executeAndAssertSqlUpdate(1, "update APP.ORDER_ITEM set STATE = 'In-Progress' where ID = 6 and ORDER_ID = 55");
                simulateOrderItemUpdateNotification(6, "state");

                signalUpdateThreadDone(); // allow the deep fetch to complete
            }
        });
        thread.start();

        OrderList orderList2 = OrderFinder.findMany(op);
        orderList2.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        logger.info("Performing deep fetch in memory with concurrent database update and notification");

        // The next call will resolve the deep fetch using the deep fetch strategy.
        // The harnessed version of the deep fetch strategy coordinates with the update thread,
        // to ensure the database updates are applied just before the results map is cached.
        Assert.assertEquals(5, orderList2.size());

        // Traverse single item relationship lookup which will use a single item operation.
        // Will hit the database with single-object retrievals as all OrderItems are now invalidated in the cache.
        Assert.assertEquals(6, orderList2.asEcList().flatCollect(getOrderItemsOfOrder).size());

        // Should reflect the latest updates because the prior statement caused these OrderItem objects to be read from database.
        Assert.assertEquals(6, orderList2.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));

        thread.join();

        allowAllFetches();

        // Should now reflect all updates to date as this is a brand new query
        OrderList orderList3 = OrderFinder.findMany(op);
        orderList3.deepFetch(harnessDeepFetchRelationship(OrderFinder.items(), timingTestCase));

        logger.info("Performing final deep fetch retrieval");

        final int retrievalCountBeforeFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        Assert.assertEquals(5, orderList3.size());

        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));
        Assert.assertEquals(6, orderList3.getItems().size());
        Assert.assertEquals(6, orderList3.getItems().asEcList().count(orderItemStateEqualsInProgress));

        final int retrievalCountAfterFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        final int retrievalCountDuringFinalFetch = retrievalCountAfterFinalFetch - retrievalCountBeforeFinalFetch;
        Assert.assertTrue("Executed too many database retrievals: " + retrievalCountDuringFinalFetch, retrievalCountDuringFinalFetch <= 2);
    }

    public void testAdhocDeepFetch_OneToMany() throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = new OrderList(new ArrayList<Order>(OrderFinder.findMany(op)));
        orderList.deepFetch(OrderFinder.items());

        final Function<Order, Iterable<OrderItem>> getOrderItemsOfOrder = new Function<Order, Iterable<OrderItem>>()
        {
            @Override
            public Iterable<OrderItem> valueOf(Order order)
            {
                return order.getItems();
            }
        };

        final Predicate<OrderItem> orderItemStateEqualsInProgress = new Predicate<OrderItem>()
        {
            @Override
            public boolean accept(OrderItem each)
            {
                return "In-Progress".equals(each.getState());
            }
        };

        Assert.assertEquals(4, orderList.size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));
        Assert.assertEquals(4, orderList.getItems().size());
        Assert.assertEquals(4, orderList.getItems().asEcList().count(orderItemStateEqualsInProgress));

        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
        simulateOrderUpdateNotification(55, "state");

        OrderList orderList2 = new OrderList(new ArrayList<Order>(OrderFinder.findMany(op)));
        orderList2.deepFetch(OrderFinder.items());

        Assert.assertEquals(5, orderList2.size());
        Assert.assertEquals(6, orderList2.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(5, orderList2.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));
        Assert.assertEquals(6, orderList2.getItems().size());
        Assert.assertEquals(5, orderList2.getItems().asEcList().count(orderItemStateEqualsInProgress));

        executeAndAssertSqlUpdate(1, "update APP.ORDER_ITEM set STATE = 'In-Progress' where ID = 6 and ORDER_ID = 55");
        simulateOrderItemUpdateNotification(6, "state");

        OrderList orderList3 = new OrderList(new ArrayList<Order>(OrderFinder.findMany(op)));
        orderList3.deepFetch(OrderFinder.items());

        Assert.assertEquals(5, orderList3.size());
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).size());
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderItemsOfOrder).count(orderItemStateEqualsInProgress));
        Assert.assertEquals(6, orderList3.getItems().size());
        Assert.assertEquals(6, orderList3.getItems().asEcList().count(orderItemStateEqualsInProgress));
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, false, false, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, true, false, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_BypassCacheInPriorQuery_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, false, false, true, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_BypassCacheInPriorQuery_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, true, false, true, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_ChildUpdateAfterParentUpdate_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, false, true, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_ChildUpdateAfterParentUpdate_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, true, true, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_ChildUpdateAfterParentUpdate_ChildUpdatesObservedBeforeFinalDeepFetch_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, false, true, false, true);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenChildFetchAndCache_ChildUpdateAfterParentUpdate_ChildUpdatesObservedBeforeFinalDeepFetch_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE, true, true, false, true);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, false, false, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, true, false, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_BypassCacheInPriorQuery_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, false, false, true, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_BypassCacheInPriorQuery_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, true, false, true, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_ChildUpdateAfterParentUpdate_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, false, true, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_ChildUpdateAfterParentUpdate_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, true, true, false, false);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_ChildUpdateAfterParentUpdate_ChildUpdatesObservedBeforeFinalDeepFetch_SimplifiedOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, false, true, false, true);
    }

    public void testDeepFetch_OneToOne_NotificationBetweenParentFetchAndChildFetch_ChildUpdateAfterParentUpdate_ChildUpdatesObservedBeforeFinalDeepFetch_ComplexOp() throws Exception
    {
        runTestDeepFetchOneToOneTest(TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH, true, true, false, true);
    }

    private void runTestDeepFetchOneToOneTest(TimingTestCase timingTestCase, boolean forceComplexOp, boolean includeChildUpdateAfterParentUpdate, boolean bypassCacheInPriorQuery, boolean observeChildUpdatesBeforeFinalDeepFetch) throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        forceComplexOp(forceComplexOp);

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = OrderFinder.findMany(op);
        orderList.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        final Function<Order, List<OrderStatus>> getOrderStatusOfOrder = new Function<Order, List<OrderStatus>>()
        {
            @Override
            public List<OrderStatus> valueOf(Order order)
            {
                final OrderStatus orderStatus = order.getOrderStatus();
                return orderStatus != null ? Collections.singletonList(orderStatus) : Collections.<OrderStatus>emptyList();
            }
        };

        final Predicate<OrderStatus> orderStatusEqualsTen = new Predicate<OrderStatus>()
        {
            @Override
            public boolean accept(OrderStatus each)
            {
                return each.getStatus() == 10;
            }
        };

        allowAllFetches();

        Assert.assertEquals(4, orderList.size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(4, orderList.getOrderStatus().size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));

        // This update and notification gives the findMany a reason to have to hit the database, as it marks the cache as dirty.
        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
        simulateOrderUpdateNotification(55, "state");

        makeFetchesWaitForSignal();

        // Simulate a database update and notification message occurring during the deep fetch
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 56");
                simulateOrderUpdateNotification(56, "state");

                signalUpdateThreadDone(); // allow the deep fetch to complete
            }
        });
        thread.start();

        OrderList orderList2 = OrderFinder.findMany(op);
        orderList2.setBypassCache(bypassCacheInPriorQuery);
        orderList2.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        logger.info("Performing deep fetch retrieval with concurrent database update and notification of the parent table");

        // The next call will resolve the deep fetch using the deep fetch strategy.
        // The harnessed version of the deep fetch strategy coordinates with the update thread,
        // to ensure the database updates are applied after the parent list is resolved but before the deep fetch was completed.
        Assert.assertEquals(5, orderList2.size());

        Assert.assertEquals(5, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(5, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));

        // Do not query orderList2.getOrderStatus() here or else it will re-fetch from database and that will mean the next deep fetch will hit the cache instead of the DB.
        // The next part of the test relies on the fact that the deep fetch will hit the database to retrieve the updated Order 56, which triggers a re-fetch of OrderStatus.
        // The test harness relies on that to happen so that it can co-ordinate the next database update to OrderStatus to happen at the same time.

        thread.join();

        if (includeChildUpdateAfterParentUpdate)
        {
            makeFetchesWaitForSignal();

            // Simulate a database update and notification message occurring during the deep fetch
            Thread thread2 = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                    executeAndAssertSqlUpdate(1, "update APP.ORDER_STATUS set STATUS = 10 where ORDER_ID = 56");
                    simulateOrderStatusUpdateNotification(56, "status");

                    signalUpdateThreadDone(); // allow the deep fetch to complete
                }
            });
            thread2.start();

            OrderList orderList3 = OrderFinder.findMany(op);
            orderList3.setBypassCache(bypassCacheInPriorQuery);
            orderList3.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

            logger.info("Performing a second deep fetch retrieval with concurrent database update and notification of the child table");

            // The next call will resolve the deep fetch using the deep fetch strategy.
            // The harnessed version of the deep fetch strategy coordinates with the update thread,
            // to ensure the database updates are applied after the parent list is resolved but before the deep fetch was completed.
            Assert.assertEquals(6, orderList3.size());

            // The act of observing the related child objects causes them to be re-fetched from database because the cached results are stale.
            // It therefore makes sense to test with and without this step, so that we also check that the next deep fetch does not retrieve stale results.
            if (observeChildUpdatesBeforeFinalDeepFetch)
            {
                Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).size());
                Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));

                Assert.assertEquals(6, orderList3.getOrderStatus().size());
                Assert.assertEquals(6, orderList3.getOrderStatus().asEcList().count(orderStatusEqualsTen));
            }

            thread2.join();
        }

        allowAllFetches();

        // Should now reflect all updates to date as this is a brand new query
        OrderList orderList4 = OrderFinder.findMany(op);
        orderList4.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        logger.info("Performing final deep fetch retrieval");

        Assert.assertEquals(6, orderList4.size());
        Assert.assertEquals(6, orderList4.asEcList().collect(getOrderStatusOfOrder).size());
        Assert.assertEquals(includeChildUpdateAfterParentUpdate ? 6 : 5, orderList4.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));

        Assert.assertEquals(6, orderList4.getOrderStatus().size());
        Assert.assertEquals(includeChildUpdateAfterParentUpdate ? 6 : 5, orderList4.getOrderStatus().asEcList().count(orderStatusEqualsTen));
    }

    public void testDeepFetch_OneToOne_SingleItemRelationshipCaching_NotificationDuringResultsMapCache_PartiallyFromServer() throws SQLException, InterruptedException
    {
        runTestDeepFetchOneToOneTest_SingleItemRelationshipCaching(false, false);
    }

    public void testDeepFetch_OneToOne_SingleItemRelationshipCaching_NotificationDuringResultsMapCache_FullyFromServer() throws SQLException, InterruptedException
    {
        runTestDeepFetchOneToOneTest_SingleItemRelationshipCaching(true, false);
    }

    public void testDeepFetch_OneToOne_SingleItemRelationshipCaching_NotificationDuringResultsMapCache_BypassCache_ComplexOp() throws SQLException, InterruptedException
    {
        runTestDeepFetchOneToOneTest_SingleItemRelationshipCaching(true, true);
    }

    public void testDeepFetch_OneToOne_SingleItemRelationshipCaching_NotificationDuringResultsMapCache_BypassCache_SimplifiedOp() throws SQLException, InterruptedException
    {
        runTestDeepFetchOneToOneTest_SingleItemRelationshipCaching(false, true);
    }

    private void runTestDeepFetchOneToOneTest_SingleItemRelationshipCaching(boolean forceComplexOp, boolean bypassCache) throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        forceComplexOp(forceComplexOp);

        TimingTestCase timingTestCase = TimingTestCase.WAIT_DURING_CHILD_CACHE_RESULTS_MAP;

        // Set up additional test data before the first deep fetch
        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 56");
        simulateOrderUpdateNotification(56, "state");

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = OrderFinder.findMany(op);
        orderList.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        final Function<Order, List<OrderStatus>> getOrderStatusOfOrder = new Function<Order, List<OrderStatus>>()
        {
            @Override
            public List<OrderStatus> valueOf(Order order)
            {
                final OrderStatus orderStatus = order.getOrderStatus();
                return orderStatus != null ? Collections.singletonList(orderStatus) : Collections.<OrderStatus>emptyList();
            }
        };

        final Predicate<OrderStatus> orderStatusEqualsTen = new Predicate<OrderStatus>()
        {
            @Override
            public boolean accept(OrderStatus each)
            {
                return each.getStatus() == 10;
            }
        };

        allowAllFetches();

        Assert.assertEquals(5, orderList.size());
        Assert.assertEquals(5, orderList.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(5, orderList.getOrderStatus().size());

        // This update and notification gives the deep fetch a reason to have to hit the database for the OrderStatus we're going to update in the test.
        // This update is to a different attribute so it will not change the assertion results.
        executeAndAssertSqlUpdate(1, "update APP.ORDER_STATUS set LAST_USER = 'Bob' where ORDER_ID = 56");
        simulateOrderStatusUpdateNotification(56, "lastUser");

        makeFetchesWaitForSignal();

        // Simulate a database update and notification message occurring just prior to the individual related object being cached.
        // This is to test that we don't cause this update to be 'lost' by caching incorrectly.
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                executeAndAssertSqlUpdate(1, "update APP.ORDER_STATUS set STATUS = 10 where ORDER_ID = 56");
                simulateOrderStatusUpdateNotification(56, "status");

                signalUpdateThreadDone(); // allow the deep fetch to complete
            }
        });
        thread.start();

        OrderList orderList2 = OrderFinder.findMany(op);
        orderList2.setBypassCache(bypassCache);
        orderList2.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        logger.info("Performing deep fetch retrieval with concurrent database update during cache of results map");

        // The next call will resolve the deep fetch using the deep fetch strategy.
        // The harnessed version of the deep fetch strategy coordinates with the update thread,
        // to ensure the database updates are applied just before the results map is cached.
        Assert.assertEquals(5, orderList2.size());

        // Traverse single item relationship lookup which will use a single item operation.
        // Will hit the database with single-object retrievals as all OrderStatus objects are now invalidated in the cache.
        Assert.assertEquals(5, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).size());

        // Should reflect the latest updates because the prior statement caused these OrderStatus objects to be read from database.
        Assert.assertEquals(5, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));

        Assert.assertEquals(5, orderList2.getOrderStatus().size());
        Assert.assertEquals(5, orderList2.getOrderStatus().asEcList().count(orderStatusEqualsTen));

        allowAllFetches();

        // Should now reflect all updates to date as this is a brand new query
        OrderList orderList3 = OrderFinder.findMany(op);
        orderList3.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        logger.info("Performing final deep fetch retrieval");

        final int retrievalCountBeforeFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        Assert.assertEquals(5, orderList3.size());
        Assert.assertEquals(5, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(5, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(5, orderList3.getOrderStatus().size());
        Assert.assertEquals(5, orderList3.getOrderStatus().asEcList().count(orderStatusEqualsTen));

        final int retrievalCountAfterFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        final int retrievalCountDuringFinalFetch = retrievalCountAfterFinalFetch - retrievalCountBeforeFinalFetch;
        Assert.assertTrue("Executed too many database retrievals: " + retrievalCountDuringFinalFetch, retrievalCountDuringFinalFetch <= 2);

    }

    public void testDeepFetch_OneToOne_ParentNotificationBeforeDeepFetch_PartiallyFromServer() throws SQLException, InterruptedException
    {
        // No single item ops will get cached for this case as they were all found in memory. This test is just for completeness.
        runTestDeepFetchOneToOneTest_ParentNotificationBeforeDeepFetch(false, false);
    }

    public void testDeepFetch_OneToOne_ParentNotificationBeforeDeepFetch_BypassCache_ComplexOp() throws SQLException, InterruptedException
    {
        runTestDeepFetchOneToOneTest_ParentNotificationBeforeDeepFetch(true, true);
    }

    public void testDeepFetch_OneToOne_ParentNotificationBeforeDeepFetch_BypassCache_SimplifiedOp() throws SQLException, InterruptedException
    {
        runTestDeepFetchOneToOneTest_ParentNotificationBeforeDeepFetch(false, true);
    }

    private void runTestDeepFetchOneToOneTest_ParentNotificationBeforeDeepFetch(boolean forceComplexOp, boolean bypassCache) throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        forceComplexOp(forceComplexOp);

        TimingTestCase timingTestCase = TimingTestCase.WAIT_BEFORE_CHILD_DEEP_FETCH;

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = OrderFinder.findMany(op);
        orderList.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        final Function<Order, List<OrderStatus>> getOrderStatusOfOrder = new Function<Order, List<OrderStatus>>()
        {
            @Override
            public List<OrderStatus> valueOf(Order order)
            {
                final OrderStatus orderStatus = order.getOrderStatus();
                return orderStatus != null ? Collections.singletonList(orderStatus) : Collections.<OrderStatus>emptyList();
            }
        };

        final Predicate<OrderStatus> orderStatusEqualsTen = new Predicate<OrderStatus>()
        {
            @Override
            public boolean accept(OrderStatus each)
            {
                return each.getStatus() == 10;
            }
        };

        allowAllFetches();

        Assert.assertEquals(4, orderList.size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(4, orderList.getOrderStatus().size());

        makeFetchesWaitForSignal();

        // Simulate a database update and notification message occurring to the parent table just prior to the deep fetch of the child table.
        // This is to test that we don't cause this update to be 'lost' by caching incorrectly.
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                waitToStartUpdateThread(); // wait until the deep fetch retrieval has reached the right point

                executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
                simulateOrderUpdateNotification(55, "state");

                signalUpdateThreadDone(); // allow the deep fetch to complete
            }
        });
        thread.start();

        OrderList orderList2 = OrderFinder.findMany(op);
        orderList2.setBypassCache(bypassCache);
        orderList2.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        logger.info("Performing deep fetch retrieval with concurrent database update to parent during deep fetch of child");

        // The next call will resolve the deep fetch using the deep fetch strategy.
        // The harnessed version of the deep fetch strategy coordinates with the update thread,
        // to ensure the database updates are applied just before deepFetchToOneMostlyInMemory.
        Assert.assertEquals(4, orderList2.size());
        Assert.assertEquals(4, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(4, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));

        // This includes the OrderStatus for order 55 because OrderList.getOrderStatus() does a new finder query which hits the database due to the stale query cache.
        // It gives the correct results but there is scope to optimise the performance if we could adopt a more sophisticated strategy.
        Assert.assertEquals(5, orderList2.getOrderStatus().size());
        Assert.assertEquals(5, orderList2.getOrderStatus().asEcList().count(orderStatusEqualsTen));

        allowAllFetches();

        // Should now reflect all updates to date as this is a brand new query
        OrderList orderList3 = OrderFinder.findMany(op);
        orderList3.deepFetch(harnessDeepFetchRelationship(OrderFinder.orderStatus(), timingTestCase));

        logger.info("Performing final deep fetch retrieval");

        final int retrievalCountBeforeFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        Assert.assertEquals(5, orderList3.size());
        Assert.assertEquals(5, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(5, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(5, orderList3.getOrderStatus().size());
        Assert.assertEquals(5, orderList3.getOrderStatus().asEcList().count(orderStatusEqualsTen));

        final int retrievalCountAfterFinalFetch = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        final int retrievalCountDuringFinalFetch = retrievalCountAfterFinalFetch - retrievalCountBeforeFinalFetch;
        Assert.assertTrue("Executed too many database retrievals: " + retrievalCountDuringFinalFetch, retrievalCountDuringFinalFetch <= 2);

    }

    public void testAdhocDeepFetch_OneToOne() throws SQLException, InterruptedException
    {
        if (OrderFinder.isFullCache())
        {
            logger.info("Skipping test - deep fetch test cases are only applicable to partial cache runtime configuration");
            return;
        }

        final Operation op = OrderFinder.state().eq("In-Progress");
        OrderList orderList = new OrderList(new ArrayList<Order>(OrderFinder.findMany(op)));
        orderList.deepFetch(OrderFinder.orderStatus());

        final Function<Order, List<OrderStatus>> getOrderStatusOfOrder = new Function<Order, List<OrderStatus>>()
        {
            @Override
            public List<OrderStatus> valueOf(Order order)
            {
                final OrderStatus orderStatus = order.getOrderStatus();
                return orderStatus != null ? Collections.singletonList(orderStatus) : Collections.<OrderStatus>emptyList();
            }
        };

        final Predicate<OrderStatus> orderStatusEqualsTen = new Predicate<OrderStatus>()
        {
            @Override
            public boolean accept(OrderStatus each)
            {
                return each.getStatus() == 10;
            }
        };

        Assert.assertEquals(4, orderList.size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(4, orderList.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(4, orderList.getOrderStatus().size());
        Assert.assertEquals(4, orderList.getOrderStatus().asEcList().count(orderStatusEqualsTen));

        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 55");
        executeAndAssertSqlUpdate(1, "update APP.ORDERS set STATE = 'In-Progress' where ORDER_ID = 56");
        simulateOrderUpdateNotification(55, "state");
        simulateOrderUpdateNotification(56, "state");

        OrderList orderList2 = new OrderList(new ArrayList<Order>(OrderFinder.findMany(op)));
        orderList2.deepFetch(OrderFinder.orderStatus());

        Assert.assertEquals(6, orderList2.size());
        Assert.assertEquals(6, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(5, orderList2.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(6, orderList2.getOrderStatus().size());
        Assert.assertEquals(5, orderList2.getOrderStatus().asEcList().count(orderStatusEqualsTen));

        executeAndAssertSqlUpdate(1, "update APP.ORDER_STATUS set STATUS = 10 where ORDER_ID = 56");
        simulateOrderStatusUpdateNotification(56, "status");

        OrderList orderList3 = new OrderList(new ArrayList<Order>(OrderFinder.findMany(op)));
        orderList3.deepFetch(OrderFinder.orderStatus());

        Assert.assertEquals(6, orderList3.size());
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).size());
        Assert.assertEquals(6, orderList3.asEcList().flatCollect(getOrderStatusOfOrder).count(orderStatusEqualsTen));
        Assert.assertEquals(6, orderList3.getOrderStatus().size());
        Assert.assertEquals(6, orderList3.getOrderStatus().asEcList().count(orderStatusEqualsTen));
    }

    private void allowAllFetches()
    {
        signalDoneUpdating.release(999);
    }

    private void makeFetchesWaitForSignal()
    {
        signalWaitingForUpdate.drainPermits();
        signalDoneUpdating.drainPermits();
    }

    private void executeAndAssertSqlUpdate(int expectedUpdateCount, String sql)
    {
        int updateCount = executeSqlUpdate(sql);
        Assert.assertEquals(expectedUpdateCount, updateCount);
    }

    private int executeSqlUpdate(String sql)
    {
        final Connection conn = ConnectionManagerForTests.getInstance().getConnection();
        try
        {
            try
            {
                logger.info(sql);
                return conn.createStatement().executeUpdate(sql); // returns the updated number of rows
            }
            finally
            {
                conn.close();
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Error executing SQL update as part of the test setup", e);
        }
    }

    private void forceComplexOp(boolean forceComplexOp)
    {
        if (forceComplexOp)
        {
            DeepRelationshipUtility.setMaxSimplifiedIn(0);
        }
    }

    private void simulateOrderUpdateNotification(int orderId, String... attributeNames)
    {
        final Order order = new Order();
        order.setOrderId(orderId); // only the primary key is relevant for refresh / mark dirty

        simulateUpdateNotification(OrderFinder.getFinderInstance(), order, attributeNames);
    }

    private void simulateOrderStatusUpdateNotification(int orderId, String... attributeNames)
    {
        final OrderStatus orderStatus = new OrderStatus();
        orderStatus.setOrderId(orderId); // only the primary key is relevant for refresh / mark dirty

        simulateUpdateNotification(OrderStatusFinder.getFinderInstance(), orderStatus, attributeNames);
    }

    private void simulateOrderItemUpdateNotification(int id, String... attributeNames)
    {
        final OrderItem orderItem = new OrderItem();
        orderItem.setId(id); // only the primary key is relevant for refresh / mark dirty

        simulateUpdateNotification(OrderItemFinder.getFinderInstance(), orderItem, attributeNames);
    }

    private void simulateUpdateNotification(RelatedFinder finder, MithraTransactionalObjectImpl businessObject, String... attributeNames)
    {
        // Simulate the invalidation of the partial cache caused by an incoming notification message.
        // This logic is equivalent to PartialCacheMithraNotificationListener.onUpdate(), but without the complexity of
        // having to ensure the object is already in the cache, in order to ensure the class update count gets incremented.

        finder.getMithraObjectPortal().getCache().markDirty(businessObject.zGetCurrentData());
        for (String attributeName : attributeNames)
        {
            finder.getAttributeByName(attributeName).incrementUpdateCount();
        }
        finder.getMithraObjectPortal().incrementClassUpdateCount();
    }

    private void resetDeepFetchRelationship(AbstractRelatedFinder relationshipFinder)
    {
        relationshipFinder.zSetDeepFetchStrategy(null);
    }

    private <R extends AbstractRelatedFinder & DeepRelationshipAttribute> R harnessDeepFetchRelationship(R relationshipFinder, TimingTestCase timingTestCase)
    {
        final DeepFetchStrategy originalStrategy = relationshipFinder.zGetDeepFetchStrategy();
        final DeepFetchStrategy newStrategy;
        if (originalStrategy instanceof SimpleToManyDeepFetchStrategy)
        {
            newStrategy = new HarnessedSimpleToManyDeepFetchStrategy(relationshipFinder.zGetMapper(), relationshipFinder.zGetOrderBy(), timingTestCase);
        }
        else if (originalStrategy instanceof SimpleToOneDeepFetchStrategy)
        {
            newStrategy = new HarnessedSimpleToOneDeepFetchStrategy(relationshipFinder.zGetMapper(), relationshipFinder.zGetOrderBy(), timingTestCase);
        }
        else
        {
            throw new IllegalStateException("Unsupported deep fetch strategy: " + originalStrategy);
        }
        relationshipFinder.zSetDeepFetchStrategy(newStrategy);
        return relationshipFinder;
    }

    private void acquireOrTimeout(Semaphore semaphore)
    {
        try
        {
            if (!semaphore.tryAcquire(5L, TimeUnit.SECONDS))
            {
                throw new RuntimeException("Timed out waiting for semaphore");
            }
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted" , e);
        }
    }

    private void signalAndWaitForUpdate(String logLabel)
    {
        logger.info(logLabel + " starting wait");
        signalWaitingForUpdate.release();
        acquireOrTimeout(signalDoneUpdating);
        logger.info(logLabel + " done waiting");
    }

    private void signalUpdateThreadDone()
    {
        logger.info("update thread done updating");
        signalDoneUpdating.release();
    }

    private void waitToStartUpdateThread()
    {
        logger.info("update thread starting wait");
        acquireOrTimeout(signalWaitingForUpdate);
        logger.info("update thread done waiting");
    }

    private class HarnessedSimpleToManyDeepFetchStrategy extends SimpleToManyDeepFetchStrategy
    {
        private final TimingTestCase timingTestCase;

        public HarnessedSimpleToManyDeepFetchStrategy(Mapper mapper, OrderBy orderBy, TimingTestCase timingTestCase)
        {
            super(mapper, orderBy);
            this.timingTestCase = timingTestCase;
        }

        @Override
        protected List getImmediateParentList(DeepFetchNode node)
        {
            final List immediateParentList = super.getImmediateParentList(node);
            if (timingTestCase == TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH)
            {
                signalAndWaitForUpdate("getImmediateParentList");
            }
            return immediateParentList;
        }

        @Override
        protected MithraList getResolvedListFromServer(boolean bypassCache, List immediateParentList, MithraList complexList, DeepFetchNode node)
        {
            final MithraList resolvedListFromServer = super.getResolvedListFromServer(bypassCache, immediateParentList, complexList, node);
            if (timingTestCase == TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE)
            {
                signalAndWaitForUpdate("getResolvedListFromServer");
            }
            return resolvedListFromServer;
        }

        @Override
        protected void associateSimplifiedResult(Operation op, List resultList, CachedQueryPair baseQuery)
        {
            if (timingTestCase == TimingTestCase.WAIT_DURING_CHILD_CACHE_SIMPLIFIED_RESULT)
            {
                signalAndWaitForUpdate("associateSimplifiedResult");
            }
            super.associateSimplifiedResult(op, resultList, baseQuery);
        }

        @Override
        protected List cacheResults(HashMap<Operation, List> opToListMap, int doNotCacheCount, CachedQueryPair baseQuery) {
            if (timingTestCase == TimingTestCase.WAIT_DURING_CHILD_CACHE_RESULTS_MAP)
            {
                signalAndWaitForUpdate("cacheResults");
            }
            return super.cacheResults(opToListMap, doNotCacheCount, baseQuery);
        }
    }

    private class HarnessedSimpleToOneDeepFetchStrategy extends SimpleToOneDeepFetchStrategy
    {
        private final TimingTestCase timingTestCase;

        public HarnessedSimpleToOneDeepFetchStrategy(Mapper mapper, OrderBy orderBy, TimingTestCase timingTestCase)
        {
            super(mapper, orderBy);
            this.timingTestCase = timingTestCase;
        }

        @Override
        protected List getImmediateParentList(DeepFetchNode node)
        {
            final List immediateParentList = super.getImmediateParentList(node);
            if (timingTestCase == TimingTestCase.WAIT_BETWEEN_PARENT_FETCH_AND_CHILD_FETCH)
            {
                signalAndWaitForUpdate("getImmediateParentList");
            }
            return immediateParentList;
        }

        @Override
        protected MithraList fetchSimplifiedJoinList(DeepFetchNode node, List parentList, boolean bypassCache)
        {
            final MithraList simplifiedJoinList = super.fetchSimplifiedJoinList(node, parentList, bypassCache);
            if (simplifiedJoinList != null && timingTestCase == TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE)
            {
                signalAndWaitForUpdate("fetchSimplifiedJoinList");
            }
            return simplifiedJoinList;
        }

        @Override
        protected void injectWaitAfterComplexList()
        {
            if (timingTestCase == TimingTestCase.WAIT_BETWEEN_CHILD_FETCH_AND_CHILD_CACHE)
            {
                signalAndWaitForUpdate("resolveComplexList");
            }
        }

        @Override
        protected List cacheResults(HashMap<Operation, List> opToListMap, int doNotCacheCount, CachedQueryPair baseQuery) {
            if (timingTestCase == TimingTestCase.WAIT_DURING_CHILD_CACHE_RESULTS_MAP)
            {
                signalAndWaitForUpdate("cacheResults");
            }
            return super.cacheResults(opToListMap, doNotCacheCount, baseQuery);
        }

        @Override
        public List deepFetch(DeepFetchNode node, boolean bypassCache, boolean forceImplicitJoin) {
            if (timingTestCase == TimingTestCase.WAIT_BEFORE_CHILD_DEEP_FETCH)
            {
                signalAndWaitForUpdate("deepFetch");
            }
            return super.deepFetch(node, bypassCache, forceImplicitJoin);
        }
    }
}
