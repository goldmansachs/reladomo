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

package com.gs.fw.common.mithra.test.cacheloader;


import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cacheloader.AdditionalOperationBuilder;
import com.gs.fw.common.mithra.cacheloader.AdditionalOperationBuilderWithPrerequisites;
import com.gs.fw.common.mithra.cacheloader.BusinessDateMilestonedTopLevelLoaderFactory;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderManagerImpl;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderMonitor;
import com.gs.fw.common.mithra.cacheloader.FullyMilestonedTopLevelLoaderFactory;
import com.gs.fw.common.mithra.cacheloader.LoadingTaskThreadPoolMonitor;
import com.gs.fw.common.mithra.cacheloader.RefreshInterval;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.LewAccountFinder;
import com.gs.fw.common.mithra.test.domain.StockFinder;
import com.gs.fw.common.mithra.test.glew.GlewScrpFinder;
import com.gs.fw.common.mithra.test.glew.LewContract;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.test.glew.LewProductFinder;
import com.gs.fw.common.mithra.test.glew.LewRelationshipFinder;
import com.gs.fw.common.mithra.test.glew.LewTransaction;
import com.gs.fw.common.mithra.test.glew.LewTransactionFinder;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.Pair;
import com.gs.fw.common.mithra.util.RenewedCacheStats;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

public class CacheLoaderManagerTest extends TestCase
{
    private static final Timestamp BUSINESS_DATE = Timestamp.valueOf("2010-10-30 23:59:00");

    private static final Timestamp MULTI_DATE_1_IN_RANGE_A = Timestamp.valueOf("2010-01-01 23:59:00");
    private static final Timestamp MULTI_DATE_2_IN_RANGE_A = Timestamp.valueOf("2010-01-02 23:59:00");
    private static final Timestamp MULTI_DATE_3_IN_RANGE_A = Timestamp.valueOf("2010-01-06 23:59:00");
    private static final Timestamp MULTI_DATE_3_OUTOF_RANGE_A = Timestamp.valueOf("2010-01-16 23:59:00");

    private static final Timestamp MULTI_DATE_1_IN_RANGE_B = Timestamp.valueOf("2009-01-01 23:59:00");
    private static final Timestamp MULTI_DATE_2_IN_RANGE_B = Timestamp.valueOf("2009-01-03 23:59:00");

    private static final Timestamp MULTI_DATE_1_LN2 = Timestamp.valueOf("2011-12-30 23:59:00");
    private static final Timestamp MULTI_DATE_2_LN2 = Timestamp.valueOf("2013-03-29 23:59:00");

    private static final Timestamp time1 = Timestamp.valueOf("2010-01-03 02:51:00.0");
    private static final Timestamp time2 = Timestamp.valueOf("2010-01-03 03:01:00.0");
    private static final Timestamp time3 = Timestamp.valueOf("2010-01-03 03:11:00.0");

    private static final UnifiedSet<String> nykTimeZones = UnifiedSet.newSetWith("America/New_York", "US/Eastern", "EST", "EST5EDT");

    private MithraTestResource mithraTestResource;
    private static final String NYK_REGION = "NYK";
    private static final String LN1_REGION = "LN1";
    private static final String LN2_REGION = "LN2";

    private static final Timestamp INITIAL_LOAD_TIME = new Timestamp(System.currentTimeMillis() - 120 * 1000);

    private static final String NYK_TEST_CONFIG =
            "<CacheLoader>\n" +
            "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK\"/>\n" +
            "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
            "</CacheLoader>";

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        assertTrue("The refresh tests mean to run in EST. They will fail in different timezone. Set timezone on the EST on the workstation.", nykTimeZones.contains(java.util.TimeZone.getDefault().getID()));
        this.mithraTestResource = new MithraTestResource("MithraCacheTestConfig.xml");
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("nystlew");
        this.mithraTestResource.createSingleDatabase(connectionManager, "nystlew", "testdata/cacheloader/CacheLoaderTest_STLEW.txt");

        this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, NYK_REGION, "testdata/cacheloader/CacheLoaderTest_NYLEW.txt");
        this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, LN1_REGION, "testdata/cacheloader/CacheLoaderTest_LN1LEW.txt");
        this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, LN2_REGION, "testdata/cacheloader/CacheLoaderTest_LN2LEW.txt");
        this.mithraTestResource.setUp();
    }

    @Override
    protected void tearDown() throws Exception
    {
        MockAdditionalOperationBuilder.reset();

        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;

        super.tearDown();
    }

    public void testInitialLoad()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(NYK_TEST_CONFIG, FastList.newListWith(BUSINESS_DATE));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 2, 3);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 10.0, 20.0, 30.0, 40.0, 50.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
        final List<LoadingTaskThreadPoolMonitor> threadPoolMonitors = loaderMonitor.getThreadPoolMonitors();
        assertEquals(1, threadPoolMonitors.size());
        assertEquals("localhost:nystlew", threadPoolMonitors.get(0).getPoolName());
    }

    public void testInitialLoadWithTwoDatesWithinRange()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad("<CacheLoader>\n" +
        "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
        "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
        "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 30.0, 20.0, 50.0, 50.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testInitialLoadWithTwoDatesWithinRangeAndDependentTaskShifted()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\" " +
                "               helperFactoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETopLevelLoaderFactory\"/>\n" +
                "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 10.0, 30.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testInitialLoadWithTwoDatesWithinRangeWithAdditionalOperationBuilderThatIsDateInvariant()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
        "<CacheLoader>\n" +
        "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\">\n" +
        "               <Param name=\"operationBuilder\" value=\"com.gs.fw.common.mithra.test.cacheloader.SmallInstrumentIdsOnlyAdditionalOperationBuilder\"/>" +
        "        </TopLevelLoader>" +
        "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
        "</CacheLoader>",
                FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 20.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testInitialLoadWithTwoDatesWithinRangeWithTopAdditionalOperationBuilderThatIsDateVariant()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\">\n" +
                "               <Param name=\"operationBuilder\" value=\"com.gs.fw.common.mithra.test.cacheloader.SmallInstrumentIdsOnlyBeforeMilestoneDateAdditionalOperationBuilder\"/>" +
                "        </TopLevelLoader>" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>",
                FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 20.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 2, 1);
    }

    public void testInitialLoadWithTwoDatesWithinRangeWithDependentAdditionalOperationBuilderThatIsDateVariant()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad("<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\">\n" +
                "            <Param name=\"operationBuilder\" value=\"com.gs.fw.common.mithra.test.cacheloader.SmallInstrumentIdsOnlyBeforeMilestoneDateAdditionalOperationBuilder\"/>" +
                "        </DependentLoader>\n" +
                "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 20.0);

        assertNull(loaderMonitor.getException());
        assertEquals("TopLevel should be loaded in date range config", 1, loaderMonitor.getLoadingTaskStates().size());
        assertEquals("Dependent should be force loaded in single date config", 2, loaderMonitor.getDependentKeyIndexMonitors().size());
    }

    public void testInitialLoadWithTwoDatesWithinRangePostFiltersUnwantedDatesInbetween()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(NYK_TEST_CONFIG, FastList.newListWith(MULTI_DATE_1_IN_RANGE_B, MULTI_DATE_2_IN_RANGE_B));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 77.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testInitialLoadWithTwoDatesWithinRangeOneDateOutOfRange()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad("<CacheLoader>\n" +
        "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
        "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
        "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_OUTOF_RANGE_A));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 7, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 30.0, 20.0, 50.0, 50.0, 100.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 2);
    }

    public void testLoadWithBizDateInThePastBringsBackNoRecords()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_LN2));

        assertCacheEmpty();

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testLoadWithOpenBizDateBringsBackOpenRecords()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>", FastList.newListWith(MULTI_DATE_2_LN2));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 30.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testLoadWithOpenAndPastBizDateBringsBackOpenRecordsOnlyInSingleDateConfig()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_LN2, MULTI_DATE_2_LN2));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 30.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 2);
    }

    public void testLoadWithOpenAndPastBizDateBringsBackOpenRecordsOnlyInDateRangeConfig()
    {
        assertCacheEmpty();

        CacheLoaderMonitor loaderMonitor = runInitialLoad(
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>", FastList.newListWith(MULTI_DATE_1_IN_RANGE_B, MULTI_DATE_2_IN_RANGE_B));

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 2, 3);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 50.0, 60.0);

        assertLoaderMonitorTaskSizes(loaderMonitor, 1);
    }

    public void testRefresh()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(NYK_TEST_CONFIG));

        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), new RefreshInterval(time1, time2), new CacheLoaderMonitor());

        assertEquals(2, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(4, LewTransactionFinder.findMany(allTransactionsOperation()).size());

        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), new RefreshInterval(time2, time3), new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(5, LewTransactionFinder.findMany(allTransactionsOperation()).size());
    }

    public void testRefreshTwoDates()
    {
        String config =
                "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(FastList.newListWith(MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A), new RefreshInterval(time1, time3), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 7, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 50.0, 50.0, 100.0);

        assertNull(loaderMonitor.getException());
    }

    public void testRefreshTwoDatesWithTopLevelDateShifted()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\" " +
                "               factoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETopLevelLoaderFactory\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(FastList.newListWith(MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A), new RefreshInterval(time1, time3), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 20.0);

        assertNull(loaderMonitor.getException());
    }

    public void testRefreshTwoDatesWithDependentDateShifted()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\" " +
                "               helperFactoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETopLevelLoaderFactory\"/>\n" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(FastList.newListWith(MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A), new RefreshInterval(time1, time3), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 7, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 30.0);

        assertNull(loaderMonitor.getException());
    }

    public void testRefreshTwoDatesWithAllDateShifted()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\" " +
                "               factoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETopLevelLoaderFactory\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\" " +
                "               helperFactoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETopLevelLoaderFactory\"/>\n" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(FastList.newListWith(MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A), new RefreshInterval(time1, time3), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 10.0);

        assertNull(loaderMonitor.getException());
    }

    public void testRefreshTopLevelNewDependantOld()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\"/>\n" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        FastList<Timestamp> businessDates = FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A);
        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(businessDates, new RefreshInterval(time1, time2), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1);
        assertListContainsExactly(GlewScrpFinder.findMany(allScrpsOperation()), GlewScrpFinder.name(), "ppp");

        assertNull(loaderMonitor.getException());

        loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(businessDates, new RefreshInterval(time2, time3), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 7, 7);
        assertListContainsExactly(GlewScrpFinder.findMany(allScrpsOperation()), GlewScrpFinder.name(), "ppp");

        assertNull(loaderMonitor.getException());
    }

    public void testRefreshTopLevelOldDependantNew()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\"/>\n" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        assertEquals(4, LewContractFinder.findManyBypassCache(allContractsOperation().and(LewContractFinder.region().eq(LN1_REGION))).size());
        LewContractFinder.getMithraObjectPortal().getCache().clear();

        FastList<Timestamp> businessDates = FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A);
        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runRefresh(businessDates, new RefreshInterval(time1, time3), loaderMonitor);

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 1, 7, 7, 7);
        assertListContainsExactly(GlewScrpFinder.findMany(allScrpsOperation()), GlewScrpFinder.typeId(), (short) 100, (short) 200, (short) 300);

        assertNull(loaderMonitor.getException());
    }

    public void testRefreshWithAdditionalOperationBuilder()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewProduct\" sourceAttributes=\"LN1\">\n" +
                "               <Param name=\"operationBuilder\"\n" +
                "                   value=\"com.gs.fw.common.mithra.test.cacheloader.MockAdditionalOperationBuilder\"/>" +
                "        </TopLevelLoader>" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewProduct.relationshipWithLeftFitler\">\n" +
                "               <Param name=\"operationBuilder\"\n" +
                "                   value=\"com.gs.fw.common.mithra.test.cacheloader.MockAdditionalOperationBuilder\"/>" +
                "        </DependentLoader>" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        FastList<Timestamp> businessDates = FastList.newListWith(MULTI_DATE_1_IN_RANGE_A, MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A);
        cacheLoaderManager.runRefresh(businessDates, new RefreshInterval(time1, time3), new CacheLoaderMonitor());

        List<Pair<Timestamp, String>> callStack = MockAdditionalOperationBuilder.getCallStack();
        String productFinderName = LewProductFinder.getFinderInstance().getFinderClassName();
        String relationshipFinderName = LewRelationshipFinder.getFinderInstance().getFinderClassName();
        assertListContainsAll(callStack,
                Pair.<Timestamp, String>of(MULTI_DATE_1_IN_RANGE_A, productFinderName), Pair.<Timestamp, String>of(MULTI_DATE_2_IN_RANGE_A, productFinderName), Pair.<Timestamp, String>of(MULTI_DATE_3_IN_RANGE_A, productFinderName),
                Pair.<Timestamp, String>of(MULTI_DATE_1_IN_RANGE_A, relationshipFinderName), Pair.<Timestamp, String>of(MULTI_DATE_2_IN_RANGE_A, relationshipFinderName), Pair.<Timestamp, String>of(MULTI_DATE_3_IN_RANGE_A, relationshipFinderName));

    }

    public void testRefreshWithAdditionalOperationBuilderWithLoadPerBusinessDateAndDateShift()
    {
        String config = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewProduct\" sourceAttributes=\"LN1\" " +
                "               factoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETlewTopLevelLoaderFactory\">\n" +
                "               <Param name=\"operationBuilder\"\n" +
                "                   value=\"com.gs.fw.common.mithra.test.cacheloader.MockAdditionalOperationBuilder\"/>" +
                "        </TopLevelLoader>" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewProduct.relationshipWithLeftFitler\" " +
                "               helperFactoryClass=\"com.gs.fw.common.mithra.test.cacheloader.PYETlewTopLevelLoaderFactory\">\n" +
                "               <Param name=\"operationBuilder\"\n" +
                "                   value=\"com.gs.fw.common.mithra.test.cacheloader.MockAdditionalOperationBuilder\"/>" +
                "        </DependentLoader>" +
                "</CacheLoader>";

        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        FastList<Timestamp> businessDates = FastList.newListWith(MULTI_DATE_1_IN_RANGE_A);
        cacheLoaderManager.runRefresh(businessDates, new RefreshInterval(time1, time3), new CacheLoaderMonitor());

        List<Pair<Timestamp, String>> callStack = MockAdditionalOperationBuilder.getCallStack();
        String productFinderName = LewProductFinder.getFinderInstance().getFinderClassName();
        String relationshipFinderName = LewRelationshipFinder.getFinderInstance().getFinderClassName();
        Timestamp pye = new PYETlewTopLevelLoaderFactory().shiftBusinessDate(MULTI_DATE_1_IN_RANGE_A);
        assertListContainsAll(callStack,
                Pair.<Timestamp, String>of(pye, productFinderName),
                Pair.<Timestamp, String>of(pye, productFinderName),
                Pair.<Timestamp, String>of(pye, relationshipFinderName),
                Pair.<Timestamp, String>of(pye, relationshipFinderName));

    }

    public void testLimitedLoad()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(NYK_TEST_CONFIG));

        UnifiedMap<String, AdditionalOperationBuilder> withAdditionalOperations = UnifiedMap.newWithKeysValues(LewContract.class.getName(), null);
        cacheLoaderManager.runQualifiedLoad(FastList.newListWith(BUSINESS_DATE), withAdditionalOperations, false, new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(0, LewTransactionFinder.findMany(allTransactionsOperation()).size());

        withAdditionalOperations = UnifiedMap.newWithKeysValues(LewTransaction.class.getName(), null);
        cacheLoaderManager.runQualifiedLoad(FastList.newListWith(BUSINESS_DATE), withAdditionalOperations, false, new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(5, LewTransactionFinder.findMany(allTransactionsOperation()).size());
    }

    public void testLimitedLoadWithDependency()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(NYK_TEST_CONFIG));

        UnifiedMap<String, AdditionalOperationBuilder> withAdditionalOperations = UnifiedMap.newWithKeysValues(LewContract.class.getName(), null);
        cacheLoaderManager.runQualifiedLoad(FastList.newListWith(BUSINESS_DATE), withAdditionalOperations, true, new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(5, LewTransactionFinder.findMany(allTransactionsOperation()).size());
    }

    public void testLimitedLoadWithAdditionalOperation()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(NYK_TEST_CONFIG));

        AdditionalOperationBuilder operationBuilder = new LewContractFilter();
        UnifiedMap<String, AdditionalOperationBuilder> withAdditionalOperations =
                UnifiedMap.newWithKeysValues(LewContract.class.getName(), operationBuilder);
        cacheLoaderManager.runQualifiedLoad(FastList.newListWith(BUSINESS_DATE), withAdditionalOperations, true, new CacheLoaderMonitor());

        assertEquals(1, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(2, LewTransactionFinder.findMany(allTransactionsOperation()).size());
    }

    public void testLimitedLoadOfDependent()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(NYK_TEST_CONFIG));

        UnifiedMap<String, AdditionalOperationBuilder> withAdditionalOperations =
                UnifiedMap.newWithKeysValues(LewContract.class.getName(), null);
        cacheLoaderManager.runQualifiedLoad(FastList.newListWith(BUSINESS_DATE), withAdditionalOperations, false, new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(0, LewTransactionFinder.findMany(allTransactionsOperation()).size());

        AdditionalOperationBuilder operationBuilder = new AdditionalOperationBuilder()
        {
            public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
            {
                return LewTransactionFinder.instrumentId().eq(2);
            }

            @Override
            public boolean isBusinessDateInvariant()
            {
                return true;
            }
        };
        withAdditionalOperations = UnifiedMap.newWithKeysValues(LewTransaction.class.getName(), operationBuilder);
        cacheLoaderManager.runQualifiedLoad(FastList.newListWith(BUSINESS_DATE), withAdditionalOperations, true, new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(2, LewTransactionFinder.findMany(allTransactionsOperation()).size());
    }

    public void testLoadWithFilteredMapper()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();

        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\"/>\n" +
                "</CacheLoader>";

        cacheLoaderManager.loadConfiguration(createStringInputStream(testConfiguration));

        final Timestamp initialLoadEndTime = new Timestamp(System.currentTimeMillis() - 120 * 1000);
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());

        assertEquals(3, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(2, GlewScrpFinder.findMany(allScrpsOperation()).size());
    }

    public void testNonRegional()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();

        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.LewAccount\"" +
                "              factoryClass=\"" + FullyMilestonedTopLevelLoaderFactory.class.getName() + "\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.domain.LewAccount.contractsToLoad\" sourceAttributes=\"NYK\"/>\n" +
                "</CacheLoader>";

        cacheLoaderManager.loadConfiguration(createStringInputStream(testConfiguration));

        final Timestamp initialLoadEndTime = new Timestamp(System.currentTimeMillis() - 120 * 1000);
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());

        assertEquals(1, LewAccountFinder.findMany(allAccountsOperation()).size());

        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), new RefreshInterval(time2, time3), new CacheLoaderMonitor());

        assertEquals(1, LewContractFinder.findMany(allContractsOperation()).size());
    }

    public void testPrerequisite()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();

        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK\"/>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.Stock\"" +
                "              prerequisiteClasses=\"com.gs.fw.common.mithra.test.glew.LewContract\"" +
                "              factoryClass=\"" + BusinessDateMilestonedTopLevelLoaderFactory.class.getName() + "\"/>\n" +
                "</CacheLoader>";

        cacheLoaderManager.loadConfiguration(createStringInputStream(testConfiguration));

        final Timestamp initialLoadEndTime = new Timestamp(System.currentTimeMillis() - 120 * 1000);
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());

        assertEquals(1, StockFinder.findMany(allStocksOperation()).size());
    }

    public void testOperationBuilderPrerequisite()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();

        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK\"/>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.Stock\"\n" +
                "              factoryClass=\"" + BusinessDateMilestonedTopLevelLoaderFactory.class.getName() + "\">\n" +
                "              <Param name=\"operationBuilder\" value=\"" + TestOperationBuilder.class.getName() + "\"/>\n" +
                "        </TopLevelLoader>\n" +
                "</CacheLoader>";
        cacheLoaderManager.loadConfiguration(createStringInputStream(testConfiguration));

        final Timestamp initialLoadEndTime = new Timestamp(System.currentTimeMillis() - 120 * 1000);
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());

        assertEquals(1, StockFinder.findMany(allStocksOperation()).size());
    }

    public void testLimitedLoadOfDependentWithOwnerObjectsPassedIn()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();

        cacheLoaderManager.loadConfiguration(createStringInputStream("<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"LN1\"/>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.Stock\"\n" +
                "              factoryClass=\"" + BusinessDateMilestonedTopLevelLoaderFactory.class.getName() + "\">\n" +
                "              <Param name=\"operationBuilder\" value=\"" + TestOperationBuilder.class.getName() + "\"/>\n" +
                "        </TopLevelLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.LewAccount\"" +
                "              factoryClass=\"" + FullyMilestonedTopLevelLoaderFactory.class.getName() + "\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.domain.LewAccount.contractsToLoad\" sourceAttributes=\"NYK\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.lewTransaction\"/>\n" +
                "</CacheLoader>"));

        assertEquals(0, LewContractFinder.findMany(allContractsOperation()).size());
        assertEquals(0, LewTransactionFinder.findMany(allTransactionsOperation()).size());

        Operation renewOperation = allContractsOperation().and(LewContractFinder.region().eq("LN1").and(LewContractFinder.businessDateFrom().greaterThanEquals(MULTI_DATE_2_IN_RANGE_B)));
        RenewedCacheStats renewResult = new MithraRuntimeCacheController(LewContractFinder.class).renewCacheForOperation(renewOperation);
        FastList<MithraDataObject> renewedObjects = FastList.newList();
        renewedObjects.addAll(renewResult.getInserted());
        renewedObjects.addAll(renewResult.getUpdated());

        cacheLoaderManager.loadDependentObjectsFor(renewedObjects, FastList.newListWith(MULTI_DATE_2_IN_RANGE_A, MULTI_DATE_3_IN_RANGE_A), new Timestamp(System.currentTimeMillis() - 120 * 1000), new CacheLoaderMonitor());

        assertListContainsExactly(LewContractFinder.findMany(allContractsOperation()), LewContractFinder.instrumentId(), 7, 7, 7);
        assertListContainsExactly(LewTransactionFinder.findMany(allTransactionsOperation()), LewTransactionFinder.tradeQty(), 50.0, 50.0, 100.0);


        Operation renewOperation2 = allContractsOperation().and(LewContractFinder.region().eq("LN1").and(
                LewContractFinder.businessDateFrom().greaterThanEquals(MULTI_DATE_1_IN_RANGE_B)).and(
                LewContractFinder.instrumentId().eq(1)
        ));
        renewResult = new MithraRuntimeCacheController(LewContractFinder.class).renewCacheForOperation(renewOperation2);
        FastList<MithraDataObject> renewedObjects2 = FastList.newList();
        renewedObjects2.addAll(renewResult.getInserted());
        renewedObjects2.addAll(renewResult.getUpdated());

        cacheLoaderManager.loadDependentObjectsFor(renewedObjects, FastList.newListWith(MULTI_DATE_1_IN_RANGE_B), new Timestamp(System.currentTimeMillis() - 120 * 1000), new CacheLoaderMonitor());

        assertListContainsExactly(LewContractFinder.findMany(renewOperation2), LewContractFinder.instrumentId(), 1);
        assertEquals(0, LewTransactionFinder.findMany(allTransactionsOperation().and(LewTransactionFinder.instrumentId().eq(1))).size());
    }

    private static void assertCacheEmpty()
    {
        assertEquals(0, LewContractFinder.findMany(allContractsOperation()).count());
        assertEquals(0, LewTransactionFinder.findMany(allTransactionsOperation()).count());
        assertEquals(0, GlewScrpFinder.findMany(allScrpsOperation()).count());
        assertEquals(0, LewAccountFinder.findMany(allAccountsOperation()).count());
        assertEquals(0, StockFinder.findMany(allStocksOperation()).count());
    }

    private static Operation allContractsOperation()
    {
        return LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint());
    }

    private static Operation allTransactionsOperation()
    {
        return LewTransactionFinder.businessDate().equalsEdgePoint().and(LewTransactionFinder.processingDate().equalsEdgePoint());
    }

    private static Operation allScrpsOperation()
    {
        return GlewScrpFinder.businessDate().equalsEdgePoint().and(GlewScrpFinder.processingDate().equalsEdgePoint());
    }

    private static Operation allAccountsOperation()
    {
        return LewAccountFinder.businessDate().equalsEdgePoint().and(LewAccountFinder.processingDate().equalsEdgePoint());
    }

    private static Operation allStocksOperation()
    {
        return StockFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint());
    }

    private static void assertLoaderMonitorTaskSizes(CacheLoaderMonitor loaderMonitor, int numberOfExpectedLoadingTasks)
    {
        assertLoaderMonitorTaskSizes(loaderMonitor, numberOfExpectedLoadingTasks, numberOfExpectedLoadingTasks);
    }
    private static void assertLoaderMonitorTaskSizes(CacheLoaderMonitor loaderMonitor, int numberOfExpectedLoadingTasks, int numberOfExpectedDependentTasks)
    {
        assertNull(loaderMonitor.getException());
        assertEquals("" + loaderMonitor.getLoadingTaskStates(), numberOfExpectedLoadingTasks, loaderMonitor.getLoadingTaskStates().size());
        assertEquals("" + loaderMonitor.getDependentKeyIndexMonitors(), numberOfExpectedDependentTasks, loaderMonitor.getDependentKeyIndexMonitors().size());
    }

    private static CacheLoaderMonitor runInitialLoad(String config, List<Timestamp> businessDates)
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(config));

        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runInitialLoad(businessDates, INITIAL_LOAD_TIME, loaderMonitor);

        return loaderMonitor;
    }

    private static <T> void assertListContainsAll(Collection<T> collection, T... items)
    {
        UnifiedSet<T> set = UnifiedSet.newSet(collection);
        for (T item : items)
        {
            assertTrue("Expected to contain " + item + " but contains: " + collection.toString(), set.contains(item));
        }
    }

    private static <E,T> void assertListContainsExactly(List<E> list, Attribute<E, T> attribute, T... items)
    {
        FastList<T> actualValues = FastList.newList();
        for(E e: list)
        {
            actualValues.add(attribute.valueOf(e));
        }

        assertEquals(items.length, actualValues.size());
        for (T item : items)
        {
            assertTrue("Expected to contain " + item + " but contains: " + actualValues.toString(), actualValues.contains(item));
        }
    }

    private static InputStream createStringInputStream(String testConfiguration)
    {
        return new ByteArrayInputStream(testConfiguration.getBytes());
    }

    public static class LewContractFilter implements AdditionalOperationBuilder
    {
        public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
        {
            return LewContractFinder.instrumentId().eq(1);
        }

        @Override
        public boolean isBusinessDateInvariant()
        {
            return true;
        }
    }

    public static class TestOperationBuilder implements AdditionalOperationBuilderWithPrerequisites
    {
        @Override
        public List<String> getPrerequisitesClassNames()
        {
            return FastList.newListWith(LewContract.class.getName());
        }

        @Override
        public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
        {
            return StockFinder.all();
        }

        @Override
        public boolean isBusinessDateInvariant()
        {
            return true;
        }
    }
}