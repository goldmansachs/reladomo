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

package com.gs.fw.common.mithra.test.cacheloader;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.cacheloader.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.glew.GlewScrpFinder;
import com.gs.fw.common.mithra.test.glew.LewContract;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.test.glew.LewProduct;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.sql.Timestamp;


public class DependentLoaderFactoryTest extends TestCase
{
    private static final String TOPLEVEL_CLASS = LewProduct.class.getName();
    private static final String RELATIONSHIP_DEFINITION = TOPLEVEL_CLASS + ".relationshipWithLeftFitler";
    private static final Timestamp BUSINESS_DATE = Timestamp.valueOf("2011-01-03 23:59:00.0");
    private MithraTestResource mithraTestResource;
    private static final String NYK_REGION = "NYK";

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        this.mithraTestResource = new MithraTestResource("MithraCacheTestConfig.xml");
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("nystlew");
        this.mithraTestResource.createSingleDatabase(connectionManager, "nystlew", "testdata/cacheloader/CacheLoaderTest_STLEW.txt");

        this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, NYK_REGION, "testdata/cacheloader/CacheLoaderTest_NYLEW.txt");
        this.mithraTestResource.setUp();
    }

    @Override
    protected void tearDown() throws Exception
    {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
        super.tearDown();
    }

    public void testSetupRelationship()
    {
        DependentLoaderFactoryImpl factory = new DependentLoaderFactoryImpl();
        factory.setRelationship(RELATIONSHIP_DEFINITION);
        assertTrue(factory.toString(), factory.toString().contains("LewProduct.role = 7"));
    }

    public void testRegionAttributeLogic()
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK,LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\"/>\n" +
                "</CacheLoader>";
        UnifiedSet<String> expected = UnifiedSet.newSetWith("NYK", "LN2");

        this.assertRegionsOnDependent(testConfiguration, expected, 0);
    }

    public void testRegionAttributeLogicForManyTopLevelLoaders()
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK,LN2\"/>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"TKO\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\"/>\n" +
                "</CacheLoader>";
        UnifiedSet<String> expected = UnifiedSet.newSetWith("NYK", "LN2", "TKO");

        this.assertRegionsOnDependent(testConfiguration, expected, 0);
    }

    public void testRegionAttributeLogicForHardcoded()
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK,LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\"" +
                "               sourceAttributes=\"TKO\"/>\n" +
                "</CacheLoader>";
        UnifiedSet<String> expected = UnifiedSet.newSetWith("TKO");

        this.assertRegionsOnDependent(testConfiguration, expected, 0);
    }

    public void testRegionAttributeLogicForGraphLoops()
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK,LN2\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.product\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewProduct.relationshipWithLeftFitler\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewRelationship.underlier\"/>\n" +
                "</CacheLoader>";
        UnifiedSet<String> expected = UnifiedSet.newSetWith("NYK", "LN2");

        this.assertRegionsOnDependent(testConfiguration, expected, 0);
        this.assertRegionsOnDependent(testConfiguration, expected, 1);
        this.assertRegionsOnDependent(testConfiguration, expected, 2);
    }

    public void testSameDependentTwice()
    {
        String mockClass = MockOperationBuilder.class.getName();
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\">" +
                "               <Param name=\"operationBuilder\" value=\"" + mockClass + "(aaa)\"/>" +
                "        </DependentLoader>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrp\">" +
                "               <Param name=\"operationBuilder\" value=\"" + mockClass + "(bbb)\"/>" +
                "        </DependentLoader>\n" +
                "</CacheLoader>";

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(new ByteArrayInputStream(testConfiguration.getBytes()));
        CacheLoaderMonitor monitor = new CacheLoaderMonitor();
        final Timestamp initialLoadEndTime = new Timestamp(System.currentTimeMillis() - 120 * 1000);
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, monitor);
        assertEquals(1, monitor.getLoadingTaskStates().size());
        assertEquals(2, monitor.getDependentKeyIndexMonitors().size());
        assertEquals(2, GlewScrpFinder.findMany(GlewScrpFinder.businessDate().equalsEdgePoint().and(GlewScrpFinder.processingDate().equalsEdgePoint())).size());

        GlewScrpFinder.getMithraObjectPortal().getCache().clear();
        GlewScrpFinder.clearQueryCache();
        monitor = new CacheLoaderMonitor();
        RefreshInterval refreshInterval = new RefreshInterval(Timestamp.valueOf("2010-01-03 03:00:30.0"), Timestamp.valueOf("2010-01-03 03:01:30.0"));
        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), refreshInterval, monitor);
        assertEquals(6, monitor.getLoadingTaskStates().size());
        assertEquals(2, monitor.getDependentKeyIndexMonitors().size());
        assertEquals("refresh must pick up new dependents for existing owners",
                2, GlewScrpFinder.findMany(GlewScrpFinder.businessDate().equalsEdgePoint().and(GlewScrpFinder.processingDate().equalsEdgePoint())).size());
    }

    public static class MockOperationBuilder implements AdditionalOperationBuilder
    {
        private String param;

        public MockOperationBuilder(String param)
        {
            this.param = param;
        }

        @Override
        public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
        {
            return GlewScrpFinder.name().eq(param);
        }

        @Override
        public boolean isBusinessDateInvariant()
        {
            return true;
        }
    }

    public void testTwoDependenciesWithDifferentOwnerFilters()
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.glew.LewContract\" sourceAttributes=\"NYK\"/>\n" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrpSynonymA\"/>" +
                "        <DependentLoader relationship=\"com.gs.fw.common.mithra.test.glew.LewContract.scrpSynonymB\"/>" +
                "</CacheLoader>";

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(new ByteArrayInputStream(testConfiguration.getBytes()));
        CacheLoaderMonitor monitor = new CacheLoaderMonitor();
        final Timestamp initialLoadEndTime = new Timestamp(System.currentTimeMillis() - 120 * 1000);
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, monitor);
        assertEquals(1, monitor.getLoadingTaskStates().size());
        assertEquals(1, monitor.getDependentKeyIndexMonitors().size());
        assertEquals(3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
        assertEquals(2, GlewScrpFinder.findMany(GlewScrpFinder.businessDate().equalsEdgePoint().and(GlewScrpFinder.processingDate().equalsEdgePoint())).size());

        GlewScrpFinder.getMithraObjectPortal().getCache().clear();
        GlewScrpFinder.clearQueryCache();
        monitor = new CacheLoaderMonitor();
        RefreshInterval refreshInterval = new RefreshInterval(Timestamp.valueOf("2010-01-03 03:00:30.0"), Timestamp.valueOf("2010-01-03 03:01:30.0"));
        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), refreshInterval, monitor);
        assertEquals(6, monitor.getLoadingTaskStates().size());
        assertEquals(1, monitor.getDependentKeyIndexMonitors().size());
        assertEquals("refresh must pick up new dependents for existing owners",
                2, GlewScrpFinder.findMany(GlewScrpFinder.businessDate().equalsEdgePoint().and(GlewScrpFinder.processingDate().equalsEdgePoint())).size());
    }

    public void testDetectBadRelationshipAttributes()
    {
        DependentLoaderFactoryImpl factory = new DependentLoaderFactoryImpl();
        final FullyMilestonedTopLevelLoaderFactory helperFactory = new FullyMilestonedTopLevelLoaderFactory();
        helperFactory.setClassToLoad(LewContract.class.getName());
        factory.setHelperFactory(helperFactory);
        this.assertValidationFailed(factory, ".productWithLeftRegion");
        this.assertValidationFailed(factory, ".productWithRightRegion");
        this.assertValidationFailed(factory, ".productWithRightAsOfAttribute");
        this.assertValidationFailed(factory, ".productWithLeftAsOfAttribute");
    }

    private void assertValidationFailed(DependentLoaderFactoryImpl factory, String relationship)
    {
        try
        {
            factory.setRelationship(LewContract.class.getName() + relationship);
            fail();
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().contains("cannnot handle"));
        }
    }

    private void assertRegionsOnDependent(String testConfiguration, UnifiedSet<String> expected, int factoryIndex)
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(new ByteArrayInputStream(testConfiguration.getBytes()));
        DependentLoaderFactory factory = cacheLoaderManager.zGetDependentSetLoaderFactories().get(factoryIndex);
        assertEquals(expected, UnifiedSet.newSet(factory.getSourceAttributes()));
    }
}