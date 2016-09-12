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
import com.gs.fw.common.mithra.cacheloader.*;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.glew.LewContract;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.util.Filter;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.List;


public class FullyMilestonedTopLevelLoaderFactoryTest extends TestCase
{
    private static final Timestamp BUSINESS_DATE = Timestamp.valueOf("2010-10-30 23:59:00");
    private static final Timestamp OLD_BUSINESS_DATE = Timestamp.valueOf("1980-11-30 23:59:00.0");

    private static final Timestamp time1 = Timestamp.valueOf("2010-01-03 02:51:00.0");
    private static final Timestamp time2 = Timestamp.valueOf("2010-01-03 03:01:00.0");

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

    public void testInitialLoad()
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        FullyMilestonedTopLevelLoaderFactory factory = new FullyMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(LewContract.class.getName());
        factory.setSourceAttributes(FastList.newListWith(NYK_REGION));
        cacheLoaderManager.zGetTopLevelLoaderFactories().add(factory);

        final Timestamp initialLoadEndTime = Timestamp.valueOf("2010-01-03 04:00:00.0");
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());
        assertEquals(3, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
    }

    public void testInitialLoadOnlyPreStartTimeInz()
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        FullyMilestonedTopLevelLoaderFactory factory = new FullyMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(LewContract.class.getName());
        factory.setSourceAttributes(FastList.newListWith(NYK_REGION));
        cacheLoaderManager.zGetTopLevelLoaderFactories().add(factory);

        final Timestamp initialLoadEndTime = Timestamp.valueOf("2010-01-03 03:07:00.0");
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());
        List<LewContract> results = LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint()));
        assertEquals(2, results.size());
        for (LewContract each : results)
        {
            assertTrue(each.getProcessingDateFrom().before(initialLoadEndTime));
        }
    }

    public void testRefresh()
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        FullyMilestonedTopLevelLoaderFactory factory = new FullyMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(LewContract.class.getName());
        factory.setSourceAttributes(FastList.newListWith(NYK_REGION));
        cacheLoaderManager.zGetTopLevelLoaderFactories().add(factory);

        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), new RefreshInterval(time1, time2), new CacheLoaderMonitor());

        assertEquals(2, LewContractFinder.findMany(LewContractFinder.businessDate().equalsEdgePoint().and(LewContractFinder.processingDate().equalsEdgePoint())).size());
    }

    public void testLoadOperation()
    {
        FullyMilestonedTopLevelLoaderFactory factory = new FullyMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(LewContract.class.getName());

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        CacheLoaderContext context = new CacheLoaderContext(cacheLoaderManager, FastList.newListWith(BUSINESS_DATE));
        assertEquals("LewContract.processingDate equalsEdgePoint & LewContract.businessDate = \"2010-10-30 23:59:00.0\"",
                factory.buildLoadOperation(new DateCluster(BUSINESS_DATE), context).toString());
    }

    public void testRefreshOperation()
    {
        AbstractLoaderFactory factory = new FullyMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(LewContract.class.getName());

        CacheLoaderContext context = new CacheLoaderContext(new CacheLoaderManagerImpl(), FastList.newListWith(BUSINESS_DATE));
        context.setRefreshInterval(new RefreshInterval(time1, time2));

        List<TaskOperationDefinition> refreshOperations = factory.buildRefreshTaskDefinitions(context, factory.buildLoadOperation(new DateCluster(BUSINESS_DATE), context));
        assertEquals(2, refreshOperations.size());
        assertEquals("LewContract.processingDate equalsEdgePoint & LewContract.businessDate = \"2010-10-30 23:59:00.0\" & LewContract.processingDateFrom >= \"2010-01-03 02:51:00.0\" & LewContract.processingDateFrom <= \"2010-01-03 03:01:00.0\"",
                String.valueOf(refreshOperations.get(0).getOperation()));
        assertEquals("LewContract.processingDate equalsEdgePoint & LewContract.businessDate = \"2010-10-30 23:59:00.0\" & LewContract.processingDateTo >= \"2010-01-03 02:51:00.0\" & LewContract.processingDateTo <= \"2010-01-03 03:01:00.0\"",
                String.valueOf(refreshOperations.get(1).getOperation()));
    }

    public void testDateFilterToKeep()
    {
        this.testInitialLoad(); // loads data
        FullyMilestonedTopLevelLoaderFactory factory = new FullyMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(LewContract.class.getName());

        assertEquals(3, LewContractFinder.getMithraObjectPortal().getCache().size());

        Filter filter = factory.createCacheFilterOfDatesToDrop(BUSINESS_DATE);
        LewContractFinder.getMithraObjectPortal().getCache().removeAll(filter);
        assertEquals(3, LewContractFinder.getMithraObjectPortal().getCache().size());

        filter = factory.createCacheFilterOfDatesToDrop(OLD_BUSINESS_DATE);
        LewContractFinder.getMithraObjectPortal().getCache().removeAll(filter);
        assertEquals(0, LewContractFinder.getMithraObjectPortal().getCache().size());
    }
}
