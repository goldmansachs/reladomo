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


import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.cacheloader.AdditionalOperationBuilder;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderConfig;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderContext;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderManagerImpl;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderMonitor;
import com.gs.fw.common.mithra.cacheloader.ConfigParameter;
import com.gs.fw.common.mithra.cacheloader.DateCluster;
import com.gs.fw.common.mithra.cacheloader.LoadOperationBuilder;
import com.gs.fw.common.mithra.cacheloader.ProcessingDateMilestonedTopLevelLoaderFactory;
import com.gs.fw.common.mithra.cacheloader.RefreshInterval;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.DatedEntity;
import com.gs.fw.common.mithra.test.domain.DatedEntityFinder;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.sql.Timestamp;
import java.util.List;


public class ProcessingDateMilestonedTopLevelLoaderFactoryTest extends TestCase
{
    private static final Timestamp BUSINESS_DATE = Timestamp.valueOf("2010-10-30 23:59:00");
    private static final Timestamp time1 = Timestamp.valueOf("2010-01-03 03:01:00.0");
    private static final Timestamp time2 = Timestamp.valueOf("2010-01-03 03:11:00.0");

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
        ProcessingDateMilestonedTopLevelLoaderFactory factory = new ProcessingDateMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(DatedEntity.class.getName());
        factory.setSourceAttributes(CacheLoaderConfig.getNoSourceAttributeList());
        cacheLoaderManager.zGetTopLevelLoaderFactories().add(factory);

        final Timestamp initialLoadEndTime = Timestamp.valueOf("2010-01-03 04:10:00.0");
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());
        assertEquals(3, DatedEntityFinder.findMany(DatedEntityFinder.processingDate().equalsEdgePoint()).size());
    }

    public void testInitialLoadOnlyPreStartTimeInz()
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        ProcessingDateMilestonedTopLevelLoaderFactory factory = new ProcessingDateMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(DatedEntity.class.getName());
        factory.setSourceAttributes(CacheLoaderConfig.getNoSourceAttributeList());
        cacheLoaderManager.zGetTopLevelLoaderFactories().add(factory);

        final Timestamp initialLoadEndTime = Timestamp.valueOf("2010-01-03 03:07:00.0");
        cacheLoaderManager.runInitialLoad(FastList.newListWith(BUSINESS_DATE), initialLoadEndTime, new CacheLoaderMonitor());
        List<DatedEntity> results = DatedEntityFinder.findMany(DatedEntityFinder.processingDate().equalsEdgePoint());

        assertEquals(2, results.size());
        for (DatedEntity each : results)
        {
            assertTrue(each.getProcessingDateFrom().before(initialLoadEndTime));
        }
    }

    public void testRefreshOperation()
    {
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        ProcessingDateMilestonedTopLevelLoaderFactory factory = new ProcessingDateMilestonedTopLevelLoaderFactory();
        factory.setClassToLoad(DatedEntity.class.getName());
        factory.setSourceAttributes(CacheLoaderConfig.getNoSourceAttributeList());
        cacheLoaderManager.zGetTopLevelLoaderFactories().add(factory);

        cacheLoaderManager.runRefresh(FastList.newListWith(BUSINESS_DATE), new RefreshInterval(time1, time2), new CacheLoaderMonitor());
        assertEquals(1, DatedEntityFinder.findMany(DatedEntityFinder.processingDate().equalsEdgePoint()).size());
    }

    public void testAdditionalOperation()
    {
        ProcessingDateMilestonedTopLevelLoaderFactory factory = new ProcessingDateMilestonedTopLevelLoaderFactory();
        factory.setParams(FastList.newListWith(new ConfigParameter("operationBuilder", MockOperationBuilder.class.getName())));
        factory.setClassToLoad(DatedEntity.class.getName());

        DateCluster dateCluster = new DateCluster(BUSINESS_DATE);
        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        CacheLoaderContext context = new CacheLoaderContext(cacheLoaderManager, FastList.newListWith(BUSINESS_DATE));
        LoadOperationBuilder loadOperationBuilder = new LoadOperationBuilder(factory.buildLoadOperation(dateCluster, context),
                FastList.<AdditionalOperationBuilder>newListWith(new MockOperationBuilder()),
                dateCluster, factory.getClassController().getFinderInstance());

        assertEquals("DatedEntity.id = 2 & DatedEntity.processingDate equalsEdgePoint",
                loadOperationBuilder.build("").toString());
    }

    public static class MockOperationBuilder implements AdditionalOperationBuilder
    {
        public Operation buildOperation(Timestamp businessDate, RelatedFinder relatedFinder)
        {
            return ((IntegerAttribute) relatedFinder.getAttributeByName("id")).eq(2);
        }

        @Override
        public boolean isBusinessDateInvariant()
        {
            return true;
        }
    }
}
