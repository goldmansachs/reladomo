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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderManagerImpl;
import com.gs.fw.common.mithra.cacheloader.CacheLoaderMonitor;
import com.gs.fw.common.mithra.cacheloader.RefreshInterval;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.ProjitoAddressFinder;
import com.gs.fw.common.mithra.test.domain.ProjitoEmployeeFinder;
import com.gs.fw.common.mithra.test.domain.ProjitoFinder;
import com.gs.fw.common.mithra.test.domain.ProjitoMeasureOfSuccessFinder;
import com.gs.fw.common.mithra.test.domain.ProjitoMembershipFinder;
import com.gs.fw.common.mithra.test.domain.ProjitoVersionFinder;
import junit.framework.TestCase;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.List;

public class CacheLoaderManagerProcessingOnlyTest extends TestCase
{
    private static final UnifiedSet<String> NYK_TIME_ZONES = UnifiedSet.newSetWith("America/New_York", "US/Eastern", "EST", "EST5EDT");

    private static final Timestamp INITIAL_LOAD_TIME = new Timestamp(System.currentTimeMillis() - 120 * 1000);

    private static final Timestamp T1 = Timestamp.valueOf("1999-12-31 00:00:00.0");
    private static final Timestamp T2 = Timestamp.valueOf("2000-02-01 12:00:00.0");
    private static final Timestamp T3 = Timestamp.valueOf("2000-03-01 12:00:00.0");

    private static final String PROCESSING_ONLY_TEST_CONFIG =
        "<CacheLoader defaultTopLevelLoaderFactory=\"com.gs.fw.common.mithra.cacheloader.ProcessingDateMilestonedTopLevelLoaderFactory\"\n" +
        "             defaultDependentLoaderHelperFactory=\"com.gs.fw.common.mithra.cacheloader.ProcessingDateMilestonedTopLevelLoaderFactory\">\n" +
        "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.Projito\"/>\n" +
        "        <TopLevelLoader classToLoad=\"com.gs.fw.common.mithra.test.domain.ProjitoMeasureOfSuccess\"/>\n" +
        "</CacheLoader>";

    private MithraTestResource mithraTestResource;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        assertTrue("The refresh tests mean to run in EST. They will fail in different timezone. Set timezone on the EST on the workstation.", NYK_TIME_ZONES.contains(java.util.TimeZone.getDefault().getID()));

        this.mithraTestResource = new MithraTestResource("MithraCacheTestProcessingOnlyConfig.xml");
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("main");
        this.mithraTestResource.createSingleDatabase(connectionManager, "main", "testdata/cacheloader/CacheLoaderTest_ProcessingOnly.txt");
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

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(PROCESSING_ONLY_TEST_CONFIG));

        CacheLoaderMonitor loaderMonitor = new CacheLoaderMonitor();
        cacheLoaderManager.runInitialLoad(Lists.mutable.<Timestamp>of(), INITIAL_LOAD_TIME, loaderMonitor);

        assertListContainsExactly(ProjitoFinder.findMany(ProjitoFinder.processingDate().equalsEdgePoint()), ProjitoFinder.id(), 1L, 2L, 3L);
        assertListContainsExactly(ProjitoMeasureOfSuccessFinder.findMany(ProjitoMeasureOfSuccessFinder.processingDate().equalsEdgePoint()), ProjitoMeasureOfSuccessFinder.id(), 1L, 2L, 2L, 3L);

        assertLoaderMonitorTaskSizes(loaderMonitor, 2, 0);
    }

    public void testRefresh()
    {
        assertCacheEmpty();

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(createStringInputStream(PROCESSING_ONLY_TEST_CONFIG));

        cacheLoaderManager.runRefresh(Lists.mutable.<Timestamp>of(), new RefreshInterval(T1, T2), new CacheLoaderMonitor());

        assertEquals(2, ProjitoFinder.findMany(ProjitoFinder.processingDate().equalsEdgePoint()).size());
        assertEquals(2, ProjitoMeasureOfSuccessFinder.findMany(ProjitoMeasureOfSuccessFinder.processingDate().equalsEdgePoint()).size());

        cacheLoaderManager.runRefresh(Lists.mutable.<Timestamp>of(), new RefreshInterval(T2, T3), new CacheLoaderMonitor());

        assertEquals(3, ProjitoFinder.findMany(ProjitoFinder.processingDate().equalsEdgePoint()).size());
        assertEquals(4, ProjitoMeasureOfSuccessFinder.findMany(ProjitoMeasureOfSuccessFinder.processingDate().equalsEdgePoint()).size());
    }

    private static void assertCacheEmpty()
    {
        assertEquals(0, ProjitoFinder.findMany(ProjitoFinder.all()).count());
        assertEquals(0, ProjitoVersionFinder.findMany(ProjitoVersionFinder.all()).count());
        assertEquals(0, ProjitoMeasureOfSuccessFinder.findMany(ProjitoMeasureOfSuccessFinder.all()).count());
        assertEquals(0, ProjitoMembershipFinder.findMany(ProjitoMembershipFinder.all()).count());
        assertEquals(0, ProjitoEmployeeFinder.findMany(ProjitoEmployeeFinder.all()).count());
        assertEquals(0, ProjitoAddressFinder.findMany(ProjitoAddressFinder.all()).count());
    }

    private static InputStream createStringInputStream(String testConfiguration)
    {
        return new ByteArrayInputStream(testConfiguration.getBytes());
    }

    private static <E, T> void assertListContainsExactly(List<E> list, Attribute<E, T> attribute, T... items)
    {
        MutableList<T> actualValues = Lists.mutable.of();
        for (E e: list)
        {
            actualValues.add(attribute.valueOf(e));
        }

        assertEquals(items.length, actualValues.size());
        for (T item : items)
        {
            assertTrue("Expected to contain " + item + " but contains: " + actualValues.toString(), actualValues.contains(item));
        }
    }

    private static void assertLoaderMonitorTaskSizes(CacheLoaderMonitor loaderMonitor, int numberOfExpectedLoadingTasks, int numberOfExpectedDependentTasks)
    {
        assertNull(loaderMonitor.getException());
        assertEquals("" + loaderMonitor.getLoadingTaskStates(), numberOfExpectedLoadingTasks, loaderMonitor.getLoadingTaskStates().size());
        assertEquals("" + loaderMonitor.getDependentKeyIndexMonitors(), numberOfExpectedDependentTasks, loaderMonitor.getDependentKeyIndexMonitors().size());
    }
}
