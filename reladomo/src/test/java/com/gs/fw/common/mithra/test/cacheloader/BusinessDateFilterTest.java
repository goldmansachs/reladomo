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
import com.gs.fw.common.mithra.cacheloader.CacheLoaderManagerImpl;
import com.gs.fw.common.mithra.cacheloader.FullyMilestonedTopLevelLoaderFactory;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.glew.*;
import com.gs.fw.common.mithra.util.BooleanFilter;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.util.KeepOnlySpecifiedDatesFilter;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.sql.Timestamp;
import java.util.Map;



public class BusinessDateFilterTest extends TestCase
{
    private static final Timestamp DATE1 = Timestamp.valueOf("2011-01-01 23:59:00.0");
    private static final Timestamp DATE2 = Timestamp.valueOf("2011-01-02 23:59:00.0");
    private static final Timestamp DATE3 = Timestamp.valueOf("2011-01-03 23:59:00.0");
    private static final Timestamp DATE4 = Timestamp.valueOf("2011-01-04 23:59:00.0");
    private static final String LEWCOTRACT_CLASS = LewContract.class.getName();
    private static final String SCRP_CLASS = GlewScrp.class.getName();
    private MithraTestResource mithraTestResource;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        this.mithraTestResource = new MithraTestResource("MithraCacheTestConfig.xml");
        this.mithraTestResource.setUp();
    }

//    @Override
    protected void tearDown() throws Exception
    {
        this.mithraTestResource.tearDown();
        this.mithraTestResource = null;
        super.tearDown();
    }

    public void testFilter()
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"" + LEWCOTRACT_CLASS + "\" sourceAttributes=\"NYK,LN2\"/>\n" +
                "        <DependentLoader relationship=\"" + LEWCOTRACT_CLASS + ".scrp\"/>\n" +
                "</CacheLoader>";

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(new ByteArrayInputStream(testConfiguration.getBytes()));

        Map<String, BooleanFilter> map = cacheLoaderManager.createCacheFilterOfDatesToKeep(FastList.newListWith(DATE1, DATE2));

        Filter filter = map.get(LEWCOTRACT_CLASS);
        assertFalse(filter.matches(buildContract(DATE1, DATE2)));
        assertFalse(filter.matches(buildContract(DATE2, DATE3)));
        assertTrue(filter.matches(buildContract(DATE3, DATE4)));

        filter = map.get(SCRP_CLASS);
        assertFalse(filter.matches(buildScrp(DATE1, DATE2)));
        assertTrue(filter.matches(buildScrp(DATE3, DATE4)));
    }

    public void testDateTransformation() // i.e. PME
    {
        String testConfiguration = "<CacheLoader>\n" +
                "        <TopLevelLoader classToLoad=\"" + LEWCOTRACT_CLASS + "\" sourceAttributes=\"LN1\"/>\n" +
                "\n" +
                "        <TopLevelLoader classToLoad=\"" + LEWCOTRACT_CLASS + "\" sourceAttributes=\"NYK,LN2\"\n" +
                "              factoryClass=\"" + DateShiftingLoaderFactory.class.getName() + "\"/>\n" +
                "</CacheLoader>";

        CacheLoaderManagerImpl cacheLoaderManager = new CacheLoaderManagerImpl();
        cacheLoaderManager.loadConfiguration(new ByteArrayInputStream(testConfiguration.getBytes()));

        Map<String, BooleanFilter> map = cacheLoaderManager.createCacheFilterOfDatesToKeep(FastList.newListWith(DATE1, DATE2));

        Filter filter = map.get(LEWCOTRACT_CLASS);
        assertFalse(filter.matches(buildContract(DATE1, DATE2)));
        assertFalse(filter.matches(buildContract(DATE2, DATE3)));
        assertFalse(filter.matches(buildContract(DATE3, DATE4)));

        map = cacheLoaderManager.createCacheFilterOfDatesToKeep(FastList.newListWith(DATE1));
        filter = map.get(LEWCOTRACT_CLASS);
        assertFalse(filter.matches(buildContract(DATE1, DATE2)));
        assertTrue(filter.matches(buildContract(DATE2, DATE3)));
        assertFalse(filter.matches(buildContract(DATE3, DATE4)));
    }

    public static class DateShiftingLoaderFactory extends FullyMilestonedTopLevelLoaderFactory
    {
        public BooleanFilter createCacheFilterOfDatesToDrop(Timestamp businessDate)
        {
            return new KeepOnlySpecifiedDatesFilter(LewContractFinder.businessDate(), FastList.newListWith(DATE3));
        }
    }

    private LewContractData buildContract (Timestamp from, Timestamp to)
    {
        LewContractData contract = LewContractDatabaseObject.allocateOnHeapData();
        contract.setBusinessDateFrom(from);
        contract.setBusinessDateTo(to);
        contract.setProcessingDateFrom(from);
        contract.setProcessingDateTo(DefaultInfinityTimestamp.getDefaultInfinity());

        return contract;
    }
    private GlewScrpData buildScrp (Timestamp from, Timestamp to)
    {
        GlewScrpData scrp = GlewScrpDatabaseObject.allocateOnHeapData();
        scrp.setBusinessDateFrom(from);
        scrp.setBusinessDateTo(to);
        scrp.setProcessingDateFrom(from);
        scrp.setProcessingDateTo(DefaultInfinityTimestamp.getDefaultInfinity());

        return scrp;
    }
}
