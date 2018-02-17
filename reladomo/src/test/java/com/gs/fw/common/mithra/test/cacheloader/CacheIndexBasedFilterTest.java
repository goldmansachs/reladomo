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
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cacheloader.CacheIndexBasedFilter;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.Stock;
import com.gs.fw.common.mithra.test.domain.StockData;
import com.gs.fw.common.mithra.test.domain.StockDatabaseObject;
import com.gs.fw.common.mithra.test.domain.StockFinder;
import com.gs.fw.common.mithra.test.domain.StockPrice;
import com.gs.fw.common.mithra.test.domain.StockPriceFinder;
import com.gs.fw.common.mithra.test.glew.LewContractData;
import com.gs.fw.common.mithra.test.glew.LewContractDatabaseObject;
import com.gs.fw.common.mithra.test.glew.LewContractFinder;
import com.gs.fw.common.mithra.test.glew.LewTransaction;
import com.gs.fw.common.mithra.test.glew.LewTransactionFinder;
import junit.framework.TestCase;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.sql.Timestamp;
import java.util.Map;


public class CacheIndexBasedFilterTest extends TestCase
{
    private static final Timestamp BUSINESS_DATE1 = Timestamp.valueOf("2010-10-30 23:59:00");
    private static final Timestamp BUSINESS_DATE2 = Timestamp.valueOf("2011-10-30 23:59:00");
    private static final Timestamp BUSINESS_DATE3 = Timestamp.valueOf("2013-10-30 23:59:00");

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

    public void testContainsWithCompleteIndex() throws Exception
    {
        Cache cache = StockFinder.getMithraObjectPortal().getCache();

        StockData data = StockDatabaseObject.allocateOnHeapData();
        data.setStockId(3);
        data.setBusinessDateFrom(BUSINESS_DATE1);
        data.setBusinessDateTo(BUSINESS_DATE2);
        data.setStockPriceId(103);
        cache.putDatedData(data);
        data = StockDatabaseObject.allocateOnHeapData();
        data.setStockId(3);
        data.setBusinessDateFrom(BUSINESS_DATE2);
        data.setBusinessDateTo(BUSINESS_DATE3);
        data.setStockPriceId(103);
        cache.putDatedData(data);

        Map<Attribute, Attribute> attributeMap1 = UnifiedMap.<Attribute, Attribute>newWithKeysValues(
                StockFinder.stockPriceId(), StockFinder.stockPriceId(),
                StockFinder.businessDate(), StockFinder.businessDate()
        );

        Stock keyHolder1 = new Stock(BUSINESS_DATE2);
        keyHolder1.setBusinessDateFrom(BUSINESS_DATE2);
        keyHolder1.setBusinessDateTo(BUSINESS_DATE3);
        keyHolder1.setStockPriceId(103);
        assertTrue(CacheIndexBasedFilter.create(cache, attributeMap1, null, "desc").matches(keyHolder1));

        Map<Attribute, Attribute> attributeMap2 = UnifiedMap.<Attribute, Attribute>newWithKeysValues(
                StockFinder.stockPriceId(), StockPriceFinder.stockPriceId(),
                StockFinder.businessDate(), StockPriceFinder.businessDate()
        );
        StockPrice keyHolder2 = new StockPrice(BUSINESS_DATE1);
        keyHolder2.setStockPriceId(103);

        assertTrue(CacheIndexBasedFilter.create(cache, attributeMap2, null, "desc").matches(keyHolder2));
    }

    public void testMatchesPKSuperset() throws Exception
    {
        Cache cache = LewContractFinder.getMithraObjectPortal().getCache();
        Map<Attribute, Attribute> relationshipAttributes = UnifiedMap.newMap();

        relationshipAttributes.put(LewContractFinder.businessDate(), LewTransactionFinder.businessDate());
        relationshipAttributes.put(LewContractFinder.acctId(), LewTransactionFinder.acctId());
        relationshipAttributes.put(LewContractFinder.instrumentId(), LewTransactionFinder.instrumentId());
        relationshipAttributes.put(LewContractFinder.region(), LewTransactionFinder.region());
        relationshipAttributes.put(LewContractFinder.processingDate(), LewTransactionFinder.processingDate());

        LewContractData data = LewContractDatabaseObject.allocateOnHeapData();
        data.setInstrumentId(3);
        data.setAcctId(7);
        data.setBusinessDateFrom(BUSINESS_DATE1);

        data.setProcessingDateFrom(BUSINESS_DATE2);
        data.setProcessingDateTo(BUSINESS_DATE3);
        data.setRegion("NYK");
        cache.putDatedData(data);


        data = LewContractDatabaseObject.allocateOnHeapData();
        data.setInstrumentId(3);
        data.setAcctId(7);
        data.setBusinessDateFrom(BUSINESS_DATE2);
        data.setBusinessDateTo(BUSINESS_DATE3);
        data.setRegion("NYK");
        cache.putDatedData(data);

        LewTransaction keyHolder1 = new LewTransaction(BUSINESS_DATE2, BUSINESS_DATE2);
        keyHolder1.setBusinessDateFrom(BUSINESS_DATE2);
        keyHolder1.setBusinessDateTo(BUSINESS_DATE3);
        keyHolder1.setProcessingDateFrom(BUSINESS_DATE2);
        keyHolder1.setProcessingDateTo(BUSINESS_DATE3);
        keyHolder1.setAcctId(7);
        keyHolder1.setInstrumentId(3);
        keyHolder1.setRegion("NYK");
        assertTrue(CacheIndexBasedFilter.create(cache, relationshipAttributes, null, "desc").matches(keyHolder1));
    }
}
