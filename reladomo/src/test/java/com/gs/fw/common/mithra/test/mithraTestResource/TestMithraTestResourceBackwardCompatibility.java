
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

package com.gs.fw.common.mithra.test.mithraTestResource;

import junit.framework.TestCase;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;
import java.sql.Timestamp;

public class TestMithraTestResourceBackwardCompatibility extends TestCase
{

    public void testSingleDatabase() throws Exception
    {
        String configFile= "mithraTestResource/MtrTestBaseRuntimeConfig.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForBaseConfigTests.getInstance();
        connectionManager.setDefaultSource("base_config_db");
        mtr1.createSingleDatabase(connectionManager, "base_config_db", "testdata/mithraTestResource/mtrTestDataForSingleDb.txt");
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(5, size);

        mtr1.tearDown();
    }

    public void testDatabaseForStringSourceaAttribute() throws Exception
    {
        String configFile= "mithraTestResource/MtrTestDeskRuntimeConfig.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        mtr1.createDatabaseForStringSourceAttribute(connectionManager,"A", "desk_db", "testdata/mithraTestResource/mtrTestDataForStringSource.txt");
        mtr1.setUp();

        Operation op = TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()));
        op = op.and(TinyBalanceFinder.balanceId().eq(10));
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));
        TinyBalance balance = TinyBalanceFinder.findOne(op);
        assertEquals(200, balance.getQuantity(), 0);
        mtr1.tearDown();
    }

    public void testDatabaseForIntSourceaAttribute() throws Exception
    {
        String configFile= "mithraTestResource/MtrTestDeskRuntimeConfig.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        mtr1.createDatabaseForIntSourceAttribute(connectionManager,1, "desk_db", "testdata/mithraTestResource/mtrTestDataForIntSource.txt");
        mtr1.setUp();

        Operation op = LocationFinder.all();
        op = op.and(LocationFinder.sourceId().eq(1));

        LocationList list = new LocationList(op);
        assertEquals(7, list.size());
        mtr1.tearDown();
    }

    public void testTest() throws Exception
    {
        BaseDomainTestResource testResource = new BaseDomainTestResource();
        testResource.setUp();

        MithraTestResource mtr2 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfig.xml");

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("test_db1");
        mtr2.createSingleDatabase(connectionManager, "test_db1", "testdata/mithraTestResource/mtrTestDataForMultiSingleDb.txt");
        mtr2.setUp();

        Operation orderOp = OrderFinder.all();
        OrderList orderList = new OrderList(orderOp);
        assertEquals(4, orderList.size());

        Operation sellerOp = SellerFinder.all();
        SellerList sellerList = new SellerList(sellerOp);
        assertEquals(4, sellerList.size());

        testResource.tearDown();
        mtr2.tearDown();
    }

    public void testTest2() throws Exception
    {
        BaseDomainTestResource testResource = new BaseDomainTestResource();
        testResource.setUp();

        MithraTestResource mtr2 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfig.xml");

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("test_db1");
        mtr2.createSingleDatabase(connectionManager, "test_db1", "testdata/mithraTestResource/mtrTestDataForMultiSingleDb.txt");
        mtr2.setUp();

        Operation orderOp = OrderFinder.all();
        OrderList orderList = new OrderList(orderOp);
        assertEquals(4, orderList.size());

        Operation prodOp = ProductSpecificationFinder.all();
        ProductSpecificationList prodList = new ProductSpecificationList(prodOp);
        assertEquals(1, prodList.size());

        testResource.tearDown();
        mtr2.tearDown();

    }


//    public void testTest3() throws Exception
//    {
//        MithraTestResource mtr = new MithraTestResource("mithraTestResource/MtrTestDeskAndSingleRuntimeConfig.xml");
//
//        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
//        connectionManager.setDefaultSource("test_db1");
//        mtr.createDatabaseForStringSourceAttribute(connectionManager, "test_db1","test_db1","mithraTestResource/mtrTestDataDeskAndSingleDbUsedForSingleDb.txt");
//        mtr.createSingleDatabase(connectionManager, "test_db1", "mithraTestResource/mtrTestDataDeskAndSingleDbUsedForDesk.txt");
//        mtr.setUp();
//
//    }


    private static class BaseDomainTestResource
    {

        private MithraTestResource mtr1;

        public void setUp() throws Exception
        {
            setUpMithraTestResource();
            createProduct(100, "100abc", "Prod 100", 19.99);
        }

        private void setUpMithraTestResource()
                throws Exception
        {
            String configFile= "mithraTestResource/MtrTestBaseRuntimeConfig.xml";
            mtr1 = new MithraTestResource(configFile);
            mtr1.setDeleteOnCreate(false);
            ConnectionManagerForTests baseConnectionManager = ConnectionManagerForBaseConfigTests.getInstance();
            baseConnectionManager.setDefaultSource("base_config_db");
            mtr1.createSingleDatabase(baseConnectionManager, "base_config_db", null);
            mtr1.setUp();
        }

        public void tearDown()
        {
            if(mtr1 != null)
            {
                mtr1.tearDown();
            }
        }

        private void createProduct(int prodId, String prodCode, String prodName, double prodPrice)
        {
            ProductSpecification productSpecs = new ProductSpecification();
            productSpecs.setProductId(prodId);
            productSpecs.setProductCode(prodCode);
            productSpecs.setProductName(prodName);
            productSpecs.setOriginalPrice(prodPrice);
            productSpecs.insert();
        }
    }

    private static class PricingTestResource
    {

        private MithraTestResource mtr1;

        public void setUp() throws Exception
        {
            setUpMithraTestResource();
        }

        private void setUpMithraTestResource()
                throws Exception
        {
            String configFile= "mithraTestResource/MtrTestPricingRuntimeConfig.xml";
            String testDataFile = "mithraTestResource/mtrTestDataForPricingDb.txt";
            mtr1 = new MithraTestResource(configFile);
            ConnectionManagerForTests baseConnectionManager = ConnectionManagerForPricingTests.getInstance();
            baseConnectionManager.setDefaultSource("PRICING");
            mtr1.createSingleDatabase(baseConnectionManager, "PRICING", testDataFile);
            mtr1.setUp();
        }

        public void tearDown()
        {
            if(mtr1 != null)
            {
                mtr1.tearDown();
            }
        }
    }




}
