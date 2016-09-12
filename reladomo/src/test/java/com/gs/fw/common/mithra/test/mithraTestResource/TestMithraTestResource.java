
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

import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraManager;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.List;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;

public class TestMithraTestResource extends TestCase
{

    public void testCreateSingleDatabase()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrTestDataForSingleDb.txt");
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(5, size);
        mtr1.tearDown();
    }

    public void testDatabaseForStringSourceAttribute()
    {
        String configFile= "mithraTestResource/MtrTestDeskRuntimeConfigWithResourceName.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("deskDb");
        mtr1.createDatabaseForStringSourceAttribute(connectionManager,"A", "testdata/mithraTestResource/mtrTestDataForStringSource.txt");
        mtr1.setUp();

        assertTestDatabaseForStringSourceAttribute();
        mtr1.tearDown();
    }

    public void testDatabaseForStringSourceAttributeWithTableSharding()
    {
        String configFile= "mithraTestResource/MtrTestDeskRuntimeConfigWithResourceName.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("deskDb");
        mtr1.createDatabaseForStringSourceAttributeWithTableSharding(connectionManager, "A", "testdata/mithraTestResource/mtrTestDataForStringSource.txt");
        mtr1.setUp();

        assertTestDatabaseForStringSourceAttribute();
        mtr1.tearDown();
    }

    private void assertTestDatabaseForStringSourceAttribute()
    {
        Operation op = TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()));
        op = op.and(TinyBalanceFinder.balanceId().eq(10));
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));
        TinyBalance balance = TinyBalanceFinder.findOne(op);
        assertEquals(200, balance.getQuantity(), 0);
    }

    public void testNestedMithraTestResource()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrTestDataForSingleDb.txt");
        mtr1.setUp();

        testDatabaseForStringSourceAttribute();
        testDatabaseForStringSourceAttribute();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(5, size);
        mtr1.tearDown();
    }

    public void testDatabaseForIntSourceAttribute() throws Exception
    {
        String configFile= "mithraTestResource/MtrTestDeskRuntimeConfig.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        mtr1.createDatabaseForIntSourceAttribute(connectionManager,1, "testdata/mithraTestResource/mtrTestDataForIntSource.txt");
        mtr1.setUp();

        assertTestDatabaseForIntSourceAttribute();
        mtr1.tearDown();
    }

    public void testDatabaseForIntSourceAttributeWithSharding() throws Exception
    {
        String configFile= "mithraTestResource/MtrTestDeskRuntimeConfig.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("testDb");
        mtr1.createDatabaseForIntSourceAttribute(connectionManager,1, "testdata/mithraTestResource/mtrTestDataForIntSource.txt");
        mtr1.setUp();

        assertTestDatabaseForIntSourceAttribute();
        mtr1.tearDown();
    }

    private void assertTestDatabaseForIntSourceAttribute()
    {
        Operation op = LocationFinder.all();
        op = op.and(LocationFinder.sourceId().eq(1));

        LocationList list = new LocationList(op);
        assertEquals(7, list.size());
    }

    public void testAddingMultipleDataFiles()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrTestDataForSingleDbOne.txt");
        mtr1.addTestDataToDatabase("testdata/mithraTestResource/mtrTestDataForSingleDbTwo.txt",cmForConfig);
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(10, size);
        mtr1.tearDown();
    }

    public void testAddingMoreTestDataAfterSetup()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrTestDataForSingleDbOne.txt");

        mtr1.setUp();
        mtr1.addTestDataToDatabase("testdata/mithraTestResource/mtrTestDataForSingleDbTwo.txt",cmForConfig);

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(10, size);
        mtr1.tearDown();
    }

    public void testAddingRuntimeConfigFile()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        mtr1.loadMithraConfiguration("mithraTestResource/MtrAdditionalTestRuntimeConfig.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrAdditionalTestDataForSingleDb.txt");

        mtr1.setUp();

        Operation op = OrderFinder.all();
        OrderList list = new OrderList(op);
        int size = list.size();
        assertEquals(4, size);
        mtr1.tearDown();

    }

    public void testAddingRuntimeConfigAfterCreatingDb()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");

        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrAdditionalTestDataForSingleDb.txt");
        mtr1.loadMithraConfiguration("mithraTestResource/MtrAdditionalTestRuntimeConfig.xml");
        mtr1.setUp();

        Operation op = OrderFinder.all();
        OrderList list = new OrderList(op);
        int size = list.size();
        assertEquals(4, size);
        mtr1.tearDown();

    }

    public void testAddingRuntimeConfigAfterSetup()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");

        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig,"testdata/mithraTestResource/mtrTestDataForSingleDb.txt");

        mtr1.setUp();
        //todo:check this with Moh
        mtr1.loadMithraConfiguration("mithraTestResource/MtrAdditionalTestRuntimeConfig.xml");
        mtr1.addTestDataToDatabase("testdata/mithraTestResource/mtrAdditionalTestDataForSingleDb.txt",cmForConfig);

        Operation op = OrderFinder.all();
        OrderList list = new OrderList(op);
        int size = list.size();
        assertEquals(4, size);
        mtr1.tearDown();

    }

    public void testLoadBcpData()
    {
        checkingLoadBcp(false);
    }

    private void checkingLoadBcp(boolean dateFormatCheck)
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig);
        List<Attribute> attributes = new ArrayList<Attribute>() ;
        attributes.add(SaleFinder.saleId());
        attributes.add(SaleFinder.saleDate());
        attributes.add(SaleFinder.sellerId());
        attributes.add(SaleFinder.description());
        attributes.add(SaleFinder.discountPercentage());
        attributes.add(SaleFinder.settleDate());
        attributes.add(SaleFinder.activeBoolean());

        if(dateFormatCheck)
        {
            mtr1.loadBcpFile("testdata/mithraTestResource/sale.bcp","~", attributes, cmForConfig, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
        }
        else
        {
            mtr1.loadBcpFile("testdata/mithraTestResource/sale.bcp","~", attributes, "yyyy-MM-dd HH:mm:ss.SSS", cmForConfig);
        }
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(5, size);
        mtr1.tearDown();
    }

    public void testLoadBcpDataWithFormat()
    {
        checkingLoadBcp(true);
    }

    public void testLoadBcpWithNullData()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig);
        List<Attribute> attributes = new ArrayList<Attribute>() ;
        attributes.add(SaleFinder.saleId());
        attributes.add(SaleFinder.saleDate());
        attributes.add(SaleFinder.sellerId());
        attributes.add(SaleFinder.nullableDouble());
        attributes.add(SaleFinder.description());
        attributes.add(SaleFinder.discountPercentage());
        attributes.add(SaleFinder.settleDate());
        attributes.add(SaleFinder.activeBoolean());

        mtr1.loadBcpFile("testdata/mithraTestResource/saleWithNulls.bcp","~", attributes, "yyyy-MM-dd HH:mm:ss.SSS", cmForConfig);
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(4, size);
        for(int i = 0; i < size; i++)
        {
            Sale sale = list.get(i);
            if(sale.getSaleId() == 1)
            {                                                                    
                assertTrue(sale.isNullableDoubleNull());
                assertEquals(1, sale.getSellerId());
                assertEquals(0.05, sale.getDiscountPercentage());
            }
            else if(sale.getSaleId() == 2)
            {
                assertTrue(sale.isNullableDoubleNull());
                assertEquals(1, sale.getSellerId());
                assertEquals(0.07, sale.getDiscountPercentage());
            }
            else if(sale.getSaleId() == 3)
            {
                assertTrue(sale.isNullableDoubleNull());
                assertEquals(1, sale.getSellerId());
                assertEquals(0.03, sale.getDiscountPercentage());
            }
            else if(sale.getSaleId() == 4)
            {
                assertFalse(sale.isNullableDoubleNull());
                assertEquals(2, sale.getSellerId());
                assertEquals(0.1, sale.getDiscountPercentage());
            }
            else
            {
                fail("Invalid saleId");

            }
        }

        mtr1.tearDown();
    }

    public void testLoadMultipleBcpData()
    {
        checkLoadMultipleBcpData(false);
    }

    public void testLoadMultipleBcpDataWithDateFormat()
    {
        checkLoadMultipleBcpData(true);
    }

    private void checkLoadMultipleBcpData(boolean dateFormatCheck)
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig);
        List<Attribute> saleAttrs = new ArrayList<Attribute>() ;
        saleAttrs.add(SaleFinder.saleId());
        saleAttrs.add(SaleFinder.saleDate());
        saleAttrs.add(SaleFinder.sellerId());
        saleAttrs.add(SaleFinder.description());
        saleAttrs.add(SaleFinder.discountPercentage());
        saleAttrs.add(SaleFinder.settleDate());
        saleAttrs.add(SaleFinder.activeBoolean());

        List<Attribute> itemAttrs = new ArrayList<Attribute>() ;
        itemAttrs.add(SalesLineItemFinder.itemId());
        itemAttrs.add(SalesLineItemFinder.saleId());
        itemAttrs.add(SalesLineItemFinder.productId());
        itemAttrs.add(SalesLineItemFinder.manufacturerId());
        itemAttrs.add(SalesLineItemFinder.quantity());

        if(dateFormatCheck)
        {
            mtr1.loadBcpFile("testdata/mithraTestResource/sale.bcp", "~", saleAttrs, "yyyy-MM-dd HH:mm:ss.SSS", cmForConfig);
            mtr1.loadBcpFile("testdata/mithraTestResource/salesLineItems.bcp", ",", itemAttrs, null, cmForConfig);
        }
        else
        {
            mtr1.loadBcpFile("testdata/mithraTestResource/sale.bcp", "~", saleAttrs, cmForConfig, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
            mtr1.loadBcpFile("testdata/mithraTestResource/salesLineItems.bcp", ",", itemAttrs, cmForConfig, null);
        }
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        assertEquals(5, list.size());

        Operation op2 = SalesLineItemFinder.all();
        SalesLineItemList list2 = new SalesLineItemList(op2);
        assertEquals(12, list2.size());
        mtr1.tearDown();
    }

    public void testTwoResources() throws Exception
    {
        String configFile= "mithraTestResource/MohMithraRuntimeConfig.xml";
        MithraTestResource mtr1 = new MithraTestResource(configFile);
        MithraTestResource mtr2 = new MithraTestResource(configFile);
        mtr1.setDeleteOnCreate(false);
        mtr2.setDeleteOnCreate(false);
        mtr1.createSingleDatabase(ConnectionManagerForTests.getInstance("mithra_qa"), "testdata/mithraTestResource/testFileOne.txt");
        mtr2.createSingleDatabase(ConnectionManagerForTests.getInstance("mithra_qa"), "testdata/mithraTestResource/testFileTwo.txt");
        mtr1.setUp();
        mtr2.setUp();

       // assert that data from both test files is now loaded
        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        int size = list.size();
        assertEquals(10, size);

        mtr1.tearDown();
        mtr2.tearDown();
    }

    public void testReadConfiguration() throws Exception
    {
        MithraManager mithra = MithraManagerProvider.getMithraManager();
        mithra.readConfiguration(getConfigXml("MithraConfigPartialCache.xml"));
        OrderFinder.getMithraObjectPortal().getCache();
        mithra.cleanUpPrimaryKeyGenerators();
        mithra.cleanUpRuntimeCacheControllers();

    }

    public void testReadConfigurationWithTablePartitionManager() throws Exception
    {
        MithraManager mithra = MithraManagerProvider.getMithraManager();
        mithra.readConfiguration(getConfigXml("MithraTestTableManagerConfig.xml"));
        mithra.fullyInitialize();
        mithra.cleanUpPrimaryKeyGenerators();
        mithra.cleanUpRuntimeCacheControllers();
    }

    public void testInsertingTransactionalTestDataProgramatically()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig);
        mtr1.setUp();

        Operation op = SaleFinder.all();
        SaleList list = new SaleList(op);
        assertEquals(0, list.size());

        Operation op2 = SaleFinder.saleId().eq(9877);
        Sale existingSale = SaleFinder.findOne(op);
        assertNull(existingSale);

        Sale sale = new Sale();
        sale.setSaleId(9876);
        sale.setDescription("Sale 9876");
        sale.setSellerId(99);
        sale.setSaleDate(new Timestamp(System.currentTimeMillis()));
        sale.setSettleDate(new Timestamp(System.currentTimeMillis()));
        sale.setActiveBoolean(false);
        sale.setDiscountPercentage(0.25);

        Sale sale2 = new Sale();
        sale2.setSaleId(9877);
        sale2.setDescription("Sale 9877");
        sale2.setSellerId(99);
        sale2.setSaleDate(new Timestamp(System.currentTimeMillis()));
        sale2.setSettleDate(new Timestamp(System.currentTimeMillis()));
        sale2.setActiveBoolean(false);
        sale2.setDiscountPercentage(0.25);
        List<Sale> saleList = new ArrayList<Sale>();

        saleList.add(sale);
        saleList.add(sale2);

        mtr1.insertTestData(saleList);

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();

        SaleList list2 = new SaleList(op);
        assertEquals(2, list2.size());
        assertNotNull(SaleFinder.findOne(op2));

        mtr1.tearDown();

    }

    public void testInsertingTransactionalTestDataProgramaticallyTwice()
    {
        this.testInsertingTransactionalTestDataProgramatically();
    }

    public void testInsertingDatedTransactionalTestDataProgramatically()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("desk_db");
        mtr1.createDatabaseForStringSourceAttribute(cmForConfig,"A");
        mtr1.setUp();

        Operation op = TinyBalanceFinder.all();
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));

        TinyBalanceList list = new TinyBalanceList(op);
        assertEquals(0, list.size());

        TinyBalance balance = new TinyBalance(new Timestamp(System.currentTimeMillis()),InfinityTimestamp.getParaInfinity());
        balance.setAcmapCode("A");
        balance.setBalanceId(9876);
        balance.setBusinessDateFrom(new Timestamp(System.currentTimeMillis()));
        balance.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        balance.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
        balance.setProcessingDateTo(InfinityTimestamp.getParaInfinity());
        balance.setQuantity(99.99);

        TinyBalance balance2 = new TinyBalance(new Timestamp(System.currentTimeMillis()),InfinityTimestamp.getParaInfinity());
        balance2.setAcmapCode("A");
        balance2.setBalanceId(9877);
        balance2.setBusinessDateFrom(new Timestamp(System.currentTimeMillis()));
        balance2.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        balance2.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
        balance2.setProcessingDateTo(InfinityTimestamp.getParaInfinity());
        balance2.setQuantity(99.99);

        List<TinyBalance> balanceList = new ArrayList<TinyBalance>();

        balanceList.add(balance);
        balanceList.add(balance2);

        mtr1.insertTestData(balanceList);

        MithraManagerProvider.getMithraManager().clearAllQueryCaches();

        TinyBalanceList list2 = new TinyBalanceList(op);
        assertEquals(2, list2.size());

        Operation op2 = TinyBalanceFinder.balanceId().eq(9876);
        op2 = op2.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op2 = op2.and(TinyBalanceFinder.acmapCode().eq("A"));
        assertNotNull(TinyBalanceFinder.findOne(op2));
        mtr1.tearDown();

    }

    public void testInsertingReadOnlyTestDataProgramatically()
    {

        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("desk_db");
        mtr1.createDatabaseForIntSourceAttribute(cmForConfig,9876);
        mtr1.createDatabaseForIntSourceAttribute(cmForConfig,9877);
        mtr1.setUp();

        Operation op = LocationFinder.all();
        op = op.and(LocationFinder.sourceId().eq(9876));

        LocationList list = new LocationList(op);
        assertEquals(0, list.size());

        Location location = new Location();
        location.setId(9999);
        location.setSourceId(9876);
        location.setGroupId(1);
        location.setGeographicLocation("Somewhere");
        location.setCountry("USA");
        location.setCity("New York");

        Location location2 = new Location();
        location2.setId(8888);
        location2.setSourceId(9876);
        location2.setGroupId(1);
        location2.setGeographicLocation("Somewhere");
        location2.setCountry("USA");
        location2.setCity("New York");

        Location location3 = new Location();
        location3.setId(7777);
        location3.setSourceId(9877);
        location3.setGroupId(1);
        location3.setGeographicLocation("Somewhere");
        location3.setCountry("USA");
        location3.setCity("New York");

        List<Location> locationList = new ArrayList<Location>();
        locationList.add(location);
        locationList.add(location2);
        locationList.add(location3);

        mtr1.insertTestData(locationList);
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();

        LocationList list2 = new LocationList(op);
        assertEquals(2, list2.size());

        Operation op2 = LocationFinder.id().eq(7777);
        op2 = op2.and(LocationFinder.sourceId().eq(9877));
        assertNotNull(LocationFinder.findOne(op2));
        mtr1.tearDown();
    }

    public void testInsertingDatedReadOnlyTestDataProgramatically()
    {
        MithraTestResource mtr1 = new MithraTestResource("mithraTestResource/MtrTestRuntimeConfigWithResourceName.xml");
        ConnectionManagerForTests cmForConfig = ConnectionManagerForTests.getInstance("mithra_qa");
        mtr1.createSingleDatabase(cmForConfig);
        mtr1.setUp();

        Operation op = TamsMithraAccountFinder.all();
        op = op.and(TamsMithraAccountFinder.businessDate().equalsEdgePoint());

        TamsMithraAccountList list = new TamsMithraAccountList(op);
        assertEquals(0, list.size());

        TamsMithraAccount account = new TamsMithraAccount(new Timestamp(System.currentTimeMillis()), InfinityTimestamp.getParaInfinity());
        account.setAccountNumber("ABCD");
        account.setCode("code");
        account.setTrialId("trial");
        account.setPnlGroupId("pnlGorupId");
        account.setBusinessDateFrom(new Timestamp(System.currentTimeMillis()));
        account.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        account.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
        account.setProcessingDateTo(InfinityTimestamp.getParaInfinity());

        TamsMithraAccount account2 = new TamsMithraAccount(new Timestamp(System.currentTimeMillis()), InfinityTimestamp.getParaInfinity());
        account2.setAccountNumber("EFGH");
        account2.setCode("code");
        account2.setTrialId("trial");
        account2.setPnlGroupId("pnlGorupId");
        account2.setBusinessDateFrom(new Timestamp(System.currentTimeMillis()));
        account2.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        account2.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
        account2.setProcessingDateTo(InfinityTimestamp.getParaInfinity());

        List<TamsMithraAccount> tamsMithraAccountList = new ArrayList<TamsMithraAccount>();
        tamsMithraAccountList.add(account);
        tamsMithraAccountList.add(account2);

        mtr1.insertTestData(tamsMithraAccountList);
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();

        TamsMithraAccountList list2 = new TamsMithraAccountList(op);
        assertEquals(2, list2.size());

        Operation op2 = TamsMithraAccountFinder.accountNumber().eq("ABCD");
        op2 = op2.and(TamsMithraAccountFinder.businessDate().equalsEdgePoint());
        assertNotNull(TamsMithraAccountFinder.findOne(op2));
        mtr1.tearDown();
    }

    protected InputStream getConfigXml(String fileName) throws FileNotFoundException
    {
        String xmlRoot = System.getProperty(MithraTestResource.ROOT_KEY);
        if (xmlRoot == null)
        {
            InputStream result = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if (result == null)
            {
                throw new RuntimeException("could not find " + fileName + " in classpath. Additionally, " + MithraTestResource.ROOT_KEY + " was not specified");
            }
            return result;
        }
        else
        {
            String fullPath = xmlRoot;
            if (!xmlRoot.endsWith(File.separator))
            {
                fullPath += File.separator;
            }
            return new FileInputStream(fullPath + fileName);
        }
    }


//    public void testTwoResourcesWithDifferentRestrictedClasses() throws Exception
//    {
//        String configFile= "mithraTestResource/MohMithraRuntimeConfig.xml";
//        MithraTestResource mtr1 = new MithraTestResource(configFile);
//        MithraTestResource mtr2 = new MithraTestResource(configFile);
//        mtr1.setRestrictedClassList(new Class[] { Sale.class} );
//        mtr2.setRestrictedClassList(new Class[] { SalesLineItem.class, ProductSpecification.class} );
//        mtr1.setDeleteOnCreate(false);
//        mtr2.setDeleteOnCreate(false);
//        mtr1.createSingleDatabase(ConnectionManagerForTests.getInstance("mithra_qa"), "testdata/mithraTestResource/testFileOne.txt");
//        mtr2.createSingleDatabase(ConnectionManagerForTests.getInstance("mithra_qa"), "testdata/mithraTestResource/testFileTwo.txt");
//        mtr1.setUp();
//        mtr2.setUp();
//
//       // assert that data from both test files is now loaded
//        Operation op = SaleFinder.all();
//        SaleList list = new SaleList(op);
//        int size = list.size();
//        assertEquals(10, size);
//
//        mtr1.tearDown();
//        mtr2.tearDown();
//    }
}
