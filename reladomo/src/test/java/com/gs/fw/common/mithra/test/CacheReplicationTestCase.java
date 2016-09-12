
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

package com.gs.fw.common.mithra.test;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.cache.AbstractDatedCache;
import com.gs.fw.common.mithra.cache.MithraReferenceThread;
import com.gs.fw.common.mithra.cache.offheap.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.multivm.MultiVmTestCase;
import com.gs.fw.common.mithra.test.util.PspBasedMithraMasterServerFactory;
import com.gs.fw.common.mithra.test.util.tinyproxy.PspServlet;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.gs.fw.common.mithra.util.StringPool;
import org.junit.Assert;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

public class CacheReplicationTestCase extends MultiVmTestCase
{
    protected static final String MITHRA_TEST_DATA_FILE_PATH = "testdata/";
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected static long START_TIME;
    protected static Timestamp START_TIME_TIMESTAMP;

    static
    {
        try
        {
            START_TIME = timestampFormat.parse("2013-01-01 00:00:00").getTime();
        }
        catch (ParseException e)
        {
            //ignore
        }
        START_TIME_TIMESTAMP = new Timestamp(START_TIME);
    }

    private Server server;

    private MithraTestResource mithraTestResource;

    public MithraTestResource getMithraTestResource()
    {
        return mithraTestResource;
    }

    public void setMithraTestResource(MithraTestResource mithraTestResource)
    {
        this.mithraTestResource = mithraTestResource;
    }

    protected InputStream getConfigXml(String fileName) throws FileNotFoundException
    {
        String xmlRoot = System.getProperty(MithraTestResource.ROOT_KEY);
        if(xmlRoot == null)
        {
            InputStream result = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if (result == null)
            {
                throw new RuntimeException("could not find "+fileName+" in classpath. Additionally, "+MithraTestResource.ROOT_KEY+" was not specified");
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

    protected void setUp() throws Exception
    {
        super.setUp();
        StringPool.getInstance().enableOffHeapSupport();
        StringPool.getInstance().getOrAddToCache("some weird string 1", true);
        StringPool.getInstance().getOrAddToCache("some weird string 2", true);
        StringPool.getInstance().getOrAddToCache("some weird string 3",true);
        PspBasedMithraMasterServerFactory.setPort(this.getApplicationPort1());
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        mithraManager.readConfiguration(this.getConfigXml("MithraConfigReplicantCache.xml"));
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
        MithraManagerProvider.getMithraManager().cleanUpRuntimeCacheControllers();
    }

    public void slaveVmOnStartup()
    {
        setupPspMithraService();
    }

    protected void setupPspMithraService()
    {
        server = new Server(this.getApplicationPort1());
        Context context = new Context (server,"/",Context.SESSIONS);
        ServletHolder holder = context.addServlet(PspServlet.class, "/PspServlet");
        holder.setInitParameter("serviceInterface.MasterCacheService", "com.gs.fw.common.mithra.cache.offheap.MasterCacheService");
        holder.setInitParameter("serviceClass.MasterCacheService", "com.gs.fw.common.mithra.cache.offheap.MasterCacheServiceImpl");
        holder.setInitOrder(10);
//        System.out.println(holder.getServlet().getClass().getName());

        try
        {
            server.start();
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not start server", e);
        }
        finally
        {
        }
    }

    public void slaveVmSetUp() // oddly, this is the master cache. "slave" here refers to the Junit spawned second JVM.
    {
        String xmlFile = System.getProperty("mithra.xml.config");

        this.setDefaultServerTimezone();

        mithraTestResource = new MithraTestResource(xmlFile);

        mithraTestResource.setRestrictedClassList(this.getRestrictedClassList());

        ConnectionManagerForTests connectionManagerForTestDb = ConnectionManagerForTests.getInstance("test_db");
        ConnectionManagerForTests connectionManagerForTestDbStringSource = ConnectionManagerForTests.getInstance("test_db_a");
        connectionManagerForTestDbStringSource.addConnectionManagerForSource("A", "A");
        connectionManagerForTestDbStringSource.addConnectionManagerForSource("B", "B");

        try
        {
            this.mithraTestResource.createSingleDatabase(connectionManagerForTestDb, "testdata/mithraMasterCacheTestSource.txt");
            this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManagerForTestDbStringSource, "A", "testdata/mithraMasterCacheTestSourceA.txt");
            this.mithraTestResource.createDatabaseForStringSourceAttribute(connectionManagerForTestDbStringSource, "B", null);

            mithraTestResource.setUp();
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not initialize mithra", e);
        }
        BitemporalOrderStatusFinder.getMithraObjectPortal().setLatestRefreshTime(123456);
        BitemporalOrderItemFinder.getMithraObjectPortal().setLatestRefreshTime(111222333);
    }

    protected void setDefaultServerTimezone()
    {
    }

    protected TimeZone getServerDatabaseTimezone()
    {
        return null;
    }

    public void slaveVmTearDown()
    {
        mithraTestResource.tearDown();
    }

    protected Class[] getRestrictedClassList()
    {
        return null;
    }

    protected void addTestClassesFromOther(MithraTestAbstract otherTest, Set<Class> toAdd)
    {
        Class[] otherList = otherTest.getRestrictedClassList();
        if (otherList != null)
        {
            for(int i=0;i<otherList.length;i++)
            {
                toAdd.add(otherList[i]);
            }
        }
    }

    public static Connection getServerSideConnection ()
    {
        return ConnectionManagerForTests.getInstance().getConnection();
    }

    public void testReplication() throws Exception
    {
        MasterCacheUplink masterCacheUplink = MithraManagerProvider.getMithraManager().getConfigManager().getMasterCacheUplink("mithra.test.master");
        checkOrderItems();
        checkTinyBalance();
        // pause
        masterCacheUplink.pause();
        // load 101K objects
        this.getRemoteSlaveVm().executeMethod("serverTestInsertLargeSalvo");
        // unpause, wait for sync

        unpauseAndWaitForSync(masterCacheUplink);
        checkInitialOrders();

        masterCacheUplink.pause();
        this.getRemoteSlaveVm().executeMethod("serverTestUpdate");
        unpauseAndWaitForSync(masterCacheUplink);
        checkUpdates();

        masterCacheUplink.pause();
        this.getRemoteSlaveVm().executeMethod("serverTestUpdateInsert");
        unpauseAndWaitForSync(masterCacheUplink);
        checkUpdatesAndInserts();

        masterCacheUplink.pause();
        this.getRemoteSlaveVm().executeMethod("serverTestDelete");
        unpauseAndWaitForSync(masterCacheUplink);
        checkDeletes();

        masterCacheUplink.pause();
        this.getRemoteSlaveVm().executeMethod("serverTestDeleteInsert");
        unpauseAndWaitForSync(masterCacheUplink);
        checkDeletesAndInserts();

        masterCacheUplink.pause();
        this.getRemoteSlaveVm().executeMethod("serverTestInsertInPlace");
        unpauseAndWaitForSync(masterCacheUplink);
        checkInPlaceInserts();

        this.getRemoteSlaveVm().executeMethod("serverDoLotsOfSlowUpdates");
        waitForSync(masterCacheUplink);

        checkLotsOfSlowUpdates();

        {
            // Test to confirm correct handling of case where the buffer contains empty pages at the end
            masterCacheUplink.pause();
            this.getRemoteSlaveVm().executeMethod("serverMakeNewEmptyPages");
            unpauseAndWaitForSync(masterCacheUplink);

            this.getRemoteSlaveVm().executeMethod("serverCreateObjectsInNewPages");
            waitForSync(masterCacheUplink);

            checkResultOfNewPagesTestOnReplicaSide();
        }

        reconcilePageVersionList();
    }

    private void reconcilePageVersionList() throws Exception
    {
        reconcilePageVersionListForSingleClass(BitemporalOrderFinder.class);
        reconcilePageVersionListForSingleClass(BitemporalOrderItemFinder.class);
        reconcilePageVersionListForSingleClass(BitemporalOrderItemStatusFinder.class);
        reconcilePageVersionListForSingleClass(BitemporalOrderStatusFinder.class);
        reconcilePageVersionListForSingleClass(TinyBalanceFinder.class);
    }

    private void reconcilePageVersionListForSingleClass(Class finderClass) throws Exception
    {
        List<Long> masterPageVersionList = (List<Long>)this.getRemoteSlaveVm().executeMethod("getPageVersionList", new Class[] { Class.class }, new Object[] { finderClass });
        List<Long> replicaPageVersionList = this.getPageVersionList(finderClass);

        Long[] masterPagesAsArray = masterPageVersionList.toArray(new Long[]{});
        Long[] replicaPagesAsArray = replicaPageVersionList.toArray(new Long[]{});
        Assert.assertArrayEquals("Off-heap page versions on replica are out of sync with master for class " + finderClass.getSimpleName(), masterPagesAsArray, replicaPagesAsArray);
    }

    private void checkOrderItems() throws ParseException
    {
        BitemporalOrderItemList allItems = BitemporalOrderItemFinder.findMany(BitemporalOrderItemFinder.businessDate().equalsEdgePoint().and(BitemporalOrderItemFinder.processingDate().equalsEdgePoint()));
        allItems.setOrderBy(BitemporalOrderItemFinder.id().ascendingOrderBy());
        assertEquals(4, allItems.size());
        BitemporalOrderItem item3 = allItems.get(2);
        //id,orderId,productId,quantity,originalPrice,discountPrice,state,businessDateFrom, businessDateTo,processingDateFrom,processingDateTo
        //3, 2, 2, 20, 15.5, 10.0, "In-Progress","2000-01-01 00:00:00.0", "9999-12-01 23:59:00.000", "2000-01-01 00:00:00.0", "9999-12-01 23:59:00.000"
        assertEquals(3, item3.getId());
        assertEquals(2, item3.getOrderId());
        assertEquals(2, item3.getProductId());
        assertEquals(20, item3.getQuantity(), 0.0);
        assertEquals(15.5, item3.getOriginalPrice(), 0.0);
        assertEquals(10.0, item3.getDiscountPrice(), 0.0);
        assertEquals("In-Progress", item3.getState());
        assertEquals(timestampFormat.parse("2000-01-01 00:00:00.0").getTime(), item3.getBusinessDateFrom().getTime());
        assertEquals(timestampFormat.parse("9999-12-01 23:59:00.000").getTime(), item3.getBusinessDateTo().getTime());
        assertEquals(timestampFormat.parse("2000-01-01 00:00:00.0").getTime(), item3.getProcessingDateFrom().getTime());
        assertEquals(timestampFormat.parse("9999-12-01 23:59:00.000").getTime(), item3.getProcessingDateTo().getTime());
        assertEquals(123456, BitemporalOrderStatusFinder.getMithraObjectPortal().getLatestRefreshTime());
        assertEquals(111222333, BitemporalOrderItemFinder.getMithraObjectPortal().getLatestRefreshTime());
    }

    private void checkTinyBalance() throws ParseException
    {
        TinyBalanceList allItems = TinyBalanceFinder.findMany(TinyBalanceFinder.businessDate().equalsEdgePoint().and(TinyBalanceFinder.processingDate().equalsEdgePoint()));
        allItems.setOrderBy(TinyBalanceFinder.balanceId().ascendingOrderBy());
        assertEquals(80, allItems.size());
        TinyBalance item3 = allItems.get(2);
        assertEquals("A", item3.getAcmapCode());
//        balanceId,quantity,businessDateFrom,businessDateTo,processingDateFrom,processingDateTo
//        3,1000,"2002-02-01 00:00:00.0","9999-12-01 23:59:00.0","2002-02-02 00:30:00.0","9999-12-01 23:59:00.0"
        assertEquals(3, item3.getBalanceId());
        assertEquals(1000, item3.getQuantity(), 0.01);
    }

    private void checkInitialOrders()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        BitemporalOrderList allOrders = BitemporalOrderFinder.findMany(edgeOp);
        allOrders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());
        assertEquals(102000-1000, allOrders.size());
        for(int i=1000;i<102000;i++)
        {
            BitemporalOrder order = allOrders.get(i - 1000);
            checkOrder(i, order);
        }
        // use the userId index
        assertNotNull(BitemporalOrderFinder.findOne(edgeOp.and(BitemporalOrderFinder.userId().eq(1000 + 10000000))));
    }

    public void checkUpdates()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        BitemporalOrderList updatedOrders = BitemporalOrderFinder.findMany(edgeOp.and(BitemporalOrderFinder.userId().greaterThanEquals(20000000)));
        assertEquals(50, updatedOrders.size());
        for(int i=0;i<updatedOrders.size();i++)
        {
            BitemporalOrder order = updatedOrders.get(i);
            assertEquals(order.getOrderId() + 20000000, order.getUserId());
        }
        assertNotNull(BitemporalOrderFinder.findOne(edgeOp.and(BitemporalOrderFinder.userId().eq(1500 + 20000000))));
        assertEquals(7890, BitemporalOrderStatusFinder.getMithraObjectPortal().getLatestRefreshTime());
        assertEquals(44455566, BitemporalOrderItemFinder.getMithraObjectPortal().getLatestRefreshTime());
        assertEquals(999000111, BitemporalOrderFinder.getMithraObjectPortal().getLatestRefreshTime());
    }

    public void checkUpdatesAndInserts()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        BitemporalOrderList updatedOrders = BitemporalOrderFinder.findMany(edgeOp.and(BitemporalOrderFinder.userId().greaterThanEquals(30000000)));
        assertEquals(50, updatedOrders.size());
        for(int i=0;i<updatedOrders.size();i++)
        {
            BitemporalOrder order = updatedOrders.get(i);
            assertEquals(order.getOrderId() + 30000000, order.getUserId());
        }
        assertNotNull(BitemporalOrderFinder.findOne(edgeOp.and(BitemporalOrderFinder.userId().eq(1501+30000000))));
        BitemporalOrderList insertedOrders = BitemporalOrderFinder.findMany(edgeOp.and(BitemporalOrderFinder.orderId().greaterThanEquals(200000)));
        insertedOrders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());
        assertEquals(11111, insertedOrders.size());
        for(int i=0;i<insertedOrders.size();i++)
        {
            checkOrder(i + 200000, insertedOrders.get(i));
        }
    }

    private void checkDeletes()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        for(int i=1502;i<102000;i+=2048)
        {
            assertNull(BitemporalOrderFinder.findOne(BitemporalOrderFinder.orderId().eq(i).and(edgeOp)));
            assertNull(BitemporalOrderFinder.findOne(BitemporalOrderFinder.userId().eq(i + 10000000).and(edgeOp)));
        }

    }

    public void checkDeletesAndInserts()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        for(int i=1502;i<102000;i+=2048)
        {
            assertNull(BitemporalOrderFinder.findOne(BitemporalOrderFinder.orderId().eq(i).and(edgeOp)));
            assertNull(BitemporalOrderFinder.findOne(BitemporalOrderFinder.userId().eq(i + 10000000).and(edgeOp)));
        }
        BitemporalOrderList insertedOrders = BitemporalOrderFinder.findMany(edgeOp.and(BitemporalOrderFinder.orderId().greaterThanEquals(300000)));
        insertedOrders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());
        assertEquals(11111, insertedOrders.size());
        for(int i=0;i<insertedOrders.size();i++)
        {
            checkOrder(i + 300000, insertedOrders.get(i));
        }
    }

    public void checkInPlaceInserts()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        BitemporalOrderList insertedOrders = BitemporalOrderFinder.findMany(edgeOp.and(BitemporalOrderFinder.orderId().greaterThanEquals(400000)));
        insertedOrders.setOrderBy(BitemporalOrderFinder.orderId().ascendingOrderBy());
        assertEquals(100, insertedOrders.size());
        for(int i=0;i<insertedOrders.size();i++)
        {
            checkOrder(i + 400000, insertedOrders.get(i));
        }
    }

    private void checkLotsOfSlowUpdates()
    {
        Operation edgeOp = BitemporalOrderFinder.businessDate().equalsEdgePoint().and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        for(int i=1999;i>1950;i--)
        {
            int id = (i % 50) * 1024 + 1501;
            BitemporalOrder order = BitemporalOrderFinder.findOne(BitemporalOrderFinder.orderId().eq(id).and(edgeOp));
            assertNotNull(order);
            assertEquals(i + 30000000, order.getUserId());
        }
    }

    private void checkOrder(int id, BitemporalOrder order)
    {
        assertEquals(id, order.getOrderId());
        if (id % 3 != 0)
        {
            assertEquals(START_TIME + id, order.getOrderDate().getTime());
        }
        else
        {
            assertNull(order.getOrderDate());
        }
        if (id % 2 == 0)
        {
            assertEquals(id + 10000000, order.getUserId());
        }
        else
        {
            assertTrue(order.isUserIdNull());
        }
        if (id % 5 == 0)
        {
            assertEquals("description "+ id, order.getDescription());
        }
        else
        {
            assertNull(order.getDescription());
        }
        if (id % 11 == 0)
        {
            assertEquals("state "+ id, order.getState());
        }
        else
        {
            assertNull(order.getState());
        }
        if (id % 233 == 0)
        {
            assertEquals("tracking "+ id, order.getTrackingId());
        }
        else
        {
            assertNull(order.getTrackingId());
        }
        assertEquals(START_TIME + id * 10, order.getProcessingDateFrom().getTime());
        if (id % 2 == 0)
        {
            assertEquals(BitemporalOrderFinder.processingDate().getInfinityDate().getTime(), order.getProcessingDateTo().getTime());
        }
        else
        {
            assertEquals(START_TIME + id * 10 + 1000000, order.getProcessingDateTo().getTime());
        }
        assertEquals(START_TIME + id * 20, order.getBusinessDateFrom().getTime());
        if (id % 2 == 0)
        {
            assertEquals(BitemporalOrderFinder.businessDate().getInfinityDate().getTime(), order.getBusinessDateTo().getTime());
        }
        else
        {
            assertEquals(START_TIME + id * 20 + 1000000, order.getBusinessDateTo().getTime());
        }
    }

    private void unpauseAndWaitForSync(MasterCacheUplink masterCacheUplink)
    {
        masterCacheUplink.unPause();
        waitForSync(masterCacheUplink);
    }

    private void waitForSync(MasterCacheUplink masterCacheUplink)
    {
        sleep(masterCacheUplink.getSyncInterval());
        long before = masterCacheUplink.getLastSuccessfulRefresh();
        while(masterCacheUplink.getLastSuccessfulRefresh() <= before)
        {
            sleep(100);
        }
    }

    public void serverTestInsertLargeSalvo()
    {
        List<BitemporalOrderData> list = FastList.newList(102000);
        for(int i=1000;i<102000;i++)
        {
            list.add(createBigOrder(i));
        }
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        cc.getMithraObjectPortal().getCache().updateCache(list, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        System.out.println("created objects on master");
    }

    public void serverTestUpdate()
    {
        List<BitemporalOrderData> updateList = FastList.newList(11111);
        for(int i=1500;i<102000;i+=2048)
        {
            BitemporalOrderData order = createBigOrder(i);
            order.setUserId(i + 20000000);
            updateList.add(order);
        }
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        cc.getMithraObjectPortal().getCache().updateCache(Collections.EMPTY_LIST, updateList, Collections.EMPTY_LIST);
        BitemporalOrderStatusFinder.getMithraObjectPortal().setLatestRefreshTime(7890);
        BitemporalOrderItemFinder.getMithraObjectPortal().setLatestRefreshTime(44455566);
        BitemporalOrderFinder.getMithraObjectPortal().setLatestRefreshTime(999000111);
        System.out.println("updated objects on master");
    }

    public void serverTestUpdateInsert()
    {
        List<BitemporalOrderData> insertList = FastList.newList(11111);
        for(int i=0;i<11111;i++)
        {
            insertList.add(createBigOrder(i + 200000));
        }

        List<BitemporalOrderData> updateList = FastList.newList(11111);
        for(int i=1501;i<102000;i+=2048)
        {
            BitemporalOrderData order = createBigOrder(i);
            order.setUserId(i + 30000000);
            updateList.add(order);
        }
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        cc.getMithraObjectPortal().getCache().updateCache(insertList, updateList, Collections.EMPTY_LIST);
        System.out.println("updated/inserted objects on master");

    }

    public void serverTestDelete()
    {
        List<BitemporalOrderData> deleteList = FastList.newList(11111);
        for(int i=1502;i<102000;i+=2048)
        {
            BitemporalOrderData order = createBigOrder(i);
            deleteList.add(order);
        }
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        cc.getMithraObjectPortal().getCache().updateCache(Collections.EMPTY_LIST, Collections.EMPTY_LIST, deleteList);
        forceGC();
        System.out.println("deleted objects on master");

    }

    public void serverTestDeleteInsert()
    {
        forceGC();
        List<BitemporalOrderData> insertList = FastList.newList(11111);
        for(int i=0;i<11111;i++)
        {
            insertList.add(createBigOrder(i + 300000));
        }

        List<BitemporalOrderData> deleteList = FastList.newList(11111);
        for(int i=1503;i<102000;i+=2048)
        {
            BitemporalOrderData order = createBigOrder(i);
            deleteList.add(order);
        }
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        cc.getMithraObjectPortal().getCache().updateCache(insertList, Collections.EMPTY_LIST, deleteList);
        System.out.println("deleted/inserted objects on master");

    }

    public void serverTestInsertInPlace()
    {
        forceGC();
        List<BitemporalOrderData> insertList = FastList.newList(100);
        for(int i=0;i<100;i++)
        {
            insertList.add(createBigOrder(i + 400000));
        }

        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        cc.getMithraObjectPortal().getCache().updateCache(insertList, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        System.out.println("in place inserted objects on master");

    }

    public void serverDoLotsOfSlowUpdates()
    {
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderFinder.class);
        // 10 refreshes takes 20 seconds. If we do an update every 10 ms, that's 2000 total updates.
        for(int i=0;i<2000;i++)
        {
            BitemporalOrderData order = createBigOrder((i % 50) * 1024 + 1501);
            order.setUserId(i + 30000000);
            cc.getMithraObjectPortal().getCache().updateCache(Collections.EMPTY_LIST, Collections.singletonList(order), Collections.EMPTY_LIST);
//            if (i > 1950)
//            {
//                System.out.println("updated page "+(i % 50)+" to "+(i+ 30000000));
//                System.out.flush();
//            }
            sleep(50);
        }
        System.out.println("slow updated objects on master");
        System.out.flush();
    }

    public void serverMakeNewEmptyPages()
    {
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderStatusFinder.class);
        // First pump in a huge amount of data to create many new pages. Then immediately remove all objects so that these pages will appear empty to the client replica.
        List<BitemporalOrderStatusData> newOrders = FastList.newList();
        for(int i=0;i<2000000;i++)
        {
            BitemporalOrderStatusData order = createOrderStatus(i + 1000);
            newOrders.add(order);
        }
        cc.getMithraObjectPortal().getCache().updateCache(FastList.newList(newOrders), FastList.newList(), Collections.EMPTY_LIST); // wrap the list as it may get modified
        cc.getMithraObjectPortal().getCache().updateCache(Collections.EMPTY_LIST, Collections.EMPTY_LIST, FastList.newList(newOrders));
        System.out.println("Made new empty pages on master");
        System.out.flush();
    }

    public void serverCreateObjectsInNewPages()
    {
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(BitemporalOrderStatusFinder.class);
        // Create a medium amount of data using brand new IDs. Provided the data created by serverMakeNewEmptyPages() has not yet been evicted, our data should be added at the end in a brand new page.
        List<BitemporalOrderStatusData> newOrders = FastList.newList();
        for(int i=2000000;i<2050000;i++)
        {
            BitemporalOrderStatusData order = createOrderStatus(i + 1000);
            newOrders.add(order);
        }
        cc.getMithraObjectPortal().getCache().updateCache(FastList.newList(newOrders), Collections.EMPTY_LIST, Collections.EMPTY_LIST); // wrap the list as it may get modified
        System.out.println("Created objects in brand new pages at the end of the storage buffer");
        System.out.flush();
    }

    public List<Long> getPageVersionList(Class finderClass) throws Exception
    {
        MithraRuntimeCacheController cc = new MithraRuntimeCacheController(finderClass);
        AbstractDatedCache cache = (AbstractDatedCache) cc.getFinderInstance().getMithraObjectPortal().getCache();

        // This is a hack to access private methods to get at the off-heap storage and page version list which we need for this test.
        // On balance this is less bad than making these internals public to the entire world.
        Method zGetDataStorage = AbstractDatedCache.class.getDeclaredMethod("zGetDataStorage");
        zGetDataStorage.setAccessible(true);
        FastUnsafeOffHeapDataStorage storage = (FastUnsafeOffHeapDataStorage) zGetDataStorage.invoke(cache);

        Method zGetPageVersionList = FastUnsafeOffHeapDataStorage.class.getDeclaredMethod("zGetPageVersionList");
        zGetPageVersionList.setAccessible(true);
        FastUnsafeOffHeapLongList pageVersionList = (FastUnsafeOffHeapLongList) zGetPageVersionList.invoke(storage);

        List<Long> listCopy = FastList.newList(pageVersionList.size());
        for (int i = 0; i < pageVersionList.size(); i++)
        {
            listCopy.add(pageVersionList.get(i));
        }
        return listCopy;
    }

    public void checkResultOfNewPagesTestOnReplicaSide()
    {
        for(int i=0;i<2000000;i++)
        {
            BitemporalOrderStatus order = BitemporalOrderStatusFinder.findOne(BitemporalOrderStatusFinder.orderId().eq(i + 1000).and(BitemporalOrderStatusFinder.processingDate().equalsEdgePoint()).and(BitemporalOrderStatusFinder.businessDate().equalsEdgePoint()));
            Assert.assertNull(order);
        }

        for(int i=2000000;i<2050000;i++)
        {
            BitemporalOrderStatus order = BitemporalOrderStatusFinder.findOne(BitemporalOrderStatusFinder.orderId().eq(i + 1000).and(BitemporalOrderStatusFinder.processingDate().equalsEdgePoint()).and(BitemporalOrderStatusFinder.businessDate().equalsEdgePoint()));
            Assert.assertNotNull(order);
            Assert.assertEquals(i + 1000, order.getOrderId());
        }
    }

    private void forceGC()
    {
        Runtime.getRuntime().gc();
        Thread.yield();
        MithraReferenceThread.getInstance().runNow();
        Runtime.getRuntime().gc();
        Thread.yield();
        MithraReferenceThread.getInstance().runNow();
    }

    private BitemporalOrderStatusData createOrderStatus(int id)
    {
        BitemporalOrderStatusData data = BitemporalOrderStatusDatabaseObject.allocateOnHeapData();
        data.setOrderId(id);
        data.setProcessingDateFrom(START_TIME_TIMESTAMP);
        data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
        data.setBusinessDateFrom(START_TIME_TIMESTAMP);
        data.setBusinessDateTo(BitemporalOrderFinder.businessDate().getInfinityDate());
        return data;
    }

    private BitemporalOrderData createBigOrder(int id)
    {
        BitemporalOrderData data = BitemporalOrderDatabaseObject.allocateOnHeapData();
        data.setOrderId(id);
        if (id % 3 != 0)
        {
            data.setOrderDate(new Timestamp(START_TIME + id));
        }
        if (id % 2 == 0)
        {
            data.setUserId(id + 10000000);
        }
        else
        {
            data.setUserIdNull();
        }
        if (id % 5 == 0)
        {
            data.setDescription("description "+id);
        }
        if (id % 11 == 0)
        {
            data.setState("state "+id);
        }
        if (id % 233 == 0)
        {
            data.setTrackingId("tracking "+id);
        }
        data.setProcessingDateFrom(new Timestamp(START_TIME + id * 10));
        if (id % 2 == 0)
        {
            data.setProcessingDateTo(BitemporalOrderFinder.processingDate().getInfinityDate());
        }
        else
        {
            data.setProcessingDateTo(new Timestamp(START_TIME + id * 10 + 1000000));
        }
        data.setBusinessDateFrom(new Timestamp(START_TIME + id * 20));
        if (id % 2 == 0)
        {
            data.setBusinessDateTo(BitemporalOrderFinder.businessDate().getInfinityDate());
        }
        else
        {
            data.setBusinessDateTo(new Timestamp(START_TIME + id * 20 + 1000000));
        }
        return data;
    }

}
