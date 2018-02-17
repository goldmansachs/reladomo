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

package com.gs.fw.common.mithra.test.mtloader;

import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.mtloader.AbortException;
import com.gs.fw.common.mithra.mtloader.AbstractMatcherThread;
import com.gs.fw.common.mithra.mtloader.DbLoadThread;
import com.gs.fw.common.mithra.mtloader.InputLoader;
import com.gs.fw.common.mithra.mtloader.PlainInputThread;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderList;
import com.gs.fw.common.mithra.util.QueueExecutor;
import com.gs.fw.common.mithra.util.SingleQueueExecutor;
import junit.framework.TestCase;
import org.eclipse.collections.api.block.function.primitive.IntFunction;
import org.eclipse.collections.impl.block.factory.Comparators;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;


public abstract class MithraMatcherThreadTestAbstract extends TestCase
{
    private MithraTestResource mithraTestResource;

    private static Logger LOGGER = LoggerFactory.getLogger(MithraMatcherThreadTestAbstract.class);
    private static Timestamp JAN_ONE_TWENTY_FIFTEEN = getTimestamp("2015-01-01 00:00:00.0");
    protected static int INDEX_SIZE = 100;

    protected abstract AbstractMatcherThread getMatcherThread(QueueExecutor bitemporalOrderExecutor, Extractor[] bitemporalOrderExtractor);

    protected static Comparator<BitemporalOrder> COMPARATOR = Comparators.byIntFunction(new IntFunction<BitemporalOrder>()
    {
        @Override
        public int intValueOf(BitemporalOrder bitemporalOrder)
        {
            return bitemporalOrder.getUserId();
        }
    });

    protected void setUp() throws Exception
    {
        super.setUp();

        String xmlFile = System.getProperty("mithra.xml.config");

        mithraTestResource = new MithraTestResource(xmlFile);

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("M");
        connectionManager.setDatabaseTimeZone(TimeZone.getDefault());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createSingleDatabase(connectionManager, "M", "testdata/mithraMatcherThreadTestSource.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    private Extractor[] getExtractor()
    {
        return new Extractor[]{BitemporalOrderFinder.orderId()};
    }

    private QueueExecutor getQueueExecutor()
    {
        QueueExecutor bitemporalOrderExecutor = new SingleQueueExecutor(5, BitemporalOrderFinder.orderId().ascendingOrderBy(), 300, BitemporalOrderFinder.getFinderInstance(), 2);
        LOGGER.info("Using Bulk Insert");
        bitemporalOrderExecutor.setUseBulkInsert();
        return bitemporalOrderExecutor;
    }

    private BitemporalOrderList getDbList()
    {
        Operation op = BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()));
        op = op.and(BitemporalOrderFinder.processingDateFrom().lessThan(new Timestamp(System.currentTimeMillis())));
        BitemporalOrderList list = new BitemporalOrderList(op);
        LOGGER.info("Finder List data size: " + list.size());
        return list;
    }

    public void testMtFeedLoad()
    {
        QueueExecutor executor = loadData();
        assertResultsDefaultComparator(executor);
    }

    public void testMtFeedLoad_noDbThread()
    {
        QueueExecutor executor = loadData_noDbThread();
        assertResultsDefaultComparator(executor);
    }

    // All three operations are asynchronous.
    protected QueueExecutor loadData()
    {
        QueueExecutor bitemporalOrderExecutor = getQueueExecutor();
        Extractor[] bitemporalOrderExtractor = getExtractor();
        AbstractMatcherThread matcherThread = getMatcherThread(bitemporalOrderExecutor, bitemporalOrderExtractor);
        matcherThread.start();

        DbLoadThread dbLoadThread = new DbLoadThread(getDbList(), null, matcherThread);
        dbLoadThread.start();

        PlainInputThread inputThread = new PlainInputThread(new InputDataLoader(), matcherThread);
        inputThread.run();

        try
        {
            matcherThread.waitTillDone();
        }
        catch (AbortException e)
        {
            LOGGER.error("Failed : " + e.getMessage(), e);
            fail();
        }

        if (bitemporalOrderExecutor.anyFailed())
        {
            fail();
        }

        return bitemporalOrderExecutor;
    }

    // All three operations are in the matcher thread (synchronous)
    protected QueueExecutor loadData_noDbThread()
    {
        QueueExecutor bitemporalOrderExecutor = this.getQueueExecutor();
        Extractor[] bitemporalOrderExtractor = this.getExtractor();
        AbstractMatcherThread matcherThread = this.getMatcherThread(bitemporalOrderExecutor, bitemporalOrderExtractor);

        matcherThread.start();
        try
        {
            matcherThread.addDbRecords(this.getDbList());
            matcherThread.setDbDone();

            matcherThread.addFileRecords(this.getFileList());
            matcherThread.setFileDone();

            matcherThread.waitTillDone();
        }
        catch (AbortException e)
        {
            LOGGER.error("Failed : " + e.getMessage(), e);
            fail();
        }

        if (bitemporalOrderExecutor.anyFailed())
        {
            fail();
        }

        return bitemporalOrderExecutor;
    }

    private void assertResultsDefaultComparator(QueueExecutor bitemporalOrderItemExecutor)
    {
        Assert.assertEquals("Total inserts: ", 2, bitemporalOrderItemExecutor.getTotalInserts());
        Assert.assertEquals("Total updates: ", 3, bitemporalOrderItemExecutor.getTotalUpdates());
        Assert.assertEquals("Total terminates: ", 1, bitemporalOrderItemExecutor.getTotalTerminates());

        Operation businessDateOperation = BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()));

        BitemporalOrder item2 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(2).and(businessDateOperation));
        Assert.assertEquals("two two", item2.getDescription());
        Assert.assertEquals(2, item2.getUserId());

        assertCommonRows(businessDateOperation);
    }

    protected void assertResultsCustomComparator(QueueExecutor bitemporalOrderItemExecutor)
    {
        Assert.assertEquals("Total inserts: ", 2, bitemporalOrderItemExecutor.getTotalInserts());
        Assert.assertEquals("Total updates: ", 2, bitemporalOrderItemExecutor.getTotalUpdates());
        Assert.assertEquals("Total terminates: ", 1, bitemporalOrderItemExecutor.getTotalTerminates());

        Operation businessDateOperation = BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()));

        BitemporalOrder item2 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(2).and(businessDateOperation));
        Assert.assertEquals("two", item2.getDescription());
        Assert.assertEquals(2, item2.getUserId());

        assertCommonRows(businessDateOperation);
    }

    private void assertCommonRows(Operation businessDateOperation)
    {
        BitemporalOrder item1 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(1).and(businessDateOperation));
        Assert.assertEquals("one", item1.getDescription());
        Assert.assertEquals(13, item1.getUserId());

        BitemporalOrder item3 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(3).and(businessDateOperation));
        Assert.assertEquals("three", item3.getDescription());
        Assert.assertEquals(33, item3.getUserId());

        BitemporalOrder item4 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(4).and(businessDateOperation));
        Assert.assertEquals("four", item4.getDescription());
        Assert.assertEquals(4, item4.getUserId());

        BitemporalOrder item5 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(5).and(businessDateOperation));
        Assert.assertEquals("fifty a", item5.getDescription());
        Assert.assertEquals(50, item5.getUserId());
        Assert.assertEquals(JAN_ONE_TWENTY_FIFTEEN, item5.getProcessingDateFrom());

        BitemporalOrder item6 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(6).and(businessDateOperation));
        Assert.assertEquals("sixty", item6.getDescription());
        Assert.assertEquals(60, item6.getUserId());
        Assert.assertNotEquals(JAN_ONE_TWENTY_FIFTEEN, item6.getProcessingDateFrom());

        BitemporalOrder item7 = BitemporalOrderFinder.findOneBypassCache(BitemporalOrderFinder.orderId().eq(7).and(businessDateOperation));
        Assert.assertNull(item7);
    }

    private static List<BitemporalOrder> getFileList()
    {
        // item1 -> new row
        // item2 -> change in description
        // item3 -> change in userId
        // item4 -> change in both description and userId
        // item5 -> no change
        // item6 -> row of milestoned entry comes again
        // item7 -> does not come any more
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        BitemporalOrder item1 = new BitemporalOrder(1, "one", 13, businessDate);
        BitemporalOrder item2 = new BitemporalOrder(2, "two two", 2, businessDate);
        BitemporalOrder item3 = new BitemporalOrder(3, "three", 33, businessDate);
        BitemporalOrder item4 = new BitemporalOrder(4, "four", 4, businessDate);
        BitemporalOrder item5 = new BitemporalOrder(5, "fifty a", 50, businessDate);
        BitemporalOrder item6 = new BitemporalOrder(6, "sixty", 60, businessDate);

        return FastList.newListWith(item1, item2, item3, item4, item5, item6);
    }

    private static Timestamp getTimestamp(String date)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        try
        {
            return new Timestamp(dateFormat.parse(date).getTime());
        }
        catch (ParseException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class InputDataLoader implements InputLoader
    {
        private boolean firstTime = true;

        @Override
        public List<? extends MithraTransactionalObject> getNextParsedObjectList()
        {
            return getFileList();
        }

        @Override
        public boolean isFileParsingComplete()
        {
            if (firstTime)
            {
                firstTime = false;
                return false;
            }
            else
            {
                return true;
            }
        }
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        super.tearDown();
    }
}
