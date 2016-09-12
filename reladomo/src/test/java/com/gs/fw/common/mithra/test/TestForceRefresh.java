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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;



public class TestForceRefresh extends MithraTestAbstract
{

    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            FileDirectory.class,
            TestEodAcctIfPnl.class,
            TestAcctEntitySegment.class,
            TestBalanceNoAcmap.class
        };
    }

    public void testForceRefreshOpBasedList() throws SQLException
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        list.forceResolve();
        assertTrue(list.size() > 0);
        int count = this.getRetrievalCount();
        list.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshNonDatedSimpleList() throws SQLException
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        list.forceResolve();
        assertTrue(list.size() > 2);
        OrderList list2 = new OrderList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshNonDatedMultiPkList() throws SQLException
    {
        FileDirectoryList list = new FileDirectoryList(FileDirectoryFinder.isDirectory().eq(1));
        list.forceResolve();
        assertTrue(list.size() > 2);
        FileDirectoryList list2 = new FileDirectoryList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshNonDatedMultiPkListInTx() throws SQLException
    {
        FileDirectoryList list = new FileDirectoryList(FileDirectoryFinder.isDirectory().eq(1));
        list.forceResolve();
        assertTrue(list.size() > 2);
        final FileDirectoryList list2 = new FileDirectoryList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list2.forceRefresh();
                return null;
            }
        });
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshSingleDatedMultiPkList() throws SQLException
    {
        Operation op = TestEodAcctIfPnlFinder.userId().eq("moh");
        op = op.and(TestEodAcctIfPnlFinder.processingDate().eq(createNowTimestamp()));
        TestEodAcctIfPnlList list = new TestEodAcctIfPnlList(op);
        list.forceResolve();
        assertTrue(list.size() > 1);
        TestEodAcctIfPnlList list2 = new TestEodAcctIfPnlList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshSingleDatedMultiPkListInTx() throws SQLException
    {
        Operation op = TestEodAcctIfPnlFinder.userId().eq("moh");
        op = op.and(TestEodAcctIfPnlFinder.processingDate().eq(createNowTimestamp()));
        TestEodAcctIfPnlList list = new TestEodAcctIfPnlList(op);
        list.forceResolve();
        assertTrue(list.size() > 1);
        final TestEodAcctIfPnlList list2 = new TestEodAcctIfPnlList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list2.forceRefresh();
                return null;
            }
        });
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshHugeNonDatedSimpleList() throws SQLException
    {
        int startOrderId = 5000;
        int countToInsert = 2034;
        OrderList list = new OrderList();
        for(int i=0;i<countToInsert;i++)
        {
            Order order = new Order();
            order.setOrderId(i+startOrderId);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(createNowTimestamp());
            list.add(order);
        }
        list.insertAll();

        OrderList list2 = new OrderList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+3, this.getRetrievalCount());
    }

    public void testForceRefreshHugeNonDatedMultiPkList() throws SQLException
    {
        int startDirId = 5000;
        int countToInsert = 2034;
        FileDirectoryList list = new FileDirectoryList();
        for(int i=0;i<countToInsert;i++)
        {
            FileDirectory fd = new FileDirectory();
            fd.setFileDirectoryId(i+startDirId);
            fd.setDrive("D");
            fd.setName("fs number"+i);
            list.add(fd);
        }
        list.insertAll();

        FileDirectoryList list2 = new FileDirectoryList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshHugeSingleDatedMultiPkList() throws SQLException
    {
        int countToInsert = 2034;
        TestEodAcctIfPnlList list = new TestEodAcctIfPnlList();
        for(int i=0;i<countToInsert;i++)
        {
            TestEodAcctIfPnl ifpnl = new TestEodAcctIfPnl(InfinityTimestamp.getParaInfinity());
            ifpnl.setAccountId("777"+i);
            ifpnl.setIfCode("I"+i/10);
            ifpnl.setBusinessDate(createNowTimestamp());
            ifpnl.setBalanceType(15);
            list.add(ifpnl);
        }
        list.insertAll();

        TestEodAcctIfPnlList list2 = new TestEodAcctIfPnlList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshBitemporalSinglePkList() throws SQLException
    {
        Timestamp now = createNowTimestamp();
        int countToInsert = 2034;
        TestAcctEntitySegmentList list = new TestAcctEntitySegmentList();
        for(int i=0;i<countToInsert;i++)
        {
            TestAcctEntitySegment aes = new TestAcctEntitySegment(now);
            aes.setAccountId("777"+i);
            aes.setEntity("e"+i);
            aes.setSegmentId(10+i);
            list.add(aes);
        }
        list.bulkInsertAll();

        TestAcctEntitySegmentList list2 = new TestAcctEntitySegmentList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+3, this.getRetrievalCount());
    }

    public void testForceRefreshBitemporalSinglePkListInTx() throws SQLException
    {
        Timestamp now = createNowTimestamp();
        int countToInsert = 2034;
        TestAcctEntitySegmentList list = new TestAcctEntitySegmentList();
        for(int i=0;i<countToInsert;i++)
        {
            TestAcctEntitySegment aes = new TestAcctEntitySegment(now);
            aes.setAccountId("777"+i);
            aes.setEntity("e"+i);
            aes.setSegmentId(10+i);
            list.add(aes);
        }
        list.insertAll();

        final TestAcctEntitySegmentList list2 = new TestAcctEntitySegmentList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list2.forceRefresh();
                return null;
            }
        });
        assertEquals(count+3, this.getRetrievalCount());
    }

    private Timestamp createNowTimestamp()
    {
        return new Timestamp(System.currentTimeMillis()/10*10);
    }

    public void testForceRefreshBitemporalMultiPkList() throws SQLException
    {
        TestBalanceNoAcmapList list2 = insertBitemporalMulitPkAndCopyToAnotherList();
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        for(int i=0;i<list2.size();i++)
        {
            TestBalanceNoAcmap testBal = list2.get(i);
            assertEquals("777"+i, testBal.getAccountId());
        }
        assertEquals(count+1, this.getRetrievalCount());
    }

    public TestBalanceNoAcmapList insertBitemporalMulitPkAndCopyToAnotherList()
    {
        Timestamp now = createNowTimestamp();
        int countToInsert = 2034;
        TestBalanceNoAcmapList list = new TestBalanceNoAcmapList();
        for(int i=0;i<countToInsert;i++)
        {
            TestBalanceNoAcmap testBal = new TestBalanceNoAcmap(now);
            testBal.setAccountId("777"+i);
            testBal.setPositionType(10+i);
            testBal.setProductId(i+2000);
            testBal.setQuantity(i*10);
            list.add(testBal);
        }
        list.bulkInsertAll();

        TestBalanceNoAcmapList list2 = new TestBalanceNoAcmapList();
        list2.addAll(list);
        return list2;
    }

    public void testForceRefreshBitemporalMultiPkListInTx() throws SQLException
    {
        Timestamp now = createNowTimestamp();
        int countToInsert = 2034;
        TestBalanceNoAcmapList list = new TestBalanceNoAcmapList();
        for(int i=0;i<countToInsert;i++)
        {
            TestBalanceNoAcmap testBal = new TestBalanceNoAcmap(now);
            testBal.setAccountId("777"+i);
            testBal.setPositionType(10+i);
            testBal.setProductId(i+2000);
            testBal.setQuantity(i*10);
            list.add(testBal);
        }
        list.insertAll();

        final TestBalanceNoAcmapList list2 = new TestBalanceNoAcmapList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                list2.forceRefresh();
                return null;
            }
        });
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testForceRefreshNonDatedSimpleListInTransaction() throws SQLException
    {
        OrderList list = new OrderList(OrderFinder.userId().eq(1));
        list.forceResolve();
        assertTrue(list.size() > 2);
        OrderList list2 = new OrderList();
        list2.addAll(list);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
        tx.commit();
    }

    public void testForceRefreshHugeNonDatedMultiPkListInTransaction() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testForceRefreshHugeNonDatedMultiPkList();
                return null;
            }
        });
    }

    public void testForceRefreshBitemporalMultiPkListInTransaction() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testForceRefreshBitemporalMultiPkList();
                return null;
            }
        });
    }
}
