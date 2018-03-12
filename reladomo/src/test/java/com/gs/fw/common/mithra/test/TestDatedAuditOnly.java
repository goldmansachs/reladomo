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

import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;



public class TestDatedAuditOnly extends MithraTestAbstract implements TestDatedAuditOnlyDatabaseChecker
{

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private TestDatedAuditOnlyDatabaseChecker checker = this;

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AuditOnlyBalance.class, AuditedOrder.class, AuditedOrderStatus.class, AuditedOrderStatusTwo.class, AuditedOrderItem.class, Order.class
        };
    }

    public void setChecker(TestDatedAuditOnlyDatabaseChecker checker)
    {
        this.checker = checker;
    }

    private AuditOnlyBalance findAuditOnlyBalanceForInfinity(int balanceId)
    {
        return findAuditOnlyBalanceForDate(balanceId, InfinityTimestamp.getParaInfinity());
    }

    private AuditOnlyBalance findAuditOnlyBalanceForDate(int balanceId, Timestamp processingTime)
    {
        return AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.acmapCode().eq("A")
                .and(AuditOnlyBalanceFinder.balanceId().eq(balanceId))
                .and(AuditOnlyBalanceFinder.processingDate().eq(processingTime)));
    }

    public void testInsertInTransaction() throws SQLException
    {
        AuditOnlyBalance tb = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.setInterest(22.1);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(2000);
            assertSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(2000);
        assertSame(tb, fromCache);
        fromCache = findAuditOnlyBalanceForInfinity(2000);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedAuditOnlyInfinityRow(2000, 12.5, 22.1);
    }

    public void testInsertInTransactionCustomInz() throws SQLException, ParseException
    {
        long customInZ = timestampFormat.parse("2002-11-29 00:00:00").getTime();

        AuditOnlyBalance tb = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tx.setProcessingStartTime(customInZ);
        try
        {
            tb = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.setInterest(22.1);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(2000);
            assertSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findAuditOnlyBalanceForDate(2000, new Timestamp(customInZ - 1000));
            assertNull(fromCache);
            fromCache = findAuditOnlyBalanceForDate(2000, new Timestamp(customInZ + 1000));
            assertNotNull(fromCache);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(2000);
        assertSame(tb, fromCache);
        fromCache = findAuditOnlyBalanceForInfinity(2000);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        fromCache = findAuditOnlyBalanceForDate(2000, new Timestamp(customInZ - 1000));
        assertNull(fromCache);
        fromCache = findAuditOnlyBalanceForDate(2000, new Timestamp(customInZ + 1000));
        assertNotNull(fromCache);

        // test the database:
        this.checker.checkDatedAuditOnlyInfinityRow(2000, 12.5, 22.1);
    }

    public void testInsertRollback() throws SQLException
    {
        AuditOnlyBalance tb = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(2000);
            assertSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.rollback();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(2000);
        assertNull(fromCache);
        // test the database:

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findAuditOnlyBalanceForInfinity(2000);
            assertSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.rollback();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }

        //test the database
        this.checker.checkDatedAuditOnlyTerminated(2000);
    }

    public void testRapidUpdate() throws SQLException, ParseException
    {
        final int balanceId = 1;
        final AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        double quantity = fromCache.getQuantity();
        for(int i=0;i<30;i++)
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                @Override
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    fromCache.setQuantity(fromCache.getQuantity() + 1);
                    return null;
                }
            });
        }
        AuditOnlyBalance x = getInfinityBalance(balanceId);
        assertEquals(quantity + 30, x.getQuantity(), 0);
    }

    public void testUpdate() throws SQLException, ParseException
    {
        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        int updateCount = AuditOnlyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance fromCache = getFutureBalance(balanceId);
            tb.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            tb.setInterest(22.1);
            assertEquals(22.1, fromCache.getInterest(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(22.1, tb.getInterest(), 0);

            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = getInfinityBalance(balanceId);
            assertSame(tb, fromCache);
            fromCache = getFutureBalance(balanceId);
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        assertTrue(AuditOnlyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount() > updateCount);
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);

        fromCache = getFutureBalance(balanceId);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedAuditOnlyInfinityRow(balanceId, 12.5, 22.1);
    }

    public void testUpdateCustomInz() throws SQLException, ParseException
    {
        long customInZ = timestampFormat.parse("2002-11-29 04:00:00").getTime();

        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tx.setProcessingStartTime(customInZ);
        try
        {
            AuditOnlyBalance fromCache = getFutureBalance(balanceId);
            tb.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            tb.setInterest(22.1);
            assertEquals(22.1, fromCache.getInterest(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(22.1, tb.getInterest(), 0);

            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = getInfinityBalance(balanceId);
            assertSame(tb, fromCache);
            fromCache = getFutureBalance(balanceId);
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);

        fromCache = getFutureBalance(balanceId);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        fromCache = getBalanceForTime(balanceId, new Timestamp(customInZ + 1000));
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);

        fromCache = getBalanceForTime(balanceId, new Timestamp(customInZ - 1000));
        assertFalse(12.5 == fromCache.getQuantity());
        assertFalse(22.1 == fromCache.getInterest());

        // test the database:
        this.checker.checkDatedAuditOnlyInfinityRow(balanceId, 12.5, 22.1);
    }

    public void testUpdateWithFlush() throws SQLException, ParseException
    {
        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance fromCache = getFutureBalance(balanceId);
            tb.setQuantity(12.5);
            tx.executeBufferedOperations();
            assertEquals(12.5, fromCache.getQuantity(), 0);
            tb.setInterest(22.1);
            assertEquals(22.1, fromCache.getInterest(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(22.1, tb.getInterest(), 0);

            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = getInfinityBalance(balanceId);
            assertSame(tb, fromCache);
            fromCache = getFutureBalance(balanceId);
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);

        fromCache = getFutureBalance(balanceId);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(22.1, fromCache.getInterest(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedAuditOnlyInfinityRow(balanceId, 12.5, 22.1);
    }

    public void testTerminate() throws SQLException, ParseException
    {
        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance fromCache = getFutureBalance(balanceId);
            tb.terminate();
            assertTrue(tb.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(fromCache);

            fromCache = getInfinityBalance(balanceId);
            assertNull(fromCache);
            fromCache = getFutureBalance(balanceId);
            assertNull(fromCache);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        assertNull(fromCache);
        fromCache = getFutureBalance(balanceId);
        assertNull(fromCache);
        // test the database:
        this.checker.checkDatedAuditOnlyTerminated(balanceId);
    }

    public void testTerminateDualInsertTerminateOther() throws SQLException, ParseException
    {
        int orderItemId = 1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditedOrderItem item = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(orderItemId));
            AuditedOrderItem other = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(2));
            AuditedOrderItem forInsert = item.getNonPersistentCopy();
            item.terminate();

            AuditedOrderItem moreInsert = forInsert.getNonPersistentCopy();
            moreInsert.setId(1000);
            moreInsert.insert();

            // buffer the two insert
            Order order = new Order();
            order.setOrderId(1000);
            order.insert();

            forInsert.setQuantity(1234);
            forInsert.insert();

            other.terminate();

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditedOrderItem fromCache = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(orderItemId));
        assertNotNull(fromCache);
        // test the database:
        fromCache = AuditedOrderItemFinder.findOneBypassCache(AuditedOrderItemFinder.id().eq(orderItemId));
        assertNotNull(fromCache);
        assertEquals(1234, fromCache.getQuantity(), 0);
    }

    public void testTerminateCustomInz() throws SQLException, ParseException
    {
        long customInZ = timestampFormat.parse("2002-11-29 04:00:00").getTime();

        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tx.setProcessingStartTime(customInZ);
        try
        {
            AuditOnlyBalance fromCache = getFutureBalance(balanceId);
            tb.terminate();
            assertTrue(tb.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(fromCache);

            fromCache = getInfinityBalance(balanceId);
            assertNull(fromCache);
            fromCache = getFutureBalance(balanceId);
            assertNull(fromCache);
            fromCache = getBalanceForTime(balanceId, new Timestamp(customInZ + 1000));
            assertNull(fromCache);
            fromCache = getBalanceForTime(balanceId, new Timestamp(customInZ - 1000));
            assertNotNull(fromCache);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        assertNull(fromCache);
        fromCache = getFutureBalance(balanceId);
        assertNull(fromCache);
        fromCache = getBalanceForTime(balanceId, new Timestamp(customInZ + 1000));
        assertNull(fromCache);
        fromCache = getBalanceForTime(balanceId, new Timestamp(customInZ - 1000));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedAuditOnlyTerminated(balanceId);
    }

    public void testUpdateTerminate() throws SQLException, ParseException
    {
        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance fromCache = getFutureBalance(balanceId);
            tb.setQuantity(77);
            tb.terminate();
            assertTrue(tb.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(fromCache);

            fromCache = getInfinityBalance(balanceId);
            assertNull(fromCache);
            fromCache = getFutureBalance(balanceId);
            assertNull(fromCache);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditOnlyBalance fromCache = getInfinityBalance(balanceId);
        assertNull(fromCache);
        fromCache = getFutureBalance(balanceId);
        assertNull(fromCache);
        // test the database:
        this.checker.checkDatedAuditOnlyTerminated(balanceId);
    }

    public void testLargeUpdate()
    {
        final int balanceTypeIdStart = 100000;
        final IntHashSet balanceTypeIds = new IntHashSet();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>() {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable {
                AuditOnlyBalanceList balanceList = new AuditOnlyBalanceList();
                for(int i = 0; i< 102; i++)
                {
                    AuditOnlyBalance newBalance = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
                    newBalance.setAcmapCode("A");
                    int currentBalanceTypeId = balanceTypeIdStart + i;
                    newBalance.setBalanceId(currentBalanceTypeId);
                    newBalance.setInterest(10000);
                    newBalance.setQuantity(20000);
                    balanceList.add(newBalance);
                    balanceTypeIds.add(currentBalanceTypeId);
                }
                balanceList.insertAll();
                return null;
            }
        });
        sleep(100);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>() {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable {
                AuditOnlyBalanceList balanceList = AuditOnlyBalanceFinder.findMany(
                        AuditOnlyBalanceFinder.acmapCode().eq("A").and(AuditOnlyBalanceFinder.balanceId().in(balanceTypeIds)));
                balanceList.setOrderBy(AuditOnlyBalanceFinder.balanceId().ascendingOrderBy());
                int i = 0;
                for(; i< 100; i++)
                {
                    balanceList.get(i).terminate();
                }
                balanceList.get(i).setQuantity(50);
                i++;
                balanceList.get(i).terminate();

                return null;
            }
        });

        AuditOnlyBalanceList liveBalanceList = AuditOnlyBalanceFinder.findMany(
                AuditOnlyBalanceFinder.acmapCode().eq("A").and(AuditOnlyBalanceFinder.balanceId().in(balanceTypeIds)));
        assertEquals("Only 1 balance should be live.", 1, liveBalanceList.size());
        assertEquals("Incorrect quantity.", 50, liveBalanceList.get(0).getQuantity(), 0.001);
        assertEquals("Incorrent interest", 10000, liveBalanceList.get(0).getInterest(), 0.001);

        AuditOnlyBalanceList allBalanceList = AuditOnlyBalanceFinder.findMany(
                AuditOnlyBalanceFinder.acmapCode().eq("A").and(
                        AuditOnlyBalanceFinder.balanceId().in(balanceTypeIds)).and(
                        AuditOnlyBalanceFinder.processingDate().equalsEdgePoint()
                ));
        assertEquals("There should be 102 out_zied records and 1 live record", 103, allBalanceList.size());
    }

    public void testPurge() throws SQLException, ParseException
    {
        int balanceId = 2;
        AuditOnlyBalance currentBalance = getInfinityBalance(balanceId);
        assertNotNull(currentBalance);
        AuditOnlyBalance pastBalance = getBalanceForYear(balanceId, 2003);
        assertNotNull(pastBalance);
        AuditOnlyBalance futureBalance = getFutureBalance(balanceId);
        assertNotNull(futureBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(futureBalance);
            this.checkQuantityThrowsException(pastBalance);

            currentBalance = getInfinityBalance(balanceId);
            assertNull(currentBalance);
            futureBalance = getFutureBalance(balanceId);
            assertNull(futureBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = getInfinityBalance(balanceId);
        assertNull(currentBalance);
        futureBalance = getFutureBalance(balanceId);
        assertNull(futureBalance);
        pastBalance = getBalanceForYear(balanceId, 2003);
        assertNull(pastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceId));
    }

    public void testPurgeForNestedTransaction() throws SQLException, ParseException
    {
        int balanceId = 2;
        AuditOnlyBalance currentBalance = getInfinityBalance(balanceId);
        assertNotNull(currentBalance);
        AuditOnlyBalance pastBalance = getBalanceForYear(balanceId, 2003);
        assertNotNull(pastBalance);
        AuditOnlyBalance futureBalance = getFutureBalance(balanceId);
        assertNotNull(futureBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        MithraTransaction nestedTx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(nestedTx));

            this.checkQuantityThrowsException(futureBalance);
            this.checkQuantityThrowsException(pastBalance);

            currentBalance = getInfinityBalance(balanceId);
            assertNull(currentBalance);
            futureBalance = getFutureBalance(balanceId);
            assertNull(futureBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);
            nestedTx.commit();
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = getInfinityBalance(balanceId);
        assertNull(currentBalance);
        futureBalance = getFutureBalance(balanceId);
        assertNull(futureBalance);
        pastBalance = getBalanceForYear(balanceId, 2003);
        assertNull(pastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceId));
    }

    public void testPurgeCombineBatchPurge() throws SQLException, ParseException
    {
        int balanceIdA = 2;
        int balanceIdB = 1;
        int balanceIdC = 3;
        AuditOnlyBalance currentBalanceA = getInfinityBalance(balanceIdA);
        assertNotNull(currentBalanceA);
        AuditOnlyBalance currentBalanceB = getInfinityBalance(balanceIdB);
        assertNotNull(currentBalanceB);
        AuditOnlyBalance currentBalanceC = getInfinityBalance(balanceIdC);
        assertNotNull(currentBalanceC);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalanceA.purge();
            currentBalanceB.purge();
            currentBalanceC.purge();

            this.checkQuantityThrowsException(currentBalanceA);
            this.checkQuantityThrowsException(currentBalanceB);
            this.checkQuantityThrowsException(currentBalanceC);

            currentBalanceA = getInfinityBalance(balanceIdA);
            assertNull(currentBalanceA);
            currentBalanceB = getInfinityBalance(balanceIdB);
            assertNull(currentBalanceB);
            currentBalanceC = getInfinityBalance(balanceIdC);
            assertNull(currentBalanceC);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalanceA = getInfinityBalance(balanceIdA);
        assertNull(currentBalanceA);
        currentBalanceB = getInfinityBalance(balanceIdB);
        assertNull(currentBalanceB);
        currentBalanceC = getInfinityBalance(balanceIdC);
        assertNull(currentBalanceC);

        // test the database:
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceIdA));
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceIdB));
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceIdC));
    }

    public void testPurgeAfterUpdate() throws SQLException, ParseException
    {
        int balanceId = 2;
        AuditOnlyBalance currentBalance = getInfinityBalance(balanceId);
        assertNotNull(currentBalance);
        AuditOnlyBalance pastBalance = getBalanceForYear(balanceId, 2003);
        assertNotNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.setQuantity(12345);
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(pastBalance);

            currentBalance = getInfinityBalance(balanceId);
            assertNull(currentBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = getInfinityBalance(balanceId);
        assertNull(currentBalance);
        pastBalance = getBalanceForYear(balanceId, 2003);
        assertNull(pastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceId));
    }

    public void testPurgeMultipleTimes() throws SQLException, ParseException
    {
        int balanceId = 2;
        AuditOnlyBalance currentBalance = getInfinityBalance(balanceId);
        assertNotNull(currentBalance);
        AuditOnlyBalance pastBalance = getBalanceForYear(balanceId, 2003);
        assertNotNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            //Assert the purge
            currentBalance.purge();
            currentBalance = getInfinityBalance(balanceId);
            assertNull(currentBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);

            //Insert again
            AuditOnlyBalance newBalance = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            newBalance.setAcmapCode("A");
            newBalance.setBalanceId(balanceId);
            newBalance.setInterest(10000);
            newBalance.setQuantity(20000);
            newBalance.insert();

            currentBalance = getInfinityBalance(balanceId);
            assertNotNull(currentBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);

            //Purge again
            currentBalance.purge();
            currentBalance = getInfinityBalance(balanceId);
            assertNull(currentBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = getInfinityBalance(balanceId);
        assertNull(currentBalance);
        pastBalance = getBalanceForYear(balanceId, 2003);
        assertNull(pastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceId));
    }

    public void testPurgeThenRollback() throws SQLException, ParseException
    {
        int balanceId = 2;
        AuditOnlyBalance currentBalance = getInfinityBalance(balanceId);
        assertNotNull(currentBalance);
        AuditOnlyBalance pastBalance = getBalanceForYear(balanceId, 2003);
        assertNotNull(pastBalance);

        int originalCount = this.checker.checkDatedAuditOnlyRowCounts(balanceId);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            //Assert the purge
            currentBalance.purge();
            currentBalance = getInfinityBalance(balanceId);
            assertNull(currentBalance);
            pastBalance = getBalanceForYear(balanceId, 2003);
            assertNull(pastBalance);

            tx.rollback();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = getInfinityBalance(balanceId);
        assertNotNull(currentBalance);
        pastBalance = getBalanceForYear(balanceId, 2003);
        assertNotNull(pastBalance);
        // test the database:
        assertEquals(originalCount, this.checker.checkDatedAuditOnlyRowCounts(balanceId));
    }

    public void testInsertForRecovery() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromProcessingDate = new Timestamp(timestampFormat.parse("2003-06-01 05:31:00").getTime());
        Timestamp queryProcessingDate = new Timestamp(timestampFormat.parse("2003-06-01 05:45:00").getTime());
        Timestamp toProcessingDate = new Timestamp(timestampFormat.parse("2003-06-01 06:11:00").getTime());

        AuditOnlyBalance testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
        assertNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
        assertNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance recoveredBalance = new AuditOnlyBalance(fromProcessingDate);
            recoveredBalance.setAcmapCode("A");
            recoveredBalance.setBalanceId(balanceId);
            recoveredBalance.setInterest(10000);
            recoveredBalance.setQuantity(20000);
            recoveredBalance.setProcessingDateFrom(fromProcessingDate);
            recoveredBalance.setProcessingDateTo(toProcessingDate);
            recoveredBalance.insertForRecovery();

            testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
            assertNull(testQuery);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
        assertNotNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
        assertNotNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
        assertNull(testQuery);

        // test the database:
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, 20000, 10000, queryProcessingDate);
    }

    public void testInsertForRecoveryMultipleTimes() throws SQLException, ParseException
    {
        int balanceId = 200;

        Timestamp timeA = new Timestamp(timestampFormat.parse("2003-06-01 05:31:00").getTime());
        Timestamp queryProcessingDateA = new Timestamp(timestampFormat.parse("2003-06-01 05:45:00").getTime());
        Timestamp timeB = new Timestamp(timestampFormat.parse("2003-06-01 06:11:00").getTime());
        Timestamp queryProcessingDateB = new Timestamp(timestampFormat.parse("2003-07-01 05:45:00").getTime());
        Timestamp timeC = InfinityTimestamp.getParaInfinity();

        assertNull(this.getBalanceForTime(balanceId, timeA));
        assertNull(this.getBalanceForTime(balanceId, queryProcessingDateA));
        assertNull(this.getBalanceForTime(balanceId, timeB));
        assertNull(this.getBalanceForTime(balanceId, queryProcessingDateB));
        assertNull(this.getBalanceForTime(balanceId, timeC));

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance recoveredBalanceA = new AuditOnlyBalance(timeA);
            recoveredBalanceA.setAcmapCode("A");
            recoveredBalanceA.setBalanceId(balanceId);
            recoveredBalanceA.setInterest(10000);
            recoveredBalanceA.setQuantity(20000);
            recoveredBalanceA.setProcessingDateFrom(timeA);
            recoveredBalanceA.setProcessingDateTo(timeB);
            recoveredBalanceA.insertForRecovery();

            AuditOnlyBalance recoveredBalanceB = new AuditOnlyBalance(timeB);
            recoveredBalanceB.setAcmapCode("A");
            recoveredBalanceB.setBalanceId(balanceId);
            recoveredBalanceB.setInterest(6000);
            recoveredBalanceB.setQuantity(-8000);
            recoveredBalanceB.setProcessingDateFrom(timeB);
            recoveredBalanceB.setProcessingDateTo(timeC);
            recoveredBalanceB.insertForRecovery();

            AuditOnlyBalance queryTimeA = this.getBalanceForTime(balanceId, timeA);
            AuditOnlyBalance queryQueryTimeA = this.getBalanceForTime(balanceId, queryProcessingDateA);
            AuditOnlyBalance queryTimeB = this.getBalanceForTime(balanceId, timeB);
            AuditOnlyBalance queryQueryTimeB = this.getBalanceForTime(balanceId, queryProcessingDateB);
            AuditOnlyBalance queryTimeC = this.getBalanceForTime(balanceId, timeC);

            assertNotNull(queryTimeA);
            assertNotNull(queryTimeB);
            assertNotNull(queryTimeC);
            assertNotNull(queryQueryTimeA);
            assertNotNull(queryQueryTimeB);

            assertEquals(10000, queryTimeA.getInterest(), 0);
            assertEquals(10000, queryQueryTimeA.getInterest(), 0);
            assertEquals(6000, queryTimeB.getInterest(), 0);
            assertEquals(6000, queryQueryTimeB.getInterest(), 0);
            assertEquals(6000, queryTimeC.getInterest(), 0);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditOnlyBalance queryTimeA = this.getBalanceForTime(balanceId, timeA);
        AuditOnlyBalance queryQueryTimeA = this.getBalanceForTime(balanceId, queryProcessingDateA);
        AuditOnlyBalance queryTimeB = this.getBalanceForTime(balanceId, timeB);
        AuditOnlyBalance queryQueryTimeB = this.getBalanceForTime(balanceId, queryProcessingDateB);
        AuditOnlyBalance queryTimeC = this.getBalanceForTime(balanceId, timeC);

        assertNotNull(queryTimeA);
        assertNotNull(queryTimeB);
        assertNotNull(queryTimeC);
        assertNotNull(queryQueryTimeA);
        assertNotNull(queryQueryTimeB);

        assertEquals(10000, queryTimeA.getInterest(), 0);
        assertEquals(10000, queryQueryTimeA.getInterest(), 0);
        assertEquals(6000, queryTimeB.getInterest(), 0);
        assertEquals(6000, queryQueryTimeB.getInterest(), 0);
        assertEquals(6000, queryTimeC.getInterest(), 0);

        // test the database:
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, 20000, 10000, timeA);
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, 20000, 10000, queryProcessingDateA);
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, -8000, 6000, timeB);
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, -8000, 6000, queryProcessingDateB);
        this.checker.checkDatedAuditOnlyInfinityRow(balanceId, -8000, 6000);
    }

    public void testInsertForRecoveryThenInsert() throws SQLException, ParseException
    {
        int balanceId = 200;

        Timestamp timeA = new Timestamp(timestampFormat.parse("2003-06-01 05:31:00").getTime());
        Timestamp queryProcessingDateA = new Timestamp(timestampFormat.parse("2003-06-01 05:45:00").getTime());
        Timestamp timeB = new Timestamp(timestampFormat.parse("2003-06-01 06:11:00").getTime());
        Timestamp queryProcessingDateB = new Timestamp(timestampFormat.parse("2003-07-01 05:45:00").getTime());
        Timestamp timeC = InfinityTimestamp.getParaInfinity();

        assertNull(this.getBalanceForTime(balanceId, timeA));
        assertNull(this.getBalanceForTime(balanceId, queryProcessingDateA));
        assertNull(this.getBalanceForTime(balanceId, timeB));
        assertNull(this.getBalanceForTime(balanceId, queryProcessingDateB));
        assertNull(this.getBalanceForTime(balanceId, timeC));

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance recoveredBalanceA = new AuditOnlyBalance(timeA);
            recoveredBalanceA.setAcmapCode("A");
            recoveredBalanceA.setBalanceId(balanceId);
            recoveredBalanceA.setInterest(10000);
            recoveredBalanceA.setQuantity(20000);
            recoveredBalanceA.setProcessingDateFrom(timeA);
            recoveredBalanceA.setProcessingDateTo(timeB);
            recoveredBalanceA.insertForRecovery();

            AuditOnlyBalance recoveredBalanceB = new AuditOnlyBalance(timeC);
            recoveredBalanceB.setAcmapCode("A");
            recoveredBalanceB.setBalanceId(balanceId);
            recoveredBalanceB.setInterest(6000);
            recoveredBalanceB.setQuantity(-8000);
            recoveredBalanceB.insert();

            AuditOnlyBalance queryTimeA = this.getBalanceForTime(balanceId, timeA);
            AuditOnlyBalance queryQueryTimeA = this.getBalanceForTime(balanceId, queryProcessingDateA);
            AuditOnlyBalance queryTimeB = this.getBalanceForTime(balanceId, timeB);
            AuditOnlyBalance queryQueryTimeB = this.getBalanceForTime(balanceId, queryProcessingDateB);
            AuditOnlyBalance queryTimeC = this.getBalanceForTime(balanceId, timeC);

            assertNotNull(queryTimeA);
            assertNotNull(queryQueryTimeA);
            assertNull(queryTimeB);
            assertNull(queryQueryTimeB);
            assertNotNull(queryTimeC);

            assertEquals(10000, queryTimeA.getInterest(), 0);
            assertEquals(10000, queryQueryTimeA.getInterest(), 0);
            assertEquals(6000, queryTimeC.getInterest(), 0);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        AuditOnlyBalance queryTimeA = this.getBalanceForTime(balanceId, timeA);
        AuditOnlyBalance queryQueryTimeA = this.getBalanceForTime(balanceId, queryProcessingDateA);
        AuditOnlyBalance queryTimeB = this.getBalanceForTime(balanceId, timeB);
        AuditOnlyBalance queryQueryTimeB = this.getBalanceForTime(balanceId, queryProcessingDateB);
        AuditOnlyBalance queryTimeC = this.getBalanceForTime(balanceId, timeC);

        assertNotNull(queryTimeA);
        assertNotNull(queryQueryTimeA);
        assertNull(queryTimeB);
        assertNull(queryQueryTimeB);
        assertNotNull(queryTimeC);

        assertEquals(10000, queryTimeA.getInterest(), 0);
        assertEquals(10000, queryQueryTimeA.getInterest(), 0);
        assertEquals(6000, queryTimeC.getInterest(), 0);

        // test the database:
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, 20000, 10000, timeA);
        this.checker.checkDatedAuditOnlyTimestampRow(balanceId, 20000, 10000, queryProcessingDateA);
        this.checker.checkDatedAuditOnlyInfinityRow(balanceId, -8000, 6000);
    }

    public void testInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromProcessingDate = new Timestamp(timestampFormat.parse("2003-06-01 05:31:00").getTime());
        Timestamp queryProcessingDate = new Timestamp(timestampFormat.parse("2003-06-01 05:45:00").getTime());
        Timestamp toProcessingDate = new Timestamp(timestampFormat.parse("2003-06-01 06:11:00").getTime());

        AuditOnlyBalance testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
        assertNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
        assertNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalance recoveredBalance = new AuditOnlyBalance(fromProcessingDate);
            recoveredBalance.setAcmapCode("A");
            recoveredBalance.setBalanceId(balanceId);
            recoveredBalance.setInterest(10000);
            recoveredBalance.setQuantity(20000);
            recoveredBalance.setProcessingDateFrom(fromProcessingDate);
            recoveredBalance.setProcessingDateTo(toProcessingDate);
            recoveredBalance.insertForRecovery();

            testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
            assertNull(testQuery);

            recoveredBalance.purge();

            testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
            assertNull(testQuery);
            testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
            assertNull(testQuery);
            testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
            assertNull(testQuery);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        testQuery = this.getBalanceForTime(balanceId, queryProcessingDate);
        assertNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, fromProcessingDate);
        assertNull(testQuery);
        testQuery = this.getBalanceForTime(balanceId, toProcessingDate);
        assertNull(testQuery);

        // test the database:
        assertEquals(0, this.checker.checkDatedAuditOnlyRowCounts(balanceId));
    }

    public void testInsertAll() throws SQLException
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            AuditOnlyBalanceList list = new AuditOnlyBalanceList();
            for(int i=0;i<30;i++)
            {
                AuditOnlyBalance tb = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
                tb.setAcmapCode("A");
                tb.setBalanceId(2000+i);
                tb.setQuantity(12.5+i);
                tb.setInterest(22.1+i);
                list.add(tb);
            }
            list.insertAll();
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        for(int i=0;i<30;i++)
        {
            this.checker.checkDatedAuditOnlyInfinityRow(2000+i, 12.5+i, 22.1+i);
        }
    }

    public void testRefresh() throws SQLException
    {
        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);
        int retrievalCount = getRetrievalCount();
        MithraPerformanceData data = AuditOnlyBalanceFinder.getMithraObjectPortal().getPerformanceData();
        int refreshBefore = data.getDataForRefresh().getTotalObjects();


        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            assertEquals(balanceId, tb.getBalanceId());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }

        assertEquals(retrievalCount + 1, getRetrievalCount());
        data = AuditOnlyBalanceFinder.getMithraObjectPortal().getPerformanceData();
        assertEquals(refreshBefore + 1, data.getDataForRefresh().getTotalObjects());
    }

    public void testTransactionalRefresh() throws SQLException
    {
        int balanceId = 1;
        AuditOnlyBalance tb = getInfinityBalance(balanceId);
        assertNotNull(tb);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = getInfinityBalance(balanceId);
            assertNotNull(tb);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }

        Timestamp now = new Timestamp(System.currentTimeMillis());
        this.checker.updateDatedAuditOnlyTimestamp(balanceId, now);
        this.checker.insertDatedAuditOnly(balanceId, 145, 166, now);

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = getInfinityBalance(balanceId);
            assertEquals(145, tb.getQuantity(), 0);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }

    }

    public void testCacheReload() throws SQLException
    {
        if (AuditOnlyBalanceFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int balanceId = 1;
            AuditOnlyBalance tb = getInfinityBalance(balanceId);
            assertNotNull(tb);

            Timestamp now = new Timestamp(System.currentTimeMillis());
            this.checker.updateDatedAuditOnlyTimestamp(balanceId, now);
            this.checker.insertDatedAuditOnly(balanceId, 145, 166, now);

            AuditOnlyBalanceFinder.reloadCache();

            tb = getInfinityBalance(balanceId);
            assertEquals(145, tb.getQuantity(), 0);
        }

    }

    public void testCacheReloadInTx() throws SQLException
    {
        if (AuditOnlyBalanceFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int balanceId = 1;
            AuditOnlyBalance tb = getInfinityBalance(balanceId);
            assertNotNull(tb);

            Timestamp now = new Timestamp(System.currentTimeMillis());
            this.checker.updateDatedAuditOnlyTimestamp(balanceId, now);
            this.checker.insertDatedAuditOnly(balanceId, 145, 166, now);

            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                @Override
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    AuditOnlyBalanceFinder.reloadCache();
                    return null;
                }
            });

            tb = getInfinityBalance(balanceId);
            assertEquals(145, tb.getQuantity(), 0);
        }
    }

    public void testLastModifiedQuerying()
    {
        Timestamp fromTime = Timestamp.valueOf("1999-12-31 22:00:00.0");
        Timestamp toTime = Timestamp.valueOf("2000-01-01 22:00:00.0");
        Operation op = AuditedOrderFinder.userId().eq(1);
        op = op.and(AuditedOrderFinder.processingDate().equalsEdgePoint());

        Operation auditedOrderProcDateOp = AuditedOrderFinder.processingDateFrom().greaterThan(fromTime);
        auditedOrderProcDateOp = auditedOrderProcDateOp.and(AuditedOrderFinder.processingDateFrom().lessThanEquals(toTime));

        Operation orderStatusProcDateOp = AuditedOrderFinder.orderStatus().processingDateFrom().greaterThan(fromTime);
        orderStatusProcDateOp = orderStatusProcDateOp.and(AuditedOrderFinder.orderStatus().processingDateFrom().lessThanEquals(toTime));

        auditedOrderProcDateOp = auditedOrderProcDateOp.or(orderStatusProcDateOp);

        op = op.and(auditedOrderProcDateOp);

        AuditedOrderList auditedOrderList = new AuditedOrderList(op);
        assertEquals("Changed AuditedOrders ", 4, auditedOrderList.size());
    }

    public void testInactivateForArchive() throws SQLException, ParseException
    {
        final int balanceId = 2;
        final AuditOnlyBalance tb = findAuditOnlyBalanceForInfinity(balanceId);
        assertNotNull(tb);
        final Timestamp inactiveTime = new Timestamp(System.currentTimeMillis());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditOnlyBalance fromCache = findAuditOnlyBalanceForDate(balanceId, new Timestamp(System.currentTimeMillis()+1000));
                tb.inactivateForArchiving(inactiveTime, null);
                assertTrue(tb.zIsParticipatingInTransaction(tx));
                try
                {
                    fromCache.getQuantity();
                    fail("should not get here");
                }
                catch(MithraDeletedException e)
                {
                    // ok
                }
                fromCache = findAuditOnlyBalanceForInfinity(balanceId);
                assertNull(fromCache);
                fromCache = findAuditOnlyBalanceForDate(balanceId, new Timestamp(System.currentTimeMillis()+1000));
                assertNull(fromCache);
                return null;
            }
        });

        // check the cache:
        AuditOnlyBalance fromCache = findAuditOnlyBalanceForInfinity(balanceId);
        assertNull(fromCache);
        AuditOnlyBalanceFinder.clearQueryCache();
        fromCache = AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.acmapCode().eq("A")
                .and(AuditOnlyBalanceFinder.balanceId().eq(balanceId))
                .and(AuditOnlyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis()))));
        assertNull(fromCache);
        fromCache = AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.acmapCode().eq("A")
                .and(AuditOnlyBalanceFinder.balanceId().eq(balanceId))
                .and(AuditOnlyBalanceFinder.processingDate().eq(new Timestamp(inactiveTime.getTime() - 1000))));
        assertNotNull(fromCache);
        assertEquals(inactiveTime, fromCache.getProcessingDateTo());
    }

    public void testEqualsEdgePoint()
    {
        AuditedOrderList orderList = AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().eq(9999).and(
                AuditedOrderFinder.processingDate().equalsEdgePoint()));
        orderList.setOrderBy(AuditedOrderFinder.processingDateFrom().ascendingOrderBy());

        assertEquals(20, orderList.size());
        for(int i=1;i<orderList.size();i++)
        {
            AuditedOrder order1 = orderList.get(i-1);
            AuditedOrder order2 = orderList.get(i);
            assertTrue(order1.getProcessingDateFrom().getTime() < order2.getProcessingDateFrom().getTime());
        }
    }

    public void testMultipleOrOperations()
    {
        Operation op = AuditedOrderFinder.orderId().eq(1);
        op = op.or(AuditedOrderFinder.orderId().eq(2));
        op = op.or(AuditedOrderFinder.orderId().eq(3));
        AuditedOrderList orderList = AuditedOrderFinder.findMany(op);
        orderList.setOrderBy(AuditedOrderFinder.processingDateFrom().ascendingOrderBy());
        assertEquals(3, orderList.size());
    }

    public void testEqualsEdgePointInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testEqualsEdgePoint();
                return null;
            }
        });
    }

    public void testEqualsEdgePointAfterChange()
    {
        final AuditedOrder order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.setUserId(1000);
                return null;
            }
        });
        AuditedOrderList orderList = AuditedOrderFinder.findMany(AuditedOrderFinder.orderId().eq(1).and(
                AuditedOrderFinder.processingDate().equalsEdgePoint()));
        orderList.setOrderBy(AuditedOrderFinder.processingDateFrom().ascendingOrderBy());

        assertEquals(2, orderList.size());
        AuditedOrder order1 = orderList.get(0);
        AuditedOrder order2 = orderList.get(1);
        assertTrue(order1.getProcessingDateFrom().getTime() < order2.getProcessingDateFrom().getTime());
        assertEquals(1000, order2.getUserId());
    }

    public void testEqualsEdgePointAfterChangeInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testEqualsEdgePointAfterChange();
                return null;
            }
        });
    }

    private AuditOnlyBalance getFutureBalance(int balanceId)
    {
        return AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.balanceId().eq(balanceId).and(
                AuditOnlyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis()+1000)).and(AuditOnlyBalanceFinder.acmapCode().eq("A"))));
    }

    private AuditOnlyBalance getBalanceForYear(int balanceId, int year)
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, 5);
        cal.set(Calendar.DAY_OF_MONTH, 15);
        Timestamp time = new Timestamp(cal.getTime().getTime());

        return this.getBalanceForTime(balanceId, time);
    }

    private AuditOnlyBalance getBalanceForTime(int balanceId, Timestamp time)
    {
        return AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.balanceId().eq(balanceId).and(
                AuditOnlyBalanceFinder.processingDate().eq(time).and(AuditOnlyBalanceFinder.acmapCode().eq("A"))));
    }

    public void testGetNonPersistentCopyInTransaction()
    {
        final AuditOnlyBalance outOfTx = getInfinityBalance(1);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tran) throws Throwable
                    {
                        AuditOnlyBalance tb = getInfinityBalance(outOfTx.getBalanceId());
                        AuditOnlyBalance copy = tb.getNonPersistentCopy();
                        assertEquals(tb.getBalanceId(), copy.getBalanceId());
                        return null;
                    }
                });
    }
    private AuditOnlyBalance getInfinityBalance(int balanceId)
    {
        return AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.balanceId().eq(balanceId).and(AuditOnlyBalanceFinder.acmapCode().eq("A")));
    }

    public void checkDatedAuditOnlyInfinityRow(int balanceId, double quantity, double interest) throws SQLException
    {
        Connection con = this.getConnection();
        try
        {
            String sql = "select POS_QUANTITY_M, POS_INTEREST_M from AUDIT_ONLY_BALANCE where BALANCE_ID = ? and " +
                    "OUT_Z = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, balanceId);
            ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(quantity, rs.getDouble(1), 0);
            assertEquals(interest, rs.getDouble(2), 0);
            assertFalse(rs.next());
            rs.close();
            ps.close();
        }
        finally
        {
            con.close();
        }
    }

    public void checkDatedAuditOnlyTimestampRow(int balanceId, double quantity, double interest, Timestamp processingDate) throws SQLException
    {
        Connection con = this.getConnection();

        try
        {
            String sql = "select POS_QUANTITY_M, POS_INTEREST_M from AUDIT_ONLY_BALANCE where BALANCE_ID = ? and " +
                    "IN_Z <= ? and OUT_Z > ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, balanceId);
            ps.setTimestamp(2, processingDate);
            ps.setTimestamp(3, processingDate);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(quantity, rs.getDouble(1), 0);
            assertEquals(interest, rs.getDouble(2), 0);
            assertFalse(rs.next());
            rs.close();
            ps.close();
        }
        finally
        {
            con.close();
        }
    }

    public void insertDatedAuditOnly(int balanceId, double quantity, double interest, Timestamp timestamp) throws SQLException
    {
        Connection con = this.getConnection();
        try
        {
            String sql = "insert into AUDIT_ONLY_BALANCE (BALANCE_ID, POS_QUANTITY_M, POS_INTEREST_M, IN_Z, OUT_Z) values (?,?,?,?,?)";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, balanceId);
            ps.setDouble(2, quantity);
            ps.setDouble(3, interest);
            ps.setTimestamp(4, timestamp);
            ps.setTimestamp(5, InfinityTimestamp.getParaInfinity());
            ps.executeUpdate();
            ps.close();
        }
        finally
        {
            con.close();
        }
    }

    public void updateDatedAuditOnlyTimestamp(int balanceId, Timestamp timestamp) throws SQLException
    {
        Connection con = this.getConnection();
        try
        {
            String sql = "update AUDIT_ONLY_BALANCE set OUT_Z = ? where BALANCE_ID = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setTimestamp(1, timestamp);
            ps.setInt(2, balanceId);
            ps.executeUpdate();
            ps.close();
        }
        finally
        {
            con.close();
        }
    }

    public void checkDatedAuditOnlyTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getConnection();
        try
        {
            String sql = "select count(*) from AUDIT_ONLY_BALANCE where BALANCE_ID = ? and " +
                    "OUT_Z = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, balanceId);
            ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertFalse(rs.next());
            rs.close();
            ps.close();
        }
        finally
        {
            con.close();
        }
    }

    public int checkDatedAuditOnlyRowCounts(int balanceId)
            throws SQLException
    {
        int counts = 0;

        Connection con = this.getConnection();
        try
        {
            String sql = "select count(*) from AUDIT_ONLY_BALANCE where BALANCE_ID = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, balanceId);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());

            counts = rs.getInt(1);
            assertFalse(rs.next());
            rs.close();
            ps.close();
        }
        finally
        {
            con.close();
        }

        return counts;
    }

    private void checkQuantityThrowsException(AuditOnlyBalance balance)
    {
        try
        {
            balance.getQuantity();
            fail("should not get here");
        }
        catch(MithraDeletedException e)
        {
            // ok
        }
    }

}
