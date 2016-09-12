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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheUpdateCausesRefreshAndLockTxParticipationMode;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.transaction.TransactionStyle;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;



public class TestDatedNonAudited extends MithraTestAbstract implements TestDatedNonAuditedDatabaseChecker
{

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private TestDatedNonAuditedDatabaseChecker checker = this;

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            NonAuditedBalance.class,
            TestAgeBalanceSheetRunRate.class
        };
    }

    public void setChecker(TestDatedNonAuditedDatabaseChecker checker)
    {
        this.checker = checker;
    }

    public Timestamp getInifinity()
    {
        return InfinityTimestamp.getParaInfinity();
    }

    protected void clearNonAuditedBalanceCache()
    {
        NonAuditedBalanceFinder.clearQueryCache();
    }

    protected void findOneNonAuditedBalance(int i, Timestamp date)
    {
        NonAuditedBalanceFinder.findOne(forId(i, date));
    }

    public void testUpdatePropagation() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Timestamp future = new Timestamp(timestampFormat.parse("2013-01-20 00:00:00").getTime());
                Timestamp past = new Timestamp(timestampFormat.parse("2011-01-20 00:00:00").getTime());
                Timestamp now = new Timestamp(timestampFormat.parse("2012-01-20 00:00:00").getTime());
                NonAuditedBalanceInterface balPast = buildNonAuditedBalance(past);
                balPast.setAcmapCode("A");
                balPast.setBalanceId(2000);
                balPast.setQuantity(-6851);
                balPast.insert();
                NonAuditedBalanceInterface balAtFuture = findNonAuditedBalanceForBusinessDate(2000, future);
                assertEquals(-6851.0, balAtFuture.getQuantity(), 0.01);
                NonAuditedBalanceInterface balNow = findNonAuditedBalanceForBusinessDate(2000, now);
                assertEquals(-6851.0, balNow.getQuantity(), 0.01);
                balNow.setQuantity(100);
                assertEquals(100.0, balNow.getQuantity(), 0.01);
                assertEquals(100.0, balAtFuture.getQuantity(), 0.01);
                return null;
            }
        }, new TransactionStyle(10000));
    }

    public void checkDatedNonAuditedInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from NON_AUDITED_BALANCE where BALANCE_ID = ? and " +
                "THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public int checkDatedNonAuditedRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return counts;
    }

    protected NonAuditedBalanceInterface findNonAuditedBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return NonAuditedBalanceFinder.findOne(NonAuditedBalanceFinder.acmapCode().eq("A")
                            .and(NonAuditedBalanceFinder.balanceId().eq(balanceId))
                            .and(NonAuditedBalanceFinder.businessDate().eq(businessDate)));
    }

    public NonAuditedBalanceInterface buildNonAuditedBalance(Timestamp businessDate)
    {
        return new NonAuditedBalance(businessDate);
    }

    public void testInsertInTransaction() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            t.printStackTrace();
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertRollback() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertNull(fromCache);
        // test the database:
        this.checker.checkDatedNonAuditedTerminated(2000);
    }

    public void testUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);
    }

    protected void removeLockNonAuditedBalance(MithraTransaction tx)
    {
        NonAuditedBalanceFinder.setTransactionModeDangerousNoLocking(tx);
    }


    public void testUpdateSameBusinesDayWithNoLocking() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            this.removeLockNonAuditedBalance(tx);

            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiKeyUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-25 18:30:00.0").getTime());
        TestAgeBalanceSheetRunRateInterface rate = findTestAgeBalanceSheetRunRate(businessDate);
        assertNotNull(rate);
        double newValue = 0;
        double newPrice = 0;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            newValue = rate.getValue()*1.2;
            newPrice = rate.getPrice()*1.3;
            rate.setValue(newValue);
            rate.setPrice(newPrice);
//            NonAuditedBalance fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
//
//            tb.setQuantity(12.5);
//            assertTrue(fromCache.getQuantity() == 12.5);
//            assertTrue(tb.zIsParticipatingInTransaction(tx));
//            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
//            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
//            assertSame(tb, fromCache);
//            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
//            assertNotSame(tb, fromCache);
//            assertTrue(fromCache.getQuantity() == 12.5);
//            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
//        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
//        NonAuditedBalance fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
//        assertSame(tb, fromCache);
//        assertTrue(fromCache.getQuantity() == 12.5);
//        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
//        assertTrue(fromCache.getQuantity() == 12.5);
//        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);
    }

    protected TestAgeBalanceSheetRunRateInterface findTestAgeBalanceSheetRunRate(Timestamp businessDate)
    {
        return TestAgeBalanceSheetRunRateFinder.findOne(
                TestAgeBalanceSheetRunRateFinder.businessDate().eq(businessDate)
                .and(TestAgeBalanceSheetRunRateFinder.tradingdeskLevelTypeId().eq(10))
                .and(TestAgeBalanceSheetRunRateFinder.tradingDeskorDeskHeadId().eq(1000)));
    }

    public void testMultipleUpdate() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-10-15 00:00:00").getTime());
        IntHashSet balanceIds = new IntHashSet();
        balanceIds.add(1);
        balanceIds.add(10);
        balanceIds.add(20);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceListInterface list = buildInSetQuery(balanceIds, businessDate);
            for(int i=0;i<list.size();i++)
            {
                NonAuditedBalanceInterface tb = list.getNonAuditedBalanceInterfaceAt(i);
                tb.setQuantity(10);
                assertTrue(tb.zIsParticipatingInTransaction(tx));
            }
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceListInterface list = buildInSetQuery(balanceIds, businessDate);
            for(int i=0;i<list.size();i++)
            {
                NonAuditedBalanceInterface tb = list.getNonAuditedBalanceInterfaceAt(i);
                tb.setQuantity(12.5);
                assertTrue(tb.zIsParticipatingInTransaction(tx));
            }
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(1, 12.5, businessDate);
        checker.checkDatedNonAuditedInfinityRow(10, 12.5, businessDate);
        checker.checkDatedNonAuditedInfinityRow(20, 12.5, businessDate);
    }

    protected NonAuditedBalanceListInterface buildInSetQuery(IntHashSet balanceIds, Timestamp businessDate)
    {
        NonAuditedBalanceList list = NonAuditedBalanceFinder.findMany(NonAuditedBalanceFinder.acmapCode().eq("A")
                        .and(NonAuditedBalanceFinder.balanceId().in(balanceIds))
                        .and(NonAuditedBalanceFinder.businessDate().eq(businessDate)));
        return list;
    }

    public void testUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(17);
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            this.changeParticipationMode(tx);

            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        NonAuditedBalanceInterface original = findNonAuditedBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    protected void changeParticipationMode(MithraTransaction tx)
    {
        tx.setTxParticipationMode(NonAuditedBalanceFinder.getMithraObjectPortal(), ReadCacheUpdateCausesRefreshAndLockTxParticipationMode.getInstance());
    }

    public void testUpdateLaterBusinesDayTwoAttributes() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            tb.setInterest(1234);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(1234, fromCache.getInterest(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(1234, fromCache.getInterest(), 0);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        NonAuditedBalanceInterface original = findNonAuditedBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminate();
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        // test the database:
        checker.checkDatedNonAuditedTerminated(1);
    }

    public void testTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminate();
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        // test the database:
        checker.checkDatedNonAuditedTerminated(1);
    }

    public void testTerminateTwiceLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            NonAuditedBalanceInterface inThePast = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()-100000));
            tb.terminate();
            NonAuditedBalanceInterface tbi = buildNonAuditedBalance(businessDate);
            tbi.setAcmapCode("A");
            tbi.setBalanceId(2000);
            tbi.setQuantity(12.5);
            tbi.insert();
            inThePast.terminate();
            tbi = buildNonAuditedBalance(businessDate);
            tbi.setAcmapCode("A");
            tbi.setBalanceId(2001);
            tbi.setQuantity(12.5);
            tbi.insert();
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);

        assertNull(findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()-1000)));

        // test the database:
        checker.checkDatedNonAuditedTerminated(1);
    }

    public void checkDatedNonAuditedTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE where BALANCE_ID = ? and " +
                "THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void testMultiSegmentUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(nextSegmentBalance, fromCache);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface priorSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(nextSegmentBalance, fromCache);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(priorSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminate();
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
            try
            {
                nextSegmentBalance.getQuantity();
                fail("should not get here");
            }
            catch(MithraDeletedException e)
            {
                // ok
            }
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        // test the database:
        checker.checkDatedNonAuditedTerminated(balanceId);
    }

    public void testMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface priorSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminate();
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
            try
            {
                nextSegmentBalance.getQuantity();
                fail("should not get here");
            }
            catch(MithraDeletedException e)
            {
                // ok
            }
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        // test the database:
        checker.checkDatedNonAuditedTerminated(balanceId);
    }

    public void testTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 40;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminate();
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        // test the database:
        checker.checkDatedNonAuditedTerminated(balanceId);
    }

    public void testUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface reversed = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, originalQuantity, until);
    }

    public void testUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface reversed = findNonAuditedBalanceForBusinessDate(balanceId, until);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertEquals(originalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, originalQuantity, until);
    }

    public void testMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        NonAuditedBalanceInterface nonIncrementedNextSegment = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        NonAuditedBalanceInterface nonIncrementedNextSegment = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        NonAuditedBalanceInterface nonIncrementedNextSegment = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.setQuantityUntil(2.5, until);
            assertEquals(2.5, fromCache.getQuantity(), 0.0);
            assertEquals(2.5, tb.getQuantity(), 0.0);
            assertEquals(2.5, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(2.5, fromCache.getQuantity(), 0.0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(2.5, fromCache.getQuantity(), 0.0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(2.5, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        NonAuditedBalanceInterface nonIncrementedNextSegment = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();

            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0.0);
            assertEquals(12.5, tb.getQuantity(), 0.0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0.0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0.0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5 + originalQuantity, businessDate);
    }

    public void testIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + quantity);
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
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(17);
            tb.incrementQuantity(12.5);
            quantity += 17 + 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == quantity);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        assertTrue(fromCache.getQuantity() == quantity);

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            quantity += 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == quantity);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checker.checkDatedNonAuditedInfinityRow(balanceId, quantity, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        NonAuditedBalanceInterface original = findNonAuditedBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity() , 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface priorSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-10 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-11 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 20;
        NonAuditedBalanceInterface priorSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testInsertWithIncrementZeroSegments() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertWithIncrementOneSegment() throws Exception
    {
        NonAuditedBalanceInterface tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        NonAuditedBalanceInterface nextSegment = this.findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, nextSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5 + nextSegmentQuantity, segmentStartDate);
    }

    public void testIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        long now = System.currentTimeMillis();
        Timestamp businessDate = new Timestamp(now - 24*3600*1000);
        Timestamp secondBusinessDate = new Timestamp(now);
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface secondTb = findNonAuditedBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            quantity += 12.5;
            assertEquals(quantity, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, secondBusinessDate);
            assertSame(secondTb, fromCache);
            assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(quantity, fromCache.getQuantity(), 0);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(quantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checker.checkDatedNonAuditedInfinityRow(balanceId, quantity+45.1, secondBusinessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        NonAuditedBalanceInterface original = findNonAuditedBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp secondBusinessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface priorSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface secondTb = findNonAuditedBalanceForBusinessDate(balanceId, secondBusinessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, secondTb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, secondBusinessDate);
            assertSame(secondTb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checker.checkDatedNonAuditedInfinityRow(balanceId, 12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testInsertThenIncrement() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertNull(tb);
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            tb.setQuantity(12.5);
            tb.insert();
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertNotNull(tb);
            tb.incrementQuantity(1000);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(2000, 1012.5, businessDate);
    }

    public void testInsertThenIncrementInOneTransaction() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertNull(tb);
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            tb.setQuantity(12.5);
            tb.insert();
            tb.incrementQuantity(1000);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(2000, 1012.5, businessDate);
    }

    public void testInsertWithIncrementTwiceTodayYesterday() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        Timestamp lbd = new Timestamp(businessDate.getTime() - 24 * 3600 * 1000);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertNull(tb);
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            tb.setQuantity(12.5);
            tb.insertWithIncrement();

            NonAuditedBalanceInterface lbdBalance = findNonAuditedBalanceForBusinessDate(2000, lbd);
            assertNull(lbdBalance);
            lbdBalance = buildNonAuditedBalance(lbd);
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(2000);
            lbdBalance.setQuantity(1000);
            lbdBalance.insertWithIncrement();
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(2000, 1012.5, businessDate);
    }

    public void testInsertThenIncrementLaterDay() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        Timestamp lbd = new Timestamp(businessDate.getTime() - 24 * 3600 * 1000);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface lbdBalance = findNonAuditedBalanceForBusinessDate(2000, lbd);
            assertNull(lbdBalance);
            lbdBalance = buildNonAuditedBalance(lbd);
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(2000);
            lbdBalance.setQuantity(1000);
            lbdBalance.insertWithIncrement();

            tb = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertNotNull(tb);
            tb.incrementQuantity(12.5);

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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(2000, 1012.5, businessDate);
    }

    public void testInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
    }

    public void testInsertUntil() throws SQLException, ParseException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertUntil(until);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
    }

    public void testInsertWithIncrementUntilOneSegment() throws Exception
    {
        NonAuditedBalanceInterface tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        NonAuditedBalanceInterface nextSegment = this.findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, nextSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(nextSegmentQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedInfinityRow(balanceId, nextSegmentQuantity, until);
    }

    public void testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5+segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5+segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5+segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5+segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checker.checkDatedNonAuditedInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp terminateDate = new Timestamp(System.currentTimeMillis());
        Timestamp businessDate = new Timestamp(terminateDate.getTime() - 24*60*60000);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findNonAuditedBalanceForBusinessDate(2000, terminateDate);
            fromCache.terminate();
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checker.checkDatedNonAuditedTerminated(2000);
    }

    public void testInsertThenTerminateSameBusinessDate() throws SQLException
    {
        NonAuditedBalanceInterface tb = null;
        Timestamp terminateDate = new Timestamp(System.currentTimeMillis());
        Timestamp businessDate = terminateDate;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = buildNonAuditedBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findNonAuditedBalanceForBusinessDate(2000, terminateDate);
            fromCache.terminate();
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(2000, businessDate);
        assertNull(fromCache);

        // test the database:
        checker.checkDatedNonAuditedTerminated(2000);
    }

    public void testTerminateLaterBusinesDayThenInsert() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        NonAuditedBalanceInterface inserted = null;
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminate();
            assertTrue(tb.zIsParticipatingInTransaction(tx));

            inserted = buildNonAuditedBalance(businessDate);
            inserted.setAcmapCode("A");
            inserted.setBalanceId(balanceId);
            inserted.setQuantity(15);
            inserted.insert();

            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertSame(inserted, fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = this.queryNonAuditedBalancFinder(balanceId, businessDate);
        assertSame(inserted, fromCache);
        assertEquals(15, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNotNull(fromCache);
        assertEquals(15, fromCache.getQuantity(), 0);
        // test the database:
        checker.checkDatedNonAuditedInfinityRow(balanceId, 15, businessDate);
    }

    protected NonAuditedBalanceInterface queryNonAuditedBalancFinder(int balanceId, Timestamp businessDate)
    {
        return NonAuditedBalanceFinder.findOne(NonAuditedBalanceFinder.acmapCode().eq("A")
                 .and(NonAuditedBalanceFinder.balanceId().greaterThan(balanceId - 1))
                 .and(NonAuditedBalanceFinder.balanceId().lessThan(balanceId + 1))
                            .and(NonAuditedBalanceFinder.businessDate().eq(businessDate)));
    }

    private Timestamp createTimestamp(MithraTransaction tx)
    {
        return new Timestamp(tx.getProcessingStartTime()/10*10);
    }

    public void testInsertThenUpdate()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        NonAuditedBalanceInterface inserted = null;
        try
        {
            inserted = buildNonAuditedBalance(createTimestamp(tx));
            inserted.setAcmapCode("A");
            inserted.setBalanceId(1234);
            inserted.setQuantity(10);
            inserted.setBusinessDateFrom(new Timestamp(200000000));
            inserted.setBusinessDateTo(this.getInifinity());
            inserted.insert();

            NonAuditedBalanceInterface updated = findNonAuditedBalanceForBusinessDate(1234, createTimestamp(tx));
            updated.setQuantity(20);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
    }

    public void testPurge() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());
        Timestamp futureBusinessDate = new Timestamp(System.currentTimeMillis() + 100000);

        NonAuditedBalanceInterface currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);
        NonAuditedBalanceInterface pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(pastBalance);
        NonAuditedBalanceInterface futureBalance = findNonAuditedBalanceForBusinessDate(balanceId, futureBusinessDate);
        assertNotNull(futureBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(futureBalance);
            this.checkQuantityThrowsException(pastBalance);

            currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
            assertNull(currentBalance);
            futureBalance = findNonAuditedBalanceForBusinessDate(balanceId, futureBusinessDate);
            assertNull(futureBalance);
            pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
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
        currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNull(currentBalance);
        futureBalance = findNonAuditedBalanceForBusinessDate(balanceId, futureBusinessDate);
        assertNull(futureBalance);
        pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNull(pastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testPurgeThenInsert() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());

        NonAuditedBalanceInterface currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);
        NonAuditedBalanceInterface pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            NonAuditedBalanceInterface newBalance = buildNonAuditedBalance(pastBusinessDate);
            newBalance.setBalanceId(balanceId);
            newBalance.setAcmapCode("A");
            newBalance.setQuantity(-9999);
            newBalance.setInterest(-5555);
            newBalance.insert();

            assertEquals(-9999, pastBalance.getQuantity(), 0);

            currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
            assertNotNull(currentBalance);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);
        pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(pastBalance);
        // test the database:
        assertEquals(1, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testPurgeAfterInsert() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2001-06-01 00:00:00").getTime());
        Timestamp pastBusinessDateTo = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00").getTime());

        NonAuditedBalanceInterface currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        NonAuditedBalanceInterface pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(currentBalance);
        assertNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface newBalance = buildNonAuditedBalance(pastBusinessDate);
            newBalance.setBalanceId(balanceId);
            newBalance.setAcmapCode("A");
            newBalance.setQuantity(-9999);
            newBalance.setInterest(-5555);
            newBalance.setBusinessDateTo(pastBusinessDateTo);
            newBalance.insert();

            currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
            pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
            assertNotNull(currentBalance);
            assertNotNull(pastBalance);

            currentBalance.purge();

            currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
            pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
            assertNull(currentBalance);
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
        currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNull(currentBalance);
        assertNull(pastBalance);

        // test the database:
        assertEquals(0, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testPurgeAfterMultipleUpdateInsertOperations() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());
        Timestamp fartherPastBusinessDate = new Timestamp(timestampFormat.parse("2002-06-01 00:00:00").getTime());

        NonAuditedBalanceInterface currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);
        NonAuditedBalanceInterface pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(pastBalance);
        NonAuditedBalanceInterface fartherPastBalance = findNonAuditedBalanceForBusinessDate(balanceId, fartherPastBusinessDate);
        assertNotNull(fartherPastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            pastBalance.incrementInterest(1000);

            Timestamp businessDateTo = new Timestamp(timestampFormat.parse("2002-06-02 00:00:00").getTime());
            NonAuditedBalanceInterface newBalance = buildNonAuditedBalance(fartherPastBusinessDate);
            newBalance.setBalanceId(balanceId);
            newBalance.setBusinessDateTo(businessDateTo);
            newBalance.setAcmapCode("A");
            newBalance.setQuantity(-9999);
            newBalance.setInterest(-5555);
            newBalance.insert();

            newBalance.incrementInterest(100);

            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));
            this.checkQuantityThrowsException(pastBalance);

            currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
            assertNull(currentBalance);
            pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
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
        currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNull(currentBalance);
        pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNull(pastBalance);
        fartherPastBalance = findNonAuditedBalanceForBusinessDate(balanceId, fartherPastBusinessDate);
        assertNull(fartherPastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testBatchPurge() throws SQLException, ParseException
    {
        int balanceIdA = 50;

        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2004-01-01 18:30:00").getTime());
        Timestamp fartherPastBusinessDate = new Timestamp(timestampFormat.parse("2003-01-01 18:30:00").getTime());

        NonAuditedBalanceInterface balanceA = findNonAuditedBalanceForBusinessDate(balanceIdA, pastBusinessDate);
        assertNotNull(balanceA);
        NonAuditedBalanceInterface balanceB = findNonAuditedBalanceForBusinessDate(balanceIdA, fartherPastBusinessDate);
        assertNotNull(balanceB);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            balanceA.terminate();
            balanceB = findNonAuditedBalanceForBusinessDate(balanceIdA, fartherPastBusinessDate);
            balanceB.purge();

            balanceA = findNonAuditedBalanceForBusinessDate(balanceIdA, pastBusinessDate);
            assertNull(balanceA);
            balanceB = findNonAuditedBalanceForBusinessDate(balanceIdA, fartherPastBusinessDate);
            assertNull(balanceB);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        balanceA = findNonAuditedBalanceForBusinessDate(balanceIdA, pastBusinessDate);
        assertNull(balanceA);
        balanceB = findNonAuditedBalanceForBusinessDate(balanceIdA, fartherPastBusinessDate);
        assertNull(balanceB);

        // test the database:
        assertEquals(0, this.checker.checkDatedNonAuditedRowCounts(balanceIdA));
    }

    public void testPurgeThenRollback() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());

        NonAuditedBalanceInterface currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);
        NonAuditedBalanceInterface pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(pastBalance);

            currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
            assertNull(currentBalance);
            pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
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
        currentBalance = findNonAuditedBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);
        pastBalance = findNonAuditedBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(pastBalance);
        // test the database:
        assertEquals(3, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testInsertForRecovery() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 06:30:00").getTime());
        Timestamp queryBusinessDate = new Timestamp(timestampFormat.parse("2003-06-02 06:30:00").getTime());
        Timestamp toBusinessDate = new Timestamp(timestampFormat.parse("2003-06-03 06:30:00").getTime());

        NonAuditedBalanceInterface testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface recoveredBalance = buildNonAuditedBalance(fromBusinessDate);
            recoveredBalance.setAcmapCode("A");
            recoveredBalance.setBalanceId(balanceId);
            recoveredBalance.setInterest(10000);
            recoveredBalance.setQuantity(20000);
            recoveredBalance.setBusinessDateFrom(fromBusinessDate);
            recoveredBalance.setBusinessDateTo(toBusinessDate);
            recoveredBalance.insertForRecovery();

            testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
            assertNotNull(testQuery);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
        assertNotNull(testQuery);

        // test the database:
        assertEquals(1, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 06:30:00").getTime());
        Timestamp queryBusinessDate = new Timestamp(timestampFormat.parse("2003-06-02 06:30:00").getTime());
        Timestamp toBusinessDate = new Timestamp(timestampFormat.parse("2003-06-03 06:30:00").getTime());

        NonAuditedBalanceInterface testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface recoveredBalance = buildNonAuditedBalance(fromBusinessDate);
            recoveredBalance.setAcmapCode("A");
            recoveredBalance.setBalanceId(balanceId);
            recoveredBalance.setInterest(10000);
            recoveredBalance.setQuantity(20000);
            recoveredBalance.setBusinessDateFrom(fromBusinessDate);
            recoveredBalance.setBusinessDateTo(toBusinessDate);
            recoveredBalance.insertForRecovery();

            testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
            assertNotNull(testQuery);

            testQuery.purge();

            testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
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
        testQuery = this.findNonAuditedBalanceForBusinessDate(balanceId, queryBusinessDate);
        assertNull(testQuery);

        // test the database:
        assertEquals(0, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testTerminateUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface reversed = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminateUntil(until);
            try
            {
                fromCache.getQuantity();
                fail("should not get here");
            }
            catch(MithraDeletedException e)
            {
                // ok
            }
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);

        // test the database:

        assertEquals(1, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testTerminateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface reversed = findNonAuditedBalanceForBusinessDate(balanceId, until);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminateUntil(until);
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);

        // test the database:

        assertEquals(2, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testMultiSegmentTerminateUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminateUntil(nextSegmentBusinessDate);
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        assertEquals(1, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testMultiSegmentTerminateUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminateUntil(nextSegmentBusinessDate);
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        assertEquals(2, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testMultiSegmentTerminateUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        NonAuditedBalanceInterface nonIncrementedNextSegment = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminateUntil(until);
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
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        assertEquals(2, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    public void testMultiSegmentTerminateUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00").getTime());
        int balanceId = 10;
        NonAuditedBalanceInterface tb = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        NonAuditedBalanceInterface nextSegmentBalance = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        NonAuditedBalanceInterface previousSegment = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        NonAuditedBalanceInterface nonIncrementedNextSegment = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.terminateUntil(until);
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
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        NonAuditedBalanceInterface fromCache = findNonAuditedBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findNonAuditedBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        assertEquals(3, this.checker.checkDatedNonAuditedRowCounts(balanceId));
    }

    private void checkQuantityThrowsException(NonAuditedBalanceInterface balance)
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

    protected Operation forId(int id, Timestamp date)
    {
        return NonAuditedBalanceFinder.acmapCode().eq("A").and(NonAuditedBalanceFinder.balanceId().greaterThanEquals(id).and(
                NonAuditedBalanceFinder.businessDate().eq(date)));
    }

    public void testInsertTerminateInsert()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                NonAuditedBalanceInterface inserted = null;
                Timestamp date = createTimestamp(tx);
                inserted = buildNonAuditedBalance(date);
                inserted.setAcmapCode("A");
                inserted.setBalanceId(1234);
                inserted.setQuantity(10);
                inserted.insert();

                clearNonAuditedBalanceCache();
                findOneNonAuditedBalance(1234,date);
                clearNonAuditedBalanceCache();

                tx.executeBufferedOperations();
                clearNonAuditedBalanceCache();

                findOneNonAuditedBalance(1234,date);
                clearNonAuditedBalanceCache();

                inserted.terminate();
                clearNonAuditedBalanceCache();

                inserted = buildNonAuditedBalance(date);
                inserted.setAcmapCode("A");
                inserted.setBalanceId(1234);
                inserted.setQuantity(20);
                inserted.insert();

                return null;
            }



        });
    }

}
