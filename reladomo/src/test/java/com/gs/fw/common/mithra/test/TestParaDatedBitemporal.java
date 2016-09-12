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
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.MultiQueueExecutor;
import com.gs.fw.common.mithra.util.SingleQueueExecutor;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;



public class TestParaDatedBitemporal extends MithraTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            ParaBalance.class,
            FundingBalance.class,
            TestPositionPrice.class
        };
    }

    public void checkParaInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from PARA_BALANCE where BALANCE_ID = ? and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(3, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertFalse(hasMoreResults);
        assertEquals(quantity, resultQuantity, 0);
        assertEquals(businessDate, resultBusinessDate);
    }

    private ParaBalance findParaBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                            .and(ParaBalanceFinder.balanceId().eq(balanceId))
                            .and(ParaBalanceFinder.businessDate().eq(businessDate)));
    }

    public void testParaUpdateSameBusinesDay() throws SQLException, ParseException
    {
        int balanceId = 2002;
        ParaBalance paraBalance = null;
        Timestamp businessDate = new Timestamp(createParaBusinessDate(new java.util.Date()).getTime());
        paraBalance = insertParaBalance(businessDate, paraBalance, balanceId);
        MithraTransaction tx;
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Timestamp firstProcessingDate = paraBalance.getProcessingDateFrom();
        try
        {
            ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            paraBalance.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, paraBalance.getQuantity(), 0);
            assertTrue(paraBalance.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(businessDate)));
            assertSame(paraBalance, fromCache);
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            assertNotSame(paraBalance, fromCache);
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
        ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(businessDate)));
        assertSame(paraBalance, fromCache);
        fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(InfinityTimestamp.getParaInfinity())));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, businessDate, firstProcessingDate);
    }

    private void checkInfinityRow(int balanceId, double balance, Timestamp businessDate, Timestamp firstProcessingDate)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z, IN_Z from PARA_BALANCE where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertTrue(balance == rs.getDouble(1));
        assertEquals(addDays(businessDate, -1), rs.getTimestamp(2));
        assertTrue(rs.getTimestamp(3).getTime() > firstProcessingDate.getTime());
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    private void checkInfinityRow(int balanceId, Timestamp businessDate, Timestamp firstProcessingDate)
            throws SQLException
    {
        this.checkInfinityRow(balanceId, 12.5, businessDate, firstProcessingDate);
    }

    private int checkRowCounts(int balanceId)
            throws SQLException
    {
        int counts = 0;

        Connection con = this.getConnection();
        try
        {
            String sql = "select count(*) from PARA_BALANCE where BALANCE_ID = ?";
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

    public void testExternalUpdate() throws SQLException, ParseException
    {
        int balanceId = 3000;
        ParaBalance paraBalance = null;
        Timestamp businessDate = new Timestamp(createParaBusinessDate(new java.util.Date()).getTime());
        paraBalance = insertParaBalance(businessDate, paraBalance, balanceId);
        Timestamp originalBusinessFrom = paraBalance.getBusinessDateFrom();
        Timestamp originalTime = paraBalance.getProcessingDateFrom();
        sleep(20); // so the IN_Z can be set to a later date
        Connection con = this.getConnection();
        try
        {
            String sql = "update PARA_BALANCE set IN_Z = ? where BALANCE_ID = 3000";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setTimestamp(1, new Timestamp(originalTime.getTime()+20));
            assertEquals(1 , ps.executeUpdate());
            ps.close();
        }
        finally
        {
            con.close();
        }

        MithraTransaction tx;
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        paraBalance.getProcessingDateFrom(); // force refresh of the object
        tx.commit();
        Operation op = ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().equalsEdgePoint())
                .and(ParaBalanceFinder.processingDate().equalsEdgePoint())
                .and(ParaBalanceFinder.processingDateFrom().eq(originalTime))
                .and(ParaBalanceFinder.businessDateFrom().eq(originalBusinessFrom));
        ParaBalanceList list = new ParaBalanceList(op);
        assertEquals(0, list.size());
    }

    public void testParaUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        int balanceId = 3000;
        ParaBalance paraBalance = null;
        Timestamp businessDate = new Timestamp(createParaBusinessDate(new java.util.Date()).getTime());
        paraBalance = insertParaBalance(businessDate, paraBalance, balanceId);
        MithraTransaction tx;
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Timestamp firstProcessingDate = paraBalance.getProcessingDateFrom();
        try
        {
            ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            paraBalance.setQuantity(10.0);
            assertEquals(10, fromCache.getQuantity(), 0);
            assertEquals(10, paraBalance.getQuantity(), 0);
            assertTrue(paraBalance.zIsParticipatingInTransaction(tx));
            paraBalance.setQuantity(12.5);
            assertEquals(12.5, paraBalance.getQuantity(), 0);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(businessDate)));
            assertSame(paraBalance, fromCache);
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            assertNotSame(paraBalance, fromCache);
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
        ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(businessDate)));
        assertSame(paraBalance, fromCache);
        fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(InfinityTimestamp.getParaInfinity())));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, businessDate, firstProcessingDate);
    }

    public void testParaUpdateSameBusinesDayTwiceWithInterlevedInserts() throws SQLException, ParseException
    {
        int balanceId = 3000;
        ParaBalance paraBalance = null;
        Timestamp businessDate = new Timestamp(createParaBusinessDate(new java.util.Date()).getTime());
        paraBalance = insertParaBalance(businessDate, paraBalance, balanceId);
        MithraTransaction tx;
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Timestamp firstProcessingDate = paraBalance.getProcessingDateFrom();
        try
        {
            ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            paraBalance.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, paraBalance.getQuantity(), 0);
            assertTrue(paraBalance.zIsParticipatingInTransaction(tx));

            ParaBalance newParaBalance = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            newParaBalance.setAcmapCode("A");
            newParaBalance.setBalanceId(20001);
            newParaBalance.setQuantity(7.9);
            newParaBalance.insert();

            paraBalance.setQuantity(10.0);
            paraBalance.setQuantity(12.5);

            newParaBalance = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            newParaBalance.setAcmapCode("A");
            newParaBalance.setBalanceId(20002);
            newParaBalance.setQuantity(7.9);
            newParaBalance.insert();


            assertEquals(12.5, paraBalance.getQuantity(), 0);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(businessDate)));
            assertSame(paraBalance, fromCache);
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            assertNotSame(paraBalance, fromCache);
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
        ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(businessDate)));
        assertSame(paraBalance, fromCache);
        fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(InfinityTimestamp.getParaInfinity())));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, businessDate, firstProcessingDate);
    }

    public void testParaPurgeCombineUpdate() throws SQLException, ParseException
    {
        int balanceId = 3000;
        ParaBalance paraBalance = null;
        Timestamp currentBusinessDate = new Timestamp(createParaBusinessDate(new java.util.Date()).getTime());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 05:31:00").getTime());

        this.insertParaBalance(pastBusinessDate, paraBalance, balanceId);

        ParaBalance currentBalance = this.findParaBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNotNull(currentBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.setQuantity(10000);
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
            currentBalance = this.findParaBalanceForBusinessDate(balanceId, currentBusinessDate);
            assertNotNull(currentBalance);

            currentBalance.setQuantity(20000);
            currentBalance.purge();

            currentBalance = this.findParaBalanceForBusinessDate(balanceId, currentBusinessDate);
            assertNull(currentBalance);

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        currentBalance = this.findParaBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNull(currentBalance);

        // test the database:
        assertEquals(0, checkRowCounts(balanceId));
    }

    public void testParaUpdateSameBusinesDayTwiceWithFlush() throws SQLException, ParseException
    {
        int balanceId = 3010;
        ParaBalance paraBalance = null;
        Timestamp businessDate = new Timestamp(createParaBusinessDate(new java.util.Date()).getTime());
        paraBalance = insertParaBalance(businessDate, paraBalance, balanceId);
        MithraTransaction tx;
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Timestamp firstProcessingDate = paraBalance.getProcessingDateFrom();
        try
        {
            ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            paraBalance.setQuantity(10.0);
            assertEquals(10, fromCache.getQuantity(), 0);
            assertEquals(10, paraBalance.getQuantity(), 0);
            assertTrue(paraBalance.zIsParticipatingInTransaction(tx));
            tx.executeBufferedOperations();
            paraBalance.setQuantity(12.5);
            assertEquals(12.5, paraBalance.getQuantity(), 0);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(businessDate)));
            assertSame(paraBalance, fromCache);
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            assertNotSame(paraBalance, fromCache);
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
        ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(businessDate)));
        assertSame(paraBalance, fromCache);
        fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                .and(ParaBalanceFinder.balanceId().eq(balanceId))
                .and(ParaBalanceFinder.businessDate().eq(InfinityTimestamp.getParaInfinity())));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, businessDate, firstProcessingDate);
    }

    private ParaBalance insertParaBalance(Timestamp businessDate, ParaBalance paraBalance, int balanceId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            paraBalance = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            paraBalance.setAcmapCode("A");
            paraBalance.setBalanceId(balanceId);
            assertEquals(balanceId, paraBalance.getBalanceId());
            paraBalance.setQuantity(7.9);
            paraBalance.insert();
            assertTrue(paraBalance.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            ParaBalance fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(businessDate)));
            assertSame(paraBalance, fromCache);
            fromCache = ParaBalanceFinder.findOne(ParaBalanceFinder.acmapCode().eq("A")
                    .and(ParaBalanceFinder.balanceId().eq(balanceId))
                    .and(ParaBalanceFinder.businessDate().eq(new Timestamp(businessDate.getTime()+1000))));
            assertNotSame(paraBalance, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
            Thread.sleep(50); // just so the next processing time will be later
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        return paraBalance;
    }

    private Timestamp addDaysAsTimestamp(java.util.Date d, int days)
    {
        return new Timestamp(addDays(d, days).getTime());
    }
    
    public void testInsertThenIncrement() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4000;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 1);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNull(tb);
            tb = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
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
        this.sleep(20);
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkParaInfinityRow(balanceId, 1012.5, new Timestamp(paraBusinessDate.getTime()));
    }

    public void testInsertThenIncrementInOneTransaction() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4020;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 1);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNull(tb);
            tb = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkParaInfinityRow(balanceId, 1012.5, new Timestamp(paraBusinessDate.getTime()));
    }

    public void testInsertWithIncrementTwiceTodayYesterday() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4030;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = new Timestamp(paraBusinessDate.getTime()+24*3600000);
        Timestamp lbd = new Timestamp(paraBusinessDate.getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNull(tb);
            tb = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            tb.setQuantity(12.5);
            tb.insertWithIncrement();

            ParaBalance lbdBalance = findParaBalanceForBusinessDate(balanceId, lbd);
            assertNull(lbdBalance);
            lbdBalance = new ParaBalance(lbd, InfinityTimestamp.getParaInfinity());
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(balanceId);
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkParaInfinityRow(balanceId, 1012.5, new Timestamp(paraBusinessDate.getTime()));
    }

    public void testInsertThenIncrementLaterDay() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4100;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 1);
        Timestamp lbd = new Timestamp(paraBusinessDate.getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            ParaBalance lbdBalance = findParaBalanceForBusinessDate(balanceId, lbd);
            assertNull(lbdBalance);
            lbdBalance = new ParaBalance(lbd, InfinityTimestamp.getParaInfinity());
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(balanceId);
            lbdBalance.setQuantity(1000);
            lbdBalance.insertWithIncrement();

            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkParaInfinityRow(balanceId, 1012.5, new Timestamp(paraBusinessDate.getTime()));
    }

    public void testInsertWithIncrementUntilTwiceTodayYesterday() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4200;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 1);
        Timestamp lbd = new Timestamp(paraBusinessDate.getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNull(tb);
            tb = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            tb.setQuantity(12.5);
            tb.insertWithIncrement();

            ParaBalance lbdBalance = findParaBalanceForBusinessDate(balanceId, lbd);
            assertNull(lbdBalance);
            lbdBalance = new ParaBalance(lbd, InfinityTimestamp.getParaInfinity());
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(balanceId);
            lbdBalance.setQuantity(1000);
            lbdBalance.insertWithIncrementUntil(businessDate);
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkParaInfinityRow(balanceId, 12.5, new Timestamp(paraBusinessDate.getTime()));
    }

    public void testInsertWithIncrementUntilTwiceTodayYesterday2() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4400;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, -5);
        Timestamp lbd = addDaysAsTimestamp(paraBusinessDate, -6);
        Timestamp until = addDaysAsTimestamp(paraBusinessDate, -3);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNull(tb);
            tb = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            tb.setQuantity(12.5);
            tb.insertWithIncrement();

            ParaBalance lbdBalance = findParaBalanceForBusinessDate(balanceId, lbd);
            assertNull(lbdBalance);
            lbdBalance = new ParaBalance(lbd, InfinityTimestamp.getParaInfinity());
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(balanceId);
            lbdBalance.setQuantity(1000);
            lbdBalance.insertWithIncrementUntil(until);
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, until);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(12.5, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkParaInfinityRow(balanceId, 12.5, addDaysAsTimestamp(until, -1));
    }


    public void testInsertThenSetUntilLaterDay() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 4600;
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 1);
        Timestamp tomorrow = addDaysAsTimestamp(paraBusinessDate, 2);
        Timestamp lbd = new Timestamp(paraBusinessDate.getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            ParaBalance lbdBalance = findParaBalanceForBusinessDate(balanceId, lbd);
            assertNull(lbdBalance);
            lbdBalance = new ParaBalance(lbd, InfinityTimestamp.getParaInfinity());
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(balanceId);
            lbdBalance.setQuantity(1000);
            lbdBalance.insertWithIncrement();

            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNotNull(tb);
            tb.setQuantityUntil(12.5, tomorrow);

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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, tb.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(1000, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        tb = null;
        ParaBalanceFinder.clearQueryCache();
        fromCache = findParaBalanceForBusinessDate(balanceId, lbd);
        assertNotNull(fromCache);
        assertEquals(1000, fromCache.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNotNull(fromCache);
        assertEquals(1000, fromCache.getQuantity(), 0);

        // test the database:
        checkParaInfinityRow(balanceId, 1000, businessDate);
    }

    public void testInsertUntilThenSetUntilLaterDay() throws SQLException
    {
        ParaBalance tb = null;
        int balanceId = 2004;
        Calendar c = new GregorianCalendar(2005, 9, 26);
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date(c.getTimeInMillis()));
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate, 1);
        Timestamp lbd = new Timestamp(paraBusinessDate.getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            ParaBalance lbdBalance = findParaBalanceForBusinessDate(balanceId, lbd);
            assertNull(lbdBalance);
            lbdBalance = new ParaBalance(lbd, InfinityTimestamp.getParaInfinity());
            lbdBalance.setAcmapCode("A");
            lbdBalance.setBalanceId(balanceId);
            lbdBalance.setQuantity(1000);
            lbdBalance.setBusinessDateTo(businessDate);
            lbdBalance.insert();

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        this.sleep(20);
        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findParaBalanceForBusinessDate(balanceId, businessDate);
            assertNotNull(tb);
            tb.setQuantityUntil(1012.5, addDaysAsTimestamp(businessDate, 1));
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
        ParaBalance fromCache = findParaBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(1012.5, tb.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        fromCache = findParaBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);

    }

    public void testPositionPriceMultipleUpdatesForCBD()
    throws Exception
    {
        Calendar cal = new GregorianCalendar();
        final java.util.Date CBD = createParaBusinessDate(createBusinessDate(cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH ) + 1), cal.get(Calendar.DATE)));
        this.updateMultipleAttributesOfPositionPriceTwiceSameBusinessDay(CBD, "7600003701", 201);
    }

    public void testPositionPriceMultipleUpdatesForLBD()
    throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.DATE, -1);
        final java.util.Date LBD = createParaBusinessDate(createBusinessDate(cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH) + 1), cal.get(Calendar.DATE)));
        this.updateMultipleAttributesOfPositionPriceTwiceSameBusinessDay(LBD, "7600003701", 201);
    }

    public void testTerminateMany()
    {
        final Timestamp businessDate = new Timestamp(createParaBusinessDate(new Date(System.currentTimeMillis() - 24*3600*1000)).getTime());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TestPositionPrice price01 = findTestPositionPrice("7880003901",99500, businessDate);
                TestPositionPriceFinder.clearQueryCache();
                findTestPositionPrice("7880003901",99500, businessDate);
                TestPositionPriceFinder.clearQueryCache();
                findTestPositionPrice("7880003901",99500, businessDate);

                price01.terminate();
                return null;
            }});
    }

    public void testUpdateUntilTwoTimes() throws SQLException, ParseException
    {
        int balanceId = 60;
        Timestamp firstDate = new Timestamp(timestampFormat.parse("2004-01-17 18:30:00").getTime());
        Timestamp secondDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp thirdDate = new Timestamp(timestampFormat.parse("2006-01-17 18:30:00").getTime());
        ParaBalance firstBalance = findParaBalanceForBusinessDate(balanceId, firstDate);
        ParaBalance secondBalance = findParaBalanceForBusinessDate(balanceId, secondDate);
        assertNotNull(firstBalance);
        assertNotNull(secondBalance);
        assertEquals(100, firstBalance.getQuantity(), 0);
        assertEquals(300, secondBalance.getQuantity(), 0);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            firstBalance.setQuantityUntil(150,secondDate);
//            tx.executeBufferedOperations();
            secondBalance.setQuantityUntil(350,thirdDate);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        firstBalance = findParaBalanceForBusinessDate(balanceId, firstDate);
        secondBalance = findParaBalanceForBusinessDate(balanceId, secondDate);
        assertNotNull(firstBalance);
        assertNotNull(secondBalance);
        assertEquals(150, firstBalance.getQuantity(), 0);
        assertEquals(350, secondBalance.getQuantity(), 0);
    }

    private void updateMultipleAttributesOfPositionPriceTwiceSameBusinessDay(java.util.Date businessDate, String accountId, int productId)
    throws Exception
    {
        TestPositionPrice testPositionPrice = this.findTestPositionPriceForBusinessDate(accountId, productId, businessDate);

        assertEquals(999, testPositionPrice.getSourceId());
        assertEquals(1.34, testPositionPrice.getMarketValue(),0);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TestParaDatedBitemporal.PositionPriceUpdate(businessDate, accountId, productId, 100, 2));
        Thread.sleep(10);

        TestPositionPrice testPositionPrice2 = this.findTestPositionPriceForBusinessDate(accountId, productId, businessDate);

        assertEquals(100, testPositionPrice2.getSourceId());
        assertEquals(3.34, testPositionPrice2.getMarketValue(),0);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TestParaDatedBitemporal.PositionPriceUpdate(businessDate, accountId, productId, 101, 3));
        Thread.sleep(10);

        TestPositionPrice testPositionPrice3 = this.findTestPositionPriceForBusinessDate(accountId, productId, businessDate);

        assertEquals(101, testPositionPrice3.getSourceId());
        assertEquals(6.34, testPositionPrice3.getMarketValue(),0);
    }

    public void testIncrementThenSet() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Timestamp prevDate = new Timestamp(timestampFormat.parse("2007-06-18 18:30:00.000").getTime());
                final Timestamp businessDate = new Timestamp(timestampFormat.parse("2007-06-19 18:30:00.000").getTime());
                final Timestamp nextBusinessDate = new Timestamp(timestampFormat.parse("2007-06-20 00:00:00.000").getTime());

                findTestPositionPriceForBusinessDateByPassCache("7607716601", 195518928, prevDate);
                TestPositionPrice price = findTestPositionPriceForBusinessDateByPassCache("7607716601", 195518928, businessDate);
                findTestPositionPriceForBusinessDateByPassCache("7607716601", 195518928, businessDate);
//                price.setSourceId(33);
                price.incrementMarketValueUntil(66, nextBusinessDate);
                price.setSourceId(45);
                return null;
            }
        });
    }

    public void xtestManyAdjustments() throws Exception
    {
        final Timestamp startDate = new Timestamp(timestampFormat.parse("2005-01-01 00:00:00").getTime());
        String[] accounts = generateAccounts();
        int[] products = generateProducts();
        createAndInsertPositionPrice(accounts, products, startDate);
        RandomRunner runner1 = new RandomRunner(accounts, products);
        RandomRunner runner2 = new RandomRunner(accounts, products);
        ExceptionCatchingThread thread1 = new ExceptionCatchingThread(runner1);
        thread1.start();
        ExceptionCatchingThread thread2 = new ExceptionCatchingThread(runner2);
        thread2.start();
        thread1.joinWithExceptionHandling();
        thread2.joinWithExceptionHandling();

    }

    private class RandomRunner implements Runnable
    {
        private String[] accounts;
        private int[] products;

        public RandomRunner(String[] accounts, int[] products)
        {
            this.accounts = accounts;
            this.products = products;
        }

        public void run()
        {
            Calendar c = Calendar.getInstance();
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            for(int i=0;i<1000;i++)
            {
                System.out.println(i);
                Calendar adjCal = (Calendar) c.clone();
                int daysAgo = (int) Math.random() * 10 + 1;
                adjCal.add(Calendar.DATE, -daysAgo);
                final Timestamp adjDate = new Timestamp(adjCal.getTimeInMillis());
                adjCal.add(Calendar.DATE, (int) Math.random() * 6 + 1);
                final Timestamp adjToDate = new Timestamp(adjCal.getTimeInMillis());
                final String[] adjAccounts = new String[50];
                final int[] adjProds = new int[adjAccounts.length];
                for(int p=0;p<adjAccounts.length;p++)
                {
                    adjAccounts[p] = accounts[((int)(Math.random()*accounts.length))];
                    adjProds[p] = products[((int)(Math.random()*products.length))];
                }
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        TestPositionPriceList list = new TestPositionPriceList();
                        for(int p=0;p<adjAccounts.length;p++)
                        {
                            list.add(findTestPositionPrice(adjAccounts[p], adjProds[p], adjDate));
                        }
                        for(int p=0;p<adjAccounts.length;p++)
                        {
                            TestPositionPrice price = list.get(p);
                            price.setMarketValueUntil(price.getMarketValue() + Math.random()*5, adjToDate);
                        }
                        return null;
                    }
                });
            }
        }
    }

    private TestPositionPrice findTestPositionPrice(String adjAccount, int adjProd, Timestamp adjDate)
    {
        Operation op = TestPositionPriceFinder.businessDate().eq(adjDate);
        op = op.and(TestPositionPriceFinder.accountId().eq(adjAccount));
        op = op.and(TestPositionPriceFinder.productId().eq(adjProd));
        op = op.and(TestPositionPriceFinder.acmapCode().eq("A"));
        return TestPositionPriceFinder.findOneBypassCache(op);
    }

    private void createAndInsertPositionPrice(String[] accounts, int[] products, Timestamp startDate)
    {
        TestPositionPriceList list = new TestPositionPriceList(accounts.length * products.length);
        for(String acct: accounts)
        {
            for(int prod: products)
            {
                TestPositionPrice pos = new TestPositionPrice(startDate);
                pos.setAccountId(acct);
                pos.setProductId(prod);
                pos.setBalanceType(901);
                pos.setPositionType(1800);
                pos.setCurrency("USD");
                pos.setAcmapCode("A");
                pos.setMarketValue(Math.random()*1000);

                list.add(pos);
            }
        }
        list.insertAll();
    }

    private int[] generateProducts()
    {
        IntHashSet set = new IntHashSet();
        while(set.size() < 25)
        {
            set.add(99500+(int)(Math.random()*100));
        }
        int[] result = set.toArray();
        return result;
    }

    private String[] generateAccounts()
    {
        String[] result = new String[25];
        HashSet set = new HashSet();
        while(set.size() < result.length)
        {
            set.add("788000"+(int)(10+Math.random()*90)+"01");
        }
        set.toArray(result);
        return result;
    }

    private TestPositionPrice findTestPositionPriceForBusinessDate(String accountId, int productId, java.util.Date businessDate)
    {
        return TestPositionPriceFinder.findOne(TestPositionPriceFinder.acmapCode().eq("A")
                .and(TestPositionPriceFinder.accountId().eq(accountId))
                .and(TestPositionPriceFinder.productId().eq(productId))
                .and(TestPositionPriceFinder.businessDate().eq(businessDate)));
    }

    private TestPositionPrice findTestPositionPriceForBusinessDateByPassCache(String accountId, int productId, java.util.Date businessDate)
    {
        return TestPositionPriceFinder.findOne(TestPositionPriceFinder.acmapCode().eq("A")
                .and(TestPositionPriceFinder.accountId().eq(accountId))
                .and(TestPositionPriceFinder.productId().eq(productId))
                .and(TestPositionPriceFinder.businessDate().eq(businessDate)));
    }


    public void testInsertTerminateSameBusinessDay()
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        ParaBalance balance = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
        balance.setQuantity(100.00);
        balance.setBalanceId(9876);
        balance.setAcmapCode("A");

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance.insert();
        tx.commit();

        ParaBalance balance2 = this.findParaBalanceForBusinessDate(9876, businessDate);
        assertEquals(100, balance2.getQuantity(),0);

        MithraTransaction tx2 = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance2.terminate();
        tx2.commit();

        ParaBalance balance3 = this.findParaBalanceForBusinessDate(9876,businessDate);
        assertNull(balance3);
    }

    public void testInsertIncrementUntilInPlace() throws SQLException
    {
        Timestamp procDate = new Timestamp(System.currentTimeMillis() - 100);
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        Timestamp until = addDaysAsTimestamp(paraBusinessDate, 1);
        ParaBalance balance = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
        balance.setQuantity(100.00);
        balance.setBalanceId(9876);
        balance.setAcmapCode("A");
        balance.setBusinessDateTo(businessDate);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance.insert();
        tx.commit();

        ParaBalance balance2 = this.findParaBalanceForBusinessDate(9876, businessDate);
        assertEquals(100, balance2.getQuantity(),0);

        long beforeUpdate = System.currentTimeMillis();
        this.sleep(100);

        MithraTransaction tx2 = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance2.incrementQuantityUntil(200, until);
        assertEquals(300, balance2.getQuantity(),0);
        tx2.commit();

        ParaBalance balance3 = this.findParaBalanceForBusinessDate(9876,businessDate);
        assertEquals(300, balance3.getQuantity(),0);
        Operation op = ParaBalanceFinder.processingDate().equalsEdgePoint();
        op = op.and(ParaBalanceFinder.acmapCode().eq("A")
                            .and(ParaBalanceFinder.balanceId().eq(9876))
                            .and(ParaBalanceFinder.businessDate().eq(businessDate)));
        ParaBalanceList paraBalanceList = new ParaBalanceList(op);
        assertEquals(1, paraBalanceList.size());
        balance3 = paraBalanceList.getParaBalanceAt(0);
        assertTrue(balance3.getProcessingDateFrom().getTime() > beforeUpdate);
        checkInfinityRow(9876, 300, businessDate, procDate);
    }

    public void testIncrementUntilAndIncrementForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp procDate = new Timestamp(System.currentTimeMillis() - 100);
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        Timestamp until = addDaysAsTimestamp(paraBusinessDate, 1);
        ParaBalance balance = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
        balance.setQuantity(100.00);
        balance.setBalanceId(9876);
        balance.setAcmapCode("A");
        balance.setBusinessDateTo(businessDate);

        ParaBalance balance2 = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
        balance2.setQuantity(200.00);
        balance2.setBalanceId(10123);
        balance2.setAcmapCode("A");
        balance2.setBusinessDateTo(InfinityTimestamp.getParaInfinity());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance.insert();
        balance2.insert();
        tx.commit();

        ParaBalance testBal1 = this.findParaBalanceForBusinessDate(9876, businessDate);
        assertEquals(100, testBal1.getQuantity(),0);

        long beforeUpdate = System.currentTimeMillis();
        this.sleep(100);

        MithraTransaction tx2 = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        balance.incrementQuantityUntil(200, until);
        balance2.setQuantity(600);
        assertEquals(300, balance.getQuantity(),0);
        assertEquals(600, balance2.getQuantity(),0);
        tx2.commit();

        ParaBalanceFinder.clearQueryCache();
        ParaBalance testBal2 = this.findParaBalanceForBusinessDate(9876,businessDate);
        assertEquals(300, testBal2.getQuantity(),0);

        ParaBalance testBal3 = this.findParaBalanceForBusinessDate(10123,businessDate);
        assertEquals(600, testBal3.getQuantity(),0);

        Operation op = ParaBalanceFinder.processingDate().equalsEdgePoint();
        op = op.and(ParaBalanceFinder.acmapCode().eq("A")
                            .and(ParaBalanceFinder.balanceId().eq(9876))
                            .and(ParaBalanceFinder.businessDate().eq(businessDate)));
        ParaBalanceList paraBalanceList = new ParaBalanceList(op);
        assertEquals(1, paraBalanceList.size());
        testBal1 = paraBalanceList.getParaBalanceAt(0);
        assertTrue(testBal1.getProcessingDateFrom().getTime() > beforeUpdate);
        checkInfinityRow(9876, 300, businessDate, procDate);
    }

    private class PositionPriceUpdate implements TransactionalCommand
    {
        TestPositionPrice price;
        int sourceId;
        double increment;
        String accountId;
        int productId;
        java.util.Date businessDate;

        public PositionPriceUpdate(java.util.Date businessDate, String accountId, int productId, int sourceId, double increment)
        {
            this.businessDate = businessDate;
            this.accountId = accountId;
            this.productId = productId;
            this.sourceId = sourceId;
            this.increment = increment;
        }

        public Object executeTransaction(MithraTransaction tx) throws Throwable
        {
            price = findTestPositionPriceForBusinessDate(accountId, productId, businessDate);
            price.setSourceId(sourceId);
            price.incrementMarketValue(increment);
            return null;
        }
    }

    public void testMultiQueueExecutor()
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        Timestamp oldBusinessDate = addDaysAsTimestamp(paraBusinessDate, -10);
        MultiQueueExecutor executor = new MultiQueueExecutor(3, ParaBalanceFinder.balanceId(), 10, ParaBalanceFinder.getFinderInstance());
        for(int i=2000;i<2500;i++)
        {
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(i);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(i));
            executor.addForInsert(bal);
        }
        executor.waitUntilFinished();
        Operation op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().greaterThanEquals(2000));
        op = op.and(ParaBalanceFinder.balanceId().lessThan(2500));
        op = op.and(ParaBalanceFinder.businessDate().eq(businessDate));
        ParaBalanceList oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().ascendingOrderBy());
        assertEquals(500, oldBalances.size());

        executor = new MultiQueueExecutor(3, ParaBalanceFinder.balanceId(), 10, ParaBalanceFinder.getFinderInstance());
        executor.setLogInterval(500);
        for(int i=2000;i<2500;i++)
        {
            if (i % 2 == 0)
            {
                ParaBalance bal = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
                bal.setBalanceId(i);
                bal.setAcmapCode("A");
                bal.setQuantity(Math.sqrt(i) + 1);
                executor.addForUpdate(oldBalances.get(i - 2000), bal);
            }
            else
            {
                executor.addForTermination(oldBalances.get(i - 2000));
            }
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(i+1000);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(i));
            executor.addForInsert(bal);
        }
        executor.waitUntilFinished();

        checkResultsOfMultiExecutor(op, businessDate, 3000);
    }

    private void checkResultsOfMultiExecutor(Operation op, Timestamp businessDate, int insertMin)
    {
        ParaBalanceList oldBalances;
        oldBalances = null;
        ParaBalanceFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();
        oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().ascendingOrderBy());
        assertEquals(250, oldBalances.size());
        for(int i = 0; i< oldBalances.size();i++)
        {
            ParaBalance bal = oldBalances.getParaBalanceAt(i);
            assertTrue(bal.getBalanceId() % 2 == 0);
            assertEquals(Math.sqrt(bal.getBalanceId()) + 1, bal.getQuantity(), 0);
        }
        op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().greaterThanEquals(insertMin));
        op = op.and(ParaBalanceFinder.businessDate().eq(businessDate));
        oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().ascendingOrderBy());
        assertEquals(500, oldBalances.size());
    }

    public void testBulkInsertMultiQueueExecutor()
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        Timestamp oldBusinessDate = addDaysAsTimestamp(paraBusinessDate, -10);
        MultiQueueExecutor executor = new MultiQueueExecutor(3, ParaBalanceFinder.balanceId(), 10, ParaBalanceFinder.getFinderInstance());
        executor.setUseBulkInsert();
        for(int i=2000;i<2500;i++)
        {
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(i);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(i));
            executor.addForInsert(bal);
        }
        executor.waitUntilFinished();
        Operation op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().greaterThanEquals(2000));
        op = op.and(ParaBalanceFinder.balanceId().lessThan(2500));
        op = op.and(ParaBalanceFinder.businessDate().eq(businessDate));
        ParaBalanceList oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().ascendingOrderBy());
        assertEquals(500, oldBalances.size());

        executor = new MultiQueueExecutor(3, ParaBalanceFinder.balanceId(), 10, ParaBalanceFinder.getFinderInstance());
        executor.setUseBulkInsert();
        executor.setLogInterval(500);
        for(int i=2000;i<2500;i++)
        {
            if (i % 2 == 0)
            {
                ParaBalance bal = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
                bal.setBalanceId(i);
                bal.setAcmapCode("A");
                bal.setQuantity(Math.sqrt(i) + 1);
                executor.addForUpdate(oldBalances.get(i - 2000), bal);
            }
            else
            {
                executor.addForTermination(oldBalances.get(i - 2000));
            }
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(i+1000);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(i));
            executor.addForInsert(bal);
        }
        executor.waitUntilFinished();
        checkResultsOfMultiExecutor(op, businessDate, 3000);

    }

    public void testBulkInsertSingleQueueExecutor()
    {
        runSingleQueueExecutor(true);
    }

    public void testSingleQueueExecutor()
    {
        runSingleQueueExecutor(false);
    }

    public void testResubmittingRunnable()
    {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                                        0L, TimeUnit.MILLISECONDS,
                                        new LinkedBlockingQueue());
        Runnable r = new Runnable()
        {
            public void run()
            {
                // do nothing... used to delay the executor;
                sleep(100);
            }
        };

        final int[]ran = new int[1];
        final boolean[]bool = new boolean[1];
        Runnable r2 = new Runnable()
        {
            public void run()
            {
                ran[0]++;
                if (ran[0] < 5)
                {
                    if (executor.isShutdown())
                    {
                        bool[0] = true;
                    }
                    executor.getQueue().add(this);
                }
            }
        };

        for(int i=0;i<10;i++)
        {
            executor.submit(r);
        }
        executor.submit(r2);
        executor.shutdown();
        try
        {
            executor.awaitTermination(100, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            fail();
        }
        assertTrue(bool[0]);
        assertEquals(5, ran[0]);
    }

    public void runSingleQueueExecutor(boolean bulkInsert)
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        Timestamp oldBusinessDate = addDaysAsTimestamp(paraBusinessDate, -10);
        SingleQueueExecutor executor = new SingleQueueExecutor(3, ParaBalanceFinder.balanceId().ascendingOrderBy(), 10,
                ParaBalanceFinder.getFinderInstance(), 12);
        if (bulkInsert) executor.setUseBulkInsert();
        for(int i=2000;i<2500;i++)
        {
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(5000-i);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(5000-i));
            executor.addForInsert(bal);
        }
        executor.waitUntilFinished();
        Operation op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().greaterThanEquals(2501));
        op = op.and(ParaBalanceFinder.balanceId().lessThanEquals(3000));
        op = op.and(ParaBalanceFinder.businessDate().eq(businessDate));
        ParaBalanceList oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().descendingOrderBy());
        assertEquals(500, oldBalances.size());
        sleep(20);
        executor = new SingleQueueExecutor(3, ParaBalanceFinder.balanceId().ascendingOrderBy(), 10,
                ParaBalanceFinder.getFinderInstance(), 12);
        if (bulkInsert) executor.setUseBulkInsert();
        executor.setLogInterval(500);
        for(int i=2000;i<2500;i++)
        {
            if (i % 2 == 0)
            {
                ParaBalance bal = new ParaBalance(businessDate, InfinityTimestamp.getParaInfinity());
                bal.setBalanceId(5000-i);
                bal.setAcmapCode("A");
                bal.setQuantity(Math.sqrt(5000-i) + 1);
                assertEquals(oldBalances.get(i - 2000).getBalanceId(), bal.getBalanceId());
                executor.addForUpdate(oldBalances.get(i - 2000), bal);
            }
            else
            {
                executor.addForTermination(oldBalances.get(i - 2000));
            }
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(20000-i);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(20000-i));
            executor.addForInsert(bal);
        }
        executor.waitUntilFinished();
        checkResultsOfMultiExecutor(op, businessDate, 15000);

    }

    public void testLargeInserts()
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        final Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        final Timestamp oldBusinessDate = addDaysAsTimestamp(paraBusinessDate, -10);
        ParaBalanceList list = new ParaBalanceList();
        for(int i=2000;i<2500;i++)
        {
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(5000-i);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(5000-i));
            list.add(bal);
        }
        list.insertAll();
        Operation op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().greaterThanEquals(2501));
        op = op.and(ParaBalanceFinder.balanceId().lessThanEquals(3000));
        op = op.and(ParaBalanceFinder.businessDate().eq(businessDate));
        final ParaBalanceList oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().descendingOrderBy());
        assertEquals(500, oldBalances.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                ParaBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                for (int i = 2000; i < 2500; i++)
                {
                    ParaBalance bal = oldBalances.get(i - 2000);
                    if (i % 2 == 0)
                    {
                        bal.setBalanceId(5000 - i);
                        bal.setQuantity(Math.sqrt(5000 - i) + 1);
                    }
                    else
                    {
                        bal.terminate();
                    }
                    ParaBalance bal2 = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
                    bal2.setBalanceId(20000 - i);
                    bal2.setAcmapCode("A");
                    bal2.setQuantity(Math.sqrt(20000 - i));
                    bal2.insert();
                }
                return null;
            }
        });
        checkResultsOfMultiExecutor(op, businessDate, 15000);

    }

    public void testInsertUpdateAndTerminates()
    {
        java.util.Date paraBusinessDate = createParaBusinessDate(new java.util.Date());
        final Timestamp businessDate = addDaysAsTimestamp(paraBusinessDate,0);
        final Timestamp oldBusinessDate = addDaysAsTimestamp(paraBusinessDate, -10);
        ParaBalanceList list = new ParaBalanceList();
        for(int i=2000;i<2500;i++)
        {
            ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
            bal.setBalanceId(5000-i);
            bal.setAcmapCode("A");
            bal.setQuantity(Math.sqrt(5000-i));
            list.add(bal);
        }
        list.insertAll();
        Operation op = ParaBalanceFinder.acmapCode().eq("A");
        op = op.and(ParaBalanceFinder.balanceId().greaterThanEquals(2501));
        op = op.and(ParaBalanceFinder.balanceId().lessThanEquals(3000));
        op = op.and(ParaBalanceFinder.businessDate().eq(businessDate));
        final ParaBalanceList oldBalances = new ParaBalanceList(op);
        oldBalances.setOrderBy(ParaBalanceFinder.balanceId().descendingOrderBy());
        assertEquals(500, oldBalances.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for(int i=2000;i<2500;i++)
                {
                    if (i % 2 == 0)
                    {
                        oldBalances.get(i - 2000).setQuantity(Math.sqrt(5000-i) + 1);
                    }
                    else
                    {
                        oldBalances.get(i - 2000).terminate();
                    }
                    ParaBalance bal = new ParaBalance(oldBusinessDate, InfinityTimestamp.getParaInfinity());
                    bal.setBalanceId(20000-i);
                    bal.setAcmapCode("A");
                    bal.setQuantity(Math.sqrt(20000-i));
                    bal.insert();
                }
                return null;
            }
        });
        checkResultsOfMultiExecutor(op, businessDate, 15000);

    }

    public void testManyUpdates()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            private void updateBalFiftyOne(String from, String to)
            {
                Timestamp businessDate = Timestamp.valueOf(from);
                final ParaBalance bal = findParaBalanceForBusinessDate(51, businessDate);
                bal.setQuantityUntil(-0.0, Timestamp.valueOf(to));
            }

            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                updateBalFiftyOne("2002-12-15 18:30:00.0", "9999-12-01 23:59:00.0");
                updateBalFiftyOne("2002-11-29 18:30:00.0", "2002-11-30 18:30:00.0");
                updateBalFiftyOne("2002-11-30 18:30:00.0", "2002-12-01 18:30:00.0");
                updateBalFiftyOne("2002-12-01 18:30:00.0", "2002-12-02 18:30:00.0");
                updateBalFiftyOne("2002-12-02 18:30:00.0", "2002-12-03 18:30:00.0");
                updateBalFiftyOne("2002-12-03 18:30:00.0", "2002-12-04 18:30:00.0");
                updateBalFiftyOne("2002-12-04 18:30:00.0", "2002-12-05 18:30:00.0");
                updateBalFiftyOne("2002-12-05 18:30:00.0", "2002-12-06 18:30:00.0");
                updateBalFiftyOne("2002-12-06 18:30:00.0", "2002-12-07 18:30:00.0");
                updateBalFiftyOne("2002-12-07 18:30:00.0", "2002-12-08 18:30:00.0");
                updateBalFiftyOne("2002-12-08 18:30:00.0", "2002-12-09 18:30:00.0");
                updateBalFiftyOne("2002-12-09 18:30:00.0", "2002-12-10 18:30:00.0");
                updateBalFiftyOne("2002-12-10 18:30:00.0", "2002-12-11 18:30:00.0");
                updateBalFiftyOne("2002-12-11 18:30:00.0", "2002-12-12 18:30:00.0");
                updateBalFiftyOne("2002-12-12 18:30:00.0", "2002-12-13 18:30:00.0");
                updateBalFiftyOne("2002-12-13 18:30:00.0", "2002-12-14 18:30:00.0");
                updateBalFiftyOne("2002-12-14 18:30:00.0", "2002-12-15 18:30:00.0");
                updateBalFiftyOne("2002-12-15 18:30:00.0", "2002-12-16 18:30:00.0");
                updateBalFiftyOne("2002-12-16 18:30:00.0", "2002-12-17 18:30:00.0");
                updateBalFiftyOne("2002-12-17 18:30:00.0", "2002-12-18 18:30:00.0");
                updateBalFiftyOne("2002-12-18 18:30:00.0", "2002-12-19 18:30:00.0");
                updateBalFiftyOne("2002-12-19 18:30:00.0", "2002-12-20 18:30:00.0");
                return null;
            }
        });
    }

    private Timestamp parseDate(String dateString)
    {
        try
        {
            return new Timestamp(new SimpleDateFormat("yyyyMMdd").parse(dateString).getTime());
        }
        catch (ParseException e)
        {
            throw new RuntimeException();
        }
    }

    private void performUpdatesOnFundingBalance(String asofDate, String untilDate, double value)
    {
        FundingBalance fndBalance;
        fndBalance = getFundingBalance(asofDate);
        fndBalance.setBalanceValueUntil(value, parseDate(untilDate));
    }

    private FundingBalance getFundingBalance(String dateStr)
    {
        Timestamp asofDate = parseDate(dateStr);
//        Timestamp asofDatePara = new Timestamp(createParaBusinessDate(asofDate).getTime());
        Timestamp asofDatePara = asofDate;
        Operation op = FundingBalanceFinder.acmapCode().eq("A");
        op = op.and(FundingBalanceFinder.businessDate().eq(asofDatePara));
        op = op.and(FundingBalanceFinder.balanceType().eq(16301));
        op = op.and(FundingBalanceFinder.productId().eq(3000));

        return FundingBalanceFinder.findOne(op);
    }

    public void testFundingBalance()
    {

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                tx.setProcessingStartTime(Timestamp.valueOf("2010-09-29 13:04:42.650").getTime());
                performUpdatesOnFundingBalance("20100913", "99991201", -0.0);
                performUpdatesOnFundingBalance("20100903", "20100904", -0.0);
                performUpdatesOnFundingBalance("20100904", "20100905", -0.0);
                performUpdatesOnFundingBalance("20100905", "20100906", -0.0);
                performUpdatesOnFundingBalance("20100906", "20100907", -0.0);
                performUpdatesOnFundingBalance("20100907", "20100908", -0.0);

                performUpdatesOnFundingBalance("20100908", "20100909", -0.0);
                performUpdatesOnFundingBalance("20100909", "20100910", -0.0);
                performUpdatesOnFundingBalance("20100910", "20100911", -0.0);
                performUpdatesOnFundingBalance("20100911", "20100912", -0.0);
                performUpdatesOnFundingBalance("20100912", "20100913", -0.0);
                return null;
            }
        }, new TransactionStyle(1000));

    }
}
