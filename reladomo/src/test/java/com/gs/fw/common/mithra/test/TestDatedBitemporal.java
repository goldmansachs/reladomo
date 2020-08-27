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
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantity;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityFinder;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityList;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;



public class TestDatedBitemporal extends MithraTestAbstract implements TestDatedBitemporalDatabaseChecker
{

    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private TestDatedBitemporalDatabaseChecker checker = this;
    private static final int DAY = 24*3600*1000;

    public void setChecker(TestDatedBitemporalDatabaseChecker checker)
    {
        this.checker = checker;
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TinyBalance.class,
            PositionQuantity.class,
            TinyBalanceWithSmallDate.class,
            TestPositionIncomeExpense.class,
            BitemporalOrder.class,
            BitemporalOrderItem.class,
            BitemporalOrderItemStatus.class,
            BitemporalOrderStatus.class,
            DatedAllTypes.class,
            ParaProduct.class
        };
    }

    public void testDoubleAttributeIncrementData()
    {
        TinyBalanceData data = TinyBalanceDatabaseObject.allocateOnHeapData();
        data.setQuantity(100);
        TinyBalanceFinder.quantity().increment(data, 50);
        assertEquals(150, data.getQuantity(), 0.0);
    }


    protected TinyBalanceInterface build(Timestamp businessDate, Timestamp processingDate)
    {
        return new TinyBalance(businessDate, processingDate);
    }

    protected TinyBalanceInterface build(Timestamp businessDate)
    {
        return new TinyBalance(businessDate);
    }

    public void testPrimaryKeyGet() throws Exception
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = null;
        tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        TinyBalance tb2 = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().equalsEdgePoint())
                .and(TinyBalanceFinder.processingDate().equalsEdgePoint())
                .and(TinyBalanceFinder.businessDateFrom().eq(tb.getBusinessDateFrom()))
                .and(TinyBalanceFinder.processingDateFrom().eq(tb.getProcessingDateFrom())));
        assertNotNull(tb2);
    }


    public Timestamp getInfinite()
    {
        return InfinityTimestamp.getParaInfinity();
    }

    public void testInsertJdbcDeleteInsert() throws SQLException
    {
        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build(businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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

        Connection con = this.getConnection("A");
        deleteTiny(con);
        con.close();

        tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build(businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(16);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            assertEquals(16, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000,  this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checkDatedBitemporalInfinityRow(2000, 16, businessDate);
    }

    protected void deleteTiny(Connection con) throws java.sql.SQLException
    {
        con.createStatement().execute("delete from TINY_BALANCE where BALANCE_ID = 2000");
    }


    public void testSetNullAndBack() throws Exception
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = null;
        double oldValue = -1;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNotNull(tb);
            oldValue = tb.getQuantity();
            TinyBalanceInterface copy = tb.getNonPersistentCopy();
            copy.setQuantityNull();
            copy.setQuantity(oldValue);
            tb.copyNonPrimaryKeyAttributesFrom(copy);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        assertEquals(oldValue, tb.getQuantity(), 0.0);
        clearCache();
        tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertEquals(oldValue, tb.getQuantity(), 0.0);
        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, oldValue, businessDate);
    }

    protected void clearCache()
    {
        TinyBalanceFinder.clearQueryCache();
    }

    public void testTripleIncrement() throws SQLException, ParseException
    {
        Timestamp pbd = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp lbd = new Timestamp(timestampFormat.parse("2005-01-15 18:30:00").getTime());
        Timestamp cbd = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        TinyBalanceInterface tb = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( lbd, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            tx.executeBufferedOperations();
            tb = findTinyBalanceForBusinessDate(2000, cbd);
            assertNotNull(tb);
            tb.incrementQuantity(-1.0);
            tx.executeBufferedOperations();
            tb = build( pbd, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            tb.setQuantity(3.5);
            tb.insertWithIncrement();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, cbd);
        assertEquals(15.0, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertEquals(15.0, fromCache.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 15, cbd);
    }

    public void testTerminateAll() throws SQLException
    {
        TinyBalanceList list = TinyBalanceFinder.findMany(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(1))
            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));

        assertTrue(list.size() > 1);

        list.terminateAll();
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID > 1 and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, this.getInfinite());
        ps.setTimestamp(2, this.getInfinite());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void testTerminateAllCustomInz() throws SQLException, ParseException
    {
        TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(1))
            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));

        assertTrue(list.size() > 1);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();

        try
        {
            tx.setProcessingStartTime(timestampFormat.parse("2002-11-29 00:00:00").getTime());
            list.terminateAll();
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }

        list = new TinyBalanceList(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(1))
            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));
        assertEquals(0, list.size());

        list = new TinyBalanceList(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(1))
            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())))
            .and(TinyBalanceFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2002-11-28 00:00:00").getTime()))));
        assertEquals(0, list.size());

        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID > 1 and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, this.getInfinite());
        ps.setTimestamp(2, this.getInfinite());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void checkDatedBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE where BALANCE_ID = ? and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, this.getInfinite());
        ps.setTimestamp(3, this.getInfinite());
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

    public void checkDatedBitemporalTimestampRow(int balanceId, double quantity, Timestamp businessDate, Timestamp processingDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE where BALANCE_ID = ? and " +
                "IN_Z < ? and OUT_Z >= ? and FROM_Z <= ? and THRU_Z > ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, processingDate);
        ps.setTimestamp(3, processingDate);
        ps.setTimestamp(4, businessDate);
        ps.setTimestamp(5, businessDate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertFalse(hasMoreResults);
    }

    public int checkDatedBitemporalRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID = ?";
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

    public void testNonPeristestCopy()
    throws Exception
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        TinyBalanceInterface tb = null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tb = findTinyBalanceForBusinessDate(20, businessDate);
        TinyBalanceInterface tb2 = tb.getNonPersistentCopy();
        assertEquals(tb.getQuantity(), tb2.getQuantity(),0);
        assertEquals(tb.getAcmapCode(), tb2.getAcmapCode());
        assertEquals(tb.getBalanceId(), tb2.getBalanceId());
        assertEquals(tb.getBusinessDateFrom(), tb2.getBusinessDateFrom());
        assertEquals(tb.getBusinessDateTo(), tb2.getBusinessDateTo());
        assertEquals(tb.getProcessingDateFrom(), tb2.getProcessingDateFrom());
        assertEquals(tb.getProcessingDateTo(), tb2.getProcessingDateTo());

        tb2.setQuantity(400);
        tb.copyNonPrimaryKeyAttributesFrom(tb2);

        assertEquals(tb.getQuantity(), tb2.getQuantity(),0);
        assertEquals(tb.getAcmapCode(), tb2.getAcmapCode());
        assertEquals(tb.getBalanceId(), tb2.getBalanceId());
        tx.commit();

    }

    public void testInsertInTransaction() throws SQLException
    {
        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build(businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, tb.getQuantity(), 0);
        assertEquals(2000, tb.getBalanceId());
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 12.5, businessDate);
    }

    public void testConstructFirstThenInsertInTransaction() throws SQLException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        TinyBalanceInterface tb = build(businessDate, this.getInfinite());
        tb.setAcmapCode("A");
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            assertEquals("A", tb.getAcmapCode());
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, tb.getQuantity(), 0);
        assertEquals(2000, tb.getBalanceId());
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertInTransactionCustomInz() throws SQLException, ParseException
    {
        long customInZ = timestampFormat.parse("2002-11-29 00:00:00").getTime();

        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tx.setProcessingStartTime(customInZ);
        try
        {
            tb = build(businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceByDates(2000, new Timestamp(businessDate.getTime()+1000), new Timestamp(customInZ + 1000));
            assertNotNull(fromCache);
            fromCache = findTinyBalanceByDates(2000, new Timestamp(businessDate.getTime()+1000), new Timestamp(customInZ - 1000));
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
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        fromCache = findTinyBalanceByDates(2000, new Timestamp(businessDate.getTime()+1000), new Timestamp(customInZ + 1000));
        assertNotNull(fromCache);
        fromCache = findTinyBalanceByDates(2000, new Timestamp(businessDate.getTime()+1000), new Timestamp(customInZ - 1000));
        assertNull(fromCache);

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertRollback() throws SQLException
    {
        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build(businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertNull(fromCache);
        // test the database:

        this.checker.checkDatedBitemporalTerminated(2000);
    }

    public void testUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);
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
                TinyBalanceInterface balPast = build(past);
                balPast.setAcmapCode("A");
                balPast.setBalanceId(2000);
                balPast.setQuantity(-6851);
                balPast.insert();
                TinyBalanceInterface balAtFuture = findTinyBalanceForBusinessDate(2000, future);
                assertEquals(-6851.0, balAtFuture.getQuantity(), 0.01);
                TinyBalanceInterface balNow = findTinyBalanceForBusinessDate(2000, now);
                assertEquals(-6851.0, balNow.getQuantity(), 0.01);
                balNow.setQuantity(100);
                assertEquals(100.0, balNow.getQuantity(), 0.01);
                assertEquals(100.0, balAtFuture.getQuantity(), 0.01);
                return null;
            }
        }, new TransactionStyle(10000));
    }
    protected TinyBalanceInterface findTinyBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return this.findTinyBalanceByDates(balanceId, businessDate, this.getInfinite());
    }

    protected TinyBalanceInterface findTinyBalanceByDates(int balanceId, Timestamp businessDate, Timestamp processingDate)
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceFinder.businessDate().eq(businessDate))
                            .and(TinyBalanceFinder.processingDate().eq(processingDate)));
    }

    protected TinyBalanceWithSmallDateInterface findTinyBalanceAsStringByDates(int balanceId, Timestamp businessDate, Timestamp processingDate)
    {
        return TinyBalanceWithSmallDateFinder.findOne(TinyBalanceWithSmallDateFinder.acmapCode().eq("A")
                            .and(TinyBalanceWithSmallDateFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceWithSmallDateFinder.businessDate().eq(businessDate))
                            .and(TinyBalanceWithSmallDateFinder.processingDate().eq(processingDate)));
    }

    protected TinyBalanceInterface findInactiveObject() throws ParseException
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(10))
                            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime())))
                            .and(TinyBalanceFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2004-05-05 00:00:00").getTime()))));
    }

    protected BitemporalOrderInterface findOrderForBusinessDate(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderFinder.findOne(BitemporalOrderFinder.orderId().eq(orderId)
                .and(BitemporalOrderFinder.businessDate().eq(businessDate))
                .and(BitemporalOrderFinder.processingDate().eq(this.getInfinite())));
    }

    protected Object findOrderItemForBusinessDate(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderItemFinder.findOne(BitemporalOrderItemFinder.orderId().eq(orderId)
                .and(BitemporalOrderItemFinder.businessDate().eq(businessDate))
                .and(BitemporalOrderItemFinder.processingDate().eq(this.getInfinite())));
    }

    protected Object findOrderStatusForBusinessDate(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderStatusFinder.findOne(BitemporalOrderStatusFinder.orderId().eq(orderId)
                .and(BitemporalOrderStatusFinder.businessDate().eq(businessDate))
                .and(BitemporalOrderStatusFinder.processingDate().eq(this.getInfinite())));
    }

    public void testNullBusinessDate()
    {
        findTinyBalanceForBusinessDate(1, null);
    }

    public void testUpdateSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);
    }

    public void testUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(17);
            tb.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceInterface original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testUpdateLaterBusinesDayAsString() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalanceWithSmallDateInterface tb = findTinyBalanceAsStringByDates(balanceId, businessDate, this.getInfinite());
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceWithSmallDateInterface fromCache = findTinyBalanceAsStringByDates(balanceId, new Timestamp(businessDate.getTime()+1000), this.getInfinite());
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceAsStringByDates(balanceId, businessDate, this.getInfinite());
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceAsStringByDates(balanceId, new Timestamp(businessDate.getTime()+1000), this.getInfinite());
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
        TinyBalanceWithSmallDateInterface fromCache = findTinyBalanceAsStringByDates(balanceId, businessDate, this.getInfinite());
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceAsStringByDates(balanceId, this.getInfinite(), this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceWithSmallDateInterface original = findTinyBalanceAsStringByDates(balanceId, originalBusinessDate, this.getInfinite());
        assertFalse(12.5 == original.getQuantity());
    }

    public void testUpdateLaterBusinesDayCustomInz() throws SQLException, ParseException
    {
        long customInZ = timestampFormat.parse("2002-11-29 00:00:00").getTime();

        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tx.setProcessingStartTime(customInZ);
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceInterface original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + originalQuantity, businessDate);
    }

    public void testIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(17);
            tb.incrementQuantity(12.5);
            quantity += 17 + 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        assertTrue(fromCache.getQuantity() == quantity);

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            quantity += 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceInterface original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(1);
    }

    protected TinyBalanceInterface findTinyBalanceForBusinessDateProcessing(int balanceId, Timestamp businessDate, Timestamp timestamp)
    {
        return  TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(timestamp)));

    }

    public void testUpdateUntilManyTimes()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-01 00:00:00").getTime());
                Timestamp nextDate = new Timestamp(businessDate.getTime()+24*3600*1000L);
                for(int i=0;i<100;i++)
                {
                    TinyBalanceInterface tb = findTinyBalanceForBusinessDate((i % 5)+1, businessDate);
                    assertNotNull(tb);
                    tb.setQuantityUntil(i+100, nextDate);
                    businessDate = nextDate;
                    nextDate = new Timestamp(businessDate.getTime()+24*3600*1000L);
                }
                return null;
            }
        });
    }

    public void testTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis() - 48*3600*1000);
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(1);
    }

    public void testTerminateLaterBusinesDayWithTwoObjects() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis() - DAY *2);
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            TinyBalanceInterface fromCache2 = findTinyBalanceForBusinessDate(balanceId, tb.getBusinessDateFrom());
            Timestamp secondTerminateDate = new Timestamp(businessDate.getTime() - DAY * 5);
            TinyBalanceInterface toTerminate2 = findTinyBalanceForBusinessDate(balanceId, secondTerminateDate);
            TinyBalanceInterface copy = tb.getNonPersistentCopy();
//            copy.setQuantity(-123);
            fromCache2.copyNonPrimaryKeyAttributesUntilFrom(copy, getInfinite());
//            assertEquals(-123, fromCache2.getQuantity(), 0);
            tb.terminate();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            assertEquals(businessDate, fromCache2.getBusinessDateTo());
            toTerminate2.terminate();
            assertEquals(secondTerminateDate, fromCache2.getBusinessDateTo());
            try
            {
                fromCache.getQuantity();
                fail("should not get here");
            }
            catch(MithraDeletedException e)
            {
                // ok
            }
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(1);
    }

    public void checkDatedBitemporalTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID = ? and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ps.setTimestamp(2, this.getInfinite());
        ps.setTimestamp(3, this.getInfinite());
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
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        findInactiveObject();
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(priorSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiSegmentTransactionParticipation() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() != 0);
            assertTrue(nextSegmentBalance.getQuantity() != 0);
            assertTrue(priorSegmentBalance.getQuantity() != 0);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
    }

    public void testMultiSegmentResetDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(priorSegmentQuantity);
            assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(priorSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, priorSegmentQuantity, businessDate);
    }

    public void testMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);

        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(10);
    }

    public void testMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        // test the database:
        this.checker.checkDatedBitemporalTerminated(10);
    }

    public void testMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity() , 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    protected void incrementQuantity(TinyBalanceInterface tb, double d)
    {
        TinyBalanceFinder.quantity().increment(tb, d);
    }

    public void testMultiSegmentIncrementDifferentBusinesDayWithAttribute() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            incrementQuantity(tb,12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-10 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-11 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 20;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 40;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis() - 5000));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(40);
    }

    public void testIncrementUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(reversed.getQuantity() == originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testIncrementUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(reversed.getQuantity() == originalQuantity);
            assertTrue(previousSegment.getQuantity() == originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, until);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(10 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.incrementQuantityUntil(2.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentIncrementUntilForSegmentsSeparately() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface segOne = findTinyBalanceForBusinessDate(balanceId, new Timestamp(segmentStartDate.getTime()+1000));
            TinyBalanceInterface segTwo = findTinyBalanceForBusinessDate(balanceId, new Timestamp(nextSegmentBusinessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, nextSegmentBusinessDate);
            nextSegmentBalance.incrementQuantityUntil(20, this.getInfinite());
            assertEquals(10 + segmentOneOriginalQuantity, segOne.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(20 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(tb, fromCache);
        assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity + 20);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(20 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity() , 0.0);
        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity + 20, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, until);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(10 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();

            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.incrementQuantityUntil(2.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertEquals(originalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.setQuantityUntil(2.5, until);
            assertEquals(2.5, fromCache.getQuantity(), 0.0);
            assertEquals(2.5, tb.getQuantity(), 0.0);
            assertEquals(2.5, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(2.5, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(2.5, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(2.5, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();

            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0.0);
            assertEquals(12.5, tb.getQuantity(), 0.0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testInsertWithIncrementZeroSegments() throws SQLException
    {
        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertWithIncrementOneSegment() throws Exception
    {
        TinyBalanceInterface tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceInterface nextSegment = this.findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + nextSegmentQuantity, segmentStartDate);
    }

    public void testInsertWithIncrementOneSegmentThenUpdate() throws Exception
    {
        TinyBalanceInterface tb = null;
        int balanceId = 1;
        double nextSegmentQuantity = -6851;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp midBusinessDate = new Timestamp(timestampFormat.parse("2002-11-20 18:30:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, midBusinessDate);
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
            assertEquals(12.5 + nextSegmentQuantity, fromCache.getQuantity(), 0);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();

            tb.setQuantity(15.8); // wipes out the next segment
            assertEquals(15.8, tb.getQuantity());
            fromCache = findTinyBalanceForBusinessDate(balanceId, midBusinessDate);
            assertNotSame(tb, fromCache);
            assertEquals(15.8, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
            assertEquals(15.8, fromCache.getQuantity(), 0);

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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 15.8, businessDate);
    }

    public void testIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        long now = System.currentTimeMillis();
        Timestamp businessDate = new Timestamp(now - DAY);
        Timestamp secondBusinessDate = new Timestamp(now);
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface secondTb = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            quantity += 12.5;
            assertEquals(quantity, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
            assertSame(secondTb, fromCache);
            assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(quantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity+45.1, secondBusinessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceInterface original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp secondBusinessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface secondTb = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, secondTb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
            assertSame(secondTb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertNull(fromCache);
    }

    public void testInsertUntil() throws SQLException, ParseException
    {
        TinyBalanceInterface tb = null;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertUntil(until);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, this.getInfinite());
        assertNull(fromCache);
    }

    public void testCascadeInsertUntil() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2000-01-01 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());

        BitemporalOrder order = null;
        BitemporalOrderItemList orderItemList;
        BitemporalOrderStatus orderStatus;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 5;
        try
        {
            order = new BitemporalOrder(businessDate, this.getInfinite());
            order.setOrderId(orderId);
            order.setOrderDate(businessDate);
            order.setUserId(2);
            order.setDescription("New order");
            order.setState("In-Progress");
            order.setTrackingId("130");

            BitemporalOrderItem orderItem = new BitemporalOrderItem(businessDate, this.getInfinite());
            orderItem.setId(orderId);
            orderItem.setOrderId(orderId);
            orderItem.setProductId(orderId);
            orderItem.setQuantity(10);
            orderItem.setOriginalPrice(25);
            orderItem.setDiscountPrice(20);
            orderItem.setState("In-Progress");
            orderItemList = new BitemporalOrderItemList();
            orderItemList.add(orderItem);

            orderStatus = new BitemporalOrderStatus(businessDate, this.getInfinite());
            orderStatus.setOrderId(orderId);
            orderStatus.setStatus(10);
            orderStatus.setLastUser("goldmaa");
            orderStatus.setLastUpdateTime(businessDate);

            order.setItems(orderItemList);
            order.setOrderStatus(orderStatus);

            order.cascadeInsertUntil(until);

            assertTrue(order.zIsParticipatingInTransaction(tx));
            BitemporalOrderInterface fromCache = findOrderForBusinessDate(orderId, businessDate);
            assertSame(order, fromCache);
            fromCache = findOrderForBusinessDate(orderId, until);
            assertNull(fromCache);
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        BitemporalOrderInterface fromCache = findOrderForBusinessDate(orderId, businessDate);
        assertSame(order, fromCache);
        assertEquals(order.getItems().size(),  fromCache.getItems().size());
        assertSame(order.getItems().get(0),  fromCache.getItems().get(0));
        assertSame(order.getOrderStatus(),  fromCache.getOrderStatus());
        fromCache = findOrderForBusinessDate(orderId, until);
        assertNull(fromCache);
    }

    public void testInsertWithIncrementUntilOneSegment() throws Exception
    {
        TinyBalanceInterface tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        TinyBalanceInterface nextSegment = this.findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate, this.getInfinite());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(nextSegmentQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, nextSegmentQuantity, until);
    }

    public void testInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        TinyBalanceInterface tb = null;
        Timestamp terminateDate = new Timestamp(System.currentTimeMillis());
        Timestamp businessDate = new Timestamp(terminateDate.getTime() - 24*60*60000);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceForBusinessDate(2000, terminateDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalTerminated(2000);
    }

    public void testInsertThenTerminateSameBusinessDate() throws SQLException
    {
        TinyBalanceInterface tb = null;
        Timestamp terminateDate = new Timestamp(System.currentTimeMillis());
        Timestamp businessDate = terminateDate;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = build( businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceForBusinessDate(2000, terminateDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertNull(fromCache);

        // test the database:
        this.checker.checkDatedBitemporalTerminated(2000);
    }

    public void testUpdateUntilForTwoLaterBusinesDays() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00").getTime());
        Timestamp secondDate = new Timestamp(timestampFormat.parse("2002-12-07 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface second = findTinyBalanceForBusinessDate(balanceId, secondDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertEquals(originalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            second.setQuantityUntil(17.5, until);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testSetOnMultipleObjects()
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {

            TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.acmapCode().eq("A")
                                .and(TinyBalanceFinder.businessDate().eq(businessDate)));
            for(int i=0;i<list.size();i++)
            {
                list.getTinyBalanceAt(i).setQuantity(100);
            }
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
    }

    public void testIncrementTwiceSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 40;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(2.5);
            tx.executeBufferedOperations();
            tb.incrementQuantity(10);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();
            tb.incrementQuantity(10);

            assertTrue(fromCache.getQuantity() == 22.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 22.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 22.5 + originalQuantity);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 22.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNull(fromCache);

        // test the database:

        this.checker.checkDatedBitemporalTerminated(40);
    }

    public void testInsertThenUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1234;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface tb = build( businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            tb.setQuantity(100);
            tb.setBusinessDateFrom(new Timestamp(1000));
            tb.setBusinessDateTo(this.getInfinite());
            tb.insert();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalanceInterface original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testPurge() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());
        Timestamp futureBusinessDate = new Timestamp(System.currentTimeMillis() + 100000);

        TinyBalanceInterface currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        TinyBalanceInterface pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        TinyBalanceInterface futureBalance = findTinyBalanceForBusinessDate(balanceId, futureBusinessDate);
        assertNotNull(currentBalance);
        assertNotNull(pastBalance);
        assertNotNull(futureBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(futureBalance);
            this.checkQuantityThrowsException(pastBalance);
            this.checkQuantityThrowsException(currentBalance);

            currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
            futureBalance = findTinyBalanceForBusinessDate(balanceId, futureBusinessDate);
            pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);

            assertNull(currentBalance);
            assertNull(futureBalance);
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
        currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        futureBalance = findTinyBalanceForBusinessDate(balanceId, futureBusinessDate);
        pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);

        assertNull(currentBalance);
        assertNull(futureBalance);
        assertNull(pastBalance);

        // test the database:
        assertEquals(0, this.checker.checkDatedBitemporalRowCounts(balanceId));
    }

    public void testPurgeThenInsert() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());

        TinyBalanceInterface currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        TinyBalanceInterface pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(currentBalance);
        assertNotNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            TinyBalanceInterface newBalance = build( pastBusinessDate);
            newBalance.setBalanceId(balanceId);
            newBalance.setAcmapCode("A");
            newBalance.setQuantity(-9999);
            newBalance.insert();

            assertEquals(-9999, pastBalance.getQuantity(), 0);

            currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
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
        currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(currentBalance);
        assertNotNull(pastBalance);
        // test the database:
        assertEquals(1, this.checker.checkDatedBitemporalRowCounts(balanceId));
    }

    public void testPurgeAfterMultipleUpdateInsertOperations() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2004-06-01 00:00:00").getTime());
        Timestamp fartherPastBusinessDate = new Timestamp(timestampFormat.parse("2002-06-01 00:00:00").getTime());

        TinyBalanceInterface currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        TinyBalanceInterface pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        TinyBalanceInterface fartherPastBalance = findTinyBalanceForBusinessDate(balanceId, fartherPastBusinessDate);

        assertNotNull(currentBalance);
        assertNotNull(pastBalance);
        assertNull(fartherPastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            pastBalance.incrementQuantity(1000);

            Timestamp businessDateTo = new Timestamp(timestampFormat.parse("2003-01-01 18:30:00").getTime());
            TinyBalanceInterface newBalance = build( fartherPastBusinessDate);
            newBalance.setBalanceId(balanceId);
            newBalance.setBusinessDateTo(businessDateTo);
            newBalance.setAcmapCode("A");
            newBalance.setQuantity(-9999);
            newBalance.insert();

            newBalance.incrementQuantity(100);

            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));
            this.checkQuantityThrowsException(pastBalance);

            currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
            pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
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
        currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        fartherPastBalance = findTinyBalanceForBusinessDate(balanceId, fartherPastBusinessDate);
        assertNull(currentBalance);
        assertNull(pastBalance);
        assertNull(fartherPastBalance);
        // test the database:
        assertEquals(0, this.checker.checkDatedBitemporalRowCounts(balanceId));
    }

    public void testBatchPurge() throws SQLException, ParseException
    {
        int balanceIdA = 10;
        int balanceIdB = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());

        TinyBalanceInterface balanceA = findTinyBalanceForBusinessDate(balanceIdA, currentBusinessDate);
        TinyBalanceInterface balanceB = findTinyBalanceForBusinessDate(balanceIdB, currentBusinessDate);
        assertNotNull(balanceA);
        assertNotNull(balanceB);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            balanceA.purge();
            balanceB.purge();

            assertTrue(balanceA.zIsParticipatingInTransaction(tx));
            assertTrue(balanceB.zIsParticipatingInTransaction(tx));
            this.checkQuantityThrowsException(balanceA);
            this.checkQuantityThrowsException(balanceB);

            balanceA = findTinyBalanceForBusinessDate(balanceIdA, currentBusinessDate);
            balanceB = findTinyBalanceForBusinessDate(balanceIdB, currentBusinessDate);
            assertNull(balanceA);
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
        balanceA = findTinyBalanceForBusinessDate(balanceIdA, currentBusinessDate);
        balanceB = findTinyBalanceForBusinessDate(balanceIdB, currentBusinessDate);
        assertNull(balanceA);
        assertNull(balanceB);

        // test the database:
        assertEquals(0, this.checker.checkDatedBitemporalRowCounts(balanceIdA));
        assertEquals(0, this.checker.checkDatedBitemporalRowCounts(balanceIdB));
    }

    public void testPurgeThenRollback() throws SQLException, ParseException
    {
        int balanceId = 50;

        Timestamp currentBusinessDate = new Timestamp(System.currentTimeMillis());
        Timestamp pastBusinessDate = new Timestamp(timestampFormat.parse("2003-06-01 00:00:00").getTime());

        TinyBalanceInterface currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        TinyBalanceInterface pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(currentBalance);
        assertNotNull(pastBalance);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            currentBalance.purge();
            assertTrue(currentBalance.zIsParticipatingInTransaction(tx));

            this.checkQuantityThrowsException(pastBalance);

            currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
            pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
            assertNull(currentBalance);
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
        currentBalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        pastBalance = findTinyBalanceForBusinessDate(balanceId, pastBusinessDate);
        assertNotNull(currentBalance);
        assertNotNull(pastBalance);
        // test the database:
        assertEquals(5, this.checker.checkDatedBitemporalRowCounts(balanceId));
    }

    public void testInsertForRecovery() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:31:00").getTime());
        Timestamp queryProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:45:00").getTime());
        Timestamp toProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 06:11:00").getTime());

        Timestamp fromBusinessDate = new Timestamp(timestampFormat.parse("2004-06-01 06:30:00").getTime());
        Timestamp queryBusinessDate = new Timestamp(timestampFormat.parse("2005-06-01 06:30:00").getTime());
        Timestamp toBusinessDate = new Timestamp(timestampFormat.parse("2006-06-01 06:30:00").getTime());

        TinyBalanceInterface testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface recoveredBalance = build( fromBusinessDate, toProcessingDate);
            recoveredBalance.setAcmapCode("A");
            recoveredBalance.setBalanceId(balanceId);
            recoveredBalance.setQuantity(20000);
            recoveredBalance.setProcessingDateFrom(fromProcessingDate);
            recoveredBalance.setProcessingDateTo(toProcessingDate);
            recoveredBalance.setBusinessDateFrom(fromBusinessDate);
            recoveredBalance.setBusinessDateTo(toBusinessDate);
            recoveredBalance.insertForRecovery();

            testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
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
        testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
        assertNotNull(testQuery);

        // test the database:
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 20000, queryBusinessDate, queryProcessingDate);
    }

    public void testInsertForRecoveryMultipleTimes() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:31:00").getTime());
        Timestamp queryProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:45:00").getTime());
        Timestamp toProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 06:11:00").getTime());

        Timestamp fromBusinessDateA = new Timestamp(timestampFormat.parse("2004-06-01 06:30:00").getTime());
        Timestamp queryBusinessDateA = new Timestamp(timestampFormat.parse("2005-06-01 06:30:00").getTime());
        Timestamp toBusinessDateA = new Timestamp(timestampFormat.parse("2006-06-01 06:30:00").getTime());

        Timestamp fromBusinessDateB = new Timestamp(timestampFormat.parse("2006-06-01 06:30:00").getTime());
        Timestamp queryBusinessDateB = new Timestamp(timestampFormat.parse("2006-07-01 06:30:00").getTime());
        Timestamp toBusinessDateB = this.getInfinite();

        TinyBalanceInterface testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDateA, queryProcessingDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface recoveredBalanceA = build( fromBusinessDateA, toProcessingDate);
            recoveredBalanceA.setAcmapCode("A");
            recoveredBalanceA.setBalanceId(balanceId);
            recoveredBalanceA.setQuantity(20000);
            recoveredBalanceA.setProcessingDateFrom(fromProcessingDate);
            recoveredBalanceA.setProcessingDateTo(toProcessingDate);
            recoveredBalanceA.setBusinessDateFrom(fromBusinessDateA);
            recoveredBalanceA.setBusinessDateTo(toBusinessDateA);
            recoveredBalanceA.insertForRecovery();

            TinyBalanceInterface recoveredBalanceB = build( fromBusinessDateB, this.getInfinite());
            recoveredBalanceB.setAcmapCode("A");
            recoveredBalanceB.setBalanceId(balanceId);
            recoveredBalanceB.setQuantity(40000);
            recoveredBalanceB.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
            recoveredBalanceB.setProcessingDateTo(this.getInfinite());
            recoveredBalanceB.setBusinessDateFrom(fromBusinessDateB);
            recoveredBalanceB.setBusinessDateTo(toBusinessDateB);
            recoveredBalanceB.insertForRecovery();

            testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDateA, queryProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDateA, toProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.findTinyBalanceForBusinessDate(balanceId, queryBusinessDateB);
            assertNotNull(testQuery);
            testQuery = this.findTinyBalanceForBusinessDate(balanceId, fromBusinessDateB);
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
        testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDateA, queryProcessingDate);
        assertNotNull(testQuery);
        testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDateA, toProcessingDate);
        assertNotNull(testQuery);
        testQuery = this.findTinyBalanceForBusinessDate(balanceId, queryBusinessDateB);
        assertNotNull(testQuery);
        testQuery = this.findTinyBalanceForBusinessDate(balanceId, fromBusinessDateB);
        assertNotNull(testQuery);

        // test the database:
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 20000, queryBusinessDateA, queryProcessingDate);
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 20000, fromBusinessDateA, toProcessingDate);
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 40000, queryBusinessDateB, this.getInfinite());
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 40000, fromBusinessDateB, this.getInfinite());
    }

    public void testInsertForRecoveryThenInsert() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:31:00").getTime());
        Timestamp queryProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:45:00").getTime());
        Timestamp toProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 06:11:00").getTime());

        Timestamp fromBusinessDateA = new Timestamp(timestampFormat.parse("2004-06-01 06:30:00").getTime());
        Timestamp queryBusinessDateA = new Timestamp(timestampFormat.parse("2005-06-01 06:30:00").getTime());
        Timestamp toBusinessDateA = new Timestamp(timestampFormat.parse("2006-06-01 06:30:00").getTime());

        Timestamp fromBusinessDateB = new Timestamp(timestampFormat.parse("2006-06-01 06:30:00").getTime());

        TinyBalanceInterface testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDateA, queryProcessingDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface recoveredBalanceA = build( fromBusinessDateA, toProcessingDate);
            recoveredBalanceA.setAcmapCode("A");
            recoveredBalanceA.setBalanceId(balanceId);
            recoveredBalanceA.setQuantity(20000);
            recoveredBalanceA.setProcessingDateFrom(fromProcessingDate);
            recoveredBalanceA.setProcessingDateTo(toProcessingDate);
            recoveredBalanceA.setBusinessDateFrom(fromBusinessDateA);
            recoveredBalanceA.setBusinessDateTo(toBusinessDateA);
            recoveredBalanceA.insertForRecovery();

            TinyBalanceInterface recoveredBalanceB = build( fromBusinessDateB);
            recoveredBalanceB.setAcmapCode("A");
            recoveredBalanceB.setBalanceId(balanceId);
            recoveredBalanceB.setQuantity(40000);
            recoveredBalanceB.setProcessingDateFrom(new Timestamp(System.currentTimeMillis()));
            recoveredBalanceB.setProcessingDateTo(this.getInfinite());
            recoveredBalanceB.insert();

            testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDateA, queryProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDateA, toProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.findTinyBalanceForBusinessDate(balanceId, fromBusinessDateB);
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
        testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDateA, queryProcessingDate);
        assertNotNull(testQuery);
        testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDateA, toProcessingDate);
        assertNotNull(testQuery);
        testQuery = this.findTinyBalanceForBusinessDate(balanceId, fromBusinessDateB);
        assertNotNull(testQuery);

        // test the database:
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 20000, queryBusinessDateA, queryProcessingDate);
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 20000, fromBusinessDateA, toProcessingDate);
        this.checker.checkDatedBitemporalTimestampRow(balanceId, 40000, fromBusinessDateB, this.getInfinite());
    }

    public void testInsertForRecoveryThenPurge() throws SQLException, ParseException
    {
        int balanceId = 230;

        Timestamp fromProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:31:00").getTime());
        Timestamp queryProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 05:45:00").getTime());
        Timestamp toProcessingDate = new Timestamp(timestampFormat.parse("2006-06-01 06:11:00").getTime());

        Timestamp fromBusinessDate = new Timestamp(timestampFormat.parse("2004-06-01 06:30:00").getTime());
        Timestamp queryBusinessDate = new Timestamp(timestampFormat.parse("2005-06-01 06:30:00").getTime());
        Timestamp toBusinessDate = new Timestamp(timestampFormat.parse("2006-06-01 06:30:00").getTime());

        TinyBalanceInterface testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
        assertNull(testQuery);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface recoveredBalanceA = build( fromBusinessDate, toProcessingDate);
            recoveredBalanceA.setAcmapCode("A");
            recoveredBalanceA.setBalanceId(balanceId);
            recoveredBalanceA.setQuantity(20000);
            recoveredBalanceA.setProcessingDateFrom(fromProcessingDate);
            recoveredBalanceA.setProcessingDateTo(toProcessingDate);
            recoveredBalanceA.setBusinessDateFrom(fromBusinessDate);
            recoveredBalanceA.setBusinessDateTo(toBusinessDate);
            recoveredBalanceA.insertForRecovery();

            testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
            assertNotNull(testQuery);
            testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDate, toProcessingDate);
            assertNotNull(testQuery);

            recoveredBalanceA.purge();

            testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
            assertNull(testQuery);
            testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDate, toProcessingDate);
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
        testQuery = this.findTinyBalanceByDates(balanceId, queryBusinessDate, queryProcessingDate);
        assertNull(testQuery);
        testQuery = this.findTinyBalanceByDates(balanceId, fromBusinessDate, toProcessingDate);
        assertNull(testQuery);

        // test the database:
        assertEquals(0, this.checker.checkDatedBitemporalRowCounts(balanceId));
    }

    private void checkQuantityThrowsException(TinyBalanceInterface balance)
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

    public void testPurgeAllToTruncate()
    {
        Operation operation = TinyBalanceFinder.acmapCode().eq("A").and(
                TinyBalanceFinder.businessDate().equalsEdgePoint()).and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        TinyBalanceList list = new TinyBalanceList(operation);
        list.purgeAll();
        TinyBalanceList listAfterPurge = new TinyBalanceList(operation);
        listAfterPurge.setBypassCache(true);
        assertEquals(0, listAfterPurge.size());
    }

    public void testMultiInsertCombinatorics() throws Exception
    {
        final Timestamp fromBusinessDate = new Timestamp(timestampFormat.parse("2004-06-01 06:30:00").getTime());
        final Timestamp queryBusinessDate = new Timestamp(timestampFormat.parse("2005-10-01 06:30:00").getTime());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                new TestPositionIncomeExpense(fromBusinessDate,  708842, 1800, 861).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708842, 1800, 812).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 805).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 861).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 830).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 807).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 812).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 804).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1801, 837).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1801, 812).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1800, 861).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1800, 861).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1800, 802).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1800, 802).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1800, 812).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1800, 812).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1801, 861).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 837).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1801, 804).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708842, 1801, 861).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1801, 886).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708842, 1801, 812).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708841, 1801, 802).insert();
                new TestPositionIncomeExpense(fromBusinessDate,  708844, 1801, 802).insert();
                return null;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = TestPositionIncomeExpenseFinder.accountId().eq("71221231");
                op = op.and(TestPositionIncomeExpenseFinder.acmapCode().eq("A"));
                op = op.and(TestPositionIncomeExpenseFinder.businessDate().eq(queryBusinessDate));
                TestPositionIncomeExpenseList list = new TestPositionIncomeExpenseList(op);
                list.setOrderBy(TestPositionIncomeExpenseFinder.balanceType().ascendingOrderBy());
                list.terminateAll();
                return null;
            }
        });

    }

    public String getTinyBalanceSqlInsert()
    {
        return "insert into TINY_BALANCE(BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values " +
                    "(10,12.5,'2006-03-16 00:00:00','9999-12-01 23:59:00.000','2007-03-26 12:19:12.910','9999-12-01 23:59:00.000')";
    }

    protected boolean isPartialCache()
    {
        return TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache();
    }

    public void testTerminatedAndReinsertedWithJdbc() throws Exception
    {
        if (isPartialCache())
        {
            final Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-03-16 00:00:00").getTime());

            final TinyBalanceInterface tb = findTinyBalanceForBusinessDate(10, businessDate);

            assertNotNull(tb);

            // delete the product and it's synonyms
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    tb.terminate();
                    return null;
                }
            });

            // insert the product again - from_z = CURRENT_BUSINESS_DATE
            String sql = "";
            sql += getTinyBalanceSqlInsert();

            Connection con = this.getConnection("A");
            con.createStatement().execute(sql);
            con.close();

            // retrieve again

            TinyBalanceInterface tb2 = findTinyBalanceForBusinessDate(10, businessDate);
            assertEquals(tb2.getBalanceId(), 10);
            assertEquals(tb.getBalanceId(), 10);
        }

    }

    public void testInactivateForArchive() throws SQLException, ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime());
        final int balanceId = 10;
        final TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        final Timestamp inactiveTime = new Timestamp(System.currentTimeMillis() - 100000);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
                fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
                assertNull(fromCache);
                fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
                assertNull(fromCache);
                return null;
            }
        });

        // check the cache:
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis()));
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNotNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(inactiveTime.getTime() - 100));
        assertNotNull(fromCache);
        assertEquals(inactiveTime, fromCache.getProcessingDateTo());
    }

    public void testInactivateForArchiveWithBusinessDate() throws SQLException, ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime());
        final int balanceId = 10;
        final TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        final Timestamp inactiveTime = new Timestamp(System.currentTimeMillis() - 100000);
        final Timestamp inactiveBusinessDate = new Timestamp(System.currentTimeMillis() - 50000);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
                tb.inactivateForArchiving(inactiveTime, inactiveBusinessDate);
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
                fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
                assertNull(fromCache);
                fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
                assertNull(fromCache);
                return null;
            }
        });

        // check the cache:
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNotNull(fromCache);
        clearCache();
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(System.currentTimeMillis()));
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertNotNull(fromCache);
        fromCache = findTinyBalanceForBusinessDateProcessing(balanceId, businessDate, new Timestamp(inactiveTime.getTime() - 100));
        assertNotNull(fromCache);
        assertEquals(inactiveTime, fromCache.getProcessingDateTo());
        assertEquals(inactiveBusinessDate, fromCache.getBusinessDateTo());
    }

    public void testTerminateUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testTerminateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, originalQuantity, until);
    }

    public void testMultiSegmentTerminateUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentTerminateUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentTerminateUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);

        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentTerminateUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00").getTime());
        int balanceId = 10;
        TinyBalanceInterface tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalanceInterface nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceInterface previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceInterface nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNull(fromCache);
            fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceInterface fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, this.getInfinite());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        // test the database:

//        this.checker.checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        this.checker.checkDatedBitemporalInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testCascadeTerminateUntil() throws ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2004-01-01 00:00:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-01 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2006-01-01 00:00:00").getTime());
        int orderId = 1;
        BitemporalOrderInterface order = findOrderForBusinessDate(orderId, businessDate);
        assertNotNull(order);
        assertNotNull(order.getItems().get(0));
        assertNotNull(order.getOrderStatus());
        String originalTrackingId = order.getTrackingId();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            BitemporalOrderInterface fromCache = findOrderForBusinessDate(orderId, until);
            order.cascadeTerminateUntil(until);
            assertTrue(order.zIsParticipatingInTransaction(tx));
            assertNull(findOrderForBusinessDate(orderId, businessDate));
            assertNull(findOrderItemForBusinessDate(orderId, businessDate));
            assertNull(findOrderStatusForBusinessDate(orderId, businessDate));

            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        BitemporalOrderInterface fromCache = findOrderForBusinessDate(orderId, businessDate);
        assertNull(fromCache);
        fromCache = findOrderForBusinessDate(orderId, until);
        assertEquals(originalTrackingId, fromCache.getTrackingId());
        fromCache = findOrderForBusinessDate(orderId, this.getInfinite());
        assertEquals(originalTrackingId, fromCache.getTrackingId());
        fromCache = findOrderForBusinessDate(orderId, segmentStartDate);
        assertEquals(originalTrackingId, fromCache.getTrackingId());
    }

    public void testOutzInTransaction() throws Exception
    {
        final TinyBalanceInterface t = findTinyBalanceByDates(10, new Timestamp(timestampFormat.parse("2005-01-20 00:00:00.0").getTime()),
                new Timestamp(timestampFormat.parse("2005-01-01 00:00:00.0").getTime()));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final TinyBalanceInterface t2 = findTinyBalanceByDates(10, new Timestamp(timestampFormat.parse("2006-01-20 00:00:00.0").getTime()),
                        new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.0").getTime()));
                assertNotNull(t.getBusinessDateFrom());
                return null;
            }
        });
    }

    public void testOutzInTransactionWithInactivation() throws Exception
    {
        final TinyBalanceInterface t = findTinyBalanceByDates(10, new Timestamp(timestampFormat.parse("2005-01-20 00:00:00.0").getTime()),
                new Timestamp(timestampFormat.parse("2006-01-01 00:00:00.0").getTime()));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final TinyBalanceInterface t2 = findTinyBalanceByDates(10, new Timestamp(timestampFormat.parse("2005-01-20 00:00:00.0").getTime()),
                        getInfinite());
                Timestamp nextDay = new Timestamp(timestampFormat.parse("2005-01-21 00:00:00.0").getTime());
                Timestamp originalTo = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
                t2.setQuantityUntil(150, nextDay);
                assertEquals(100, t.getQuantity(), 0.0);
                assertEquals(originalTo, t.getBusinessDateTo());
                assertTrue(t.getProcessingDateTo().getTime() < getInfinite().getTime());
                return null;
            }
        });
    }

    public void testEqualsEdgePoint() throws ParseException
    {
        int id = 10;
        List<TinyBalanceInterface> list = findEqualsEdgePoint(id);
        assertEquals(3, list.size());
        assertEquals(100, list.get(0).getQuantity(), 0);
        assertEquals(100, list.get(1).getQuantity(), 0);
        assertEquals(200, list.get(2).getQuantity(), 0);
        assertEquals(new Timestamp(timestampFormat.parse("2004-01-15 00:30:00.0").getTime()), list.get(0).getProcessingDateFrom());
        assertEquals(new Timestamp(timestampFormat.parse("2005-01-15 00:30:00.0").getTime()), list.get(1).getProcessingDateFrom());
    }

    protected List<TinyBalanceInterface> findEqualsEdgePoint(int id)
    {
        TinyBalanceList list = TinyBalanceFinder.findMany(TinyBalanceFinder.balanceId().eq(id).and(TinyBalanceFinder.acmapCode().eq("A")).and(
                TinyBalanceFinder.processingDate().equalsEdgePoint().and(TinyBalanceFinder.businessDate().equalsEdgePoint())));
        list.setOrderBy(TinyBalanceFinder.processingDateFrom().ascendingOrderBy());
        list.addOrderBy(TinyBalanceFinder.businessDateFrom().ascendingOrderBy());
        return new ArrayList<TinyBalanceInterface>(list);
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

    protected DatedAllTypesInterface buildAllTypes() throws Exception
    {
        return  new DatedAllTypes(new Timestamp(timestampFormat.parse("2005-01-20 00:00:00.0").getTime()));
    }

    public void testDatedAllTypes() throws Exception
    {
        final DatedAllTypesInterface allTypes = buildAllTypes();
        allTypes.setId(10);
        setAllTypes(allTypes, 1);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                allTypes.insert();
                return null;
            }
        });
        final DatedAllTypesInterface allTypes2 = allTypes.getNonPersistentCopy();
        setAllTypes(allTypes2, 2);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                allTypes.copyNonPrimaryKeyAttributesFrom(allTypes2);
                return null;
            }
        });
        assertAllTypes(allTypes, 2);
        final DatedAllTypesInterface allTypes3 = allTypes.getDetachedCopy();
        setAllTypes(allTypes3, 3);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                allTypes3.copyDetachedValuesToOriginalOrInsertIfNew();
                return null;
            }
        });
        assertAllTypes(allTypes, 3);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                allTypes.setNullablePrimitiveAttributesToNull();
                return null;
            }
        });
        assertAllTypesNull(allTypes);
        final DatedAllTypesInterface allTypes4 = allTypes.getNonPersistentCopy();
        setAllTypes(allTypes4, 4);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                allTypes.copyNonPrimaryKeyAttributesUntilFrom(allTypes4, new Timestamp(timestampFormat.parse("2006-01-20 00:00:00.0").getTime()));
                return null;
            }
        });
        assertAllTypes(allTypes, 4);
    }

    private void setAllTypes(DatedAllTypesInterface allTypes, int s)
    {
        allTypes.setBigDecimalValue(BigDecimal.valueOf(s));
        allTypes.setBooleanValue((s % 2) == 0);
        allTypes.setByteValue((byte) s);
        allTypes.setShortValue((short) s);
        allTypes.setCharValue((char) s);
        allTypes.setIntValue(s);
        allTypes.setLongValue(s);
        allTypes.setFloatValue(s);
        allTypes.setDoubleValue(s);
        allTypes.setStringValue("" + s);
        allTypes.setTimestampValue(new Timestamp(s));
        allTypes.setDateValue(new Date(s));
        allTypes.setTimeValue(Time.withMillis(1, 2, 3, 4));
        allTypes.setByteArrayValue(new byte[] { (byte) s });

        allTypes.setNullableByteValue((byte)s);
        allTypes.setNullableShortValue((short)s);
        allTypes.setNullableCharValue((char) s);
        allTypes.setNullableIntValue(s);
        allTypes.setNullableLongValue(s);
        allTypes.setNullableFloatValue(s);
        allTypes.setNullableDoubleValue(s);
        allTypes.setNullableStringValue(""+s);
        allTypes.setNullableTimestampValue(new Timestamp(s));
        allTypes.setNullableDateValue(new Date(s));
        allTypes.setNullableTimeValue(Time.withMillis(1, 2, 3, 4));
        allTypes.setNullableByteArrayValue(new byte[] { (byte) s });
    }

    private void assertAllTypes(DatedAllTypesInterface allTypes, int s)
    {
        assertEquals((s % 2) == 0, allTypes.isBooleanValue());
        assertEquals((byte)s, allTypes.getByteValue());
        assertEquals((short)s, allTypes.getShortValue());
        assertEquals((char)s, allTypes.getCharValue());
        assertEquals(s, allTypes.getIntValue());
        assertEquals(s, allTypes.getLongValue());
        assertEquals(s, allTypes.getFloatValue(), 0.0);
        assertEquals(s, allTypes.getDoubleValue(), 0.0);
        assertEquals(""+s, allTypes.getStringValue());
        assertEquals(new Timestamp(s), allTypes.getTimestampValue());
        assertEquals(new Date(s), allTypes.getDateValue());
        assertTrue(Arrays.equals(new byte[] { (byte) s }, allTypes.getByteArrayValue()));

        assertEquals((byte)s, allTypes.getNullableByteValue());
        assertEquals((short)s, allTypes.getNullableShortValue());
        assertEquals((char)s, allTypes.getNullableCharValue());
        assertEquals(s, allTypes.getNullableIntValue());
        assertEquals(s, allTypes.getNullableLongValue());
        assertEquals(s, allTypes.getNullableFloatValue(), 0.0);
        assertEquals(s, allTypes.getNullableDoubleValue(), 0.0);
        assertEquals(""+s, allTypes.getNullableStringValue());
        assertEquals(new Timestamp(s), allTypes.getNullableTimestampValue());
        assertEquals(new Date(s), allTypes.getNullableDateValue());
        assertTrue(Arrays.equals(new byte[] { (byte) s }, allTypes.getNullableByteArrayValue()));
    }

    private void assertAllTypesNull(DatedAllTypesInterface allTypes)
    {
        assertTrue(allTypes.isNullableByteValueNull());
        assertTrue(allTypes.isNullableShortValueNull());
        assertTrue(allTypes.isNullableCharValueNull());
        assertTrue(allTypes.isNullableIntValueNull());
        assertTrue(allTypes.isNullableLongValueNull());
        assertTrue(allTypes.isNullableFloatValueNull());
        assertTrue(allTypes.isNullableDoubleValueNull());
    }

    public void testMithraBusinessDates() throws InterruptedException, ParseException
    {
        final Timestamp currentBusinessDate = new Timestamp(timestampFormat.parse("2040-12-03 00:00:00").getTime());
        final int balanceId = 987654321;
        TinyBalanceInterface tinybalance = findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertNull(tinybalance);

        TinyBalanceInterface newTinyBalance = build(currentBusinessDate);
        newTinyBalance.setAcmapCode("A");
        newTinyBalance.setBalanceId(balanceId); // Primary Key
        newTinyBalance.setQuantity(100);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        newTinyBalance.insert();
        tx.commit();

        tinybalance =findTinyBalanceForBusinessDate(balanceId, currentBusinessDate);
        assertEquals(balanceId, tinybalance.getBalanceId());

        Thread.sleep(3000);

        final Timestamp badBusinessDate = new Timestamp(timestampFormat.parse("2040-12-05 00:00:00").getTime());

        MithraTransaction tx2 = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tinybalance = findTinyBalanceForBusinessDate(balanceId, badBusinessDate);
        tinybalance.setQuantity(200); // Changed attribute
        tx2.commit();

        Thread.sleep(3000);

        final Timestamp goodBusinessDate = new Timestamp(timestampFormat.parse("2040-12-04 00:00:00").getTime());

        MithraTransaction tx3 = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        tinybalance = findTinyBalanceForBusinessDate(balanceId, goodBusinessDate);
        tinybalance.setQuantity(300); // Changed attribute
        tx3.commit();

        TinyBalanceList tinybalances = TinyBalanceFinder.findMany(TinyBalanceFinder.balanceId().eq(balanceId)
                                    .and(TinyBalanceFinder.businessDate().equalsEdgePoint())
                                    .and(TinyBalanceFinder.processingDate().equalsEdgePoint()).and(TinyBalanceFinder.acmapCode().eq("A")));
        tinybalances.setOrderBy(TinyBalanceFinder.processingDateFrom().ascendingOrderBy()
                            .and(TinyBalanceFinder.processingDateTo().ascendingOrderBy())
                            .and(TinyBalanceFinder.businessDateFrom().ascendingOrderBy())
                            .and(TinyBalanceFinder.businessDateTo().ascendingOrderBy()));

        for (TinyBalance myTinyBalance : tinybalances)
        {
            assertTrue(myTinyBalance.getBusinessDateTo().compareTo(myTinyBalance.getBusinessDateFrom()) > 0);
        }
    }

    public void xtestInPlaceUpdateFollowedByBypassCache() throws SQLException
    {
        String sql = "insert into TINY_BALANCE(BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values " +
                    "(1000,12.5,'2006-03-16 00:00:00','9999-12-01 23:59:00.000','2009-01-01 10:00:00','9999-12-01 23:59:00.000')";
        this.executeStatement(sql);

        Operation baseOp = TinyBalanceFinder.balanceId().eq(1000);
        baseOp = baseOp.and(TinyBalanceFinder.acmapCode().eq("A"));
        //refresh via bypass cache:
        TinyBalanceList list = TinyBalanceFinder.findManyBypassCache(baseOp.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2006-03-16 00:00:00"))));
        assertEquals(1, list.size());

        this.executeStatement("update TINY_BALANCE set POS_QUANTITY_M = 20, IN_Z = '2009-01-01 11:00:00' where BALANCE_ID = 1000");

        //refresh via bypass cache:
        TinyBalanceFinder.findManyBypassCache(baseOp.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2006-03-16 00:00:00")))).forceResolve();


        list = TinyBalanceFinder.findMany(baseOp.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2006-03-16 00:00:00"))));
        assertEquals(1, list.size());
        assertEquals(20.0, list.get(0).getQuantity(), 0.0);
        assertEquals(Timestamp.valueOf("2009-01-01 11:00:00"), list.get(0).getProcessingDateFrom());

        list = TinyBalanceFinder.findMany(baseOp.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2006-03-16 00:00:00")).and(
                TinyBalanceFinder.processingDate().eq(Timestamp.valueOf("2009-01-01 10:30:00")))));
        assertEquals(0, list.size());

        list = TinyBalanceFinder.findMany(baseOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint()).and(TinyBalanceFinder.processingDate().equalsEdgePoint()));
        assertEquals(1, list.size());
        assertEquals(20.0, list.get(0).getQuantity(), 0.0);
        assertEquals(Timestamp.valueOf("2009-01-01 11:00:00"), list.get(0).getProcessingDateFrom());
    }

    public void testPurgeWithRollback() throws ParseException
    {
        final PositionQuantityList list = getList();
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager()
                .startOrContinueTransaction();
        threadTx.setProcessingStartTime(100);
        try
        {
            list.purgeAll();
        }
        finally
        {
            threadTx.rollback();
        }

    }

    public void testPurgeInPast() throws ParseException
    {
        final PositionQuantityList list = getList();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx)
                            throws Throwable
                    {
                        tx.setProcessingStartTime(100);
                        double sum = 0;
                        for (int i = 0; i < list.size(); i++)
                        {
                            sum += list.get(i).getQuantity();
                        }
                        list.purgeAll();
                        return null;
                    }
                });
        PositionQuantityList list2 = getList();
        list2.setBypassCache(true);
        assertEquals(0, list2.size());
    }

    public PositionQuantityList getList()
    {
        Operation genericDateOp = NoOperation.instance();
        genericDateOp = genericDateOp.and(PositionQuantityFinder.businessDate()
                .equalsEdgePoint());
        genericDateOp = genericDateOp.and(PositionQuantityFinder
                .processingDate().equalsEdgePoint());
        final PositionQuantityList list = PositionQuantityFinder
                .findMany(PositionQuantityFinder.acmapCode().eq(SOURCE_A).and(
                        genericDateOp).and(
                        PositionQuantityFinder.accountId().eq("7876412001")
                                .and(
                                PositionQuantityFinder.productId().eq(
                                        1944236))));
        list.forceResolve();
        return list;
    }

    public void testPurgeBadChaining() throws SQLException
    {
        String sql = "insert into TINY_BALANCE(BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values " +
                    "(1,12.5,'2006-03-16 00:00:00','2007-03-16 00:00:00','2009-01-01 10:00:00','9999-12-01 23:59:00.000')";
        this.executeStatement(sql);
        sql = "insert into TINY_BALANCE(BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values " +
                    "(1,12.5,'2006-03-17 00:00:00','2007-03-17 00:00:00','2009-01-01 10:00:00','9999-12-01 23:59:00.000')";
        this.executeStatement(sql);
        TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.acmapCode().eq("A").
                and(TinyBalanceFinder.balanceId().eq(1).and(TinyBalanceFinder.businessDate().equalsEdgePoint().and(TinyBalanceFinder.processingDate().equalsEdgePoint()))));
        list.forceResolve();
        list.purgeAll();
    }

    public void testSetSplitWithOptimisticLocking()
    {
        Timestamp businessDate = Timestamp.valueOf("2005-01-23 18:30:00.0");
        final TinyBalanceInterface bal = findTinyBalanceForBusinessDate(10, businessDate);
        TinyBalanceFinder.clearQueryCache();
        //prime the cache:
        final TinyBalanceInterface bal2 = findTinyBalanceForBusinessDate(10, Timestamp.valueOf("2005-01-28 18:30:00.0"));

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
//                bal2.getQuantity();
                bal.setQuantity(200);
                return null;
            }
        });
    }

    public void testMultiUpdateIncrementTwice()
    {
        final Timestamp businessDate = Timestamp.valueOf("2005-01-23 18:30:00.0");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final TinyBalanceInterface balOne = build(businessDate);
                balOne.setBalanceId(1000);
                balOne.setAcmapCode("A");
                balOne.insert();
                final TinyBalanceInterface balTwo = build(businessDate);
                balTwo.setBalanceId(2000);
                balTwo.setAcmapCode("A");
                balTwo.insert();
                tx.executeBufferedOperations();
                balOne.incrementQuantity(5.0);
                balTwo.incrementQuantity(5.0);
                balOne.incrementQuantity(5.0);
                balTwo.incrementQuantity(5.0);
                return null;
            }
        });
        clearCache();
        assertEquals(10.0, findTinyBalanceForBusinessDate(1000, businessDate).getQuantity(), 0.0);
        assertEquals(10.0, findTinyBalanceForBusinessDate(2000, businessDate).getQuantity(), 0.0);
    }

    public void testTwoUpdatesOnDifferentSources()
    {
        updateTwoSourceBasedObjects("A", "B");
    }

    public void testTwoUpdatesOnSameSource()
    {
        updateTwoSourceBasedObjects("A", "A");
    }

    private void updateTwoSourceBasedObjects(final String acmapCode1, final String acmapCode2)
    {
        final String accountId1 = "A1";
        final String accountId2 = "A2";
        final int productId = 1;
        final int positionType = 1;
        final Timestamp past = Timestamp.valueOf("2005-01-23 18:30:00.0");
        final Timestamp present = Timestamp.valueOf("2005-01-24 18:30:00.0");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final PositionQuantity pastQty1 = createPositionQuantity();
                pastQty1.setAccountId(accountId1);
                pastQty1.setQuantity(10);
                pastQty1.setAcmapCode(acmapCode1);
                pastQty1.insert();
                final PositionQuantity pastQty2 = createPositionQuantity();
                pastQty2.setAccountId(accountId2);
                pastQty2.setQuantity(10);
                pastQty2.setAcmapCode(acmapCode2);
                pastQty2.insert();
                return null;
            }

            private PositionQuantity createPositionQuantity()
            {
                final PositionQuantity positionQuantity = new PositionQuantity(past, InfinityTimestamp.getParaInfinity());
                positionQuantity.setProductId(productId);
                positionQuantity.setPositionType(positionType);
                return positionQuantity;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final PositionQuantity presentQty1 = PositionQuantityFinder.findByPrimaryKey(accountId1, productId, positionType, acmapCode1, present, InfinityTimestamp.getParaInfinity());
                presentQty1.setQuantity(20);
                final PositionQuantity presentQty2 = PositionQuantityFinder.findByPrimaryKey(accountId2, productId, positionType, acmapCode2, present, InfinityTimestamp.getParaInfinity());
                presentQty2.setQuantity(20);
                return null;
            }
        });
        PositionQuantityFinder.clearQueryCache();
        final PositionQuantity pastQty1 = PositionQuantityFinder.findByPrimaryKey(accountId1, productId, positionType, acmapCode1, past, InfinityTimestamp.getParaInfinity());
        assertEquals(10.0, pastQty1.getQuantity(), 0.0);
        final PositionQuantity pastQty2 = PositionQuantityFinder.findByPrimaryKey(accountId2, productId, positionType, acmapCode2, past, InfinityTimestamp.getParaInfinity());
        assertEquals(10.0, pastQty2.getQuantity(), 0.0);
        final PositionQuantity presentQty1 = PositionQuantityFinder.findByPrimaryKey(accountId1, productId, positionType, acmapCode1, present, InfinityTimestamp.getParaInfinity());
        assertEquals(20.0, presentQty1.getQuantity(), 0.0);
        final PositionQuantity presentQty2 = PositionQuantityFinder.findByPrimaryKey(accountId2, productId, positionType, acmapCode2, present, InfinityTimestamp.getParaInfinity());
        assertEquals(20.0, presentQty2.getQuantity(), 0.0);
    }

    public void testIncrementPast()
    {
        final Timestamp past = Timestamp.valueOf("2005-01-23 18:30:00.0");
        final Timestamp present = Timestamp.valueOf("2005-01-24 18:30:00.0");
        final String acmapCode = "A";
        final String accountId = "A1";
        final int productId = 1;

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable {
                final PositionQuantity pastQty1 = createPositionQuantity(past);
                pastQty1.setPositionType(1);
                pastQty1.setQuantity(10);
                pastQty1.insert();
                final PositionQuantity pastQty2 = createPositionQuantity(past);
                pastQty2.setPositionType(2);
                pastQty2.setQuantity(15);
                pastQty2.insert();
                final PositionQuantity presentQty1 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 1, acmapCode, present, InfinityTimestamp.getParaInfinity());
                presentQty1.incrementQuantity(5);
                final PositionQuantity presentQty2 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 2, acmapCode, present, InfinityTimestamp.getParaInfinity());
                presentQty2.incrementQuantity(5);
                return null;
            }

            private PositionQuantity createPositionQuantity(Timestamp businessDate) {
                final PositionQuantity positionQuantity = new PositionQuantity(businessDate, InfinityTimestamp.getParaInfinity());
                positionQuantity.setAcmapCode(acmapCode);
                positionQuantity.setAccountId(accountId);
                positionQuantity.setProductId(productId);
                return positionQuantity;
            }
        });
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable {
                final PositionQuantity pastQty1 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 1, acmapCode, past, InfinityTimestamp.getParaInfinity());
                pastQty1.incrementQuantity(5);
                ParaProduct paraProduct = new ParaProduct();
                paraProduct.setAcmapCode(acmapCode);
                paraProduct.setGsn("1");
                paraProduct.insert();
                final PositionQuantity pastQty2 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 2, acmapCode, past, InfinityTimestamp.getParaInfinity());
                pastQty2.incrementQuantity(5);
                return null;
            }
        });
        PositionQuantityFinder.clearQueryCache();
        final PositionQuantity pastQty1 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 1, acmapCode, past, InfinityTimestamp.getParaInfinity());
        assertEquals(15.0, pastQty1.getQuantity(), 0.0);
        final PositionQuantity pastQty2 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 2, acmapCode, past, InfinityTimestamp.getParaInfinity());
        assertEquals(20.0, pastQty2.getQuantity(), 0.0);
        final PositionQuantity presentQty1 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 1, acmapCode, present, InfinityTimestamp.getParaInfinity());
        assertEquals(20.0, presentQty1.getQuantity(), 0.0);
        final PositionQuantity presentQty2 = PositionQuantityFinder.findByPrimaryKey(accountId, productId, 2, acmapCode, present, InfinityTimestamp.getParaInfinity());
        assertEquals(25.0, presentQty2.getQuantity(), 0.0);
    }

    public void testMultiUpdateIncrementDiffValuesTwice()
    {
        final Timestamp businessDate = Timestamp.valueOf("2005-01-23 18:30:00.0");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final TinyBalanceInterface balOne = build(businessDate);
                balOne.setBalanceId(1000);
                balOne.setAcmapCode("A");
                balOne.insert();
                final TinyBalanceInterface balTwo = build(businessDate);
                balTwo.setBalanceId(2000);
                balTwo.setAcmapCode("A");
                balTwo.insert();
                tx.executeBufferedOperations();
                balOne.incrementQuantity(5.0);
                balTwo.incrementQuantity(6.0);
                balOne.incrementQuantity(5.0);
                balTwo.incrementQuantity(6.0);
                return null;
            }
        });
        clearCache();
        assertEquals(10.0, findTinyBalanceForBusinessDate(1000, businessDate).getQuantity(), 0.0);
        assertEquals(12.0, findTinyBalanceForBusinessDate(2000, businessDate).getQuantity(), 0.0);
    }

    public void testManyUpdates()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            private void updateTinyBalOne(String from, String to)
            {
                Timestamp businessDate = Timestamp.valueOf(from);
                final TinyBalanceInterface bal = findTinyBalanceForBusinessDate(1, businessDate);
                bal.setQuantityUntil(-0.0, Timestamp.valueOf(to));
            }

            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                updateTinyBalOne("2002-12-15 00:00:00.0", "9999-12-01 23:59:00.0");
                updateTinyBalOne("2002-11-29 00:00:00.0", "2002-11-30 00:00:00.0");
                updateTinyBalOne("2002-11-30 00:00:00.0", "2002-12-01 00:00:00.0");
                updateTinyBalOne("2002-12-01 00:00:00.0", "2002-12-02 00:00:00.0");
                updateTinyBalOne("2002-12-02 00:00:00.0", "2002-12-03 00:00:00.0");
                updateTinyBalOne("2002-12-03 00:00:00.0", "2002-12-04 00:00:00.0");
                updateTinyBalOne("2002-12-04 00:00:00.0", "2002-12-05 00:00:00.0");
                updateTinyBalOne("2002-12-05 00:00:00.0", "2002-12-06 00:00:00.0");
                updateTinyBalOne("2002-12-06 00:00:00.0", "2002-12-07 00:00:00.0");
                updateTinyBalOne("2002-12-07 00:00:00.0", "2002-12-08 00:00:00.0");
                updateTinyBalOne("2002-12-08 00:00:00.0", "2002-12-09 00:00:00.0");
                updateTinyBalOne("2002-12-09 00:00:00.0", "2002-12-10 00:00:00.0");
                updateTinyBalOne("2002-12-10 00:00:00.0", "2002-12-11 00:00:00.0");
                updateTinyBalOne("2002-12-11 00:00:00.0", "2002-12-12 00:00:00.0");
                updateTinyBalOne("2002-12-12 00:00:00.0", "2002-12-13 00:00:00.0");
                updateTinyBalOne("2002-12-13 00:00:00.0", "2002-12-14 00:00:00.0");
                updateTinyBalOne("2002-12-14 00:00:00.0", "2002-12-15 00:00:00.0");
                updateTinyBalOne("2002-12-15 00:00:00.0", "2002-12-16 00:00:00.0");
                updateTinyBalOne("2002-12-16 00:00:00.0", "2002-12-17 00:00:00.0");
                updateTinyBalOne("2002-12-17 00:00:00.0", "2002-12-18 00:00:00.0");
                updateTinyBalOne("2002-12-18 00:00:00.0", "2002-12-19 00:00:00.0");
                updateTinyBalOne("2002-12-19 00:00:00.0", "2002-12-20 00:00:00.0");
                return null;
            }
        });
    }

    public void testInactiveObjectOptimisticEnrollment() throws ParseException
    {
        final TinyBalanceInterface inactiveObject = this.findInactiveObject();
        assertEquals(10, inactiveObject.getBalanceId());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                assertEquals(10, inactiveObject.getBalanceId());
                return null;
            }
        });
    }

    public void testInactivateForArchiveLoader() throws Exception
    {
        Timestamp start = new Timestamp(timestampFormat.parse("2011-07-01 00:00:00.000").getTime());
        Timestamp end = new Timestamp(timestampFormat.parse("2011-07-01 12:00:00.000").getTime());

        InactivateForArchivingLoader loader = new InactivateForArchivingLoader(start, end, TinyBalanceFinder.getFinderInstance(), "A", "B");
        loader.startAndWaitUntilFinished();
        Operation op = TinyBalanceFinder.acmapCode().eq("B");
        op = op.and(TinyBalanceFinder.balanceId().greaterThan(8700));
        op = op.and(TinyBalanceFinder.processingDate().eq(TinyBalanceFinder.processingDate().getInfinityDate()));
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());

        assertEquals(0, TestBalanceFinder.findManyBypassCache(op).size());
    }

    public void testMissingMany() throws Exception
    {
        for(int i=0;i<3;i++)
        {
            testMissingMutation();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                @Override
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    retrieveQuantityAsOf(Calendar.getInstance()).purge();
                    return null;
                }
            });
        }
    }

    public void testDeletedState() throws Exception
    {
        final Calendar cal = Calendar.getInstance();
        cal.set(2004, Calendar.SEPTEMBER, 19, 18, 30, 0);

        final PositionQuantity quantityToInsert = new PositionQuantity(new Timestamp(cal.getTimeInMillis()), InfinityTimestamp.getParaInfinity());
        quantityToInsert.setAcmapCode("A");
        quantityToInsert.setAccountId("7777777777");
        quantityToInsert.setProductId(5555);
        quantityToInsert.setPositionType(1800);
        quantityToInsert.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        quantityToInsert.setQuantity(25000);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                quantityToInsert.insert();
                return null;
            }
        });
        cal.set(2010, Calendar.SEPTEMBER, 19, 18, 30, 0);
        final PositionQuantity toPurge = retrieveQuantityAsOf(cal);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                retrieveQuantityAsOf(cal).purge();
                return null;
            }
        });
        assertTrue(toPurge.isDeletedOrMarkForDeletion());
        cal.set(2002, Calendar.SEPTEMBER, 19, 18, 30, 0);

        final PositionQuantity quantityToInsert2 = new PositionQuantity(new Timestamp(cal.getTimeInMillis()), InfinityTimestamp.getParaInfinity());
        quantityToInsert2.setAcmapCode("A");
        quantityToInsert2.setAccountId("7777777777");
        quantityToInsert2.setProductId(5555);
        quantityToInsert2.setPositionType(1800);
        quantityToInsert2.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        quantityToInsert2.setQuantity(28000);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                quantityToInsert2.insert();
                return null;
            }
        });
        cal.set(2010, Calendar.SEPTEMBER, 19, 18, 30, 0);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                PositionQuantity notPurged = retrieveQuantityAsOf(cal);
                assertEquals(28000, notPurged.getQuantity(), 0.001);
                return null;
            }
        }, new TransactionStyle(10000));
        PositionQuantity notPurged = retrieveQuantityAsOf(cal);
        assertEquals(28000, notPurged.getQuantity(), 0.001);

    }

    public void testMissingMutation() throws Exception
    {
        final Calendar cal = Calendar.getInstance();
        cal.set(2004, Calendar.SEPTEMBER, 19, 18, 30, 0);

        //create posQuantity w/quantity 25000 out of tran (as-of 9/19/2004)
        final PositionQuantity quantityToInsert = new PositionQuantity(new Timestamp(cal.getTimeInMillis()), InfinityTimestamp.getParaInfinity());
        quantityToInsert.setAcmapCode("A");
        quantityToInsert.setAccountId("7777777777");
        quantityToInsert.setProductId(5555);
        quantityToInsert.setPositionType(1800);
        //set its busTo non-inf, then to infinity
//        cal.set(2006, Calendar.JULY,  29, 19, 0, 0);
//        quantityToInsert.setBusinessDateTo(new Timestamp(cal.getTimeInMillis()));
        quantityToInsert.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
        quantityToInsert.setQuantity(25000);
        //insert it (plain insert) in a tran
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                quantityToInsert.insert();
                return null;
            }
        });


        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                cal.set(2004, Calendar.SEPTEMBER, 19, 18, 30, 0);
                //retrieve as-of 9/19/2004
                PositionQuantity quantityToMutate = retrieveQuantityAsOf(cal);
                //mutate the quantity to 17000 until 10/30/2009
                cal.set(2009, Calendar.OCTOBER, 30, 18, 30, 0);
                quantityToMutate.setQuantityUntil(17000, new Timestamp(cal.getTimeInMillis()));

                //retrieve as-of 10/31/2009
                cal.set(2009, Calendar.OCTOBER, 31, 18, 30, 0);
                quantityToMutate = retrieveQuantityAsOf(cal);
                //mutate the quantity to 17000 (implicitly to infinity)
                quantityToMutate.setQuantity(17000);
                return null;
            }
        });


        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                //retrieve as-of 9/19/2004
                cal.set(2004, Calendar.SEPTEMBER, 19, 18, 30, 0);
                PositionQuantity quantityToMutate = retrieveQuantityAsOf(cal);

                //mutate quantity to 25000 until 10/30/2009
                cal.set(2009, Calendar.OCTOBER, 30, 18, 30, 0);
                quantityToMutate.setQuantityUntil(25000, new Timestamp(cal.getTimeInMillis()));
                //retrieve as-of 10/30/2009
                cal.set(2009, Calendar.OCTOBER, 30, 18, 30, 0);
                quantityToMutate = retrieveQuantityAsOf(cal);
                //mutate quantity to 33000 until 10/31/2009
                cal.set(2009, Calendar.OCTOBER, 31, 18, 30, 0);
                quantityToMutate.setQuantityUntil(33000, new Timestamp(cal.getTimeInMillis()));
                //retrieve asof 10/31/2009
                cal.set(2009, Calendar.OCTOBER, 31, 18, 30, 0);
                quantityToMutate = retrieveQuantityAsOf(cal);
                //mutate quantity to 25000 (implicitly to infinity)
                quantityToMutate.setQuantity(25000);

                //retrieve as-of 9/19/2004, check quantity is 25000
                cal.set(2004, Calendar.SEPTEMBER, 19, 18, 30, 0);
                PositionQuantity quantityToVerify = retrieveQuantityAsOf(cal);
                assertEquals("quantity is not correct", 25000d, quantityToVerify.getQuantity(), .001);

                //retrieve as-of 12/29/2007, check quantity is 25000 (should fail)
                cal.set(2007, Calendar.DECEMBER, 29, 18, 30, 0);
                quantityToVerify = retrieveQuantityAsOf(cal);
                assertEquals("quantity is not correct", 25000d, quantityToVerify.getQuantity(), .001);
                return null;
            }
        }, new TransactionStyle(10000));

    }

    private PositionQuantity retrieveQuantityAsOf(Calendar cal)
    {
        Operation op = PositionQuantityFinder.acmapCode().eq("A");
        op = op.and(PositionQuantityFinder.accountId().eq("7777777777"));
        op = op.and(PositionQuantityFinder.productId().eq(5555));
        op = op.and(PositionQuantityFinder.businessDate().eq(new Timestamp(cal.getTimeInMillis())));
        return PositionQuantityFinder.findOne(op);
    }

}
