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
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.TinyBalanceUtc;
import com.gs.fw.common.mithra.test.domain.TinyBalanceUtcFinder;
import com.gs.fw.common.mithra.test.domain.TinyBalanceUtcList;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;



public class TestUtcDatedBitemporal extends MithraTestAbstract
{

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TinyBalanceUtc.class            
        };
    }

    public void testTerminateAll() throws SQLException
    {
        TinyBalanceUtcList list = new TinyBalanceUtcList(TinyBalanceUtcFinder.acmapCode().eq("A").and(TinyBalanceUtcFinder.balanceId().greaterThan(1))
            .and(TinyBalanceUtcFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));

        assertTrue(list.size() > 1);

        list.terminateAll();
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID > 1 and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void checkInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE_UTC where BALANCE_ID = ? and " +
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
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public void testInsertInTransaction() throws SQLException
    {
        TinyBalanceUtc tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new TinyBalanceUtc(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertRollback() throws SQLException
    {
        TinyBalanceUtc tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new TinyBalanceUtc(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
        assertNull(fromCache);
        // test the database:

        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID= 2000 and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0,  rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, 12.5, businessDate);
    }

    private TinyBalanceUtc findTinyBalanceUtcForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                            .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceUtcFinder.businessDate().eq(businessDate)));
    }

    public void testUpdateSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, quantity, businessDate);
    }

    public void testUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(17);
            tb.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        TinyBalanceUtc original = findTinyBalanceUtcForBusinessDate(balanceId, originalBusinessDate);;
        assertFalse(12.5 == original.getQuantity());
    }

    public void testIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, 12.5 + originalQuantity, businessDate);
    }

    public void testIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(17);
            tb.incrementQuantity(12.5);
            quantity += 17 + 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        assertTrue(fromCache.getQuantity() == quantity);

        // test the database:
        checkInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            quantity += 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checkInfinityRow(balanceId, quantity, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        TinyBalanceUtc original = findTinyBalanceUtcForBusinessDate(balanceId, originalBusinessDate);;
        assertFalse(12.5 == original.getQuantity());
    }

    public void testTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceUtcFinder.clearQueryCache();
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID = 1 and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis() - 48*3600*1000);
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 50000))));
        assertNotNull(fromCache);
        TinyBalanceUtcFinder.clearQueryCache();
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 50000))));
        assertNotNull(fromCache);
        // test the database:
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID = 1 and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testMultiSegmentUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checkInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiSegmentUpdateDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(priorSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checkInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testMultiSegmentTransactionParticipation() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(priorSegmentQuantity);
            assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(nextSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(priorSegmentBalance, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checkInfinityRow(balanceId, priorSegmentQuantity, businessDate);
    }

    public void testMultiSegmentTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(nextSegmentBusinessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceUtcFinder.clearQueryCache();
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(nextSegmentBusinessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID = 10 and " +
                " OUT_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testMultiSegmentTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceUtcFinder.clearQueryCache();
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID = 10 and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity() , 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testTripleSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-10 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-11 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 20;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, 12.5 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        int balanceId = 40;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        assertNotNull(tb);
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertNull(fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceUtcFinder.clearQueryCache();
        fromCache = TinyBalanceUtcFinder.findOne(TinyBalanceUtcFinder.acmapCode().eq("A")
                .and(TinyBalanceUtcFinder.balanceId().eq(balanceId))
                .and(TinyBalanceUtcFinder.businessDate().eq(businessDate))
                .and(TinyBalanceUtcFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_UTC where BALANCE_ID = 40 and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public void testIncrementUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc reversed = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(reversed.getQuantity() == originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, originalQuantity, until);
    }

    public void testIncrementUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc reversed = findTinyBalanceUtcForBusinessDate(balanceId, until);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(reversed.getQuantity() == originalQuantity);
            assertTrue(previousSegment.getQuantity() == originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, originalQuantity, until);
    }

    public void testMultiSegmentIncrementUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, until);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(10 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.incrementQuantityUntil(2.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, until);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(10 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();

            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.incrementQuantityUntil(2.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testUpdateUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc reversed = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, originalQuantity, until);
    }

    public void testUpdateUntilForLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-12-05 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00.0").getTime());
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc reversed = findTinyBalanceUtcForBusinessDate(balanceId, until);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertEquals(originalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        checkInfinityRow(balanceId, originalQuantity, until);
    }

    public void testMultiSegmentSetUntilSameBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfFirstSegment() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-20 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwice() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.setQuantityUntil(2.5, until);
            assertEquals(2.5, fromCache.getQuantity(), 0.0);
            assertEquals(2.5, tb.getQuantity(), 0.0);
            assertEquals(2.5, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(2.5, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(2.5, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(2.5, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testMultiSegmentSetUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);;
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalanceUtc previousSegment = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        TinyBalanceUtc nonIncrementedNextSegment = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
            assertSame(fromCache, previousSegment);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            tx.executeBufferedOperations();

            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0.0);
            assertEquals(12.5, tb.getQuantity(), 0.0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+2000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5, fromCache.getQuantity(), 0.0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == segmentTwoOriginalQuantity);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5, nextSegmentBalance.getQuantity() , 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        assertSame(fromCache, previousSegment);
        assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, until);
        assertSame(fromCache, nonIncrementedNextSegment);
        assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, segmentTwoOriginalQuantity, until);
    }

    public void testInsertWithIncrementZeroSegments() throws SQLException
    {
        TinyBalanceUtc tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new TinyBalanceUtc(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertWithIncrementOneSegment() throws Exception
    {
        TinyBalanceUtc tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00.0").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        TinyBalanceUtc nextSegment = this.findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new TinyBalanceUtc(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkInfinityRow(balanceId, 12.5 + nextSegmentQuantity, segmentStartDate);
    }

    public void testIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        long now = System.currentTimeMillis();
        Timestamp businessDate = new Timestamp(now - 24*3600*1000);
        Timestamp secondBusinessDate = new Timestamp(now);
        int balanceId = 1;
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc secondTb = findTinyBalanceUtcForBusinessDate(balanceId, secondBusinessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            quantity += 12.5;
            assertEquals(quantity, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, secondBusinessDate);
            assertSame(secondTb, fromCache);
            assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(quantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        checkInfinityRow(balanceId, quantity+45.1, secondBusinessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        TinyBalanceUtc original = findTinyBalanceUtcForBusinessDate(balanceId, originalBusinessDate);;
        assertFalse(12.5 == original.getQuantity());
    }

    public void testMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00.0").getTime());
        Timestamp secondBusinessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00.0").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00.0").getTime());
        int balanceId = 10;
        TinyBalanceUtc priorSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalanceUtc tb = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        TinyBalanceUtc secondTb = findTinyBalanceUtcForBusinessDate(balanceId, secondBusinessDate);
        TinyBalanceUtc nextSegmentBalance = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, secondTb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, secondBusinessDate);
            assertSame(secondTb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentBalance);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentBalance);
        assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

//        checkInfinityRow(1, 12.5 + segmentOneOriginalQuantity, businessDate);
        checkInfinityRow(balanceId, 12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBusinessDate);
    }

    public void testInsertWithIncrementUntilZeroSegments() throws SQLException, ParseException
    {
        TinyBalanceUtc tb = null;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00.0").getTime());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new TinyBalanceUtc(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(2000, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
    }

    public void testInsertWithIncrementUntilOneSegment() throws Exception
    {
        TinyBalanceUtc tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00.0").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00.0").getTime());
        TinyBalanceUtc nextSegment = this.findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            tb = new TinyBalanceUtc(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
            assertSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertNotSame(tb, fromCache);
            fromCache = findTinyBalanceUtcForBusinessDate(balanceId, segmentStartDate);
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
        TinyBalanceUtc fromCache = findTinyBalanceUtcForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceUtcForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(nextSegmentQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        checkInfinityRow(balanceId, nextSegmentQuantity, until);
    }

}
