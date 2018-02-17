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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceAbstract;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import com.gs.fw.common.mithra.test.domain.TinyBalanceList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class TestDatedBitemporalOptimisticLocking extends MithraTestAbstract implements TestDatedBitemporalDatabaseChecker
{

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private TestDatedBitemporalDatabaseChecker checker = this;

    public void setChecker(TestDatedBitemporalDatabaseChecker checker)
    {
        this.checker = checker;
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TinyBalance.class
        };
    }

    public void testSetNullAndBack() throws Exception
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = null;
        tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double oldValue = -1;
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            oldValue = tb.getQuantity();
            TinyBalance copy = tb.getNonPersistentCopy();
            copy.setQuantityNull();
            copy.setQuantity(oldValue);
            tb.copyNonPrimaryKeyAttributesFrom((TinyBalanceAbstract)copy);
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
        TinyBalanceFinder.clearQueryCache();
        tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertEquals(oldValue, tb.getQuantity(), 0.0);
        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, oldValue, businessDate);
    }

    private MithraTransaction startOptimisticTransaction()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
        return tx;
    }

    public void testTripleIncrement() throws SQLException, ParseException
    {
        Timestamp pbd = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp lbd = new Timestamp(timestampFormat.parse("2005-01-15 18:30:00").getTime());
        Timestamp cbd = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        TinyBalance tb = null;
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(lbd, InfinityTimestamp.getParaInfinity());
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
            tb = new TinyBalance(pbd, InfinityTimestamp.getParaInfinity());
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, cbd);
        assertEquals(15.0, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(15.0, fromCache.getQuantity(), 0.0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 15, cbd);
    }

    public void testTerminateAll() throws SQLException
    {
        TinyBalanceList list = new TinyBalanceList(TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(1))
            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));

        assertTrue(list.size() > 1);

        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = this.getRetrievalCount();
            list.terminateAll();
            assertEquals(count, this.getRetrievalCount());
            tx.commit();
        }
        catch(Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE where BALANCE_ID > 1 and " +
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

    public void checkDatedBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE where BALANCE_ID = ? and " +
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

    public void testNonPeristentCopy()
    throws Exception
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        TinyBalance tb = null;
        tb = findTinyBalanceForBusinessDate(20, businessDate);
        MithraTransaction tx = startOptimisticTransaction();
        TinyBalance tb2 = tb.getNonPersistentCopy();
        assertEquals(tb.getQuantity(), tb2.getQuantity(),0);
        assertEquals(tb.getAcmapCode(), tb2.getAcmapCode());
        assertEquals(tb.getBalanceId(), tb2.getBalanceId());
        assertEquals(tb.getBusinessDateFrom(), tb2.getBusinessDateFrom());
        assertEquals(tb.getBusinessDateTo(), tb2.getBusinessDateTo());
        assertEquals(tb.getProcessingDateFrom(), tb2.getProcessingDateFrom());
        assertEquals(tb.getProcessingDateTo(), tb2.getProcessingDateTo());

        tb2.setQuantity(400);
        tb.copyNonPrimaryKeyAttributesFrom((TinyBalanceAbstract)tb2);

        assertEquals(tb.getQuantity(), tb2.getQuantity(),0);
        assertEquals(tb.getAcmapCode(), tb2.getAcmapCode());
        assertEquals(tb.getBalanceId(), tb2.getBalanceId());
        tx.commit();

    }

    public void testInsertInTransaction() throws SQLException
    {
        TinyBalance tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertRollback() throws SQLException
    {
        TinyBalance tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertNull(fromCache);
        // test the database:

        this.checker.checkDatedBitemporalTerminated(2000);
    }

    public void testUpdateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);
    }

    private TinyBalance findTinyBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceFinder.businessDate().eq(businessDate)));
    }

    private TinyBalance findInactiveObject() throws ParseException
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(10))
                            .and(TinyBalanceFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime())))
                            .and(TinyBalanceFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2004-05-05 00:00:00").getTime()))));
    }

    public void testUpdateSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);
    }

    public void testUpdateSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(17);
            tb.setQuantity(12.5);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);
    }

    public void testUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalance original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + originalQuantity, businessDate);
    }

    public void testIncrementSameBusinesDayRollback() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.incrementQuantity(12.5);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() == 12.5 + quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementSameBusinesDayTwice() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(17);
            tb.incrementQuantity(12.5);
            quantity += 17 + 12.5;
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        assertTrue(fromCache.getQuantity() == quantity);

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);
    }

    public void testIncrementLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.incrementQuantity(12.5);
            quantity += 12.5;
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() == quantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == quantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertTrue(fromCache.getQuantity() == quantity);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalance original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testTerminateSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceFinder.clearQueryCache();
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(1);
    }

    public void testTerminateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis() - 48*3600*1000);
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 50000))));
        assertNotNull(fromCache);
        TinyBalanceFinder.clearQueryCache();
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 50000))));
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
        ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(3, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        findInactiveObject();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = this.getRetrievalCount();
            findInactiveObject();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() != 0);
            assertTrue(nextSegmentBalance.getQuantity() != 0);
            assertTrue(priorSegmentBalance.getQuantity() != 0);
            assertEquals(count, this.getRetrievalCount());
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
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.setQuantity(priorSegmentQuantity);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == priorSegmentQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == priorSegmentQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(nextSegmentBusinessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceFinder.clearQueryCache();
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(nextSegmentBusinessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
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
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double priorSegmentQuantity = priorSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceFinder.clearQueryCache();
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(10);
    }

    public void testMultiSegmentIncrementSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity() , 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        TinyBalance inactive = findInactiveObject();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertSame(inactive, findInactiveObject());
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
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
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.incrementQuantity(12.5);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + segmentOneOriginalQuantity);
            assertTrue(nextSegmentBalance.getQuantity() == 12.5 + segmentTwoOriginalQuantity);
            assertTrue(priorSegmentBalance.getQuantity() == segmentOneOriginalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
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

    public void testTerminateSameBusinesDayForAlreadyTerminated() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 40;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNull(fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        TinyBalanceFinder.clearQueryCache();
        fromCache = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                .and(TinyBalanceFinder.businessDate().eq(businessDate))
                .and(TinyBalanceFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 5000))));
        assertNotNull(fromCache);
        // test the database:
        this.checker.checkDatedBitemporalTerminated(40);
    }

    public void testIncrementUntilSameBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-01-02 00:00:00").getTime());
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance reversed = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.incrementQuantityUntil(12.5, until);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(reversed.getQuantity() == originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(reversed.getQuantity() == originalQuantity);
            assertTrue(previousSegment.getQuantity() == originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertTrue(fromCache.getQuantity() == originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        TinyBalance inactive = findInactiveObject();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertSame(inactive, findInactiveObject());
            tb.incrementQuantityUntil(12.5, nextSegmentBusinessDate);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(12.5, until);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, until);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(10 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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

    public void testMultiSegmentIncrementUntilForLaterBusinesDayUntilMiddleOfNextSegmentTwiceWithFlush() throws SQLException, ParseException
    {
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2005-01-30 18:30:00").getTime());
        int balanceId = 10;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantityUntil(10, until);
            assertEquals(10 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
            assertEquals(10 + segmentOneOriginalQuantity, tb.getQuantity(), 0.0);
            assertEquals(10 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance reversed = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.setQuantityUntil(12.5, until);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertEquals(originalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, nextSegmentBusinessDate);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(12.5, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(12.5, until);
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(2.5, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        TinyBalance nonIncrementedNextSegment = findTinyBalanceForBusinessDate(balanceId, until);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantityUntil(10, until);
            assertEquals(10, fromCache.getQuantity(), 0.0);
            assertEquals(10, tb.getQuantity(), 0.0);
            assertEquals(10, nextSegmentBalance.getQuantity(), 0.0);
            assertEquals(segmentOneOriginalQuantity, previousSegment.getQuantity(), 0.0);
            assertEquals(segmentTwoOriginalQuantity, nonIncrementedNextSegment.getQuantity(), 0.0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0.0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = null;
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(2000, 12.5, businessDate);
    }

    public void testInsertWithIncrementOneSegment() throws Exception
    {
        TinyBalance tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalance nextSegment = this.findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrement();
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5 + nextSegmentQuantity, segmentStartDate);
    }

    public void testIncrementLaterBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        long now = System.currentTimeMillis();
        Timestamp businessDate = new Timestamp(now - 24*3600*1000);
        Timestamp secondBusinessDate = new Timestamp(now);
        int balanceId = 1;
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance secondTb = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertNotNull(tb);
        double quantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            quantity += 12.5;
            assertEquals(quantity, fromCache.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(quantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(quantity + 45.1, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checker.checkDatedBitemporalInfinityRow(balanceId, quantity+45.1, secondBusinessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalance original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testMultiSegmentIncrementDifferentBusinesDayOnTwoDays() throws SQLException, ParseException
    {
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp secondBusinessDate = new Timestamp(timestampFormat.parse("2005-01-17 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int balanceId = 10;
        TinyBalance priorSegmentBalance = findTinyBalanceForBusinessDate(balanceId, priorSegmentBusinessDate);
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance secondTb = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        TinyBalance nextSegmentBalance = findTinyBalanceForBusinessDate(balanceId, nextSegmentBusinessDate);
        assertNotNull(tb);
        double segmentOneOriginalQuantity = tb.getQuantity();
        double segmentTwoOriginalQuantity = nextSegmentBalance.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            findInactiveObject();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(12.5);
            secondTb.incrementQuantity(45.1);
            assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
            assertEquals(12.5 + segmentOneOriginalQuantity, tb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, secondTb.getQuantity(), 0);
            assertEquals(12.5 + 45.1 + segmentTwoOriginalQuantity, nextSegmentBalance.getQuantity(), 0);
            assertEquals(segmentOneOriginalQuantity, priorSegmentBalance.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, secondBusinessDate);
        assertSame(secondTb, fromCache);
        assertEquals(12.5 + 45.1 + segmentOneOriginalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        TinyBalance tb = null;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(2000, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);
    }

    public void testInsertWithIncrementUntilOneSegment() throws Exception
    {
        TinyBalance tb = null;
        int balanceId = 1;
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-20 00:00:00").getTime());
        Timestamp segmentStartDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2003-11-20 00:00:00").getTime());
        TinyBalance nextSegment = this.findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        double nextSegmentQuantity = nextSegment.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate, InfinityTimestamp.getParaInfinity());
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            assertEquals(balanceId, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insertWithIncrementUntil(until);
            assertEquals(12.5 + nextSegmentQuantity, nextSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(nextSegmentQuantity, fromCache.getQuantity(), 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:
        this.checker.checkDatedBitemporalInfinityRow(balanceId, nextSegmentQuantity, until);
    }

    public void testInsertThenTerminateLaterBusinessDate() throws SQLException
    {
        TinyBalance tb = null;
        Timestamp terminateDate = new Timestamp(System.currentTimeMillis());
        Timestamp businessDate = new Timestamp(terminateDate.getTime() - 24*60*60000);
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            tb = new TinyBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(2000);
            assertEquals(2000, tb.getBalanceId());
            tb.setQuantity(12.5);
            tb.insert();
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(2000, businessDate);
        assertSame(tb, fromCache);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        TinyBalance second = findTinyBalanceForBusinessDate(balanceId, secondDate);
        TinyBalance reversed = findTinyBalanceForBusinessDate(balanceId, until);
        TinyBalance previousSegment = findTinyBalanceForBusinessDate(balanceId, segmentStartDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            tb.setQuantityUntil(12.5, until);
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            assertEquals(12.5, fromCache.getQuantity(), 0);
            assertEquals(12.5, tb.getQuantity(), 0);
            assertEquals(originalQuantity, reversed.getQuantity(), 0);
            assertEquals(originalQuantity, previousSegment.getQuantity(), 0);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
            second.setQuantityUntil(17.5, until);
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertEquals(12.5, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, until);
        assertEquals(originalQuantity, fromCache.getQuantity(), 0);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
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
        MithraTransaction tx = startOptimisticTransaction();
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
        TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        double originalQuantity = tb.getQuantity();
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.incrementQuantity(2.5);
            tx.executeBufferedOperations();
            tb.incrementQuantity(10);
            assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.getQuantity() == 12.5 + originalQuantity);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertSame(tb, fromCache);
        assertTrue(fromCache.getQuantity() == 12.5 + originalQuantity);
        fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertNull(fromCache);

        // test the database:

        this.checker.checkDatedBitemporalTerminated(40);
    }

    public void testInsertThenUpdateLaterBusinesDay() throws SQLException, ParseException
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        int balanceId = 1234;
        MithraTransaction tx = startOptimisticTransaction();
        try
        {
            TinyBalance tb = new TinyBalance(businessDate);
            tb.setAcmapCode("A");
            tb.setBalanceId(balanceId);
            tb.setQuantity(100);
            tb.setBusinessDateFrom(new Timestamp(1000));
            tb.setBusinessDateTo(InfinityTimestamp.getParaInfinity());
            tb.insert();
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
            tb.setQuantity(12.5);
            assertTrue(fromCache.getQuantity() == 12.5);
            assertTrue(tb.zIsParticipatingInTransaction(tx));
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
        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, InfinityTimestamp.getParaInfinity());
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        // test the database:

        this.checker.checkDatedBitemporalInfinityRow(balanceId, 12.5, businessDate);

        Timestamp originalBusinessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        TinyBalance original = findTinyBalanceForBusinessDate(balanceId, originalBusinessDate);
        assertFalse(12.5 == original.getQuantity());
    }

    public void testInClauseWithBusinessDate() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                IntHashSet set = new IntHashSet();
                for(int i=1;i<=2;i++) set.add(i);
                final Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-03-16 00:00:00").getTime());
                Operation operation = TinyBalanceFinder.acmapCode().eq("A")
                        .and(TinyBalanceFinder.balanceId().in(set))
                        .and(TinyBalanceFinder.businessDate().eq(businessDate));
                TinyBalanceList list = new TinyBalanceList(operation);
                list.forceResolve();
                return null;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                IntHashSet set = new IntHashSet();
                for(int i=1;i<=2;i++) set.add(i);
                final Timestamp businessDate = new Timestamp(timestampFormat.parse("2007-03-16 00:00:00").getTime());
                Operation operation = TinyBalanceFinder.acmapCode().eq("A")
                        .and(TinyBalanceFinder.balanceId().in(set))
                        .and(TinyBalanceFinder.businessDate().eq(businessDate));
                TinyBalanceList list = new TinyBalanceList(operation);
                for(int i=0;i<list.size();i++)
                {
                    list.get(i).setQuantity(i+100);
                }
                return null;
            }
        });
        TinyBalanceFinder.clearQueryCache();
        System.gc();
        Thread.yield();
        System.gc();
        Thread.yield();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                IntHashSet set = new IntHashSet();
                for(int i=1;i<=2;i++) set.add(i);
                final Timestamp businessDate = new Timestamp(timestampFormat.parse("2006-03-16 00:00:00").getTime());
                Operation operation = TinyBalanceFinder.acmapCode().eq("A")
                        .and(TinyBalanceFinder.balanceId().in(set))
                        .and(TinyBalanceFinder.businessDate().eq(businessDate));
                TinyBalanceList list = new TinyBalanceList(operation);
                list.forceResolve();
                for(int i=0;i<list.size();i++)
                {
                    list.get(i).setQuantity(i+100);
                }
                return null;
            }
        });
    }
}
