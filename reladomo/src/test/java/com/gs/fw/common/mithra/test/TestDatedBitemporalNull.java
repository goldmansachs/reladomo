
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

import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantity;
import com.gs.fw.common.mithra.util.NullDataTimestamp;

import java.sql.*;
import java.text.ParseException;
import java.util.List;
import java.util.ArrayList;

public class  TestDatedBitemporalNull extends TestDatedBitemporal implements TestDatedBitemporalDatabaseChecker
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TinyBalanceNull.class,
            TinyBalance.class,
            PositionQuantity.class,
            TinyBalanceWithSmallDateNull.class,
            TestPositionIncomeExpenseNull.class,
            BitemporalOrderNull.class,
            BitemporalOrderItemNull.class,
            BitemporalOrderStatusNull.class,
            DatedAllTypesNull.class
        };
    }

    protected void deleteTiny(Connection con) throws java.sql.SQLException
    {
        con.createStatement().execute("delete from TINY_BALANCE_NULL where BALANCE_ID = 2000");
    }

    public int checkDatedBitemporalRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_NULL where BALANCE_ID = ?";
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


    public void checkDatedBitemporalTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_NULL where BALANCE_ID = ? and " +
                " OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }


    protected DatedAllTypesInterface buildAllTypes() throws Exception
    {
        return  new DatedAllTypesNull(new Timestamp(timestampFormat.parse("2005-01-20 00:00:00.0").getTime()));
    }

    public void checkDatedBitemporalInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE_NULL where BALANCE_ID = ? and " +
                "OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        System.out.println(rs.getDouble(1));
        System.out.println(rs.getTimestamp(2));
        boolean hasMoreResults = rs.next();
        if (hasMoreResults)
        {
            System.out.println(rs.getDouble(1));
            System.out.println(rs.getTimestamp(2));
            while (rs.next())
            {
                System.out.println(rs.getDouble(1));
                System.out.println(rs.getTimestamp(2));
            }
        }
        rs.close();
        ps.close();
        con.close();
        assertEquals(quantity, resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public void testMultiInsertCombinatorics() throws Exception
    {
        final Timestamp fromBusinessDate = new Timestamp(timestampFormat.parse("2004-06-01 06:30:00").getTime());
        final Timestamp queryBusinessDate = new Timestamp(timestampFormat.parse("2005-10-01 06:30:00").getTime());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708842, 1800, 861).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708842, 1800, 812).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 805).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 861).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 830).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 807).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 812).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 804).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1801, 837).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1801, 812).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1800, 861).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1800, 861).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1800, 802).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1800, 802).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1800, 812).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1800, 812).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1801, 861).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 837).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1801, 804).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708842, 1801, 861).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1801, 886).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708842, 1801, 812).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708841, 1801, 802).insert();
                new TestPositionIncomeExpenseNull(fromBusinessDate,  708844, 1801, 802).insert();
                return null;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = TestPositionIncomeExpenseNullFinder.accountId().eq("71221231");
                op = op.and(TestPositionIncomeExpenseNullFinder.acmapCode().eq("A"));
                op = op.and(TestPositionIncomeExpenseNullFinder.businessDate().eq(queryBusinessDate));
                TestPositionIncomeExpenseNullList list = new TestPositionIncomeExpenseNullList(op);
                list.setOrderBy(TestPositionIncomeExpenseNullFinder.balanceType().ascendingOrderBy());
                list.terminateAll();
                return null;
            }
        });

    }

    public void testCascadeInsertUntil() throws ParseException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2000-01-01 00:00:00.0").getTime());
        Timestamp until = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());

        BitemporalOrderNull order = null;
        BitemporalOrderItemNullList orderItemList;
        BitemporalOrderStatusNull orderStatus;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        int orderId = 5;
        try
        {
            order = new BitemporalOrderNull(businessDate, this.getInfinite());
            order.setOrderId(orderId);
            order.setOrderDate(businessDate);
            order.setUserId(2);
            order.setDescription("New order");
            order.setState("In-Progress");
            order.setTrackingId("130");

            BitemporalOrderItemNull orderItem = new BitemporalOrderItemNull(businessDate, this.getInfinite());
            orderItem.setId(orderId);
            orderItem.setOrderId(orderId);
            orderItem.setProductId(orderId);
            orderItem.setQuantity(10);
            orderItem.setOriginalPrice(25);
            orderItem.setDiscountPrice(20);
            orderItem.setState("In-Progress");
            orderItemList = new BitemporalOrderItemNullList();
            orderItemList.add(orderItem);

            orderStatus = new BitemporalOrderStatusNull(businessDate, this.getInfinite());
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

    public void checkDatedBitemporalTimestampRow(int balanceId, double quantity, Timestamp businessDate, Timestamp processingDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from TINY_BALANCE_NULL where BALANCE_ID = ? and " +
                "IN_Z < ? and coalesce(OUT_Z,'9999-12-31 00:00:00.000') >= ? and FROM_Z <= ? and coalesce(THRU_Z,'9999-12-31 00:00:00.000') > ?";
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

    public void testTerminateAll() throws SQLException
    {
        TinyBalanceNullList list = TinyBalanceNullFinder.findMany(TinyBalanceNullFinder.acmapCode().eq("A").and(TinyBalanceNullFinder.balanceId().greaterThan(1))
            .and(TinyBalanceNullFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));

        assertTrue(list.size() > 1);

        list.terminateAll();
        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_NULL where BALANCE_ID > 1 and " +
                "OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
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
        TinyBalanceNullList list = TinyBalanceNullFinder.findMany(TinyBalanceNullFinder.acmapCode().eq("A").and(TinyBalanceNullFinder.balanceId().greaterThan(1))
            .and(TinyBalanceNullFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));

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

        list = TinyBalanceNullFinder.findMany(TinyBalanceNullFinder.acmapCode().eq("A").and(TinyBalanceNullFinder.balanceId().greaterThan(1))
            .and(TinyBalanceNullFinder.businessDate().eq(new Timestamp(System.currentTimeMillis()))));
        assertEquals(0, list.size());

        list = TinyBalanceNullFinder.findMany(TinyBalanceNullFinder.acmapCode().eq("A").and(TinyBalanceNullFinder.balanceId().greaterThan(1))
            .and(TinyBalanceNullFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())))
            .and(TinyBalanceNullFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2002-11-28 00:00:00").getTime()))));
        assertEquals(0, list.size());

        Connection con = this.getConnection();
        String sql = "select count(*) from TINY_BALANCE_NULL where BALANCE_ID > 1 and " +
                "OUT_Z is null and THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void testPurgeAllToTruncate()
    {
        Operation operation = TinyBalanceNullFinder.acmapCode().eq("A").and(
                TinyBalanceNullFinder.businessDate().equalsEdgePoint()).and(TinyBalanceNullFinder.processingDate().equalsEdgePoint());
        TinyBalanceNullList list = TinyBalanceNullFinder.findMany(operation);
        list.purgeAll();
        TinyBalanceNullList listAfterPurge = TinyBalanceNullFinder.findMany(operation);
        listAfterPurge.setBypassCache(true);
        assertEquals(0, listAfterPurge.size());
    }

    public void testSetOnMultipleObjects()
    {
        Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {

            TinyBalanceNullList list = new TinyBalanceNullList(TinyBalanceNullFinder.acmapCode().eq("A")
                                .and(TinyBalanceNullFinder.businessDate().eq(businessDate)));
            for(int i=0;i<list.size();i++)
            {
                list.getTinyBalanceNullAt(i).setQuantity(100);
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

    protected boolean isPartialCache()
    {
        return TinyBalanceNullFinder.getMithraObjectPortal().getCache().isPartialCache();
    }

    protected TinyBalanceInterface findInactiveObject() throws ParseException
    {
        return TinyBalanceNullFinder.findOne(TinyBalanceNullFinder.acmapCode().eq("A")
                            .and(TinyBalanceNullFinder.balanceId().eq(10))
                            .and(TinyBalanceNullFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime())))
                            .and(TinyBalanceNullFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2004-05-05 00:00:00").getTime()))));
    }


    protected TinyBalanceInterface findTinyBalanceForBusinessDateProcessing(int balanceId, Timestamp businessDate, Timestamp timestamp)
    {
        return  TinyBalanceNullFinder.findOne(TinyBalanceNullFinder.acmapCode().eq("A")
                .and(TinyBalanceNullFinder.balanceId().eq(balanceId))
                .and(TinyBalanceNullFinder.businessDate().eq(businessDate))
                .and(TinyBalanceNullFinder.processingDate().eq(timestamp)));

    }

    protected TinyBalanceWithSmallDateInterface findTinyBalanceAsStringByDates(int balanceId, Timestamp businessDate, Timestamp processingDate)
    {
        return TinyBalanceWithSmallDateNullFinder.findOne(TinyBalanceWithSmallDateNullFinder.acmapCode().eq("A")
                            .and(TinyBalanceWithSmallDateNullFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceWithSmallDateNullFinder.businessDate().eq(businessDate))
                            .and(TinyBalanceWithSmallDateNullFinder.processingDate().eq(processingDate)));
    }

    protected BitemporalOrderInterface findOrderForBusinessDate(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderNullFinder.findOne(BitemporalOrderNullFinder.orderId().eq(orderId)
                .and(BitemporalOrderNullFinder.businessDate().eq(businessDate))
                .and(BitemporalOrderNullFinder.processingDate().eq(this.getInfinite())));
    }

    public String getTinyBalanceSqlInsert()
    {
        return "insert into TINY_BALANCE_NULL(BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values " +
                    "(10,12.5,'2006-03-16 00:00:00',null,'2007-03-26 12:19:12.910',null)";
    }

    public Timestamp getInfinite()
    {
        return NullDataTimestamp.getInstance();
    }

    protected Object findOrderItemForBusinessDate(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderItemNullFinder.findOne(BitemporalOrderItemNullFinder.orderId().eq(orderId)
                .and(BitemporalOrderItemNullFinder.businessDate().eq(businessDate))
                .and(BitemporalOrderItemNullFinder.processingDate().eq(this.getInfinite())));
    }

    protected Object findOrderStatusForBusinessDate(int orderId, Timestamp businessDate)
    {
        return BitemporalOrderStatusNullFinder.findOne(BitemporalOrderStatusNullFinder.orderId().eq(orderId)
                .and(BitemporalOrderStatusNullFinder.businessDate().eq(businessDate))
                .and(BitemporalOrderStatusNullFinder.processingDate().eq(this.getInfinite())));
    }

    protected List<TinyBalanceInterface> findEqualsEdgePoint(int id)
    {
        TinyBalanceNullList list = TinyBalanceNullFinder.findMany(TinyBalanceNullFinder.balanceId().eq(id).and(TinyBalanceNullFinder.acmapCode().eq("A")).and(
                TinyBalanceNullFinder.processingDate().equalsEdgePoint().and(TinyBalanceNullFinder.businessDate().equalsEdgePoint())));
        list.setOrderBy(TinyBalanceNullFinder.processingDateFrom().ascendingOrderBy());
        list.addOrderBy(TinyBalanceNullFinder.businessDateFrom().ascendingOrderBy());
        return new ArrayList<TinyBalanceInterface>(list);
    }

    public void testPrimaryKeyGet() throws Exception
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-11-29 00:00:00").getTime());
        int balanceId = 1;
        TinyBalanceInterface tb = null;
        tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        TinyBalanceNull tb2 = TinyBalanceNullFinder.findOne(TinyBalanceNullFinder.acmapCode().eq("A")
                .and(TinyBalanceNullFinder.balanceId().eq(balanceId))
                .and(TinyBalanceNullFinder.businessDate().equalsEdgePoint())
                .and(TinyBalanceNullFinder.processingDate().equalsEdgePoint())
                .and(TinyBalanceNullFinder.businessDateFrom().eq(tb.getBusinessDateFrom()))
                .and(TinyBalanceNullFinder.processingDateFrom().eq(tb.getProcessingDateFrom())));
        assertNotNull(tb2);
    }

    protected void clearCache()
    {
        TinyBalanceNullFinder.clearQueryCache();
    }

    protected TinyBalanceInterface findTinyBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return this.findTinyBalanceByDates(balanceId, businessDate, this.getInfinite());
    }

    protected TinyBalanceInterface findTinyBalanceByDates(int balanceId, Timestamp businessDate, Timestamp processingDate)
    {
        return TinyBalanceNullFinder.findOne(TinyBalanceNullFinder.acmapCode().eq("A")
                            .and(TinyBalanceNullFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceNullFinder.businessDate().eq(businessDate))
                            .and(TinyBalanceNullFinder.processingDate().eq(processingDate)));
    }

    protected TinyBalanceInterface build(Timestamp businessDate, Timestamp processingDate)
    {
        return new TinyBalanceNull(businessDate, processingDate);
    }

    protected TinyBalanceInterface build(Timestamp businessDate)
    {
        return new TinyBalanceNull(businessDate);
    }

    protected void incrementQuantity(TinyBalanceInterface tb, double d)
    {
        TinyBalanceNullFinder.quantity().increment(tb, d);
    }

    public void testMithraBusinessDates() throws InterruptedException, ParseException
    {
        // ignore this test for null infinity
    }

    @Override
    public void testInactivateForArchiveLoader() throws Exception
    {
        // ignore this test for null infinity
    }
}
