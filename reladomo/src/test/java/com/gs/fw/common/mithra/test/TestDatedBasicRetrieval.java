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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;



public class TestDatedBasicRetrieval extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TamsAccount.class,
            DatedAccount.class,
            DatedTrial.class,
            DatedPnlGroup.class,
            Account.class,
            StockPrice.class,
            Stock.class,
            Adjustment.class,
            AuditedOrder.class
        };
    }

    public void testTamsAccountRetrieval() throws SQLException
    {
        Timestamp asOf = new Timestamp(System.currentTimeMillis());
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        TamsAccount tamsAccount2 = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertSame(tamsAccount2, tamsAccount);
        asOf = new Timestamp(System.currentTimeMillis() + 10000);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        TamsAccount tamsAccount3 = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertSame(tamsAccount3.zGetCurrentData(), tamsAccount.zGetCurrentData());
        asOf = new Timestamp(System.currentTimeMillis() + 50000);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        TamsAccount tamsAccount4 = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount4);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testAllQuery()
    {
        DatedAccountList list = new DatedAccountList(DatedAccountFinder.all().and(DatedAccountFinder.deskId().eq(SOURCE_A)));
        assertTrue(list.size() > 0);
    }

    public void testTamsAccountInfinityRetrieval()
    {
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(TamsAccountFinder.deskId().eq(SOURCE_A)));
        assertNotNull(tamsAccount);
    }

    public void testTamsAccountOr()
    {
        TamsAccountList list = new TamsAccountList(TamsAccountFinder.deskId().eq(SOURCE_A).and(
                TamsAccountFinder.accountNumber().eq("7410161001").or(TamsAccountFinder.accountNumber().eq("7410162002"))));
        list.forceResolve();
        assertEquals(2,list.size());
    }

    public void testWithInfinityThenWithout()
    {
        Operation op = TamsAccountFinder.deskId().eq(SOURCE_A).and(
                TamsAccountFinder.accountNumber().eq("7410161001"));
        op = op.and(TamsAccountFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        op = op.and(TamsAccountFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));
        TamsAccount account = TamsAccountFinder.findOne(op);
        assertNotNull(account);
        assertNotNull(TamsAccountFinder.findOne(TamsAccountFinder.deskId().eq(SOURCE_A).and(
                TamsAccountFinder.accountNumber().eq("7410161001"))));
    }

    public void testAsOfAttributeEqualsInfinity()
    {
        Operation baseOp = TamsAccountFinder.deskId().eq(SOURCE_A);
        baseOp = baseOp.and(TamsAccountFinder.accountNumber().eq("7616030301"));
        baseOp = baseOp.and(TamsAccountFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        assertEquals("003A", TamsAccountFinder.findOne(baseOp.and(TamsAccountFinder.businessDate().eq(Timestamp.valueOf("2002-01-01 00:00:00.0")))).getTrialId());
        assertEquals("004A", TamsAccountFinder.findOne(baseOp.and(TamsAccountFinder.businessDate().eq(TamsAccountFinder.businessDate().getInfinityDate()))).getTrialId());
        assertEquals("004A", TamsAccountFinder.findOne(baseOp.and(TamsAccountFinder.businessDate().equalsInfinity())).getTrialId());
    }

    public void testDatedAccountInfinityRetreival()
    {
        DatedAccount datedAccount = DatedAccountFinder.findOne(DatedAccountFinder.id().eq(1).and(DatedAccountFinder.deskId().eq("A")));
        assertNotNull(datedAccount);
    }


    public void testDeepFetchOfDatedRelationships()
    {
        DatedAccountFinder.DatedAccountSingleFinder finder = DatedAccountFinder.getFinderInstance();
        DatedAccountList list = new DatedAccountList(finder.id().eq(1).and(finder.deskId().eq("A")));
        list.deepFetch(finder.datedTrial());
        list.deepFetch(finder.datedPnlGroup());
        list.forceResolve();
    }

    public void testDeepFetchManyToOneWithoutSourceId()
    {
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        StockList stocks = new StockList(StockFinder.stockId().eq(1));
        stocks.deepFetch(StockFinder.stockPrice());
        stocks.forceResolve();
        if (StockFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            assertEquals(count , MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
        else
        {
            assertEquals(count + 2, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testDatedFullCache()
    {
        if (StockFinder.getMithraObjectPortal().getCache().isFullCache())
        {
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            StockList stocks = new StockList(StockFinder.stockId().eq(1));
            assertEquals(1, stocks.size());
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testTwoLevelDeepDatedRelationship()
    {
        Adjustment adjustment = AdjustmentFinder.findOne(AdjustmentFinder.id().eq(1).and(AdjustmentFinder.deskId().eq("A")));
        assertNotNull(adjustment);

        assertNotNull(adjustment.getDatedAccount());
        assertNotNull(adjustment.getDatedAccount().getDatedTrial());
        assertNotNull(adjustment.getDatedAccount().getDatedPnlGroup());

        AdjustmentList list = new AdjustmentList(
                AdjustmentFinder.datedAccount().datedTrial().name().eq("001A").and(
                AdjustmentFinder.deskId().eq("A").and(AdjustmentFinder.originalAmount().greaterThan(0))
        ));

        list.forceResolve();
        assertTrue(list.size() >= 1);
    }

    public void testFinderWithRelationshipToDated()
    {
        HashSet codeSet = new HashSet();
        codeSet.add("xyz");
        AccountList ul = new AccountList(AccountFinder.tamsAccount().code().eq("12345").and(AccountFinder.deskId().eq("A")).and(AccountFinder.code().in(codeSet)));
        ul.forceResolve();
    }

    public void testChangingData() throws Exception
    {
        Timestamp asOf = new Timestamp(System.currentTimeMillis());
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        Connection con = this.getConnection(SOURCE_A);
        String sql = "update TAMS_ACCOUNT set PNLGROUP_ID = ? where ACCOUNT_NUMBER = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, "test");
        ps.setString(2, "7410161001");
        int updatedRows = ps.executeUpdate();
        con.close();
        assertEquals(1, updatedRows);
        TamsAccountList tal = new TamsAccountList(TamsAccountFinder.deskId().eq(SOURCE_A).and(TamsAccountFinder.businessDate().eq(asOf)));
        tal.forceResolve();
        tamsAccount = TamsAccountFinder.findOneBypassCache(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        assertEquals("test", tamsAccount.getPnlGroupId());

    }

    public void testSettingBusinessToDate() throws Exception
    {
        long time = System.currentTimeMillis()-10000;
        Timestamp asOf = new Timestamp(time);
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        Connection con = this.getConnection(SOURCE_A);
        String sql = "update TAMS_ACCOUNT set PNLGROUP_ID = ?, THRU_Z = ? where ACCOUNT_NUMBER = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, "test");
        ps.setTimestamp(2, new Timestamp(time+1000));
        ps.setString(3, "7410161001");
        int updatedRows = ps.executeUpdate();
        con.close();
        assertEquals(1, updatedRows);

        tamsAccount = TamsAccountFinder.findOneBypassCache(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        assertEquals("test", tamsAccount.getPnlGroupId());
    }

    public void testSettingProcessingToDate() throws Exception
    {
        long time = System.currentTimeMillis()-10000;
        Timestamp asOf = new Timestamp(time);
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        Connection con = this.getConnection(SOURCE_A);
        String sql = "update TAMS_ACCOUNT set OUT_Z = ? where ACCOUNT_NUMBER = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        ps.setTimestamp(1, now);
        ps.setString(2, "7410161001");
        int updatedRows = ps.executeUpdate();
        assertEquals(1, updatedRows);
        ps.close();

        sql = "insert into TAMS_ACCOUNT (ACCOUNT_NUMBER, ACCT_8_DIG_C,TRIAL_ID,PNLGROUP_ID,FROM_Z,THRU_Z,IN_Z,OUT_Z) values (?, ?, ?, ?, ?, ?, ?, ?)";
        ps = con.prepareStatement(sql);
        ps.setString(1, "7410161001");
        ps.setString(2, "74101610");
        ps.setString(3, "testTrialId");
        ps.setString(4, "testPnlGroupId");
        ps.setTimestamp(5, tamsAccount.getBusinessDateFrom());
        ps.setTimestamp(6, tamsAccount.getBusinessDateTo());
        ps.setTimestamp(7, now);
        ps.setTimestamp(8, tamsAccount.getProcessingDateTo());
        updatedRows = ps.executeUpdate();
        con.close();
        assertEquals(1, updatedRows);

        tamsAccount = TamsAccountFinder.findOneBypassCache(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        assertEquals("testPnlGroupId", tamsAccount.getPnlGroupId());
    }

    private TamsAccount findTamsAccount(String accountNumber, Timestamp businessDate)
    {
        return TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq(accountNumber).and(
            TamsAccountFinder.deskId().eq(SOURCE_A)).and(TamsAccountFinder.businessDate().eq(businessDate)));

    }

    public void testClearDatedReadOnlyCache() throws SQLException
    {
        if (TamsAccountFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            String accountNumber = "7410161001";
            Timestamp asOf = new Timestamp(System.currentTimeMillis());
            TamsAccount tamsAccount = findTamsAccount(accountNumber, asOf);
            String code = tamsAccount.getCode();
            assertNotNull(tamsAccount);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertSame(tamsAccount, findTamsAccount(accountNumber, asOf));
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

            Connection con = this.getConnection(SOURCE_A);
            String sql = "update TAMS_ACCOUNT set ACCT_8_DIG_C = ? where ACCOUNT_NUMBER = ?";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setString(1, "test");
            ps.setString(2, accountNumber);
            int updatedRows = ps.executeUpdate();
            assertEquals(1, updatedRows);
            ps.close();
            con.close();

            TamsAccountFinder.clearQueryCache();
            assertEquals(code, tamsAccount.getCode());
            assertSame(tamsAccount, findTamsAccount(accountNumber, asOf));
            assertEquals("test", tamsAccount.getCode());
            assertTrue(count < MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
        }
    }

    public void testDefaulting() throws Exception
    {
        Operation op = TamsAccountFinder.deskId().eq(SOURCE_A).and(TamsAccountFinder.accountNumber().lessThanEquals("7410162002"));
        Timestamp now = new Timestamp(System.currentTimeMillis());
        op = op.and(TamsAccountFinder.businessDate().eq(now));

        TamsAccountList list = TamsAccountFinder.findMany(op);
        assertTrue(list.size() > 1);
        TamsAccountList list2 = TamsAccountFinder.findMany(op.and(TamsAccountFinder.processingDate().eq(now)));
        assertTrue(list2.size() > 1);
        for(int i=0;i<list2.size();i++)
        {
            assertEquals(now, list2.get(i).getProcessingDate());
        }
    }

    public void testDefaultingNoSource()
    {
        Operation op = AuditedOrderFinder.orderId().lessThan(5);
        AuditedOrderList list = AuditedOrderFinder.findMany(op);
        assertTrue(list.size() > 1);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        AuditedOrderList list2 = AuditedOrderFinder.findMany(op.and(AuditedOrderFinder.processingDate().eq(now)));
        assertTrue(list2.size() > 1);
        for(int i=0;i<list2.size();i++)
        {
            assertEquals(now, list2.get(i).getProcessingDate());
        }
    }
}
