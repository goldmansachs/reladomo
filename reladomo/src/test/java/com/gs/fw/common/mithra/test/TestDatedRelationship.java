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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.AuditOnlyBalance;
import com.gs.fw.common.mithra.test.domain.AuditOnlyBalanceList;
import com.gs.fw.common.mithra.test.domain.AuditedOrder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItem;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemStatus;
import com.gs.fw.common.mithra.test.domain.AuditedOrderList;
import com.gs.fw.common.mithra.test.domain.AuditedOrderStatus;
import com.gs.fw.common.mithra.test.domain.AuditedOrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItem;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderItemStatus;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderList;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatus;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.DatedEntity;
import com.gs.fw.common.mithra.test.domain.DatedEntityDesc;
import com.gs.fw.common.mithra.test.domain.DatedEntityDescFinder;
import com.gs.fw.common.mithra.test.domain.DatedEntityDescType;
import com.gs.fw.common.mithra.test.domain.DatedEntityFinder;
import com.gs.fw.common.mithra.test.domain.DatedEntityList;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.test.domain.NonAuditedBalance;
import com.gs.fw.common.mithra.test.domain.NonAuditedBalanceList;
import com.gs.fw.common.mithra.test.domain.TamsAccount;
import com.gs.fw.common.mithra.test.domain.TestBalance;
import com.gs.fw.common.mithra.test.domain.TestBalanceFinder;
import com.gs.fw.common.mithra.test.domain.TestBalanceList;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import com.gs.fw.common.mithra.test.domain.TinyBalanceList;
import com.gs.fw.common.mithra.test.domain.dated.DatedTable;
import com.gs.fw.common.mithra.test.domain.dated.DatedTableFinder;
import com.gs.fw.common.mithra.test.tax.Address;
import com.gs.fw.common.mithra.test.tax.Filing;
import com.gs.fw.common.mithra.test.tax.FilingFinder;
import com.gs.fw.common.mithra.test.tax.FilingList;
import com.gs.fw.common.mithra.test.tax.Form;
import com.gs.fw.common.mithra.test.tax.FormAddress;
import com.gs.fw.common.mithra.test.tax.FormRole;
import com.gs.fw.common.mithra.test.tax.Jurisdiction;
import com.gs.fw.common.mithra.test.tax.JurisdictionList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;



public class TestDatedRelationship extends MithraTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TestBalance.class,
            TamsAccount.class,
            DatedTable.class,
            DatedEntityDesc.class,
            DatedEntityDescType.class,
            DatedEntity.class,
            TinyBalance.class,
            NonAuditedBalance.class,
            AuditOnlyBalance.class,
            AuditedOrder.class,
            AuditedOrderItem.class,
            AuditedOrderStatus.class,
            AuditedOrderItemStatus.class,
            BitemporalOrder.class,
            BitemporalOrderStatus.class,
            BitemporalOrderItem.class,
            BitemporalOrderItemStatus.class,
            Filing.class,
            Form.class,
            FormAddress.class,
            Address.class,
            FormRole.class,
            Jurisdiction.class
        };
    }

    public TestDatedRelationship(String s)
    {
        super(s);
    }

    private List createDates()
    {
        ArrayList dates  = new ArrayList();
        Calendar cal = null;

        for (int i=1; i<=10; i++)
        {
            cal = Calendar.getInstance();
            cal.set(2005, Calendar.JANUARY,i,0,0,0);
            cal.set(Calendar.MILLISECOND,0);
            dates.add( new Timestamp(cal.getTime().getTime()));
        }

        return dates;
    }

    public void testNotExistsWithOr()
    {
        Operation or = AuditedOrderFinder.itemStatus().notExists().or(AuditedOrderFinder.itemStatus().status().eq(10));
        assertEquals(6, AuditedOrderFinder.findMany(or).size());
    }

    public void testEqualsEdgePoint()
    {
        TestBalanceList list = new TestBalanceList(TestBalanceFinder.acmapCode().eq(SOURCE_A).and(TestBalanceFinder.businessDate().equalsEdgePoint()));
        list.deepFetch(TestBalanceFinder.tamsAccount());
        for(int i=0;i<list.size();i++)
        {
            TestBalance testBalance = list.getTestBalanceAt(i);
            assertEquals(testBalance.getBusinessDateFrom(), testBalance.getBusinessDate());
        }
    }

    public void testEqualsEdgePointInRelationship()
    {
        TestBalanceList list = new TestBalanceList(TestBalanceFinder.acmapCode().eq(SOURCE_A).and(TestBalanceFinder.tamsAccount().businessDate().equalsEdgePoint()));
        list.deepFetch(TestBalanceFinder.tamsAccount());
        for(int i=0;i<list.size();i++)
        {
            TestBalance testBalance = list.getTestBalanceAt(i);
            assertEquals(testBalance.getTamsAccount().getBusinessDate(), testBalance.getBusinessDate());
            assertEquals(testBalance.getTamsAccount().getBusinessDate(), testBalance.getTamsAccount().getBusinessDateTo());
        }
    }

    public void testEqualsEdgePointInRelationshipOperation()
    {
        TestBalanceList list = new TestBalanceList(TestBalanceFinder.acmapCode().eq(SOURCE_A).and(TestBalanceFinder.tamsAccount().code().eq("76160303")).and(
                TestBalanceFinder.businessDate().equalsEdgePoint()));
        list.deepFetch(TestBalanceFinder.tamsAccount());
        assertTrue(list.size() > 0);
        for(int i=0;i<list.size();i++)
        {
            TestBalance testBalance = list.getTestBalanceAt(i);
            assertEquals(testBalance.getTamsAccount().getBusinessDate(), testBalance.getBusinessDate());
            assertEquals(testBalance.getTamsAccount().getBusinessDate(), testBalance.getTamsAccount().getBusinessDateTo());
        }
    }

    public void testDatedRelationShip()
    throws Exception
    {
        Operation op = DatedEntityFinder.fromZ().in(new HashSet(createDates()));
        DatedEntityFinder.clearQueryCache();
        DatedEntityList list = new DatedEntityList(op);
        list.deepFetch(DatedEntityFinder.datedEntityDesc());
        list.deepFetch(DatedEntityFinder.datedEntityDesc().datedEntityDescType());

        assertTrue(list.size()==10);

        DatedEntity dn = (DatedEntity)list.get(0);
        // todo: ryuy: add some asserts here
//        String testStr = dn.getDatedEntityDesc().getDatedEntityDescType().getType();
    }

    public void testDatedExists() throws ParseException
    {
        Operation op = DatedEntityDescFinder.id().eq(10);
        op = op.and(DatedEntityDescFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2003-05-01 00:00:00").getTime())));
        op = op.and(DatedEntityDescFinder.datedEntityDescType().exists());

        assertNull(DatedEntityDescFinder.findOne(op));
    }

    public void testDatedChainedExists() throws ParseException, IOException
    {
        Operation op = DatedTableFinder.id().eq(10);
        op = op.and(DatedTableFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2003-05-01 00:00:00").getTime())));

        assertNotNull(DatedTableFinder.findOne(op));

        op = op.and(DatedTableFinder.datedEntityDescType().exists());
        assertNull(DatedTableFinder.findOne(op));
    }

    public void testDeepFetchFromBitemporalToAuditOnly() throws ParseException, IOException, SQLException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2003-05-01 00:00:00").getTime());
        TinyBalanceList list = new TinyBalanceList();

        for(int i=0;i < 2000; i++)
        {
            TinyBalance b = new TinyBalance(businessDate);
            b.setBalanceId(5000+i);
            b.setQuantity(i);
            b.setAcmapCode("A");
            list.add(b);
        }
        list.insertAll();

        NonAuditedBalanceList list2 = new NonAuditedBalanceList();

        for(int i=0;i < 2000; i++)
        {
            NonAuditedBalance b = new NonAuditedBalance(businessDate);
            b.setBalanceId(5000+i);
            b.setQuantity(i+1);
            b.setInterest(i);
            b.setAcmapCode("A");
            list2.add(b);
        }
        list2.insertAll();

        AuditOnlyBalanceList list3 = new AuditOnlyBalanceList();

        for(int i=0;i < 2000; i++)
        {
            AuditOnlyBalance b = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            b.setBalanceId(5000+i);
            b.setQuantity(i+1);
            b.setInterest(i);
            b.setAcmapCode("A");
            list3.add(b);
        }
        list3.insertAll();

        Connection con = this.getConnection();
        Statement statement = con.createStatement();
        String sql1 = "CREATE INDEX tinybalidx ON TINY_BALANCE (POS_QUANTITY_M, THRU_Z, OUT_Z)";
        statement.execute(sql1);
        String sql2 = "CREATE INDEX aobalidx ON AUDIT_ONLY_BALANCE (POS_INTEREST_M, OUT_Z)";
        statement.execute(sql2);
        statement.close();
        con.close();

        Operation op = TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(4999));
        op = op.and(TinyBalanceFinder.businessDate().eq(businessDate));
        TinyBalanceList toFind = new TinyBalanceList(op);
//        toFind.deepFetch(TinyBalanceFinder.auditedBalanceByInterest().auditedBalanceByInterest());
        toFind.deepFetch(TinyBalanceFinder.auditedBalanceByInterestForQuantity(5).auditedBalanceByInterestForQuantity(10));
        toFind.deepFetch(TinyBalanceFinder.chainedFiltered(15));
//        toFind.deepFetch(TinyBalanceFinder.nonAuditedBalanceByInterest().auditedBalanceByInterest());
        toFind.forceResolve();
    }

    public void testDeepFetchFromBitemporalToAuditOnlyEqualsEdgePoint() throws ParseException, IOException
    {
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2003-05-01 00:00:00").getTime());
        TinyBalanceList list = new TinyBalanceList();

        for(int i=0;i < 2000; i++)
        {
            TinyBalance b = new TinyBalance(businessDate);
            b.setBalanceId(5000+i);
            b.setQuantity(i);
            b.setAcmapCode("A");
            list.add(b);
        }
        list.insertAll();

        NonAuditedBalanceList list2 = new NonAuditedBalanceList();

        for(int i=0;i < 2000; i++)
        {
            NonAuditedBalance b = new NonAuditedBalance(businessDate);
            b.setBalanceId(5000+i);
            b.setQuantity(i+1);
            b.setInterest(i);
            b.setAcmapCode("A");
            list2.add(b);
        }
        list2.insertAll();

        AuditOnlyBalanceList list3 = new AuditOnlyBalanceList();

        for(int i=0;i < 2000; i++)
        {
            AuditOnlyBalance b = new AuditOnlyBalance(InfinityTimestamp.getParaInfinity());
            b.setBalanceId(5000+i);
            b.setQuantity(i+1);
            b.setInterest(i);
            b.setAcmapCode("A");
            list3.add(b);
        }
        list3.insertAll();

        Operation op = TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThan(4999));
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint().and(TinyBalanceFinder.processingDate().equalsEdgePoint()));
        TinyBalanceList toFind = new TinyBalanceList(op);
//        toFind.deepFetch(TinyBalanceFinder.auditedBalanceByInterest().auditedBalanceByInterest());
        toFind.deepFetch(TinyBalanceFinder.auditedBalanceByInterestForQuantity(5).auditedBalanceByInterestForQuantity(10));
        toFind.deepFetch(TinyBalanceFinder.chainedFiltered(15));
//        toFind.deepFetch(TinyBalanceFinder.nonAuditedBalanceByInterest().auditedBalanceByInterest());
        toFind.forceResolve();
    }

    public void testBitemporalEqualsEdgePoint()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                long now = System.currentTimeMillis();
                for(int i=0;i<2000;i++)
                {
                    Timestamp businessDate = new Timestamp(now +i/10);
                    Timestamp procFrom = new Timestamp(businessDate.getTime() - 1000);
                    BitemporalOrder order = new BitemporalOrder(businessDate, procFrom);
                    order.setOrderId(i+2000);
                    order.setBusinessDateFrom(businessDate);
                    order.setBusinessDateTo(new Timestamp(businessDate.getTime() + 10000));
                    order.setProcessingDateFrom(procFrom);
                    order.setProcessingDateTo(new Timestamp(businessDate.getTime() + 1000));
                    order.insertForRecovery();
                }

                for(int i=0;i<2000;i++)
                {
                    Timestamp businessDate = new Timestamp(now +i/10);
                    Timestamp procFrom = new Timestamp(businessDate.getTime() - 1000);
                    BitemporalOrderItem item = new BitemporalOrderItem(businessDate, procFrom);
                    item.setId(i+3000);
                    item.setOrderId(i+2000);
                    item.setBusinessDateFrom(businessDate);
                    item.setBusinessDateTo(new Timestamp(businessDate.getTime() + 10000));
                    item.setProcessingDateFrom(procFrom);
                    item.setProcessingDateTo(new Timestamp(businessDate.getTime() + 1000));
                    item.insertForRecovery();
                }
                return null;
            }
        });
        Operation op = BitemporalOrderFinder.orderId().greaterThan(1999);
        op = op.and(BitemporalOrderFinder.businessDate().equalsEdgePoint());
        op = op.and(BitemporalOrderFinder.processingDate().equalsEdgePoint());
        BitemporalOrderList list = BitemporalOrderFinder.findMany(op);
        list.deepFetch(BitemporalOrderFinder.items());
        assertEquals(2000, list.size());
        for(BitemporalOrder order: list) assertEquals(1, order.getItems().size());
    }

    public void testDatedToNonDatedViaDeepRelationship()
    {
        Operation op = FilingFinder.businessDate().equalsEdgePoint()
                .and(FilingFinder.businessGroupId().eq(1));
        op = op.and(FilingFinder.form().formRole().formRoleName().eq("aaa"));
        op = op.and(FilingFinder.taxPeriodId().eq(1));
        op = op.and(FilingFinder.extensionEstimatedPaymentNumber().eq(1));
        op = op.and(FilingFinder.attachedTo().form().addresses().exists());
        FilingList filingList = new FilingList(op);
        JurisdictionList jurisdictionList = filingList.getAttachedTos().getForms().getJurisdictions();
        jurisdictionList.forceResolve();
        filingList.getForms().getAddresses().getAddress().forceResolve();
    }


    public void testInSource()
    {
        UnifiedSet set = UnifiedSet.newSetWith("A", "B");
        Operation op = TinyBalanceFinder.acmapCode().in(set);
        op = op.and(TinyBalanceFinder.auditedBalanceByInterest().balanceId().greaterThan(12));
        op = op.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2006-03-16 00:00:00")));
        TinyBalanceFinder.findMany(op).forceResolve();
    }

    public void testInTransactionCachedQuery() throws Exception
    {
        final Timestamp buzDate = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = BitemporalOrderItemFinder.businessDate().eq(buzDate).and(BitemporalOrderItemFinder.state().eq("In-Progress")).
                        and(BitemporalOrderItemFinder.processingDate().eq(BitemporalOrderItemFinder.processingDate().getInfinityDate()));
                BitemporalOrderItemList items = BitemporalOrderItemFinder.findMany(op);
                assertEquals(4, items.size());
                IntHashSet set = new IntHashSet();
                set.add(2);
                set.add(3);
                BitemporalOrderItemList secondTry = BitemporalOrderItemFinder.findMany(op.and(BitemporalOrderItemFinder.order().description().eq("First order")));
                assertEquals(1, secondTry.size());
                return null;
            }
        });
    }

    public void testBitemporalMappedDefaulting() throws Exception
    {
        final Timestamp buzDate = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        Operation op = BitemporalOrderFinder.orderId().eq(1).and(BitemporalOrderFinder.businessDate().eq(buzDate));
        BitemporalOrderList list = BitemporalOrderFinder.findMany(op);
        list.deepFetch(BitemporalOrderFinder.orderStatus());

        list.forceResolve();

        Operation op2 = BitemporalOrderStatusFinder.order().orderId().eq(1).and(BitemporalOrderStatusFinder.order().businessDate().eq(buzDate));
        Operation eqEpOp = BitemporalOrderStatusFinder.processingDate().equalsEdgePoint();
        assertEquals(2, BitemporalOrderStatusFinder.findMany(op2.and(eqEpOp)).size());
    }

    public void testTemporalMappedDefaulting() throws Exception
    {
        final Timestamp buzDate = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        Operation op = AuditedOrderFinder.orderId().eq(1);
        AuditedOrderList list = AuditedOrderFinder.findMany(op);
        list.deepFetch(AuditedOrderFinder.orderStatus());

        list.forceResolve();

        Operation op2 = AuditedOrderStatusFinder.order().orderId().eq(1);
        Operation eqEpOp = AuditedOrderStatusFinder.processingDate().equalsEdgePoint();
        assertEquals(2, AuditedOrderStatusFinder.findMany(op2.and(eqEpOp)).size());
    }
}
