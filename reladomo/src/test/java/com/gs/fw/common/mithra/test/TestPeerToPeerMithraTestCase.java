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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;



public class TestPeerToPeerMithraTestCase extends PeerToPeerMithraServerTestCase
{
    public static final Logger logger = LoggerFactory.getLogger(TestPeerToPeerMithraTestCase.class);
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    protected Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderItem.class,
            ExchangeRate.class,
            Employee.class,
            Product.class,
            TinyBalance.class,
            Contract.class
        };
    }

    public void testPeerToPeerInsert()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int orderId = 999999;
        Operation op = OrderFinder.orderId().eq(orderId);
        Order order0 = OrderFinder.findOne(op);
        assertNull(order0);
        this.getRemoteWorkerVm().executeMethod("peerInsertOrder", new Class[]{int.class}, new Object[]{new Integer(orderId)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        Order order1 = OrderFinder.findOne(op);
        assertNotNull(order1);
    }

    public void testBatchInsertNotification()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().greaterThanEquals(990000);
        OrderList list0 = new OrderList(op);
        assertEquals(0, list0.size());
        this.getRemoteWorkerVm().executeMethod("peerInsertOrderList", new Class[]{int.class, int.class}, new Object[]{new Integer(997000), new Integer(1500)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList list1 = new OrderList(op);
        assertEquals(1500, list1.size());
        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteWorkerVm().executeMethod("peerInsertOrderList", new Class[]{int.class, int.class}, new Object[]{new Integer(998500), new Integer(1500)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList list2 = new OrderList(op);
        assertEquals(3000, list2.size());
    }

    public void testInsertNotificationWithCompoundPrimaryKey()
            throws Exception
    {
        int updateClassCount = ExchangeRateFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        Operation op = ExchangeRateFinder.acmapCode().eq("A");
        op = op.and(ExchangeRateFinder.source().eq(11));
        op = op.and(ExchangeRateFinder.currency().eq("USD"));
        op = op.and(ExchangeRateFinder.date().eq(ts));

        ExchangeRateList list0 = new ExchangeRateList(op);
        assertEquals(0, list0.size());
        this.getRemoteWorkerVm().executeMethod("peerInsertExchangeRate", new Class[]{String.class, String.class, int.class, Timestamp.class, double.class}, new Object[]{"A", "USD", new Integer(11), ts, new Double(1.40)});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());

        ExchangeRateList list1 = new ExchangeRateList(op);
        assertEquals(1, list1.size());
    }

    public void testUpdateNotificationWithCompoundPrimaryKey()
            throws Exception
    {
        int updateClassCount = ExchangeRateFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-09-30 18:30:00.0").getTime());
        Operation op = ExchangeRateFinder.acmapCode().eq("A");
        op = op.and(ExchangeRateFinder.source().eq(10));
        op = op.and(ExchangeRateFinder.currency().eq("USD"));
        op = op.and(ExchangeRateFinder.date().eq(ts));

        ExchangeRate exchangeRate0 = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate0);
        assertEquals(1.0, exchangeRate0.getExchangeRate(), 0.0);
        this.getRemoteWorkerVm().executeMethod("peerUpdateExchangeRate", new Class[]{String.class, String.class, int.class, Timestamp.class, double.class}, new Object[]{"A", "USD", new Integer(10), ts, new Double(1.40)});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());
        ExchangeRate exchangeRate1 = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate1);
        assertEquals(1.40, exchangeRate1.getExchangeRate(), 0.0);
    }

    public void testBatchInsertDatedObjects()
            throws Exception
    {
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();

        Operation op = TinyBalanceFinder.acmapCode().eq("A");
        op = op.and(TinyBalanceFinder.balanceId().greaterThan(1000));
        op = op.and(TinyBalanceFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()));
        op = op.and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        TinyBalanceList list = new TinyBalanceList(op);
        assertEquals(0, list.size());
        this.getRemoteWorkerVm().executeMethod("peerInsertTinyBalanceList", new Class[]{String.class, int.class, int.class}, new Object[]{"A", new Integer(1001), new Integer(1250)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());

        TinyBalanceList list1 = new TinyBalanceList(op);
        assertEquals(1250, list1.size());

    }



    public void testDatedObjectInsert()
            throws Exception
    {
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int balanceId = 1235;
        String sourceAttribute = "B";
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2005-12-05 12:00:00.0").getTime());
        Operation op0 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
                  op0 = op0.and(TinyBalanceFinder.balanceId().eq(balanceId));
                  op0 = op0.and(TinyBalanceFinder.businessDate().eq(businessDate0));

        TinyBalanceList list0 = new TinyBalanceList(op0);
        assertEquals(0, list0.size());
        this.getRemoteWorkerVm().executeMethod("serverInsertNewTinyBalance", new Class[]{String.class, int.class, Timestamp.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
    }

//    class com.gs.fw.common.mithra.test.domain.TinyBalance
//    balanceId,quantity,businessDateFrom,businessDateTo,processingDateFrom,processingDateTo
//    1234,100.00,"2005-12-01 18:30:00.0","9999-12-01 23:59:00.0","2005-12-01 19:30:00.0","2005-12-15 18:49:00.0"
//    1234,100.00,"2005-12-01 18:30:00.0","2005-12-15 18:30:00.0","2005-12-15 18:49:00.0","9999-12-01 23:59:00.0"
//    1234,200.00,"2005-12-15 18:30:00.0","9999-12-01 23:59:00.0","2005-12-15 18:30:00.0","9999-12-01 23:59:00.0"

    public void testDatedObjectUpdate()
    throws Exception
    {
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int balanceId = 1234;
        String sourceAttribute = "B";
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2005-12-10 12:00:00.0").getTime());

        Operation op0 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
                  op0 = op0.and(TinyBalanceFinder.balanceId().eq(balanceId));
                  op0 = op0.and(TinyBalanceFinder.businessDate().eq(businessDate0));

        TinyBalanceList list0 = new TinyBalanceList(op0);
        assertEquals(1, list0.size());
        TinyBalance balance0 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance0.getBalanceId());
        assertEquals(100.00, balance0.getQuantity(),0);

        this.getRemoteWorkerVm().executeMethod("serverUpdateTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0, new Double(150.00)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
        TinyBalance balance1 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance1.getBalanceId());
        assertEquals(150.00, balance1.getQuantity(),0);
    }

    public void testDatedObjectIncrement()
    throws Exception
    {
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int balanceId = 1234;
        String sourceAttribute = "B";
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2005-12-10 12:00:00.0").getTime());

        Operation op0 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
                  op0 = op0.and(TinyBalanceFinder.balanceId().eq(balanceId));
                  op0 = op0.and(TinyBalanceFinder.businessDate().eq(businessDate0));

        TinyBalanceList list0 = new TinyBalanceList(op0);
        assertEquals(1, list0.size());
        TinyBalance balance0 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance0.getBalanceId());
        assertEquals(100.00, balance0.getQuantity(),0);

        this.getRemoteWorkerVm().executeMethod("serverIncrementTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0, new Double(150.00)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
        TinyBalance balance1 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance1.getBalanceId());
        assertEquals(250.00, balance1.getQuantity(),0);
    }

    public void testDatedObjectAdjustments()
    throws Exception
    {
        //Insert initial balance for balance id 999 of 100.00 on 1/1/2005
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2005-12-05 12:00:00.0").getTime());
        Timestamp businessDate1 = new Timestamp(timestampFormat.parse("2005-12-12 12:00:00.0").getTime());
        Timestamp businessDate2 = new Timestamp(timestampFormat.parse("2005-12-16 12:00:00.0").getTime());
        String sourceAttribute = "B";
        int balanceId = 1234;
        //Get the balance on 12/05/2005 for balance 1234
        Operation operation0 = TinyBalanceFinder.acmapCode().eq(sourceAttribute)
                                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                                .and(TinyBalanceFinder.businessDate().eq(businessDate0))
                                .and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        TinyBalanceList list0 = new TinyBalanceList(operation0);
        assertEquals(1, list0.size());
        TinyBalance balance0 = (TinyBalance)list0.get(0);
        assertEquals(100.00, balance0.getQuantity(), 0);

        //Get the balance on 12/12/2005 for balance 1234
        Operation operation1 = TinyBalanceFinder.acmapCode().eq(sourceAttribute)
                                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                                .and(TinyBalanceFinder.businessDate().eq(businessDate1))
                                .and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        TinyBalanceList list1 = new TinyBalanceList(operation1);
        assertEquals(1, list1.size());
        TinyBalance balance1 = (TinyBalance)list1.get(0);
        assertEquals(100.00, balance1.getQuantity(), 0);


        //Get the balance on 12/16/2005 for balance 1234
        Operation operation2 = TinyBalanceFinder.acmapCode().eq(sourceAttribute)
                                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                                .and(TinyBalanceFinder.businessDate().eq(businessDate2))
                                .and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        TinyBalanceList list2 = new TinyBalanceList(operation2);
        assertEquals(1, list2.size());
        TinyBalance balance2 = (TinyBalance)list2.get(0);
        assertEquals(200.00, balance2.getQuantity(), 0);

        //Get the balance on currentTime as of 12/16/2005 for balance 1234
        Operation operation3 = TinyBalanceFinder.acmapCode().eq(sourceAttribute)
                                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                                .and(TinyBalanceFinder.businessDate().eq(businessDate2))
                                .and(TinyBalanceFinder.processingDate().eq(currentTime));

        TinyBalanceList list3 = new TinyBalanceList(operation3);
        assertEquals(1, list3.size());
        TinyBalance balance3 = (TinyBalance)list3.get(0);
        assertEquals(200.00, balance3.getQuantity(), 0);

        //Get the current balance
        Operation operation4 = TinyBalanceFinder.acmapCode().eq(sourceAttribute)
                                .and(TinyBalanceFinder.balanceId().eq(balanceId))
                                .and(TinyBalanceFinder.businessDate().eq(InfinityTimestamp.getParaInfinity()))
                                .and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        TinyBalanceList list4 = new TinyBalanceList(operation4);
        assertEquals(1, list4.size());
        TinyBalance balance4 = (TinyBalance)list4.get(0);
        assertEquals(200.00, balance4.getQuantity(), 0);

        //Oooppsss We just found out a trade that was done on 12/10/2005 that increased the balance by 50
        Timestamp businessDate3 = new Timestamp(timestampFormat.parse("2005-12-10 18:30:00.0").getTime());
        this.getRemoteWorkerVm().executeMethod("serverIncrementTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate3, new Double(50)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());

        //Get the balance on 12/05/2005 for balance 1234
        TinyBalanceList list0a = new TinyBalanceList(operation0);
        assertEquals(1, list0a.size());
        TinyBalance balance0a = (TinyBalance)list0.get(0);
        assertEquals(100.00, balance0a.getQuantity(), 0);

        //Get the balance on 12/12/2005 for balance 1234
        TinyBalanceList list1a = new TinyBalanceList(operation1);
        assertEquals(1, list1a.size());
        TinyBalance balance1a = (TinyBalance)list1a.get(0);
        assertEquals(150.00, balance1a.getQuantity(), 0);


        //Get the balance on 12/16/2005 for balance 1234
        TinyBalanceList list2a = new TinyBalanceList(operation2);
        assertEquals(1, list2a.size());
        TinyBalance balance2a = (TinyBalance)list2a.get(0);
        assertEquals(250.00, balance2a.getQuantity(), 0);

        //Get the balance on currentTime as of 12/16/2005 for balance 1234
        TinyBalanceList list3a = new TinyBalanceList(operation3);
        assertEquals(1, list3a.size());
        TinyBalance balance3a = (TinyBalance)list3a.get(0);
        assertEquals(200.00, balance3a.getQuantity(), 0);

        //Get the current balance
        TinyBalanceList list4a = new TinyBalanceList(operation4);
        assertEquals(1, list4a.size());
        TinyBalance balance4a= (TinyBalance)list4a.get(0);
        assertEquals(250.00, balance4a.getQuantity(), 0);

    }


    public void peerInsertOrder(int orderId)
    {
        Order order = new Order();
        order.setNullablePrimitiveAttributesToNull();
        order.setOrderId(orderId);
        order.insert();
        logger.info("Peer Inserted Order with ID "+orderId);
    }

    public void peerIncrementPreviousTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
        tinyBalance.incrementQuantity(newQuantity);
        tx.commit();
    }

    public void peerInsertOrderList(int initialOrderId, int listSize)
    {
        OrderList list = new OrderList();
        Order order = null;
        for (int i = 0; i < listSize; i++)
        {
            order = new Order();
            order.setOrderId(initialOrderId+i);
            order.setDescription("TestOrder");
            list.add(order);
        }
        list.insertAll();
    }

    public void peerInsertNewTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalance balance = new TinyBalance(businessDate);
        balance.setAcmapCode(sourceAttribute);
        balance.setBalanceId(balanceId);
        balance.insert();
        tx.commit();
    }

    public void peerInsertTinyBalanceList(String sourceAttribute, int initialBalanceId, int listSize)
    {
        TinyBalanceList list = new TinyBalanceList();

        for(int i = 0; i < listSize; i++)
        {
            TinyBalance balance = new TinyBalance(new Timestamp(System.currentTimeMillis()));
            balance.setAcmapCode(sourceAttribute);
            balance.setBalanceId(initialBalanceId+i);
            balance.setQuantity(100.00+i);
            list.add(balance);
        }
        list.insertAll();
    }

    public void serverInsertNewTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance balance = new TinyBalance(businessDate);
       balance.setAcmapCode(sourceAttribute);
       balance.setBalanceId(balanceId);
       balance.insert();
       tx.commit();
   }

   public void serverUpdateTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
       tinyBalance.setQuantity(newQuantity);
       tx.commit();
   }

   public void serverIncrementUntilTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity, Timestamp untilTimestamp)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
       tinyBalance.incrementQuantityUntil(newQuantity, untilTimestamp);
       tx.commit();
   }

   public void serverIncrementTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
       tinyBalance.incrementQuantity(newQuantity);
       tx.commit();
   }

    public void peerInsertExchangeRate(String acmapCode, String currency, int source, Timestamp date, double rate)
    {
        ExchangeRate erate = new ExchangeRate();
        erate.setAcmapCode(acmapCode);
        erate.setCurrency(currency);
        erate.setSource(source);
        erate.setDate(date);
        erate.setExchangeRate(rate);
        erate.insert();
    }

    public void peerUpdateExchangeRate(String acmapCode, String currency, int source, Timestamp date, double rate)
    {
        Operation op = ExchangeRateFinder.acmapCode().eq(acmapCode);
        op = op.and(ExchangeRateFinder.currency().eq(currency));
        op = op.and(ExchangeRateFinder.source().eq(source));
        op = op.and(ExchangeRateFinder.date().eq(date));
        ExchangeRate erate = ExchangeRateFinder.findOne(op);
        erate.setExchangeRate(rate);
    }
}
