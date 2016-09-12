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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;


public class TestMultiClientNotificationTestCase extends RemoteMithraClientTestCase
{
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
            Contract.class,
            SpecialAccount.class
        };
    }

    public void testInsertNotification()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().eq(999999);
        OrderList orderList = new OrderList(op);
        assertEquals(0, orderList.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteInsertOrder", new Class[]{int.class, boolean.class}, new Object[]{new Integer(999999), new Boolean(true)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList orderList2 = new OrderList(op);
        assertEquals(1, orderList2.size());
    }

    

    public void testInsertNotificationWithSourceAttribute()
    throws Exception
    {
        int updateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().eq(2));
        EmployeeList list0 = new EmployeeList(op);
        assertEquals(0, list0.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteInsertEmployee", new Class[]{int.class, int.class, String.class, boolean.class}, new Object[]{new Integer(0), new Integer(2), "abc@abc.com", new Boolean(true)});
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());
        EmployeeList list1 = new EmployeeList(op);
        assertEquals(1, list1.size());
    }

    public void testNotificationWithMultipleRegistrations()
    throws Exception
    {
        int orderUpdateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int employeeUpdateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op0 = OrderFinder.orderId().greaterThan(0);
        Operation op1 = EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().greaterThan(0));
        OrderList list0 = new OrderList(op0);
        EmployeeList list1 = new EmployeeList(op1);
        int prevSize = list0.size();
        assertEquals(1, list1.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteInsertEmployee", new Class[]{int.class, int.class, String.class, boolean.class}, new Object[]{new Integer(0), new Integer(2), "abc@abc.com", new Boolean(true)});
        this.getRemoteClientVm().executeMethod("remoteInsertOrder", new Class[]{int.class, boolean.class}, new Object[]{new Integer(999999), new Boolean(true)});
        waitForMessages(employeeUpdateClassCount, EmployeeFinder.getMithraObjectPortal());
        waitForMessages(orderUpdateClassCount,OrderFinder.getMithraObjectPortal());
        OrderList list2 = new OrderList(op0);
        EmployeeList list3 = new EmployeeList(op1);
        assertEquals(prevSize + 1, list2.size());
        assertEquals(2, list3.size());
    }

    public void testBatchInsertNotification()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().greaterThanEquals(999900).and(OrderFinder.orderId().lessThanEquals(999999));
        OrderList list0 = new OrderList(op);
        assertEquals(0, list0.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteInsertOrderList", new Class[]{int.class, int.class}, new Object[]{new Integer(999900), new Integer(50)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList list1 = new OrderList(op);
        assertEquals(50, list1.size());
        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteClientVm().executeMethod("remoteInsertOrderList", new Class[]{int.class, int.class}, new Object[]{new Integer(999950), new Integer(50)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList list2 = new OrderList(op);
        assertEquals(100, list2.size());
    }

    public void testBatchInsertingWithMultipleDestinations()
    throws Exception
    {
        int updateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op0 = EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().greaterThanEquals(1));
        Operation op1 = EmployeeFinder.sourceId().eq(1).and(EmployeeFinder.id().greaterThanEquals(1));
        EmployeeList list0 = new EmployeeList(op0);
        assertEquals(1, list0.size());
        EmployeeList list1 = new EmployeeList(op1);
        assertEquals(1, list1.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteInsertEmployeeListWithMultipleDestinations", new Class[]{}, new Object[]{});
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());
        EmployeeList list2 = new EmployeeList(op0);
        EmployeeList list3 = new EmployeeList(op1);
        assertEquals(11, list2.size());
        assertEquals(11, list3.size());
    }

    public void testNotificationRegistrationWithRelationships()
    throws Exception
    {
        Operation op0 = OrderFinder.items().productInfo().productCode().eq("AA");
        OrderList list0 = new OrderList(op0);
        list0.forceResolve();
        waitForRegistrationToComplete();
        List registrations = MithraManagerProvider.getMithraManager().getNotificationEventManager().getNotificationSubscribers();
        assertTrue(registrations.contains(OrderFinder.class.getName()));
        assertTrue(registrations.contains(OrderItemFinder.class.getName()));
        assertTrue(registrations.contains(ProductFinder.class.getName()));
    }

    public void testDeleteNotification()
    throws Exception
    {
        int updateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = EmployeeFinder.sourceId().eq(1).and(EmployeeFinder.id().eq(1));
        EmployeeList list0 = new EmployeeList(op);
        assertEquals(1, list0.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteDeleteEmployee", new Class[]{int.class, int.class, boolean.class}, new Object[]{new Integer(1), new Integer(1), new Boolean(true) });
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());
        EmployeeList list1 = new EmployeeList(op);
        assertEquals(0, list1.size());
    }

    public void testBatchDeleteNotification()
    throws Exception
    {
        int updateClassCount = ExchangeRateFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = ExchangeRateFinder.acmapCode().eq("A").and(ExchangeRateFinder.source().eq(10));
        ExchangeRateList list0 = new ExchangeRateList(op);
        assertEquals(4, list0.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteBatchDeleteAllExchangeRates", new Class[]{String.class}, new Object[]{"A"});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());
        ExchangeRateList list1 = new ExchangeRateList(op);
        assertEquals(0, list1.size());
    }


    public void testNotificationUpdatingSingleAttribute()
    throws Exception
    {
        int updateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op0 = EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().eq(1));
        Employee employee0 = EmployeeFinder.findOne(op0);
        assertEquals(1, employee0.getId());
        assertEquals("john.doe@blah.com", employee0.getEmail());
        String newEmail = "john.perez@blah.com";
        String newName = employee0.getName();
        String newPhone = employee0.getPhone();
        String newDesignation = employee0.getDesignation();
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteUpdateEmployee", new Class[]{int.class, int.class, String.class, String.class, String.class, String.class}, new Object[]{new Integer(0), new Integer(1), newName, newPhone, newDesignation, newEmail});
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());
        Employee employee1 = EmployeeFinder.findOne(op0);
        assertEquals(newEmail, employee1.getEmail());
        assertEquals(newName, employee1.getName());
        assertEquals(newPhone, employee1.getPhone());
        assertEquals(newDesignation, employee1.getDesignation());
    }

    public void testNotificationUpdatingMultipleAttributes()
    throws Exception
    {
        int updateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op0 = EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().eq(1));
        Employee employee0 = EmployeeFinder.findOne(op0);
        assertEquals(1, employee0.getId());
        assertEquals("john.doe@blah.com", employee0.getEmail());
        assertEquals("John Doe", employee0.getName());
        assertEquals("111-111-1111", employee0.getPhone());
        assertEquals("Analyst", employee0.getDesignation());
        String newEmail = "john.perez@blah.com";
        String newName = "John Perez";
        String newPhone = "222-222-2222";
        String newDesignation = "Trader";
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteUpdateEmployee", new Class[]{int.class, int.class, String.class, String.class, String.class, String.class}, new Object[]{new Integer(0), new Integer(1), newName, newPhone, newDesignation, newEmail});
        waitForMessages(updateClassCount, EmployeeFinder.getMithraObjectPortal());
        Employee employee1 = EmployeeFinder.findOne(op0);
        assertEquals(newEmail, employee1.getEmail());
        assertEquals(newName, employee1.getName());
        assertEquals(newPhone, employee1.getPhone());
        assertEquals(newDesignation, employee1.getDesignation());

    }

    public void testUpdateBatchNotification()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        String state = "In-Progress";
        String newState = "Completed";
        Operation op0 = OrderFinder.state().eq(state);
        Operation op1 = OrderFinder.state().eq(newState);
        OrderList list0 = new OrderList(op0);
        assertEquals(4, list0.size());
        OrderList list1 = new OrderList(op1);
        assertEquals(0, list1.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteBatchUpdateOrderState", new Class[]{String.class, String.class}, new Object[]{state, newState});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList list2 = new OrderList(op0);
        assertEquals(0, list2.size());
        OrderList list3 = new OrderList(op1);
        assertEquals(4, list3.size());
    }

    public void testSingleUpdateNotification()
    throws Exception
    {
        int updateClassCount = ExchangeRateFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = ExchangeRateFinder.acmapCode().eq("A").and(ExchangeRateFinder.currency().eq("USD"));
        ExchangeRate rate0 = ExchangeRateFinder.findOne(op);
        assertEquals(1.0, rate0.getExchangeRate(),0);
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteUpdateExchangeRate", new Class[]{String.class, String.class, double.class, boolean.class}, new Object[]{"USD", "A", new Double(2.0), new Boolean(false)});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());
        ExchangeRate rate1 = ExchangeRateFinder.findOne(op);
        assertEquals(2.0, rate1.getExchangeRate(),0);
    }


    public void testMassDelete()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().greaterThan(0);
        OrderList list = new OrderList(op);
        assertTrue(list.size() > 0);
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteDeleteAllOrdersUsingOperation", new Class[]{}, new Object[]{});
        waitForMessages(updateClassCount,OrderFinder.getMithraObjectPortal());
        OrderList list2 = new OrderList(op);
        assertEquals(0, list2.size());
    }

    public void testMassDelete2()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().eq(1);
        OrderList list = new OrderList(op);
        assertEquals(1, list.size());
        waitForRegistrationToComplete();
        this.getRemoteClientVm().executeMethod("remoteDeleteAllOrdersUsingOperation", new Class[]{}, new Object[]{});
        waitForMessages(updateClassCount,OrderFinder.getMithraObjectPortal());
        OrderList list2 = new OrderList(op);
        assertEquals(0, list2.size());
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
        this.getRemoteClientVm().executeMethod("remoteInsertNewTinyBalance", new Class[]{String.class, int.class, Timestamp.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0});
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

        this.getRemoteClientVm().executeMethod("remoteUpdateTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0, new Double(150.00)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
        TinyBalance balance1 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance1.getBalanceId());
        assertEquals(150.00, balance1.getQuantity(),0);
        getLogger().info("balance-0: "+balance0+" | balance-1: "+balance1);
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

        this.getRemoteClientVm().executeMethod("remoteIncrementTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0, new Double(150.00)});
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
        this.getRemoteClientVm().executeMethod("remoteIncrementTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate3, new Double(50)});
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


    public void remoteInsertOrder(int orderId, boolean inTransaction)
    {
        MithraTransaction tx = null;
        if(inTransaction)
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        }
        Order order = new Order();
        order.setOrderId(orderId);
        order.setDescription("TestOrder");
        order.insert();
        if(inTransaction)
        {
            tx.commit();
        }
    }

    public void remoteInsertEmployee(int sourceId, int id, String email,  boolean inTransaction)
    {
        MithraTransaction tx = null;
        if(inTransaction)
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        }
        Employee employee = new Employee();
        employee.setSourceId(sourceId);
        employee.setId(id);
        employee.setEmail(email);
        employee.insert();
        if(inTransaction)
        {
            tx.commit();
        }
    }

    public void remoteInsertOrderList(int initialOrderId, int listSize)
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

    public void remoteInsertEmployeeListWithMultipleDestinations()
    {
        EmployeeList list = new EmployeeList();
        Employee employee = null;
        for(int i = 0; i < 20; i++)
        {
            employee = new Employee();
            employee.setSourceId((i%2 == 0?0:1));
            employee.setId(2+i);
            employee.setEmail("employee"+i+"@abc.com");
            list.add(employee);
        }
        list.insertAll();
    }

    public void remoteIncrementPreviousTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
        tinyBalance.incrementQuantity(newQuantity);
        tx.commit();
    }

    public void remoteDeleteAllOrdersUsingOperation()
    {
        Operation op = OrderFinder.all();
        OrderList list = new OrderList(op);
        list.deleteAll();
    }

    public void remoteInsertOrderItem(int orderItemId, int orderId, boolean inTransaction)
    {
        MithraTransaction tx = null;
        if(inTransaction)
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        }
        OrderItem item = new OrderItem();
        item.setId(orderItemId);
        item.setOrderId(orderId);
        item.insert();

        if(inTransaction)
        {
            tx.commit();
        }
    }

    public void remoteDeleteEmployee(int sourceId, int id, boolean inTransaction)
    {
        MithraTransaction tx = null;
        if(inTransaction)
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        }
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(sourceId).and(EmployeeFinder.id().eq(id)));
        employee.delete();
        if(inTransaction)
        {
            tx.commit();
        }
    }

    public void remoteBatchDeleteAllExchangeRates(String acmapCode)
    {
        ExchangeRateList list = new ExchangeRateList(ExchangeRateFinder.acmapCode().eq(acmapCode).and(ExchangeRateFinder.all()));
        assertTrue(!list.isEmpty());
        list.deleteAll();
    }

    public void remoteUpdateExchangeRate(String currency, String acmapCode, double rate, boolean inTransaction)
    {
        MithraTransaction tx = null;
        if(inTransaction)
        {
            tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        }
        ExchangeRate erate = ExchangeRateFinder.findOne(ExchangeRateFinder.acmapCode().eq(acmapCode).and(ExchangeRateFinder.currency().eq(currency)));
        erate.setExchangeRate(rate);
        if(inTransaction)
        {
            tx.commit();
        }
    }

    public void remoteBatchUpdateOrderState(String state, String newState)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        OrderList list = new OrderList(OrderFinder.state().eq(state));
        for(int i = 0; i < list.size(); i++)
        {
            Order order = (Order) list.get(i);
            order.setState(newState);
        }
        tx.commit();
    }

    public void remoteUpdateEmployee(int acmapCode, int employeeId, String newName, String newPhone, String newDesignation, String newEmail)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(acmapCode).and(EmployeeFinder.id().eq(employeeId)));
        employee.setName(newName);
        employee.setPhone(newPhone);
        employee.setEmail(newEmail);
        employee.setDesignation(newDesignation);
        tx.commit();
    }

   public void remoteInsertNewTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance balance = new TinyBalance(businessDate);
       balance.setAcmapCode(sourceAttribute);
       balance.setBalanceId(balanceId);
       balance.insert();
       tx.commit();
   }

   public void remoteUpdateTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
       tinyBalance.setQuantity(newQuantity);
       tx.commit();
   }

   public void remoteIncrementUntilTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity, Timestamp untilTimestamp)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
       tinyBalance.incrementQuantityUntil(newQuantity, untilTimestamp);
       tx.commit();
   }

   public void remoteIncrementTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
   {
       MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
       TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
       tinyBalance.incrementQuantity(newQuantity);
       tx.commit();
   }

}
