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

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;



public class TestNotificationMessages extends RemoteMithraNotificationTestCase
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
            FullyCachedTinyBalance.class,
            AuditOnlyBalance.class,
            AuditedOrder.class,
            AuditedOrderItem.class,
            Contract.class,
            Division.class,
            SpecialAccount.class
        };
    }

    private static final Timestamp VERY_OLD_BUSINESS_DATE_FOR_SQL_INSERT = Timestamp.valueOf("2001-01-01 12:00:00.0");
    private static final double VERY_OLD_BALANCE_FOR_SQL_INSERT = -999.9;

//    public void testNotificationRegistrationAfterInsert()
//    throws Exception
//    {
//        String description = "Order 999";
//        String orderState = "In-Progress";
//        String newOrderState = "Completed";
//        String trackingId = "Trackind ID 999";
//        int userId = 999;
//        int orderId = 999;
//        Order order = new Order();
//        order.setDescription(description);
//	    order.setOrderDate(new Timestamp(System.currentTimeMillis()));
//	    order.setOrderId(orderId);
//	    order.setState(orderState);
//	    order.setTrackingId(trackingId);
//	    order.setUserId(userId);
//        order.insert();
//        waitForRegistrationToComplete();
//        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
//        waitForRegistrationToComplete();
//        this.getRemoteSlaveVm().executeMethod("serverUpdateOrder", new Class[]{int.class, String.class, String.class, String.class, int.class},
//                new Object[]{new Integer(orderId), description, newOrderState, trackingId, new Integer(userId) });
//        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
//
//        Operation op = OrderFinder.orderId().eq(orderId);
//        Order order1 = OrderFinder.findOne(op) ;
//        assertEquals(newOrderState, order1.getState());
//    }
//
//    public void testNotificationRegistrationAfterInsertWithSourceAttribute()
//    throws Exception
//    {
//        String currency = "ABC";
//        int source = 11;
//        Timestamp ts = new Timestamp(System.currentTimeMillis());
//        ExchangeRate rate = new ExchangeRate();
//        rate.setAcmapCode("A");
//        rate.setCurrency(currency);
//        rate.setSource(source);
//        rate.setDate(ts);
//        rate.setExchangeRate(1.0);
//        rate.insert();
//        waitForRegistrationToComplete();
//        int updateClassCount = ExchangeRateFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
//
//        waitForRegistrationToComplete();
//        this.getRemoteSlaveVm().executeMethod("serverInsertExchangeRate", new Class[]{String.class, String.class, int.class, Timestamp.class, double.class}, new Object[]{"A", currency, new Integer(source), new Timestamp(System.currentTimeMillis()), new Double(1.40)});
//
//        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());
//
//
//        Operation op = ExchangeRateFinder.acmapCode().eq("A");
//        op = op.and(ExchangeRateFinder.source().eq(source));
//        op = op.and(ExchangeRateFinder.currency().eq(currency));
//
//        ExchangeRateList list  = new ExchangeRateList(op);
//
//        assertEquals(2, list.size());
//
//    }
//
//    public void testNotificationRegistrationAfterInsertList()
//    throws Exception
//    {
//        String description = "Order 999";
//        String orderState = "In-Progress";
//        String newOrderState = "Completed";
//        String trackingId = "Trackind ID 999";
//        int userId = 999;
//        int orderId = 999;
//
//        OrderList list = new OrderList();
//
//        for(int i = 0; i < 1001; i++)
//        {
//            Order order = new Order();
//            order.setDescription(description);
//            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
//            order.setOrderId(orderId + i);
//            order.setState(orderState);
//            order.setTrackingId(trackingId);
//            order.setUserId(userId);
//            list.add(order);
//        }
//        list.insertAll();
//
//        waitForRegistrationToComplete();
//        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
//        waitForRegistrationToComplete();
//        this.getRemoteSlaveVm().executeMethod("serverUpdateOrder", new Class[]{int.class, String.class, String.class, String.class, int.class},
//                new Object[]{new Integer(orderId), description, newOrderState, trackingId, new Integer(userId) });
//        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
//
//        Operation op = OrderFinder.orderId().eq(orderId);
//        Order order1 = OrderFinder.findOne(op) ;
//        assertEquals(newOrderState, order1.getState());
//
//    }
//
//    public void testNotificationRegistrationAfterInsertListWithSourceAttribute()
//    throws Exception
//    {
//        int divisionId = 1001;
//        String newDivisionName = "New Division Name";
//        String newState= "New State";
//        String newCity = "New City";
//
//        DivisionList list = new DivisionList();
//        for(int i = 0; i < 2002; i++)
//        {
//            Division division = new Division();
//            String sourceId = (i % 2)==0?"A":"B";
//            division.setSourceId(sourceId);
//            division.setCity("City");
//            division.setState("State");
//            division.setDivisionName("Division "+i);
//            division.setDivisionId(1000+i);
//            list.add(division);
//        }
//        list.insertAll();
//
//        waitForRegistrationToComplete();
//        int updateClassCount = DivisionFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
//        waitForRegistrationToComplete();
//
//        this.getRemoteSlaveVm().executeMethod("serverUpdateDivision", new Class[]{int.class, String.class, String.class, String.class, String.class},
//                new Object[]{new Integer(divisionId),"B", newDivisionName, newState, newCity });
//        waitForMessages(updateClassCount, DivisionFinder.getMithraObjectPortal());
//
//
//        Operation op = DivisionFinder.divisionId().eq(divisionId).and(DivisionFinder.sourceId().eq("B"));
//        Division division1 = DivisionFinder.findOne(op) ;
//
//        assertEquals(newDivisionName,division1.getDivisionName());
//        assertEquals(newState,division1.getState());
//        assertEquals(newCity,division1.getCity());
//    }
//
//
//    public void testNotificationRegistrationWithNoTransactionalObject()
//    throws Exception
//    {
//        Operation op = ContractFinder.quantity().greaterThan(1000000.00);
//        ContractList contractList = new ContractList(op);
//        contractList.setBypassCache(true);
//        assertTrue(contractList.size() > 0);
//        waitForRegistrationToComplete();
//        List subscribers = MithraManagerProvider.getMithraManager().getNotificationEventManager().getNotificationSubscribers();
//        assertTrue(subscribers.contains(ContractFinder.class.getName()));
//
//    }

    public void testInsertNotification()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().eq(999999);
        OrderList orderList = new OrderList(op);
        assertEquals(0, orderList.size());
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertOrder", new Class[]{int.class}, new Object[]{new Integer(999999)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList orderList2 = new OrderList(op);
        assertEquals(1, orderList2.size());
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
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertExchangeRate", new Class[]{String.class, String.class, int.class, Timestamp.class, double.class}, new Object[]{"A", "USD", new Integer(11), ts, new Double(1.40)});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());

        ExchangeRateList list1 = new ExchangeRateList(op);
        assertEquals(1, list1.size());
    }

    //"USD",10, "2004-09-30 18:30:00.0", 1
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
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverUpdateExchangeRate", new Class[]{String.class, String.class, int.class, Timestamp.class, double.class}, new Object[]{"A", "USD", new Integer(10), ts, new Double(1.40)});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());
        ExchangeRate exchangeRate1 = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate1);
        assertEquals(1.40, exchangeRate1.getExchangeRate(), 0.0);
    }

    public void testInsertNotificationWithSourceAttribute()
    throws Exception
    {
        int updateClassCount = EmployeeFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = EmployeeFinder.sourceId().eq(0).and(EmployeeFinder.id().eq(2));
        EmployeeList list0 = new EmployeeList(op);
        assertEquals(0, list0.size());
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertEmployee", new Class[]{int.class, int.class, String.class}, new Object[]{new Integer(0), new Integer(2), "abc@abc.com"});
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
        int prevSize =  list0.size();
        assertEquals(1, list1.size());
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertEmployee", new Class[]{int.class, int.class, String.class}, new Object[]{new Integer(0), new Integer(2), "abc@abc.com"});
        this.getRemoteSlaveVm().executeMethod("serverInsertOrder", new Class[]{int.class}, new Object[]{new Integer(999999)});
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
        this.getRemoteSlaveVm().executeMethod("serverInsertOrderList", new Class[]{int.class, int.class}, new Object[]{new Integer(999900), new Integer(50)});
        waitForMessages(updateClassCount, OrderFinder.getMithraObjectPortal());
        OrderList list1 = new OrderList(op);
        assertEquals(50, list1.size());
        updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverInsertOrderList", new Class[]{int.class, int.class}, new Object[]{new Integer(999950), new Integer(50)});
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
        this.getRemoteSlaveVm().executeMethod("serverInsertEmployeeListWithMultipleDestinations", new Class[]{}, new Object[]{});
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
        this.getRemoteSlaveVm().executeMethod("serverDeleteEmployee", new Class[]{int.class, int.class}, new Object[]{new Integer(1), new Integer(1) });
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
        this.getRemoteSlaveVm().executeMethod("serverBatchDeleteAllExchangeRates", new Class[]{String.class}, new Object[]{"A"});
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
        this.getRemoteSlaveVm().executeMethod("serverUpdateEmployee", new Class[]{int.class, int.class, String.class, String.class, String.class, String.class}, new Object[]{new Integer(0), new Integer(1), newName, newPhone, newDesignation, newEmail});
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
        this.getRemoteSlaveVm().executeMethod("serverUpdateEmployee", new Class[]{int.class, int.class, String.class, String.class, String.class, String.class}, new Object[]{new Integer(0), new Integer(1), newName, newPhone, newDesignation, newEmail});
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
        this.getRemoteSlaveVm().executeMethod("serverBatchUpdateOrderState", new Class[]{String.class, String.class}, new Object[]{state, newState});
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
        this.getRemoteSlaveVm().executeMethod("serverUpdateExchangeRate", new Class[]{String.class, String.class, double.class}, new Object[]{"USD", "A", new Double(2.0)});
        waitForMessages(updateClassCount, ExchangeRateFinder.getMithraObjectPortal());
        ExchangeRate rate1 = ExchangeRateFinder.findOne(op);
        assertEquals(2.0, rate1.getExchangeRate(),0);
    }


    public void testMassDelete()
    throws Exception
    {
        int updateClassCount = OrderFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Operation op = OrderFinder.orderId().greaterThan(0);
        op = op.and(OrderFinder.orderId().lessThan(5));
        OrderList list = new OrderList(op);
        assertTrue(list.size() > 0);
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverDeleteAllOrdersUsingOperation", new Class[]{}, new Object[]{});
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
        this.getRemoteSlaveVm().executeMethod("serverDeleteAllOrdersUsingOperation", new Class[]{}, new Object[]{});
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
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertNewTinyBalance", new Class[]{String.class, int.class, Timestamp.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
    }

    public void testPurgeDatedObjectOnPartialCache()
            throws Exception
    {
        assertTinyBalanceIsPartiallyCached();

        int balanceId = 8764;
        int balanceId2 = 8765;
        String sourceAttribute = "A";

        Operation op = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op = op.and(TinyBalanceFinder.balanceId().eq(balanceId));
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(TinyBalanceFinder.processingDate().equalsEdgePoint());

        TinyBalanceList allRowsList = new TinyBalanceList(op);
        assertEquals(21, allRowsList.size());

        Operation op2 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op2 = op2.and(TinyBalanceFinder.balanceId().eq(balanceId2));
        op2 = op2.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op2 = op2.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        TinyBalanceList allRowsList2 = TinyBalanceFinder.findMany(op2);
        assertEquals(36, allRowsList2.size());

        waitForRegistrationToComplete();
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverPurgeTinyBalance", new Class[]{String.class, int.class}, new Object[]{sourceAttribute, new Integer(balanceId)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op);
        assertEquals(0, list1.size());

        // only balanceId was purged, not balanceId2
        TinyBalanceList list2 = new TinyBalanceList(op2);
        assertEquals(36, list2.size());
    }

    public void testPurgeDatedObjectOnFullCache()
            throws Exception
    {
        assertFullyCachedTinyBalanceIsFullyCached();

        int balanceId = 8764;
        int balanceId2 = 8765;

        Operation op = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        op = op.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());

        FullyCachedTinyBalanceList allRowsList = FullyCachedTinyBalanceFinder.findMany(op);
        assertEquals(21, allRowsList.size());

        Operation op2 = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId2);
        op2 = op2.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        op2 = op2.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());
        FullyCachedTinyBalanceList allRowsList2 = FullyCachedTinyBalanceFinder.findMany(op2);
        assertEquals(36, allRowsList2.size());

        waitForRegistrationToComplete();
        int updateClassCount = FullyCachedTinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverPurgeFullyCachedTinyBalance", new Class[]{int.class}, new Object[]{new Integer(balanceId)});
        waitForMessages(updateClassCount, FullyCachedTinyBalanceFinder.getMithraObjectPortal());
        FullyCachedTinyBalanceList list1 = new FullyCachedTinyBalanceList(op);
        assertEquals(0, list1.size());

        // only balanceId was purged, not balanceId2
        FullyCachedTinyBalanceList list2 = new FullyCachedTinyBalanceList(op2);
        assertEquals(36, list2.size());
    }

    public void testPurgeWithDateFilterOnPartialCache()
            throws Exception
    {
        assertTinyBalanceIsPartiallyCached();

        int balanceId = 8764;
        int balanceId2 = 8765;
        String sourceAttribute = "A";

        Operation op = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op = op.and(TinyBalanceFinder.balanceId().eq(balanceId));
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(TinyBalanceFinder.processingDate().equalsEdgePoint());

        TinyBalanceList allRowsList = TinyBalanceFinder.findMany(op);
        assertEquals(21, allRowsList.size());

        Operation op2 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op2 = op2.and(TinyBalanceFinder.balanceId().eq(balanceId2));
        op2 = op2.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op2 = op2.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        TinyBalanceList allRowsList2 = TinyBalanceFinder.findMany(op2);
        assertEquals(36, allRowsList2.size());

        waitForRegistrationToComplete();
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverPurgeTinyBalanceWithDateOperation", new Class[]{String.class}, new Object[]{sourceAttribute});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op);
        assertEquals(5, list1.size());

        Timestamp businessDate0 = Timestamp.valueOf("2007-10-05 12:00:00.0");
        Timestamp businessDate1 = Timestamp.valueOf("2007-10-09 12:00:00.0");
        Timestamp businessDate2 = Timestamp.valueOf("2007-10-03 12:00:00.0");
        Timestamp businessDate3 = Timestamp.valueOf("2007-08-15 12:00:00.0");
        Timestamp businessDateOther = Timestamp.valueOf("2007-09-01 12:00:00.0");
        Operation baseOperation = TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.acmapCode().eq(sourceAttribute));
        assertEquals(19644703.65548992, TinyBalanceFinder.findOne(TinyBalanceFinder.businessDate().eq(businessDate0).and(baseOperation)).getQuantity());
        assertEquals(19644705.31548992, TinyBalanceFinder.findOne(TinyBalanceFinder.businessDate().eq(businessDate1).and(baseOperation)).getQuantity());
        assertEquals(-595573638.12757134, TinyBalanceFinder.findOne(TinyBalanceFinder.businessDate().eq(businessDate2).and(baseOperation)).getQuantity());
        assertEquals(19644705.2698561, TinyBalanceFinder.findOne(TinyBalanceFinder.businessDate().eq(businessDate3).and(baseOperation)).getQuantity());
        assertNull(TinyBalanceFinder.findOne(TinyBalanceFinder.businessDate().eq(businessDateOther).and(baseOperation)));

        TinyBalanceList list2 = new TinyBalanceList(op2);
        assertEquals(5, list2.size());
    }

    public void testPurgeWithDateFilterOnFullCache()
            throws Exception
    {
        assertFullyCachedTinyBalanceIsFullyCached();

        int balanceId = 8764;
        int balanceId2 = 8765;

        Operation op = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        op = op.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());

        FullyCachedTinyBalanceList allRowsList = FullyCachedTinyBalanceFinder.findMany(op);
        assertEquals(21, allRowsList.size());

        Operation op2 = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId2);
        op2 = op2.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        op2 = op2.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());
        FullyCachedTinyBalanceList allRowsList2 = FullyCachedTinyBalanceFinder.findMany(op2);
        assertEquals(36, allRowsList2.size());

        waitForRegistrationToComplete();
        int updateClassCount = FullyCachedTinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverPurgeFullyCachedTinyBalanceWithDateOperation", new Class[]{}, new Object[]{});
        waitForMessages(updateClassCount, FullyCachedTinyBalanceFinder.getMithraObjectPortal());
        FullyCachedTinyBalanceList list1 = new FullyCachedTinyBalanceList(op);
        assertEquals(5, list1.size());

        Timestamp businessDate0 = Timestamp.valueOf("2007-10-05 12:00:00.0");
        Timestamp businessDate1 = Timestamp.valueOf("2007-10-09 12:00:00.0");
        Timestamp businessDate2 = Timestamp.valueOf("2007-10-03 12:00:00.0");
        Timestamp businessDate3 = Timestamp.valueOf("2007-08-15 12:00:00.0");
        Timestamp businessDateOther = Timestamp.valueOf("2007-09-01 12:00:00.0");
        Operation baseOperation = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        assertEquals(19644703.65548992, FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate0).and(baseOperation)).getQuantity());
        assertEquals(19644705.31548992, FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate1).and(baseOperation)).getQuantity());
        assertEquals(-595573638.12757134, FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate2).and(baseOperation)).getQuantity());
        assertEquals(19644705.2698561, FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate3).and(baseOperation)).getQuantity());
        assertNull(FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.businessDate().eq(businessDateOther).and(baseOperation)));

        FullyCachedTinyBalanceList list2 = new FullyCachedTinyBalanceList(op2);
        assertEquals(5, list2.size());
    }

//    class com.gs.fw.common.mithra.test.domain.TinyBalance
//    balanceId,quantity,businessDateFrom,businessDateTo,processingDateFrom,processingDateTo
//    1234,100.00,"2005-12-01 18:30:00.0","9999-12-01 23:59:00.0","2005-12-01 19:30:00.0","2005-12-15 18:49:00.0"
//    1234,100.00,"2005-12-01 18:30:00.0","2005-12-15 18:30:00.0","2005-12-15 18:49:00.0","9999-12-01 23:59:00.0"
//    1234,200.00,"2005-12-15 18:30:00.0","9999-12-01 23:59:00.0","2005-12-15 18:30:00.0","9999-12-01 23:59:00.0"

    public void testDatedObjectUpdateOnPartialCache()
    throws Exception
    {
        assertTinyBalanceIsPartiallyCached();

        int balanceId = 1234;
        String sourceAttribute = "B";
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2005-12-10 12:00:00.0").getTime());

        Operation allDatesOp = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        allDatesOp = allDatesOp.and(TinyBalanceFinder.balanceId().eq(balanceId));
        allDatesOp = allDatesOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        allDatesOp = allDatesOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());

        TinyBalanceList allDatesList = TinyBalanceFinder.findMany(allDatesOp);
        assertEquals(3, allDatesList.size());

        Operation op0 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op0 = op0.and(TinyBalanceFinder.balanceId().eq(balanceId));
        op0 = op0.and(TinyBalanceFinder.businessDate().eq(businessDate0));

        TinyBalanceList list0 = new TinyBalanceList(op0);
        assertEquals(1, list0.size());
        TinyBalance balance0 = list0.get(0);
        assertEquals(1234, balance0.getBalanceId());
        assertEquals(100.00, balance0.getQuantity(),0);
        waitForRegistrationToComplete();
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverUpdateTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0, new Double(150.00)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
        TinyBalance balance1 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance1.getBalanceId());
        assertEquals(150.00, balance1.getQuantity(),0);

        TinyBalanceList allDatesList2 = TinyBalanceFinder.findMany(allDatesOp);
        assertEquals(5, allDatesList2.size());
    }

    public void testDatedObjectUpdateOnFullCache()
            throws Exception
    {

        int balanceId = 8764;
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2007-11-05 12:00:00.0").getTime());

        assertFullyCachedTinyBalanceIsFullyCached();

        Operation op0 = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        op0 = op0.and(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate0));

        Operation allDatesOp = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        allDatesOp = allDatesOp.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        allDatesOp = allDatesOp.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());

        FullyCachedTinyBalanceList allDatesList = FullyCachedTinyBalanceFinder.findMany(allDatesOp);
        assertEquals(21, allDatesList.size());

        // Verify magic value balance does not exist before the test starts
        Operation magicBalanceOperation = FullyCachedTinyBalanceFinder.businessDate().eq(VERY_OLD_BUSINESS_DATE_FOR_SQL_INSERT)
                .and(FullyCachedTinyBalanceFinder.balanceId().eq(balanceId));
        assertNull(FullyCachedTinyBalanceFinder.findOne(magicBalanceOperation));

        FullyCachedTinyBalanceList list0 = new FullyCachedTinyBalanceList(op0);
        assertEquals(1, list0.size());
        FullyCachedTinyBalance balance0 = list0.get(0);
        assertEquals(8764, balance0.getBalanceId());
        assertEquals(-1336922075.5390835, balance0.getQuantity(), 0);
        waitForRegistrationToComplete();
        double newBalanceValue = 1234.5;
        int updateClassCount = FullyCachedTinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverUpdateFullyCachedTinyBalance", new Class[]{int.class, Timestamp.class, double.class}, new Object[]{new Integer(balanceId), businessDate0, newBalanceValue});
        waitForMessages(updateClassCount, FullyCachedTinyBalanceFinder.getMithraObjectPortal());
        FullyCachedTinyBalanceList list1 = new FullyCachedTinyBalanceList(op0);
        assertEquals(1, list1.size());
        assertEquals(newBalanceValue, list1.get(0).getQuantity());

        FullyCachedTinyBalanceList allDatesList2 = new FullyCachedTinyBalanceList(allDatesOp);
        assertEquals(24, allDatesList2.size());  // == 21 + 1 newly inserted THRU_Z'd row + 1 newly inserted row with new value + 1 magic value row directly inserted to database by the test harness

        // The existence of this magic value balance object in our cache proves that the cache was refreshed from DB.
        // See the commentary in insertMagicValueDirectToDbForFullyCachedTinyBalance() for the rationale behind this test.
        assertEquals(VERY_OLD_BALANCE_FOR_SQL_INSERT, FullyCachedTinyBalanceFinder.findOne(magicBalanceOperation).getQuantity());
    }

    public void testDatedObjectTerminateBitemporalOnPartialCache()
    throws Exception
    {
        assertTinyBalanceIsPartiallyCached();

        int balanceId = 1234;
        String sourceAttribute = "B";
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2005-12-10 12:00:00.0").getTime());

        Operation op0 = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op0 = op0.and(TinyBalanceFinder.balanceId().eq(balanceId));
        op0 = op0.and(TinyBalanceFinder.businessDate().eq(businessDate0));

        Operation allDatesOp = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        allDatesOp = allDatesOp.and(TinyBalanceFinder.balanceId().eq(balanceId));
        allDatesOp = allDatesOp.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        allDatesOp = allDatesOp.and(TinyBalanceFinder.processingDate().equalsEdgePoint());

        TinyBalanceList allDatesList = new TinyBalanceList(allDatesOp);
        assertEquals(3, allDatesList.size());

        TinyBalanceList list0 = new TinyBalanceList(op0);
        assertEquals(1, list0.size());
        TinyBalance balance0 = list0.get(0);
        assertEquals(1234, balance0.getBalanceId());
        assertEquals(100.00, balance0.getQuantity(), 0);
        waitForRegistrationToComplete();
        int updateClassCount = TinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverTerminateTinyBalance", new Class[]{String.class, int.class, Timestamp.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(0, list1.size());

        TinyBalanceList allDatesList2 = new TinyBalanceList(allDatesOp);
        assertEquals(4, allDatesList2.size());
    }

    public void testDatedObjectTerminateBitemporalOnFullCache()
    throws Exception
    {
        int balanceId = 8764;
        Timestamp businessDate0 = new Timestamp(timestampFormat.parse("2007-11-05 12:00:00.0").getTime());

        assertFullyCachedTinyBalanceIsFullyCached();

        Operation op0 = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
                  op0 = op0.and(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate0));

        Operation allDatesOp = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        allDatesOp = allDatesOp.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        allDatesOp = allDatesOp.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());

        FullyCachedTinyBalanceList allDatesList = FullyCachedTinyBalanceFinder.findMany(allDatesOp);
        assertEquals(21, allDatesList.size());

        // Verify magic value balance does not exist before the test starts
        Operation magicBalanceOperation = FullyCachedTinyBalanceFinder.businessDate().eq(VERY_OLD_BUSINESS_DATE_FOR_SQL_INSERT)
                .and(FullyCachedTinyBalanceFinder.balanceId().eq(balanceId));
        assertNull(FullyCachedTinyBalanceFinder.findOne(magicBalanceOperation));

        FullyCachedTinyBalanceList list0 = new FullyCachedTinyBalanceList(op0);
        assertEquals(1, list0.size());
        FullyCachedTinyBalance balance0 = list0.get(0);
        assertEquals(8764, balance0.getBalanceId());
        assertEquals(-1336922075.5390835, balance0.getQuantity(), 0);
        waitForRegistrationToComplete();
        int updateClassCount = FullyCachedTinyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        this.getRemoteSlaveVm().executeMethod("serverTerminateFullyCachedTinyBalance", new Class[]{int.class, Timestamp.class}, new Object[]{new Integer(balanceId), businessDate0});
        waitForMessages(updateClassCount, FullyCachedTinyBalanceFinder.getMithraObjectPortal());
        FullyCachedTinyBalanceList list1 = new FullyCachedTinyBalanceList(op0);
        assertEquals(0, list1.size());

        FullyCachedTinyBalanceList allDatesList2 = new FullyCachedTinyBalanceList(allDatesOp);
        assertEquals(23, allDatesList2.size()); // == 21 + 1 newly inserted THRU_Z'd row + 1 magic value row directly inserted to database by the test harness

        // The existence of this magic value balance object in our cache proves that the cache was refreshed from DB.
        // See the commentary in insertMagicValueDirectToDbForFullyCachedTinyBalance() for the rationale behind this test.
        assertEquals(VERY_OLD_BALANCE_FOR_SQL_INSERT, FullyCachedTinyBalanceFinder.findOne(magicBalanceOperation).getQuantity());
    }

    private void assertTinyBalanceIsPartiallyCached()
    {
        // TinyBalance must be defined as partially cached on the client VM (i.e. in MithraConfigClientCache.xml)
        // Even when we run the full cache test suite, the client side remains partially cached - only the server side becomes fully cached.
        assertFalse(TinyBalanceFinder.getMithraObjectPortal().isFullyCached());
        assertTrue(TinyBalanceFinder.getMithraObjectPortal().isPartiallyCached());
    }

    private void assertFullyCachedTinyBalanceIsFullyCached()
    {
        // FullyCachedTinyBalance must be defined as fully cached on the client VM (i.e. in MithraConfigClientCache.xml)
        // Even when we run the partial cache test suite, the client side remains fully cached - only the server side becomes partially cached.
        assertTrue(FullyCachedTinyBalanceFinder.getMithraObjectPortal().isFullyCached());
        assertFalse(FullyCachedTinyBalanceFinder.getMithraObjectPortal().isPartiallyCached());
    }

    public void testDatedObjectTerminateRelatedAuditOnly()
    throws Exception
    {
        int updateCount = AuditedOrderItemFinder.processingDateTo().getUpdateCount();

        Operation op = AuditedOrderFinder.items().originalPrice().eq(15.5);
        AuditedOrder order = AuditedOrderFinder.findOne(op);
        assertNotNull(order);
        assertEquals(2, order.getOrderId());

        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverTerminateAuditedOrderItem", new Class[]{int.class}, new Object[]{new Integer(3)});
        waitForMessages(updateCount, AuditedOrderItemFinder.processingDateTo());
        AuditedOrder order2 = AuditedOrderFinder.findOne(op);
        assertNull(order2);
    }

    public void testDatedObjectTerminateAuditOnly()
    throws Exception
    {
        int updateClassCount = AuditOnlyBalanceFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int balanceId = 1;
        String sourceAttribute = "A";

        Operation op0 = AuditOnlyBalanceFinder.acmapCode().eq(sourceAttribute);
                  op0 = op0.and(AuditOnlyBalanceFinder.balanceId().eq(balanceId));

        AuditOnlyBalance balance = AuditOnlyBalanceFinder.findOne(op0);
        assertNotNull(balance);
        assertEquals(1, balance.getBalanceId());
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverTerminateAuditOnlyBalance", new Class[]{String.class, int.class}, new Object[]{sourceAttribute, new Integer(balanceId)});
        waitForMessages(updateClassCount, AuditOnlyBalanceFinder.getMithraObjectPortal());
        AuditOnlyBalance balance2 = AuditOnlyBalanceFinder.findOne(op0);
        assertNull(balance2);
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
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverIncrementTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate0, new Double(150.00)});
        waitForMessages(updateClassCount, TinyBalanceFinder.getMithraObjectPortal());
        TinyBalanceList list1 = new TinyBalanceList(op0);
        assertEquals(1, list1.size());
        TinyBalance balance1 = (TinyBalance)list0.get(0);
        assertEquals(1234, balance1.getBalanceId());
        assertEquals(250.00, balance1.getQuantity(), 0);
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
         waitForRegistrationToComplete();
        //Oooppsss We just found out a trade that was done on 12/10/2005 that increased the balance by 50
        Timestamp businessDate3 = new Timestamp(timestampFormat.parse("2005-12-10 18:30:00.0").getTime());
        this.getRemoteSlaveVm().executeMethod("serverIncrementTinyBalance", new Class[]{String.class, int.class, Timestamp.class, double.class}, new Object[]{sourceAttribute, new Integer(balanceId), businessDate3, new Double(50)});
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

    public void serverInsertNewTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalance balance = new TinyBalance(businessDate);
        balance.setAcmapCode(sourceAttribute);
        balance.setBalanceId(balanceId);
        balance.insert();
        tx.commit();
    }

    public void serverPurgeTinyBalance(String sourceAttribute, int balanceId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op = op.and(TinyBalanceFinder.balanceId().eq(balanceId));
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(TinyBalanceFinder.processingDate().equalsEdgePoint());

        TinyBalanceList list = new TinyBalanceList(op);
        list.purgeAll();
        tx.commit();        
    }

    public void serverPurgeFullyCachedTinyBalance(int balanceId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Operation op = FullyCachedTinyBalanceFinder.balanceId().eq(balanceId);
        op = op.and(FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());

        FullyCachedTinyBalanceList list = new FullyCachedTinyBalanceList(op);
        list.purgeAll();
        tx.commit();
    }

    public void serverPurgeTinyBalanceWithDateOperation(String sourceAttribute)
    {
        Timestamp businessDate0 = Timestamp.valueOf("2007-10-05 12:00:00.0");
        Timestamp businessDate1 = Timestamp.valueOf("2007-10-09 12:00:00.0");
        Timestamp businessDate2 = Timestamp.valueOf("2007-10-03 12:00:00.0");
        Timestamp businessDate3 = Timestamp.valueOf("2007-08-15 12:00:00.0");
        Operation op = TinyBalanceFinder.acmapCode().eq(sourceAttribute);
        op = op.and(TinyBalanceFinder.businessDate().equalsEdgePoint());
        op = op.and(TinyBalanceFinder.processingDate().equalsEdgePoint());
        op = op.and(TinyBalanceFinder.businessDateFrom().greaterThan(businessDate0).or(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate0)));
        op = op.and(TinyBalanceFinder.businessDateFrom().greaterThan(businessDate1).or(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate1)));
        op = op.and(TinyBalanceFinder.businessDateFrom().greaterThan(businessDate2).or(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate2)));
        op = op.and(TinyBalanceFinder.businessDateFrom().greaterThan(businessDate3).or(TinyBalanceFinder.businessDateTo().lessThanEquals(businessDate3)));

        TinyBalanceList list = new TinyBalanceList(op);
        list.purgeAllInBatches(5);
    }

    public void serverPurgeFullyCachedTinyBalanceWithDateOperation()
    {
        Timestamp businessDate0 = Timestamp.valueOf("2007-10-05 12:00:00.0");
        Timestamp businessDate1 = Timestamp.valueOf("2007-10-09 12:00:00.0");
        Timestamp businessDate2 = Timestamp.valueOf("2007-10-03 12:00:00.0");
        Timestamp businessDate3 = Timestamp.valueOf("2007-08-15 12:00:00.0");
        Operation op = FullyCachedTinyBalanceFinder.businessDate().equalsEdgePoint();
        op = op.and(FullyCachedTinyBalanceFinder.processingDate().equalsEdgePoint());
        op = op.and(FullyCachedTinyBalanceFinder.businessDateFrom().greaterThan(businessDate0).or(FullyCachedTinyBalanceFinder.businessDateTo().lessThanEquals(businessDate0)));
        op = op.and(FullyCachedTinyBalanceFinder.businessDateFrom().greaterThan(businessDate1).or(FullyCachedTinyBalanceFinder.businessDateTo().lessThanEquals(businessDate1)));
        op = op.and(FullyCachedTinyBalanceFinder.businessDateFrom().greaterThan(businessDate2).or(FullyCachedTinyBalanceFinder.businessDateTo().lessThanEquals(businessDate2)));
        op = op.and(FullyCachedTinyBalanceFinder.businessDateFrom().greaterThan(businessDate3).or(FullyCachedTinyBalanceFinder.businessDateTo().lessThanEquals(businessDate3)));

        FullyCachedTinyBalanceList list = new FullyCachedTinyBalanceList(op);
        list.purgeAllInBatches(5);
    }

    public void serverUpdateTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate, double newQuantity)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
        tinyBalance.setQuantity(newQuantity);
        tx.commit();
    }

    public void serverUpdateFullyCachedTinyBalance(int balanceId, Timestamp businessDate, double newQuantity) throws SQLException
    {
        // Create evidence to enable the client to prove that it really did get its cache refreshed from the database
        insertMagicValueDirectToDbForFullyCachedTinyBalance(balanceId);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        FullyCachedTinyBalance tinyBalance = FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.balanceId().eq(balanceId).and(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate).and(FullyCachedTinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()))));
        tinyBalance.setQuantity(newQuantity);
        tx.commit();
    }
    
    public void serverTerminateTinyBalance(String sourceAttribute, int balanceId, Timestamp businessDate)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        TinyBalance tinyBalance = TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq(sourceAttribute).and(TinyBalanceFinder.balanceId().eq(balanceId).and(TinyBalanceFinder.businessDate().eq(businessDate).and(TinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity())))));
        tinyBalance.terminate();
        tx.commit();
    }

    public void serverTerminateFullyCachedTinyBalance(int balanceId, Timestamp businessDate) throws SQLException
    {
        // Create evidence to enable the client to prove that it really did get its cache refreshed from the database
        insertMagicValueDirectToDbForFullyCachedTinyBalance(balanceId);

        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        FullyCachedTinyBalance tinyBalance = FullyCachedTinyBalanceFinder.findOne(FullyCachedTinyBalanceFinder.balanceId().eq(balanceId).and(FullyCachedTinyBalanceFinder.businessDate().eq(businessDate).and(FullyCachedTinyBalanceFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()))));
        tinyBalance.terminate();
        tx.commit();
    }

    private void insertMagicValueDirectToDbForFullyCachedTinyBalance(int balanceId) throws SQLException
    {
        // To allow us to prove that the client cache really did perform a database reload for the primary key impacted by notification:
        // We sneakily insert a 'magic value' row into the database using JDBC without Mithra's knowledge.
        // The test can assert the existence of this new row in the client cache to verify the cache really did reload this key from database.

        // This is necessary because the client is using Mithra remote service (3 tier) architecture - backed by the slave VM cache.
        // The slave VM cache is the same place where we perform Mithra updates to generate the notifications for the test.
        // So when the client processes the notification, the slave VM cache by definition already has the updated Mithra objects in its cache.
        // That makes it an unrealistic test for most 2 tier scenarios. True, the client's own cache does not have the update, but we have
        // no way of knowing if the client has just reloaded the data that is already in the remote cache or if it has actually bypassed
        // the remote cache to re-query the database.

        // This magic value is not representative of a real world scenario but at least allows us to prove that the database refresh happens.
        // The alternative would be to have the client connect to the same H2 database instance as the slave VM. That would be tricky to set up.

        Calendar cal = GregorianCalendar.getInstance();
        cal.setTime(VERY_OLD_BUSINESS_DATE_FOR_SQL_INSERT);
        cal.add(Calendar.DAY_OF_YEAR, 1);
        Timestamp dayAfterOldDate = new Timestamp(cal.getTimeInMillis());
        Statement stmt = ConnectionManagerForTests.getInstance().getConnection().createStatement();
        stmt.executeUpdate("insert into FULLY_CACHED_TINY_BALANCE (BALANCE_ID, POS_QUANTITY_M, FROM_Z, THRU_Z, IN_Z, OUT_Z) values (" + balanceId + ", " + VERY_OLD_BALANCE_FOR_SQL_INSERT + ", '" + VERY_OLD_BUSINESS_DATE_FOR_SQL_INSERT + "', '" + dayAfterOldDate + "', '" + VERY_OLD_BUSINESS_DATE_FOR_SQL_INSERT + "', '9999-12-01 23:59:00.0')");
    }

    public void serverTerminateAuditedOrderItem(int orderItemId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(orderItemId)).terminate();
        tx.commit();
    }

    public void serverTerminateAuditOnlyBalance(String sourceAttribute, int balanceId)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        AuditOnlyBalance balance = AuditOnlyBalanceFinder.findOne(AuditOnlyBalanceFinder.acmapCode().eq(sourceAttribute).and(AuditOnlyBalanceFinder.balanceId().eq(balanceId)));
        balance.terminate();
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

    public void serverDeleteAllOrdersUsingOperation()
    {
        Operation op = OrderFinder.all();
        OrderList list = new OrderList(op);
        list.deleteAll();
    }

    public void serverInsertOrder(int orderId)
    {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setDescription("TestOrder");
        order.insert();
    }

    public void serverUpdateOrder(int orderId, String description, String orderState, String trackingId, int userId )
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(orderId));
        order.setDescription(description);
	    order.setState(orderState);
	    order.setTrackingId(trackingId);
	    order.setUserId(userId);
        tx.commit();
    }

    public void serverInsertEmployee(int sourceId, int id, String email)
    {
        Employee employee = new Employee();
        employee.setSourceId(sourceId);
        employee.setId(id);
        employee.setEmail(email);
        employee.insert();
    }

    public void serverInsertEmployeeListWithMultipleDestinations()
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

    public void serverInsertOrderItem(int orderItemId, int orderId)
    {
        OrderItem item = new OrderItem();
        item.setId(orderItemId);
        item.setOrderId(orderId);
        item.insert();
    }

    public void serverInsertOrderList(int initialOrderId, int listSize)
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

    public void serverInsertExchangeRate(String acmapCode, String currency, int source, Timestamp date, double rate)
    {
        ExchangeRate erate = new ExchangeRate();
        erate.setAcmapCode(acmapCode);
        erate.setCurrency(currency);
        erate.setSource(source);
        erate.setDate(date);
        erate.setExchangeRate(rate);
        erate.insert();
    }

    public void serverUpdateExchangeRate(String acmapCode, String currency, int source, Timestamp date, double rate)
    {
        Operation op = ExchangeRateFinder.acmapCode().eq(acmapCode);
        op = op.and(ExchangeRateFinder.currency().eq(currency));
        op = op.and(ExchangeRateFinder.source().eq(source));
        op = op.and(ExchangeRateFinder.date().eq(date));
        ExchangeRate erate = ExchangeRateFinder.findOne(op);
        erate.setExchangeRate(rate);
    }

    public void serverDeleteEmployee(int sourceId, int id)
    {
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(sourceId).and(EmployeeFinder.id().eq(id)));
        employee.delete();
    }

    public void serverBatchDeleteAllExchangeRates(String acmapCode)
    {
        ExchangeRateList list = new ExchangeRateList(ExchangeRateFinder.acmapCode().eq(acmapCode).and(ExchangeRateFinder.all()));
        assertTrue(!list.isEmpty());
        list.deleteAll();
    }

    public void serverUpdateExchangeRate(String currency, String acmapCode, double rate)
    {
        ExchangeRate erate = ExchangeRateFinder.findOne(ExchangeRateFinder.acmapCode().eq(acmapCode).and(ExchangeRateFinder.currency().eq(currency)));
        erate.setExchangeRate(rate);
    }

    public void serverBatchUpdateOrderState(String state, String newState)
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

    public void serverUpdateEmployee(int acmapCode, int employeeId, String newName, String newPhone, String newDesignation, String newEmail)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Employee employee = EmployeeFinder.findOne(EmployeeFinder.sourceId().eq(acmapCode).and(EmployeeFinder.id().eq(employeeId)));
        employee.setName(newName);
        employee.setPhone(newPhone);
        employee.setEmail(newEmail);
        employee.setDesignation(newDesignation);
        tx.commit();
    }

    public void serverUpdateDivision(int divisionId, String sourceId, String newDivisionName, String newState, String newCity)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Division division = DivisionFinder.findOne(DivisionFinder.divisionId().eq(divisionId).and(DivisionFinder.sourceId().eq(sourceId)));
        division.setDivisionName(newDivisionName);
        division.setState(newState);
        division.setCity(newCity);
        tx.commit();
    }

}
