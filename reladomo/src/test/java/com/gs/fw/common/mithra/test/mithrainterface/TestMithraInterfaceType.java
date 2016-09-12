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

package com.gs.fw.common.mithra.test.mithrainterface;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.inherited.*;
import com.gs.fw.common.mithra.test.domain.testmithraimport.*;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TestMithraInterfaceType extends MithraTestAbstract
{
    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        OrderStatus.class,
                        BitemporalOrder.class,
                        BitemporalOrderStatus.class,
                        BitemporalOrderItem.class,
                        Order.class,
                        AuditedOrder.class,
                        AuditedOrderItem.class,
                        OrderWithAuditedStatus.class,
                        Account.class,
                        TxCow.class,
                        TxAnimal.class,
                        TxMammal.class,
                        TxMonkey.class,
                        ReadOnlyOrder.class,
                        DatedReadOnlyOrder.class,
                        TransactionalOrder.class,
                        DatedTransactionalOrder.class,
                        TransactionalWithSuperOrder.class,
                        DatedTransactionalWithSuperOrder.class,
                        RelatedOrder.class
                };
    }


    public void testSimpleMithraInterface() throws Exception
    {
        OrderableFinder<Order> finder = OrderFinder.getFinderInstance();
        Orderable orderable = finder.findOne(getOneOrderIdOperation(finder, 1));
        assertEquals(1, orderable.getOrderId());

        //Test Dated/NonDated Transactional/ReadOnly
        //Test using imported mithra interface
        SimpleOrderInterfaceFinder<ReadOnlyOrder> readOnlyFinder = ReadOnlyOrderFinder.getFinderInstance();
        SimpleOrderInterface readOnlyOrderable = readOnlyFinder.findOne(getOneSimpleOrderIdOperation(readOnlyFinder, 1));
        assertEquals(1, readOnlyOrderable.getOrderId());

        SimpleOrderInterfaceFinder<DatedReadOnlyOrder> datedReadOnlyFinder = DatedReadOnlyOrderFinder.getFinderInstance();
        SimpleOrderInterface datedReadOnlyOrderable = datedReadOnlyFinder.findOne(getOneSimpleOrderIdOperation(datedReadOnlyFinder, 1));
        assertEquals(1, datedReadOnlyOrderable.getOrderId());

        SimpleOrderInterfaceFinder<TransactionalOrder> transactionalFinder = TransactionalOrderFinder.getFinderInstance();
        SimpleOrderInterface transactionalOrderable = transactionalFinder.findOne(getOneSimpleOrderIdOperation(transactionalFinder, 1));
        assertEquals(1, transactionalOrderable.getOrderId());

        SimpleOrderInterfaceFinder<DatedTransactionalOrder> datedTransactionalFinder = DatedTransactionalOrderFinder.getFinderInstance();
        SimpleOrderInterface datedTransactionalOrderable = datedTransactionalFinder.findOne(getOneSimpleOrderIdOperation(datedTransactionalFinder, 1));
        assertEquals(1, datedTransactionalOrderable.getOrderId());

        SimpleOrderInterfaceFinder<TransactionalWithSuperOrder> transactionalWithSuperFinder = TransactionalWithSuperOrderFinder.getFinderInstance();
        SimpleOrderInterface transactionalWithSuperOrderable = transactionalWithSuperFinder.findOne(getOneSimpleOrderIdOperation(transactionalWithSuperFinder, 1));
        assertEquals(1, transactionalWithSuperOrderable.getOrderId());

        SimpleOrderInterfaceFinder<DatedTransactionalWithSuperOrder> datedTransactionalWithSuperFinder = DatedTransactionalWithSuperOrderFinder.getFinderInstance();
        SimpleOrderInterface datedTransactionalWithSuperOrderable = datedTransactionalWithSuperFinder.findOne(getOneSimpleOrderIdOperation(datedTransactionalWithSuperFinder, 1));
        assertEquals(1, datedTransactionalWithSuperOrderable.getOrderId());
    }

    public void testDeclaredSourceAttributes() throws Exception
    {
        AccountInterfaceFinder<Account> finder = AccountFinder.getFinderInstance();
        assertNull(finder.findOne(finder.deskId().eq("A").and(finder.accountNumber().eq("nulltrial"))));
        assertNotNull(finder.findOne(finder.deskId().eq("A").and(finder.accountNumber().eq("null trial"))));
        assertNotNull(finder.findOne(finder.deskId().eq("A").and(finder.accountNumber().eq("with trial"))));
    }

    public void testDeclaredAsOfAttributes() throws Exception
    {
        final Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        DatedOrderableFinder<BitemporalOrder> finder2 = BitemporalOrderFinder.getFinderInstance();
        Operation op = finder2.businessDate().eq(businessDate);
        BitemporalOrderList datedOrderableList = (BitemporalOrderList) finder2.findMany(op);
        assertNotNull(datedOrderableList);
        assertEquals(4, datedOrderableList.size());
    }

    public void testDeclaredRelationShips() throws ParseException
    {
        OrderableFinder<Order> finder = OrderFinder.getFinderInstance();
        OrderList list = (OrderList) finder.findMany(finder.orderStatus().all());
        assertNotNull(list);
        assertEquals(2, list.size());
    }


    public void testInheritedAndDeclaredRelationshipsInMithraInterface() throws Exception
    {
        final Timestamp businessDate = new Timestamp(System.currentTimeMillis());

        DatedOrderItemRelationInterfaceFinder<BitemporalOrder> finder = BitemporalOrderFinder.getFinderInstance();

        Operation declaredRelationshipOperation = finder.expensiveItems(10.5).exists();
        declaredRelationshipOperation = declaredRelationshipOperation.and(finder.businessDate().eq(businessDate));
        assertEquals(2, finder.findMany(declaredRelationshipOperation).size());

        Operation inheritedOperationFromSuperInterface = finder.itemForProduct(1).orderId().eq(2);
        inheritedOperationFromSuperInterface = inheritedOperationFromSuperInterface.and(finder.businessDate().eq(businessDate));
        assertEquals(1, finder.findMany(inheritedOperationFromSuperInterface).size());

        Operation inheritedOperationFromSuperSuperInterface = finder.orderStatus().lastUser().eq("Fred");
        inheritedOperationFromSuperSuperInterface = inheritedOperationFromSuperSuperInterface.and(finder.businessDate().eq(businessDate));
        assertEquals(1, finder.findMany(inheritedOperationFromSuperSuperInterface).size());
    }


    public void testMithraInterfaceExtendsOneMithraInterface() throws Exception
    {

        final Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        final Timestamp orderDate = new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime());

        DatedOrderableFinder<BitemporalOrder> baseTypeFinder = BitemporalOrderFinder.getFinderInstance();
        Operation delaredAttributesOperation = baseTypeFinder.orderDate().eq(orderDate);
        delaredAttributesOperation = delaredAttributesOperation.and(baseTypeFinder.businessDate().eq(businessDate));
        assertEquals(1, baseTypeFinder.findMany(delaredAttributesOperation).size());

        Operation inheritedAttributesOperation = baseTypeFinder.orderId().eq(1);
        inheritedAttributesOperation = inheritedAttributesOperation.and(baseTypeFinder.businessDate().eq(businessDate));
        DatedOrderable baseInterfaceType = baseTypeFinder.findOne(inheritedAttributesOperation);
        assertEquals(1, baseInterfaceType.getOrderId());

        OrderableFinder<BitemporalOrder> superTypeFinder = BitemporalOrderFinder.getFinderInstance();
        inheritedAttributesOperation = getOneOrderIdOperation(superTypeFinder, 1);
        inheritedAttributesOperation = inheritedAttributesOperation.and(baseTypeFinder.businessDate().eq(businessDate));
        Orderable orderable = superTypeFinder.findOne(inheritedAttributesOperation);
        assertEquals(1, orderable.getOrderId());

    }


    public void testMithraInterfaceExtendsTwoMithraInterfaces() throws ParseException
    {

        SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2004-01-01 00:00:00.0").getTime());

        DatedOrderItemRelationInterfaceFinder<BitemporalOrder> baseInterface = BitemporalOrderFinder.getFinderInstance();
        Operation operation = baseInterface.expensiveItems(20.00).exists().and(baseInterface.businessDate().eq(businessDate));
        BitemporalOrder order = baseInterface.findOne(operation);
        assertNotNull(order);

        BitemporalOrderList orderList = (BitemporalOrderList) baseInterface.findMany(baseInterface.itemForProduct(1).all().
                and(((DatedOrderItemRelationInterfaceFinder) baseInterface).businessDate().eq(businessDate)));

        assertEquals(orderList.size(), 4);
    }


    public void testMithraInterfaceExtends2SuperInterfacesEachExtendingCommonSuper()
    {
        AuditedOrderableFinder<OrderWithAuditedStatus> finder = OrderWithAuditedStatusFinder.getFinderInstance();
        OrderWithAuditedStatus orderWithAuditedStatus = finder.findOne(finder.orderId().eq(1));
        assertNotNull(orderWithAuditedStatus);

        Operation operation = finder.state().eq("In-Progress");
        operation = operation.and(finder.trackingId().eq("125")); // super 1
        operation = operation.and(finder.userId().eq(1)); // super 2
        operation = operation.and(finder.orderId().eq(3)); // Common super of the super1 and super 2

        orderWithAuditedStatus = finder.findOne(operation);

        assertEquals(orderWithAuditedStatus.getOrderId(), 3);

    }


    public void testSuperInterfaceTablePerSubClass()
    {
        TxMammalInterfaceFinder<TxAnimal> finder = TxMonkeyFinder.getFinderInstance();
        assertNotNull(finder);
        TxAnimal txAnimal = finder.findOne(finder.bodyTemp().eq(95.3));
        assertNotNull(txAnimal);
        assertTrue(txAnimal instanceof TxMonkey);

        TailedAnimalFinder<TxAnimal> finder2 = TxMonkeyFinder.getFinderInstance();
        txAnimal = finder2.findOne(finder2.tailLength().eq(9.5));
        assertNotNull(txAnimal);
        assertTrue(txAnimal instanceof TxMonkey);

    }

    private Operation getOneOrderIdOperation(OrderableFinder finder, int orderId)
    {
        return finder.orderId().eq(orderId);
    }

    private Operation getOneSimpleOrderIdOperation(SimpleOrderInterfaceFinder finder, int orderId)
    {
        return finder.orderId().eq(orderId).and(finder.order().exists());
    }
}
