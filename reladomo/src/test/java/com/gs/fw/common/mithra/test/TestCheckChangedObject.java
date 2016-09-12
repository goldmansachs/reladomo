
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
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;
import java.util.concurrent.Exchanger;

public class TestCheckChangedObject extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[]
                   {
                      Order.class,
                      TinyBalance.class,
                      VariousTypes.class,
                      Sale.class
                   };
    }

    public void testChanged()
            throws Exception
    {
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));

        Order newOrder = new Order();
        newOrder.copyNonPrimaryKeyAttributesFrom(order);
        newOrder.setOrderId(999);

        assertFalse(newOrder.nonPrimaryKeyAttributesChanged(order));

        newOrder.setUserIdNull();
        assertTrue(newOrder.nonPrimaryKeyAttributesChanged(order));
    }

    public void testChangedWithDatedObjects()
    {
        Operation op = TinyBalanceFinder.acmapCode().eq("A");
        op = op.and(TinyBalanceFinder.balanceId().eq(1));
        op = op.and(TinyBalanceFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));

        TinyBalance balance = TinyBalanceFinder.findOne(op);

        TinyBalance newBalance = new TinyBalance(new Timestamp(System.currentTimeMillis()));
        newBalance.copyNonPrimaryKeyAttributesFrom((TinyBalanceAbstract)balance);
        newBalance.setAcmapCode("A");

        assertFalse(newBalance.nonPrimaryKeyAttributesChanged(balance));

        newBalance.setQuantityNull();
        assertTrue(newBalance.nonPrimaryKeyAttributesChanged(balance));

    }

    public void testChangedDoubleWithTolerance()
    {
        Operation op = SaleFinder.saleId().eq(1);
        Sale sale = SaleFinder.findOne(op);

        Sale newSale = new Sale();
        newSale.copyNonPrimaryKeyAttributesFrom(sale);

        newSale.setDiscountPercentage(0.0577);
        assertTrue(sale.nonPrimaryKeyAttributesChanged(newSale));
        assertFalse(sale.nonPrimaryKeyAttributesChanged(newSale, 0.01));

        newSale.setDiscountPercentage(0.15);
        assertTrue(sale.nonPrimaryKeyAttributesChanged(newSale));
        assertTrue(sale.nonPrimaryKeyAttributesChanged(newSale, 0.01));
    }

    public void testChangedFloatWithTolerance()
    {
        Operation op = VariousTypesFinder.id().eq(1);
        VariousTypes obj = VariousTypesFinder.findOne(op);

        VariousTypes newObj = new VariousTypes();
        newObj.copyNonPrimaryKeyAttributesFrom(obj);

        newObj.setFloatColumn(15.01f);
        assertTrue(obj.nonPrimaryKeyAttributesChanged(newObj));
        assertFalse(obj.nonPrimaryKeyAttributesChanged(newObj, 0.02));

        newObj.setFloatColumn(15.99f);
        assertTrue(obj.nonPrimaryKeyAttributesChanged(newObj));
        assertTrue(obj.nonPrimaryKeyAttributesChanged(newObj, 0.01));

        newObj.setFloatColumn(15.0001f);
        assertTrue(obj.nonPrimaryKeyAttributesChanged(newObj));
        assertTrue(obj.nonPrimaryKeyAttributesChanged(newObj, 0.00000000001));
    }

    public void xtestChangedObjectsInDifferentThreads()
    {
        final Exchanger exchanger = new Exchanger();
        Runnable threadRunnable1 = new Runnable(){
            public void run()
            {
                Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                Order newOrder = new Order();
                newOrder.setOrderId(999);
                newOrder.copyNonPrimaryKeyAttributesFrom(order);

                // 1st meeting
                waitForOtherThread(exchanger);

                //2nd meeting
                waitForOtherThread(exchanger);

                assertFalse(order.nonPrimaryKeyAttributesChanged(newOrder));

                //3rd meeting
                waitForOtherThread(exchanger);
                //4th meeting
                waitForOtherThread(exchanger);
                assertTrue(order.nonPrimaryKeyAttributesChanged(newOrder));
            }
        };
        Runnable threadRunnable2 = new Runnable(){
            public void run()
            {
                final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                //1st meeting
                waitForOtherThread(exchanger);

                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        order.setState("NEW_STATE");
                        //2nd meeting
                        waitForOtherThread(exchanger);
                        //3rd meeting
                        waitForOtherThread(exchanger);
                        return null;
                    }
                });
                //4th meeting
                waitForOtherThread(exchanger);
            }
        };
        assertTrue(runMultithreadedTest(threadRunnable1, threadRunnable2));
    }

}
