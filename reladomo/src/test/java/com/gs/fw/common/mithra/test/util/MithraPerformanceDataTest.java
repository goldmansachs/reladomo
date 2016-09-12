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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.MithraPerformanceData;
import org.junit.Assert;

import java.sql.Timestamp;

public class MithraPerformanceDataTest extends MithraTestAbstract
{
    @Override
    protected Class[] getRestrictedClassList()
    {
        return new Class[]{Order.class, OrderItem.class};
    }

    public void testNoTransactionPerformanceDataIsRecordedIfFlagIsNotSet()
    {
        MithraManagerProvider.getMithraManager().setCaptureTransactionLevelPerformanceData(false);

        int retrieveCountBeforeAnything = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderList result = OrderFinder.findMany(OrderFinder.all());
                result.forceResolve();
                Assert.assertNull(tx.getTransactionPerformanceData());
                Assert.assertEquals(0, tx.getDatabaseRetrieveCount());
                return null;
            }
        });

        Assert.assertEquals(1, OrderFinder.getMithraObjectPortal().getPerformanceData().getDataForFind().getTotalOperations());
        Assert.assertEquals(1, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - retrieveCountBeforeAnything);
    }

    public void testNoTransactionPerformanceDataForCursorIsRecordedIfFlagIsNotSet()
    {
        MithraManagerProvider.getMithraManager().setCaptureTransactionLevelPerformanceData(false);

        int retrieveCountBeforeAnything = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OrderList result = OrderFinder.findMany(OrderFinder.all());
                result.forEachWithCursor(new DoWhileProcedure()
                {
                    public boolean execute(Object object)
                    {
                        return false;
                    }
                });
                Assert.assertNull(tx.getTransactionPerformanceData());
                Assert.assertEquals(0, tx.getDatabaseRetrieveCount());
                return null;
            }
        });

        Assert.assertEquals(1, OrderFinder.getMithraObjectPortal().getPerformanceData().getDataForFind().getTotalOperations());
        Assert.assertEquals(1, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - retrieveCountBeforeAnything);
    }

    public void testTransactionPerformanceDataRecordedForCursor() throws Exception
    {
        int retrieveCountBeforeAnything = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        MithraManagerProvider.getMithraManager().setCaptureTransactionLevelPerformanceData(true);

        // non tx insert
        Order order = new Order();
        order.setOrderId(-73);
        order.insert();

        // non tx select
        OrderList results = OrderFinder.findManyBypassCache(OrderFinder.all());
        results.forEachWithCursor(new DoWhileProcedure()
        {
            public boolean execute(Object object)
            {
                return false;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = new Order();
                order.setOrderId(-74);
                order.insert();

                OrderList result = OrderFinder.findMany(OrderFinder.orderId().eq(1));
                result.forEachWithCursor(new DoWhileProcedure()
                {
                    public boolean execute(Object object)
                    {
                        return false;
                    }
                });

                result.get(0).setOrderDate(new Timestamp(73L));

                tx.executeBufferedOperations();

                Assert.assertNotNull(tx.getTransactionPerformanceData());
                Assert.assertEquals(1, tx.getDatabaseRetrieveCount());

                MithraPerformanceData transactionPerformanceDataForOrder = tx.getTransactionPerformanceDataFor(OrderFinder.getMithraObjectPortal());
                MithraPerformanceData transactionPerformanceDataForOrderItem = tx.getTransactionPerformanceDataFor(OrderItemFinder.getMithraObjectPortal());
                Assert.assertEquals(1, transactionPerformanceDataForOrder.getDataForFind().getTotalOperations());
                Assert.assertEquals(1, transactionPerformanceDataForOrder.getDataForUpdate().getTotalOperations());
                Assert.assertEquals(1, transactionPerformanceDataForOrder.getDataForInsert().getTotalOperations());

                return null;
            }
        });

        MithraPerformanceData performanceDataForOrder = OrderFinder.getMithraObjectPortal().getPerformanceData();
        MithraPerformanceData performanceDataForOrderItem = OrderItemFinder.getMithraObjectPortal().getPerformanceData();
        Assert.assertEquals(2, performanceDataForOrder.getDataForFind().getTotalOperations());
        Assert.assertEquals(1, performanceDataForOrder.getDataForUpdate().getTotalOperations());
        Assert.assertEquals(2, performanceDataForOrder.getDataForInsert().getTotalOperations());
        Assert.assertEquals(2, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - retrieveCountBeforeAnything);
    }

    public void testTransactionPerformanceDataRecorded() throws Exception
    {
        int retrieveCountBeforeAnything = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        MithraManagerProvider.getMithraManager().setCaptureTransactionLevelPerformanceData(true);

        // non tx insert
        Order order = new Order();
        order.setOrderId(-73);
        order.insert();

        // non tx select
        OrderList results = OrderFinder.findManyBypassCache(OrderFinder.all());
        results.deepFetch(OrderFinder.items());
        results.forceResolve();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Order order = new Order();
                order.setOrderId(-74);
                order.insert();

                OrderList result = OrderFinder.findMany(OrderFinder.orderId().eq(1));
                result.deepFetch(OrderFinder.items());
                result.forceResolve();

                result.get(0).setOrderDate(new Timestamp(73L));

                tx.executeBufferedOperations();

                Assert.assertNotNull(tx.getTransactionPerformanceData());
                Assert.assertEquals(2, tx.getDatabaseRetrieveCount());

                MithraPerformanceData transactionPerformanceDataForOrder = tx.getTransactionPerformanceDataFor(OrderFinder.getMithraObjectPortal());
                MithraPerformanceData transactionPerformanceDataForOrderItem = tx.getTransactionPerformanceDataFor(OrderItemFinder.getMithraObjectPortal());
                Assert.assertEquals(1, transactionPerformanceDataForOrder.getDataForFind().getTotalOperations());
                Assert.assertEquals(1, transactionPerformanceDataForOrder.getDataForUpdate().getTotalOperations());
                Assert.assertEquals(1, transactionPerformanceDataForOrder.getDataForInsert().getTotalOperations());
                Assert.assertEquals(1, transactionPerformanceDataForOrderItem.getDataForFind().getTotalOperations());

                return null;
            }
        });

        MithraPerformanceData performanceDataForOrder = OrderFinder.getMithraObjectPortal().getPerformanceData();
        MithraPerformanceData performanceDataForOrderItem = OrderItemFinder.getMithraObjectPortal().getPerformanceData();
        Assert.assertEquals(2, performanceDataForOrder.getDataForFind().getTotalOperations());
        Assert.assertEquals(1, performanceDataForOrder.getDataForUpdate().getTotalOperations());
        Assert.assertEquals(2, performanceDataForOrder.getDataForInsert().getTotalOperations());
        Assert.assertEquals(2, performanceDataForOrderItem.getDataForFind().getTotalOperations());
        Assert.assertEquals(4, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount() - retrieveCountBeforeAnything);
    }
}
