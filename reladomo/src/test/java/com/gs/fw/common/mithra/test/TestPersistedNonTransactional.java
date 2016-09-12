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
import com.gs.fw.common.mithra.test.domain.*;

public class TestPersistedNonTransactional extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                OrderAsTxReadOnly.class, AuditedOrderAsTxReadOnly.class, OrderStatusAsTxReadOnly.class
        };
    }

    /* Order and OrderStatus are both configured with ReadOnly txParticipation*/
    public void testRetrievingFromMithraCacheUsingRelationship()
    {
        OrderAsTxReadOnly order = OrderAsTxReadOnlyFinder.findOne(OrderAsTxReadOnlyFinder.orderId().eq(1));
        //first retrieval goes to DB and retrieves successfully
        assertNotNull(order.getOrderStatus());
        //second retrieval goes to cache and throws
        assertNotNull(order.getOrderStatus());
    }

    public void testNonTxRead()
    {
        OrderAsTxReadOnlyList orders = OrderAsTxReadOnlyFinder.findMany(OrderAsTxReadOnlyFinder.userId().eq(1));
        assertEquals(3, orders.size());

        AuditedOrderAsTxReadOnlyList moreOrders = AuditedOrderAsTxReadOnlyFinder.findMany(AuditedOrderAsTxReadOnlyFinder.userId().eq(1));
        assertEquals(4, moreOrders.size());
    }

    public void testNonTxReadInTx()
    {
        final OrderAsTxReadOnlyList orders = OrderAsTxReadOnlyFinder.findMany(OrderAsTxReadOnlyFinder.userId().eq(1));
        assertEquals(3, orders.size());

        final AuditedOrderAsTxReadOnlyList moreOrders = AuditedOrderAsTxReadOnlyFinder.findMany(AuditedOrderAsTxReadOnlyFinder.userId().eq(1));
        assertEquals(4, moreOrders.size());
        int count = this.getRetrievalCount();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                orders.setOrderBy(OrderAsTxReadOnlyFinder.orderId().ascendingOrderBy());
                assertEquals(1, orders.get(0).getOrderId());
                assertEquals(1, orders.get(0).getUserId());

                moreOrders.setOrderBy(AuditedOrderAsTxReadOnlyFinder.orderId().ascendingOrderBy());
                assertEquals(1, moreOrders.get(0).getOrderId());
                assertEquals(1, moreOrders.get(0).getUserId());

                return null;
            }
        });
        assertEquals(count, this.getRetrievalCount());
    }


}
