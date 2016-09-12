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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;

import java.sql.SQLException;



public class TestOr extends TestSqlDatatypes
{

    public void testSimpleOr() throws SQLException
    {
        String sql = "select * from PARA_DESK where SIZE_DOUBLE > 20000.0 or SIZE_DOUBLE < 50.0";
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.sizeDouble().greaterThan(20000.0).or(ParaDeskFinder.sizeDouble().lessThan(50)));
        this.genericRetrievalTest(sql, desks);
    }

    public void testOrWithHiddenAll()
    {
        IntHashSet set = IntHashSet.newSetWith(1, 2, 3);
        IntHashSet empty = new IntHashSet();
        Operation op = OrderFinder.userId().in(set).and(OrderFinder.orderId().notIn(empty));
        assertEquals(OrderFinder.findMany(OrderFinder.all()).size(), OrderFinder.findMany(op).size());
    }

    public void testDoubleMappedOr()
    {
        Operation firstOr = OrderFinder.items().discountPrice().greaterThan(0).or(OrderFinder.userId().eq(1));
        Operation secondOr = OrderFinder.items().discountPrice().lessThan(20).or(OrderFinder.userId().eq(2));
        Operation op = firstOr.and(secondOr);
        OrderFinder.findMany(op).forceResolve();
    }

    public void testOuterJoinLikeOr()
    {
        Operation op = OrderFinder.description().startsWith("S").or(OrderFinder.orderStatus().status().notEq(10));
        OrderFinder.findMany(op).forceResolve();
    }

    public void testEqualsEdgePointWithOr()
    {
        Operation op = AuditedOrderItemFinder.processingDate().equalsEdgePoint();
        Operation orOp = AuditedOrderItemFinder.quantity().greaterThan(10).or(AuditedOrderItemFinder.quantity().lessThan(2));
        assertTrue(OrderItemFinder.findMany(op.and(orOp)).size() > 0);
    }

    public void testEqualsEdgePointWithOr2()
    {
        Operation op = AuditedOrderItemFinder.processingDate().equalsEdgePoint();
        Operation orOne = AuditedOrderItemFinder.quantity().greaterThan(10).and(op);
        Operation orTwo = AuditedOrderItemFinder.quantity().lessThan(2).and(op);
        assertTrue(OrderItemFinder.findMany(orOne.or(orTwo)).size() > 0);
    }
}
