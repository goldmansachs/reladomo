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

import com.gs.collections.impl.set.mutable.primitive.DoubleHashSet;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.test.domain.*;

import java.util.List;

public class TestSubQueryCache extends MithraTestAbstract
{
    public void testGreaterThanEqualityInteger()
    {
        Operation op = OrderFinder.userId().greaterThan(0);
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderSubQuery(op, subOp);
    }

    public void testGreaterThanEqualsEqualityInteger()
    {
        Operation op = OrderFinder.userId().greaterThanEquals(2);
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderSubQuery(op, subOp);
    }

    public void testLessThanEqualityInteger()
    {
        Operation op = OrderFinder.userId().lessThan(3);
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderSubQuery(op, subOp);
    }

    public void testLessThanEqualsEqualityInteger()
    {
        Operation op = OrderFinder.userId().lessThanEquals(2);
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderSubQuery(op, subOp);
    }

    public void testRangeMultiEquality()
    {
        Operation op = OrderFinder.userId().greaterThan(0);
        Operation subOp = OrderFinder.userId().eq(2).and(OrderFinder.state().eq("In-Progress"));
        assertOrderSubQuery(op, subOp);
    }

    public void testRangeEqualityIntegerNotInRange()
    {
        Operation op = OrderFinder.userId().greaterThan(2);
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderNotSubQuery(op, subOp);
    }

    public void testLimitedRangeEqualityInteger()
    {
        Operation op = OrderFinder.userId().greaterThan(0).and(OrderFinder.userId().lessThan(10));
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderSubQuery(op, subOp);
    }

    public void testAndEq()
    {
        Operation op = OrderFinder.userId().eq(1).and(OrderFinder.items().originalPrice().greaterThan(0));
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderNotSubQuery(op, subOp);
    }

    public void testOrAnd()
    {
        Operation op = OrderFinder.userId().eq(1).and(OrderFinder.orderDate().greaterThan(newTimestamp("2004-01-12 00:00:00"))).or(OrderFinder.state().eq("Gummed up"));
        Operation subOp = OrderFinder.userId().eq(1).and(OrderFinder.orderDate().greaterThan(newTimestamp("2004-02-12 00:00:00")));
        assertOrderSubQuery(op, subOp);
    }

    public void testAndMultiEquality()
    {
        Operation op = OrderFinder.description().greaterThan("A").and(OrderFinder.orderId().lessThan(1000));
        Operation subOp = OrderFinder.description().eq("First order").and(OrderFinder.orderId().eq(1));
        assertOrderSubQuery(op, subOp);
    }

    public void testMappedOperationNotSubqueried()
    {
        Operation op = OrderFinder.userId().eq(1);
        Operation subOp = op.and(OrderFinder.items().originalPrice().greaterThan(2));
        assertOrderNotSubQuery(op, subOp);
    }

    public void testExactMappedOperation()
    {
        Operation mappedOp = OrderFinder.items().originalPrice().greaterThan(2);

        Operation op = OrderFinder.userId().greaterThan(0).and(mappedOp);
        Operation subOp = OrderFinder.userId().eq(1).and(mappedOp);

        assertOrderSubQuery(op, subOp);
    }

    public void testNotNullWithRanged()
    {
        Operation op = OrderFinder.userId().isNotNull();
        Operation subOp = OrderFinder.userId().greaterThan(1);
        assertOrderSubQuery(op, subOp);
    }

    public void testNotNullWithEquality()
    {
        Operation op = OrderFinder.userId().isNotNull();
        Operation subOp = OrderFinder.userId().eq(1);
        assertOrderSubQuery(op, subOp);
    }

    public void testNotNullIn()
    {
        Operation op = OrderFinder.userId().isNotNull();
        Operation subOp = OrderFinder.userId().in(createSetOneTwo());
        assertOrderSubQuery(op, subOp);
    }

    public void testNotEqualityWithEquality()
    {
        Operation op = OrderFinder.userId().notEq(2);
        Operation subOp = OrderFinder.userId().eq(1);
        assertOrderSubQuery(op, subOp);
    }

    public void testNotEqualityIn()
    {
        Operation op = OrderFinder.userId().notEq(3);
        Operation subOp = OrderFinder.userId().in(createSetOneTwo());
        assertOrderSubQuery(op, subOp);
    }

    public void testNotEqualityInNoMatch()
    {
        Operation op = OrderFinder.userId().notEq(2);
        Operation subOp = OrderFinder.userId().in(createSetOneTwo());
        assertOrderNotSubQuery(op, subOp);
    }

    public void testNotEqualityWithEqualitySameValue()
    {
        Operation op = OrderFinder.userId().notEq(1);
        Operation subOp = OrderFinder.userId().eq(1);
        assertOrderNotSubQuery(op, subOp);
    }

    public void testNotEqualityNotIn()
    {
        Operation op = OrderFinder.userId().notEq(1);
        Operation subOp = OrderFinder.userId().notIn(createSetOneTwo());
        assertOrderSubQuery(op, subOp);
    }

    public void testInWithIsNull()
    {
        Order order = new Order();
        order.setOrderId(1000);
        order.setUserIdNull();
        order.insert();

        Operation op = OrderFinder.userId().in(createSetOneTwo());
        Operation subOp = OrderFinder.userId().isNull();
        assertOrderNotSubQuery(op, subOp);
    }

    public void testInInNoMatch()
    {
        Operation op = OrderFinder.userId().in(createSetOneTwo());

        IntHashSet set2 = createSetOneTwo();
        set2.add(3);
        Operation subOp = OrderFinder.userId().in(set2);
        assertOrderNotSubQuery(op, subOp);
    }

    public void testInIn()
    {
        IntHashSet set2 = createSetOneTwo();
        set2.add(3);
        Operation op = OrderFinder.userId().in(set2);

        Operation subOp = OrderFinder.userId().in(createSetOneTwo());
        assertOrderSubQuery(op, subOp);
    }

    public void testNotInWithIsNull()
    {
        Order order = new Order();
        order.setOrderId(1000);
        order.setUserIdNull();
        order.insert();

        Operation op = OrderFinder.userId().notIn(createSetOneTwo());
        Operation subOp = OrderFinder.userId().isNull();
        assertOrderNotSubQuery(op, subOp);
    }

    public void testNotInEquality()
    {
        Order order = new Order();
        order.setOrderId(1000);
        order.setUserIdNull();
        order.insert();

        Operation op = OrderFinder.userId().notIn(createSetOneTwo());
        Operation subOp = OrderFinder.userId().eq(3);
        assertOrderSubQuery(op, subOp);
    }

    public void testNotInEqualityInSet()
    {
        Order order = new Order();
        order.setOrderId(1000);
        order.setUserIdNull();
        order.insert();

        Operation op = OrderFinder.userId().notIn(createSetOneTwo());
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderNotSubQuery(op, subOp);
    }

    public void testNotInNotIn()
    {
        IntHashSet set2 = createSetOneTwo();
        set2.add(10);

        Operation op = OrderFinder.userId().notIn(createSetOneTwo());
        Operation subOp = OrderFinder.userId().notIn(set2);
        assertOrderSubQuery(op, subOp);
    }

    public void testNotInNotInNoMatch()
    {
        IntHashSet set2 = createSetOneTwo();
        set2.add(10);

        Operation op = OrderFinder.userId().notIn(set2);
        Operation subOp = OrderFinder.userId().notIn(createSetOneTwo());
        assertOrderNotSubQuery(op, subOp);
    }

    public void testNotInIn()
    {
        Operation op = OrderFinder.userId().notIn(createSetOneTwo());
        Operation subOp = OrderFinder.userId().in(createSetOneTwo());
        assertOrderNotSubQuery(op, subOp);
    }

    public void testSelfEqualityIn()
    {
        Operation op = OrderFinder.userId().filterEq(OrderFinder.orderId());
        Operation subOp = OrderFinder.userId().in(createSetOneTwo());
        assertOrderNotSubQuery(op, subOp);
    }

    public void testOrEqualityInteger()
    {
        Operation op = OrderFinder.userId().greaterThan(0).or(OrderFinder.orderId().isNotNull());
        Operation subOp = OrderFinder.userId().eq(2);
        assertOrderSubQuery(op, subOp);
    }

    public void testRangeWithTripleAnd()
    {
        Operation op = OrderFinder.userId().greaterThan(0);
        Operation subOp = OrderFinder.userId().eq(2);
        subOp = subOp.and(OrderFinder.orderId().greaterThan(0));
        subOp = subOp.and(OrderFinder.description().isNotNull());
        assertOrderSubQuery(op, subOp);
    }

    public void testRangeWithTripleAndUnfilterable()
    {
        Operation op = OrderFinder.userId().greaterThan(0);
        Operation subOp = OrderFinder.userId().eq(1);
        subOp = subOp.and(OrderFinder.orderId().greaterThan(0));
        subOp = subOp.and(OrderFinder.items().originalPrice().greaterThan(0));
        assertOrderNotSubQuery(op, subOp);
    }

    public void testRangeWithDoubleAndShapeExact()
    {
        Operation op = OrderFinder.userId().eq(1);
        Operation subOp = OrderFinder.userId().eq(2);
        subOp = subOp.and(OrderFinder.orderId().greaterThan(0));
        assertOrderNotSubQuery(op, subOp);
    }

    public void testRangeWithTripleAndShapeExact()
    {
        Operation op = OrderFinder.userId().eq(1);
        Operation subOp = OrderFinder.userId().eq(2);
        subOp = subOp.and(OrderFinder.orderId().greaterThan(0));
        subOp = subOp.and(OrderFinder.description().isNotNull());
        assertOrderNotSubQuery(op, subOp);
    }


    // double tests
    public void testGreaterThanEqualityDouble()
    {
        Operation op = OrderItemFinder.originalPrice().greaterThan(0);
        Operation subOp = OrderItemFinder.originalPrice().eq(10.5);
        assertSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testGreaterThanEqualsEqualityDouble()
    {
        Operation op = OrderItemFinder.originalPrice().greaterThanEquals(2);
        Operation subOp = OrderItemFinder.originalPrice().eq(10.5);
        assertSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testLessThanEqualityDouble()
    {
        Operation op = OrderItemFinder.originalPrice().lessThan(11);
        Operation subOp = OrderItemFinder.originalPrice().eq(10.5);
        assertSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testLessThanEqualsEqualityDouble()
    {
        Operation op = OrderItemFinder.originalPrice().lessThanEquals(11);
        Operation subOp = OrderItemFinder.originalPrice().eq(10.5);
        assertSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testInInNoMatchDouble()
    {
        Operation op = OrderItemFinder.originalPrice().in(DoubleHashSet.newSetWith(10.5, 15.5));
        Operation subOp = OrderItemFinder.originalPrice().in(DoubleHashSet.newSetWith(10.5, 15.5, 20.5));
        assertNotSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testInInDouble()
    {
        Operation op = OrderItemFinder.originalPrice().in(DoubleHashSet.newSetWith(10.5, 15.5, 20.5));
        Operation subOp = OrderItemFinder.originalPrice().in(DoubleHashSet.newSetWith(10.5, 15.5));
        assertSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testNotInNotInDouble()
    {
        insertOrderItem();

        Operation op = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5));
        Operation subOp = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5, 20.5));
        assertSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    private void insertOrderItem()
    {
        OrderItem item = new OrderItem();
        item.setId(1000);
        item.setOrderId(1);
        item.setOriginalPrice(1.5);
        item.insert();
    }

    public void testNotInNotInNoMatchDouble()
    {
        insertOrderItem();

        Operation op = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5, 20.5));
        Operation subOp = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5));
        assertNotSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testNotInNotInNoMatchSameSizeDouble()
    {
        insertOrderItem();

        Operation op = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5, 20.5));
        Operation subOp = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5, 1.5));
        assertNotSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testNotInInDouble()
    {
        Operation op = OrderItemFinder.originalPrice().notIn(DoubleHashSet.newSetWith(10.5, 15.5));
        Operation subOp = OrderItemFinder.originalPrice().in(DoubleHashSet.newSetWith(10.5, 15.5));
        assertNotSubQuery(op, subOp, OrderItemFinder.getFinderInstance());
    }

    public void testEqualsEdgePointNoMatch()
    {
        Operation op = AuditedOrderFinder.processingDate().equalsEdgePoint();
        Operation subOp = AuditedOrderFinder.processingDate().equalsInfinity();
        assertNotSubQuery(op, subOp, AuditedOrderFinder.getFinderInstance());
    }

    public void testEqualsEdgePointInAnd()
    {
        Operation op = AuditedOrderFinder.description().greaterThan("A").and(AuditedOrderFinder.processingDate().equalsEdgePoint());
        Operation subOp = AuditedOrderFinder.description().eq("First order").and(AuditedOrderFinder.processingDate().equalsEdgePoint());
        assertSubQuery(op, subOp, AuditedOrderFinder.getFinderInstance());
    }

    public void testEqualsEdgePoint()
    {
        Operation op = AuditedOrderFinder.processingDate().equalsEdgePoint();
        Operation subOp = AuditedOrderFinder.description().eq("First order").and(AuditedOrderFinder.processingDate().equalsEdgePoint());
        assertSubQuery(op, subOp, AuditedOrderFinder.getFinderInstance());
    }

    public void testEqualsEdgePointInAndNoMatch()
    {
        Operation op = AuditedOrderFinder.description().greaterThan("A").and(AuditedOrderFinder.processingDate().equalsEdgePoint());
        Operation subOp = AuditedOrderFinder.description().eq("First order");
        assertNotSubQuery(op, subOp, AuditedOrderFinder.getFinderInstance());
    }

    public void testEqualsEdgePointInAnd2()
    {
        Operation op = AuditedOrderFinder.orderId().greaterThan(0).and(AuditedOrderFinder.processingDate().equalsEdgePoint());
        Operation subOp = AuditedOrderFinder.orderId().greaterThan(1);
        assertNotSubQuery(op, subOp, AuditedOrderFinder.getFinderInstance());
    }

    // util methods
    private IntHashSet createSetOneTwo()
    {
        return IntHashSet.newSetWith(1, 2);
    }

    private void assertOrderNotSubQuery(Operation op, Operation subOp)
    {
        RelatedFinder finderInstance = OrderFinder.getFinderInstance();
        assertNotSubQuery(op, subOp, finderInstance);
    }

    private void assertNotSubQuery(Operation op, Operation subOp, RelatedFinder finderInstance)
    {
        List many = finderInstance.findMany(op);
        assertTrue(!many.isEmpty());

        int retrievalCount = this.getRetrievalCount();
        List subList = finderInstance.findMany(subOp);
        int subSize = subList.size();
        assertTrue(subSize > 0);
        if (!OrderFinder.getMithraObjectPortal().isFullyCached())
        {
            assertEquals(retrievalCount + 1, this.getRetrievalCount());
        }
    }

    private void assertOrderSubQuery(Operation op, Operation subOp)
    {
        RelatedFinder finderInstance = OrderFinder.getFinderInstance();
        assertSubQuery(op, subOp, finderInstance);
    }

    private void assertSubQuery(Operation op, Operation subOp, RelatedFinder finderInstance)
    {
        List many = finderInstance.findMany(op);
        assertTrue(!many.isEmpty());

        int retrievalCount = this.getRetrievalCount();
        List subList = finderInstance.findMany(subOp);
        int subSize = subList.size();
        assertTrue(subSize > 0);
        assertEquals(retrievalCount, this.getRetrievalCount());

        assertEquals(subSize, finderInstance.findManyBypassCache(subOp).size());
    }
}
