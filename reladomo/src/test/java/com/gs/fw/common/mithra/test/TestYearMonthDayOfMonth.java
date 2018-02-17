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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

public class TestYearMonthDayOfMonth extends MithraTestAbstract
{
    public void testYearRetrieval()
    {
        Operation eq = ParaDeskFinder.closedDate().year().eq(1981);
        ParaDeskList paraDesk = ParaDeskFinder.findMany(eq);
        assertEquals(13, paraDesk.size());

        MutableIntSet set = new IntHashSet();
        set.addAll(1990, 1981);
        Operation eq2 = ParaDeskFinder.closedDate().year().in(set);
        ParaDeskList paraDesk2 = ParaDeskFinder.findMany(eq2);
        assertEquals(13, paraDesk2.size());

        Order order = OrderFinder.findOne(OrderFinder.orderStatus().expectedDate().year().eq(2005));
        assertEquals("2005-01-01", order.getOrderStatus().getExpectedDate().toString());
    }

    public void testMonthRetrieval()
    {
        Operation eq = ParaDeskFinder.closedDate().month().eq(6);
        ParaDeskList paraDesk = ParaDeskFinder.findMany(eq);
        assertEquals(13, paraDesk.size());

        MutableIntSet set = new IntHashSet();
        set.addAll(12, 6);
        Operation eq2 = ParaDeskFinder.closedDate().month().in(set);
        ParaDeskList paraDesk2 = ParaDeskFinder.findMany(eq2);
        assertEquals(16, paraDesk2.size());

        Order order = OrderFinder.findOne(OrderFinder.orderStatus().expectedDate().month().eq(1));
        assertEquals("2005-01-01", order.getOrderStatus().getExpectedDate().toString());
    }

    public void testDayOfMonthRetrieval()
    {
        Operation eq = ParaDeskFinder.closedDate().dayOfMonth().eq(8);
        ParaDeskList paraDesk = ParaDeskFinder.findMany(eq);
        assertEquals(13, paraDesk.size());

        MutableIntSet set = new IntHashSet();
        set.addAll(1, 8);
        Operation eq2 = ParaDeskFinder.closedDate().dayOfMonth().in(set);
        ParaDeskList paraDesk2 = ParaDeskFinder.findMany(eq2);
        assertEquals(20, paraDesk2.size());

        Order order = OrderFinder.findOne(OrderFinder.orderStatus().expectedDate().dayOfMonth().eq(1));
        assertEquals("2005-01-01", order.getOrderStatus().getExpectedDate().toString());
    }

    public void testTimestampYearRetrieval()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.orderDate().year().eq(2004));
        assertEquals(7, list.size());

        MutableIntSet set = new IntHashSet();
        set.addAll(2004, 1981);
        Operation eq2 = OrderFinder.orderDate().year().in(set);
        OrderList paraDesk2 = OrderFinder.findMany(eq2);
        assertEquals(7, paraDesk2.size());

        Operation operation = OrderFinder.orderStatus().lastUpdateTime().year().eq(2004);
        Order order = OrderFinder.findOne(operation);
        assertEquals("2004-01-12 00:00:00.0", order.getOrderStatus().getLastUpdateTime().toString());
    }

    public void testTimestampMonthRetrieval()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.orderDate().month().eq(4));
        assertEquals(4, list.size());

        MutableIntSet set = new IntHashSet();
        set.addAll(1, 2, 3);
        Operation eq2 = OrderFinder.orderDate().month().in(set);
        OrderList orders = OrderFinder.findMany(eq2);
        assertEquals(3, orders.size());

        Operation operation = OrderFinder.orderStatus().lastUpdateTime().month().eq(1);
        Order order = OrderFinder.findOne(operation);
        assertEquals("2004-01-12 00:00:00.0", order.getOrderStatus().getLastUpdateTime().toString());
    }

    public void testTimestampDayOfMonthRetrieval()
    {
        OrderList list = OrderFinder.findMany(OrderFinder.orderDate().dayOfMonth().eq(12));
        assertEquals(7, list.size());

        MutableIntSet set = new IntHashSet();
        set.addAll(1, 2, 3, 4, 10, 12);
        Operation eq2 = OrderFinder.orderDate().dayOfMonth().in(set);
        OrderList paraDesk2 = OrderFinder.findMany(eq2);
        assertEquals(7, paraDesk2.size());

        Operation operation = OrderFinder.orderStatus().lastUpdateTime().dayOfMonth().eq(12);
        Order order = OrderFinder.findOne(operation);
        assertEquals("2004-01-12 00:00:00.0", order.getOrderStatus().getLastUpdateTime().toString());
    }
}
