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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.MithraArrayTupleTupleSet;
import com.gs.fw.common.mithra.util.TupleSet;

import java.util.Calendar;

public class TestYearMonthTuple extends MithraTestAbstract
{
    public void testValueOf()
    {
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.all());
        list.setOrderBy(ParaDeskFinder.closedDate().ascendingOrderBy());
        Calendar cal = Calendar.getInstance();
        for(ParaDesk desk : list)
        {
            cal.setTime(desk.getClosedDate());
            assertEquals(cal.get(Calendar.YEAR), ParaDeskFinder.closedDate().year().intValueOf(desk));
            assertEquals(cal.get(Calendar.MONTH) + 1, ParaDeskFinder.closedDate().month().intValueOf(desk));
            assertEquals(cal.get(Calendar.DAY_OF_MONTH), ParaDeskFinder.closedDate().dayOfMonth().intValueOf(desk));
        }
    }

    public void testTupleYearMonthSet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(1981, 6);
        set.add(1900, 1);
        set.add(1900, 2);
        set.add(1900, 3);
        set.add(1900, 4);
        set.add(1900, 5);
        set.add(1900, 6);
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.closedDate().year().tupleWith(ParaDeskFinder.closedDate().month()).in(set));
        assertEquals(17, list.size());
    }

    public void testTupleYearMonthTinySet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(1981, 6);
        set.add(1900, 1);
        Operation in = ParaDeskFinder.closedDate().year().tupleWith(ParaDeskFinder.closedDate().month()).in(set);
        System.out.println(in);
        ParaDeskList list = new ParaDeskList(in);
        assertEquals(17, list.size());
    }

    public void testTupleYearDayOfMonthTinySet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(1981, 8);
        set.add(1900, 1);
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.closedDate().year().tupleWith(ParaDeskFinder.closedDate().dayOfMonth()).in(set));
        assertEquals(17, list.size());
    }

    public void testTupleMonthDayOfMonthTinySet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(6, 8);
        set.add(12, 1);
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.closedDate().month().tupleWith(ParaDeskFinder.closedDate().dayOfMonth()).in(set));
        assertEquals(16, list.size());
    }

    public void testTimestampValueOf()
    {
        OrderList list = new OrderList(OrderFinder.all());
        list.setOrderBy(OrderFinder.orderDate().ascendingOrderBy());
        Calendar cal = Calendar.getInstance();
        for(Order order : list)
        {
            cal.setTime(order.getOrderDate());
            assertEquals(cal.get(Calendar.YEAR), OrderFinder.orderDate().year().intValueOf(order));
            assertEquals(cal.get(Calendar.MONTH) + 1, OrderFinder.orderDate().month().intValueOf(order));
            assertEquals(cal.get(Calendar.DAY_OF_MONTH), OrderFinder.orderDate().dayOfMonth().intValueOf(order));
        }
    }

    public void testTimestampTupleYearMonthSet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(2004, 6);
        set.add(2004, 1);
        set.add(2004, 2);
        set.add(2004, 3);
        set.add(2004, 5);
        set.add(2004, 6);
        OrderList list = new OrderList(OrderFinder.orderDate().year().tupleWith(OrderFinder.orderDate().month()).in(set));
        assertEquals(3, list.size());
    }

    public void testTimestampTupleYearMonthTinySet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(2004, 2);
        set.add(2004, 1);
        Operation in = OrderFinder.orderDate().year().tupleWith(OrderFinder.orderDate().month()).in(set);
        System.out.println(in);
        OrderList list = new OrderList(in);
        assertEquals(2, list.size());
    }

    public void testTimestampTupleYearDayOfMonthTinySet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(2004, 4);
        set.add(2004, 12);
        Operation in = OrderFinder.orderDate().year().tupleWith(OrderFinder.orderDate().dayOfMonth()).in(set);
        OrderList list = new OrderList(in);
        assertEquals(7, list.size());
    }

    public void testTimestampTupleMonthDayOfMonthTinySet()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add(1, 12);
        set.add(2, 12);
        OrderList list = new OrderList(OrderFinder.orderDate().month().tupleWith(OrderFinder.orderDate().dayOfMonth()).in(set));
        assertEquals(2, list.size());
    }
}
