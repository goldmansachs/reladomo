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
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.DatedAllTypesFinder;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.TradeFinder;
import com.gs.fw.common.mithra.test.domain.child.ChildTypeFinder;
import com.gs.fw.common.mithra.test.domain.child.ChildTypeList;
import com.gs.fw.common.mithra.test.domain.criters.PetTypeFinder;
import com.gs.fw.common.mithra.test.domain.parent.ParentTypeFinder;
import java.sql.Timestamp;


public class TestOtherOperationsToString extends MithraTestAbstract
{
    public void testAllOperation()
    {
        Operation all = DatedAllTypesFinder.all();
        assertEquals("all of DatedAllTypes", all.toString());
        Operation andAll = DatedAllTypesFinder.booleanValue().eq(false).and(all);
        assertEquals("DatedAllTypes.booleanValue = false", andAll.toString());
        Operation orAll = DatedAllTypesFinder.booleanValue().eq(false).or(all);
        assertEquals("all of DatedAllTypes", orAll.toString());
    }

    public void testNoneOperation()
    {
        Operation none = new None(DatedAllTypesFinder.booleanValue());
        assertEquals("NONE!", none.toString());

        Operation andNone = DatedAllTypesFinder.booleanValue().eq(false).and(none);
        assertEquals("NONE!", andNone.toString());

        Operation orNone = DatedAllTypesFinder.booleanValue().eq(false).or(none);
        assertEquals(" ( ( DatedAllTypes.booleanValue = false ) | ( NONE! ) )", orNone.toString());
    }

    public void testNoOperation()
    {
        Operation no = NoOperation.instance();
        assertEquals("NoOperation", no.toString());

        Operation andNo = DatedAllTypesFinder.booleanValue().eq(false).and(no);
        assertEquals("DatedAllTypes.booleanValue = false", andNo.toString());

        Operation orNo = DatedAllTypesFinder.booleanValue().eq(false).or(no);
        assertEquals("DatedAllTypes.booleanValue = false", orNo.toString());
    }

    public void testExistsWithAnd()
    {
        Operation op = ChildTypeFinder.id().eq(102).and(ChildTypeFinder.pets().exists());
        assertEquals("ChildType.id = 102 & ChildType.pets(ChildType.isAlive = true).ownerId = 102", op.toString());
    }

    public void testExistsWithOr()
    {
        Operation op1 = ChildTypeFinder.id().eq(102).and(ChildTypeFinder.pets().exists());
        Operation op2 = ChildTypeFinder.id().eq(101).and(ChildTypeFinder.pets().exists());
        Operation op = op1.or(op2);
        assertEquals(" ( ( ChildType.id = 102 & ChildType.pets(ChildType.isAlive = true).ownerId = 102 ) | ( ChildType.id = 101 & ChildType.pets(ChildType.isAlive = true).ownerId = 101 ) )", op.toString());
    }

    public void testNotExistsWithAnd()
    {
        Operation op = ChildTypeFinder.id().eq(104).and(ChildTypeFinder.pets().notExists());
        assertEquals("ChildType.id = 104 & ChildType.pets(ChildType.isAlive = true) not exists", op.toString());
    }

    public void testNotExistsWithOr()
    {
        Operation op1 = ChildTypeFinder.id().eq(102).and(ChildTypeFinder.pets().notExists());
        Operation op2 = ChildTypeFinder.id().eq(101).and(ChildTypeFinder.pets().notExists(PetTypeFinder.genome().eq("foo")));
        Operation op = op1.or(op2);
        assertEquals(" ( ( ChildType.id = 102 & ChildType.pets(ChildType.isAlive = true) not exists ) | ( ChildType.id = 101 & ChildType.pets(ChildType.isAlive = true) { PetType.genome = \"foo\" } not exists ) )", op.toString());
        ChildTypeList ct = ChildTypeFinder.findMany(op);
        assertEquals(" ( ( PetType.[ -> ChildType: ownerId = id](ChildType.isAlive = true).id = 102 " +
                "& PetType.[ -> ChildType: ownerId = id](ChildType.isAlive = true).pets(ChildType.isAlive = true) not exists ) " +
                "| ( PetType.[ -> ChildType: ownerId = id](ChildType.isAlive = true).id = 101 " +
                "& PetType.[ -> ChildType: ownerId = id](ChildType.isAlive = true).pets(ChildType.isAlive = true) { PetType.genome = \"foo\" } not exists ) )", ct.getPets().getOperation().toString());
    }

    public void testNotExistsWithRelationships()
    {
        Operation op = OrderStatusFinder.order().items().notExists(OrderItemFinder.state().eq("done"));
        op = op.and(OrderStatusFinder.orderId().eq(1));
        assertEquals("OrderStatus.orderId = 1 & OrderStatus.order.items { OrderItem.state = \"done\" } not exists", op.toString());
    }

    public void testParameterizedRelationships()
    {
        Operation stringParamOperation = ParentTypeFinder.nameEquals("value").exists();
        assertEquals("ParentType.nameEquals(ParentType filters: none, ParentType filters: ParentType.name = \"value\") exists", stringParamOperation.toString());

        Operation doubleParamOperation = OrderFinder.cheapItems(1.00).exists();
        assertEquals("Order.cheapItems(OrderItem.originalPrice < 1.0) exists", doubleParamOperation.toString());

        Operation intParamOperation = OrderFinder.itemForProduct(2).exists();
        assertEquals("Order.itemForProduct(OrderItem.productId = 2) exists", intParamOperation.toString());

        IntHashSet intSet = IntHashSet.newSetWith(3, 4);
        Operation inParamOperation = OrderFinder.itemForProductSet(intSet).exists();
        assertTrue("Order.itemForProductSet(OrderItem.productId in [3, 4]) exists".equals(inParamOperation.toString())
                   || "Order.itemForProductSet(OrderItem.productId in [4, 3]) exists".equals(inParamOperation.toString()));

        Operation timestampParamOperation = TradeFinder.tradesByTradeRef(Timestamp.valueOf("2010-12-31 23:59:00.0"), Timestamp.valueOf("2011-01-01 23:59:00.0")).exists();
        assertEquals("Trade.tradesByTradeRef(Trade filters: none, Trade filters: Trade.processingDate = \"2011-01-01 23:59:00.0\" & Trade.businessDate = \"2010-12-31 23:59:00.0\") exists", timestampParamOperation.toString());
    }
}