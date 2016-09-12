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

import com.gs.fw.common.mithra.test.domain.*;



public class TestFilterEquality extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            OrderItem.class,
                Trial.class,
                Account.class,
                TamsAccount.class,
                BusinessUnitGroup.class

        };
    }

    public void testDeepFilterEq()
    {
        //todo: fix full cache case
        if (!AccountFinder.isFullCache())
        {
            AccountFinder.findMany(AccountFinder.deskId().eq("A").and(AccountFinder.trial().bug().id().filterEq(AccountFinder.tamsAccount().code()))).forceResolve();
        }
    }

    public void testDoubleSelfEquality() throws Exception
    {
        OrderItemList list = new OrderItemList(OrderItemFinder.orderId().eq(2));
        OrderItemList shortList = new OrderItemList(OrderItemFinder.orderId().eq(2).and(
                OrderItemFinder.originalPrice().filterEq(OrderItemFinder.discountPrice())));
        assertTrue(shortList.size() > 0);
        assertTrue(list.size() > shortList.size());
        for(int i=0;i<shortList.size();i++)
        {
            OrderItem item = shortList.getOrderItemAt(i);
            assertTrue(item.getOriginalPrice() == item.getDiscountPrice());
        }
    }

    public void testRelatedDoubleEquality() throws Exception
    {
        OrderList list = new OrderList(OrderFinder.items().originalPrice().filterEq(OrderFinder.items().discountPrice()));
        assertTrue(list.size() > 0);
        Order order = list.getOrderAt(0);
        assertTrue(order.getItems().size() > 0);
    }
}
