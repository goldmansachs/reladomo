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

import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Group;
import com.gs.fw.common.mithra.test.domain.GroupFinder;
import com.gs.fw.common.mithra.test.domain.Location;
import com.gs.fw.common.mithra.test.domain.LocationFinder;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.User;
import com.gs.fw.common.mithra.test.domain.UserFinder;
import com.gs.fw.common.mithra.test.domain.UserGroup;
import com.gs.fw.common.mithra.test.domain.UserList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.SQLException;



public class TestMappedOperation extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Order.class,
            Group.class,
            UserGroup.class,
            Location.class,
                OrderItem.class,
                Product.class,
            User.class
        };
    }

    public void testEqualityAfterMapping()
    {
        Operation throughRel = OrderFinder.items().state().eq("x");
        Operation mapped = new MappedOperation(OrderFinder.zGetOrderItemsReverseMapper(), OrderItemFinder.state().eq("x"));
        assertEquals(throughRel, mapped);
        throughRel = OrderFinder.items().discountPrice().absoluteValue().greaterThan(6);
        mapped = new MappedOperation(OrderFinder.zGetOrderItemsReverseMapper(), OrderItemFinder.discountPrice().absoluteValue().greaterThan(6));
        assertEquals(throughRel, mapped);

        throughRel = OrderFinder.items().state().eq("x").and(OrderFinder.items().discountPrice().absoluteValue().greaterThan(6));
        mapped = new MappedOperation(OrderFinder.zGetOrderItemsReverseMapper(),
                OrderItemFinder.state().eq("x").and(OrderItemFinder.discountPrice().absoluteValue().greaterThan(6)));
        assertEquals(throughRel, mapped);

        throughRel = UserFinder.groups().locations().city().eq("NY");
        mapped = new MappedOperation(UserFinder.zGetUserGroupsReverseMapper(),
                new MappedOperation(GroupFinder.zGetGroupLocationsReverseMapper(), LocationFinder.city().eq("NY")));

        assertEquals(throughRel, mapped);

        throughRel = UserFinder.groups().locations().city().eq("NY").and(UserFinder.groups().manager().id().eq(1));
        mapped = new MappedOperation(UserFinder.zGetUserGroupsReverseMapper(),
                new MappedOperation(GroupFinder.zGetGroupLocationsReverseMapper(), LocationFinder.city().eq("NY")));
        mapped = mapped.and(new MappedOperation(UserFinder.zGetUserGroupsReverseMapper(),
                new MappedOperation(GroupFinder.zGetGroupManagerReverseMapper(), UserFinder.id().eq(1))));
        throughRel.equals(mapped);
        assertEquals(throughRel, mapped);

        new UserList(throughRel.and(UserFinder.sourceId().eq(0))).forceResolve();
    }

    public void testQualifiedMapper() throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
        OrderList ol = new OrderList(OrderFinder.state().eq(OrderFinder.items().state()));
        String sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i where o.ORDER_ID = i.ORDER_ID and o.STATE = i.STATE";

        this.genericRetrievalTest(sql, ol);

        ol = new OrderList(OrderFinder.items().originalPrice().eq(OrderFinder.items().discountPrice()));
        sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i where o.ORDER_ID = i.ORDER_ID and i.ORIGINAL_PRICE = i.DISCOUNT_PRICE";
        this.genericRetrievalTest(sql, ol);
    }

    public void testToManyRelationshipInOperation() throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
        Operation op = OrderFinder.items().quantity().greaterThan(10);
        OrderList ol = new OrderList(op);

        String sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i where o.ORDER_ID = i.ORDER_ID and i.QUANTITY > 10";

        this.genericRetrievalTest(sql, ol);

        op = op.and(OrderFinder.items().productInfo().productCode().endsWith("A"));
        ol = new OrderList(op);

        sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i, APP.PRODUCT p where o.ORDER_ID = i.ORDER_ID and i.QUANTITY > 10" +
                " and i.PRODUCT_ID = p.PROD_ID and p.CODE like '%A'";

        this.genericRetrievalTest(sql, ol);

        op = OrderFinder.items().quantity().greaterThan(10);
        op = op.and(OrderFinder.userId().eq(1));
        op = op.and(OrderFinder.items().originalPrice().lessThan(20));
        ol = new OrderList(op);

        sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i where o.ORDER_ID = i.ORDER_ID and i.QUANTITY > 10" +
                " and i.ORIGINAL_PRICE < 20 and o.USER_ID = 1";

        this.genericRetrievalTest(sql, ol);
    }

    public void testToManyRelationshipLargeInClause() throws SQLException
    {
        IntHashSet set = new IntHashSet();
        for(int i=0;i<2000;i++)
        {
            set.add(i);
        }
        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
        Operation op = OrderFinder.items().productId().in(set);
        OrderList ol = new OrderList(op);

        String sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i where o.ORDER_ID = i.ORDER_ID";

        this.genericRetrievalTest(sql, ol);
    }

    public void testToManyRelationshipDeepFetch() throws SQLException
    {
        OrderList toInsert = new OrderList();
        for(int i=0;i<1000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setState("x"+i);
            order.setUserId(i);
            order.setDescription("d"+i);
            toInsert.add(order);
        }
        toInsert.insertAll();
        OrderItemList itemsToInsert = new OrderItemList();
        for(int i=0;i<1000;i++)
        {
            OrderItem item = new OrderItem();
            item.setId(i*2+10000);
            item.setOrderId(i+10000);
            item.setState("x"+i);
            item.setQuantity(15);
            itemsToInsert.add(item);
            item = new OrderItem();
            item.setId(i*2+10000+1);
            item.setOrderId(i+10000);
            item.setState("x"+i);
            item.setQuantity(17);
            itemsToInsert.add(item);
        }
        itemsToInsert.insertAll();

        this.setMithraTestObjectToResultSetComparator(new OrderTestResultSetComparator());
        Operation op = OrderFinder.items().quantity().greaterThan(10);
        OrderList ol = new OrderList(op);
        ol.deepFetch(OrderFinder.items());

        String sql = "select distinct o.* from APP.ORDERS o, APP.ORDER_ITEM i where o.ORDER_ID = i.ORDER_ID and i.QUANTITY > 10";

        this.genericRetrievalTest(sql, ol);
    }
}
