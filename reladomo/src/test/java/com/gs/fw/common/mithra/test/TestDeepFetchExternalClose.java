
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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

public class TestDeepFetchExternalClose extends MithraTestAbstract
{
    public void testDatedDeepFetchWithExternalCloseOfRelated() throws SQLException
    {
        Operation operation = AuditedOrderFinder.processingDate().eq(Timestamp.valueOf("2013-01-01 12:00:00.0"))
                .and(AuditedOrderFinder.orderId().eq(1));
        AuditedOrderList orderList = new AuditedOrderList(operation);
        orderList.deepFetch(AuditedOrderFinder.orderStatus());

        assertEquals(10, orderList.get(0).getOrderStatus().getStatus());

        this.closeAuditedOrderStatus();

        AuditedOrderList orderListAfter = new AuditedOrderList(operation);
        orderListAfter.deepFetch(AuditedOrderFinder.orderStatus());
        orderListAfter.setBypassCache(true);

        assertNull(orderListAfter.get(0).getOrderStatus());

        assertNotNull(AuditedOrderStatusFinder.findOne(AuditedOrderStatusFinder.orderId().eq(1).and(AuditedOrderStatusFinder.processingDate().eq(Timestamp.valueOf("2012-01-01 12:00:00.0")))));
    }

    private void closeAuditedOrderStatus() throws SQLException
    {
        final Connection connection = getConnection();
        Statement statement = null;
        try
        {
            statement = connection.createStatement();
            statement.executeUpdate("update AUDITED_ORDER_STATUS set OUT_Z = '2013-01-01 11:00:00.0' where ORDER_ID = 1 and OUT_Z > '9999-01-01'");
        }
        finally
        {
            if (statement != null)
            {
                statement.close();
            }
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    public void testDeepFetchWithExternalCloseOfRelated() throws SQLException
    {
        Operation operation = OrderFinder.orderId().eq(1);
        OrderList orderList = new OrderList(operation);
        orderList.deepFetch(OrderFinder.orderStatus());

        assertEquals(10, orderList.get(0).getOrderStatus().getStatus());

        this.closeOrderStatus();

        OrderList orderListAfter = new OrderList(operation);
        orderListAfter.deepFetch(OrderFinder.orderStatus());
        orderListAfter.setBypassCache(true);

        assertNull(orderListAfter.get(0).getOrderStatus());
    }

    private void closeOrderStatus() throws SQLException
    {
        final Connection connection = getConnection();
        Statement statement = null;
        try
        {
            statement = connection.createStatement();
            statement.executeUpdate("DELETE APP.ORDER_STATUS where ORDER_ID = 1");
        }
        finally
        {
            if (statement != null)
            {
                statement.close();
            }
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    private void closeProductSynonym() throws SQLException
    {
        final Connection connection = getConnection();
        Statement statement = null;
        try
        {
            statement = connection.createStatement();
            statement.executeUpdate("DELETE APP.PRODUCT_SYN where PROD_ID = 1 and SYN_TYPE='CUS'");
        }
        finally
        {
            if (statement != null)
            {
                statement.close();
            }
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    public void testParametrizedNonDated() throws Exception
    {
        ProductList list = ProductFinder.findMany(ProductFinder.productId().greaterThan(0));
        list.deepFetch(ProductFinder.synonymByType("CUS"));
        list.setOrderBy(ProductFinder.productId().ascendingOrderBy());

        assertNotNull(list.get(0).getSynonymByType("CUS"));

        closeProductSynonym();

        ProductList after = ProductFinder.findMany(ProductFinder.productId().greaterThan(0));
        after.deepFetch(ProductFinder.synonymByType("CUS"));
        after.setOrderBy(ProductFinder.productId().ascendingOrderBy());
        after.setBypassCache(true);

        assertNull(after.get(0).getSynonymByType("CUS"));

    }
}
