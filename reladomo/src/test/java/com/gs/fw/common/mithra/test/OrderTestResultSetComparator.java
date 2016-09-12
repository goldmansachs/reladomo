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

import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderData;

import java.sql.ResultSet;
import java.sql.SQLException;



public class OrderTestResultSetComparator implements MithraTestObjectToResultSetComparator
{

    public Object getPrimaryKeyFrom(Object mithraObject) throws SQLException
    {
        return Integer.valueOf(((Order) mithraObject).getOrderId());
    }

    public Object getPrimaryKeyFrom(ResultSet rs) throws SQLException
    {
        return Integer.valueOf(rs.getInt("ORDER_ID"));
    }

    public Object createObjectFrom(ResultSet rs) throws SQLException
    {
        OrderData data = new OrderData();
        data.setOrderId(rs.getInt(1));
        data.setOrderDate(rs.getTimestamp(2));
        data.setUserId(rs.getInt(3));
        data.setDescription(rs.getString(4));
        data.setState(rs.getString(5));
        data.setTrackingId(rs.getString(6));

        Order order = new Order();
        order.zSetNonTxData(data);
        return order;
    }


}
