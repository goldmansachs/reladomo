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

package com.gs.fw.common.mithra.aggregate;

import com.gs.fw.finder.OrderBy;


public class AggregateChainedOrderBy implements OrderBy
{

    private OrderBy[] orderBys;

    public AggregateChainedOrderBy(OrderBy first, OrderBy second)
    {
        this.orderBys = new com.gs.fw.finder.OrderBy[2];
        this.orderBys[0] = first;
        this.orderBys[1] = second;
    }

    private AggregateChainedOrderBy(OrderBy[] newOrderbys)
    {
        this.orderBys = newOrderbys;
    }

    public com.gs.fw.finder.OrderBy and(OrderBy other)
    {
        OrderBy[] newOrderbys = new OrderBy[orderBys.length + 1];
        System.arraycopy(orderBys, 0, newOrderbys, 0, orderBys.length);
        newOrderbys[orderBys.length] = other;
        return new AggregateChainedOrderBy(newOrderbys);
    }

    public int compare(Object left, Object right)
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            int result = orderBys[i].compare(left, right);
            if (result != 0) return result;
        }
        return 0;
    }
}
