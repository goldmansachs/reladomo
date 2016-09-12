
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

package com.gs.fw.common.mithra.finder.orderby;

import com.gs.fw.common.mithra.finder.AsOfEqualityChecker;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;

import java.util.Arrays;
import java.util.Set;

public class ChainedOrderBy implements OrderBy
{

    private OrderBy[] orderBys;

    public ChainedOrderBy(OrderBy first, OrderBy second)
    {
        this.orderBys = new OrderBy[2];
        this.orderBys[0] = first;
        this.orderBys[1] = second;
    }

    private ChainedOrderBy(OrderBy[] newOrderbys)
    {
        this.orderBys = newOrderbys;
    }

    public OrderBy and(com.gs.fw.finder.OrderBy other)
    {
        OrderBy[] newOrderbys = new OrderBy[orderBys.length + 1];
        System.arraycopy(orderBys, 0, newOrderbys, 0, orderBys.length);
        newOrderbys[orderBys.length] = (com.gs.fw.common.mithra.finder.orderby.OrderBy) other;
        return new ChainedOrderBy(newOrderbys);
    }

    public void generateMapperSql(SqlQuery sqlQuery)
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            orderBys[i].generateMapperSql(sqlQuery);
        }
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            orderBys[i].addDepenedentAttributesToSet(set);
        }
    }

    public void registerSourceOperation(MithraDatabaseIdentifierExtractor extractor)
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            orderBys[i].registerSourceOperation(extractor);
        }
    }

    public void registerAsOfAttributes(AsOfEqualityChecker asOfEqualityChecker)
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            orderBys[i].registerAsOfAttributes(asOfEqualityChecker);
        }
    }

    public boolean mustUseServerSideOrderBy()
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            if (orderBys[i].mustUseServerSideOrderBy()) return true;
        }
        return false;
    }

    public void generateSql(SqlQuery query)
    {
        for (int i = 0; i < orderBys.length; i++)
        {
            orderBys[i].generateSql(query);
        }
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

    public int hashCode()
    {
        return Arrays.hashCode(this.orderBys);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        boolean result = false;
        if (obj instanceof ChainedOrderBy)
        {
            result = Arrays.equals(this.orderBys, (((ChainedOrderBy) obj).orderBys));
        }
        return result;
    }
}
