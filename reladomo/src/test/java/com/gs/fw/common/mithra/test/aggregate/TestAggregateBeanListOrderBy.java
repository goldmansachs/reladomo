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

package com.gs.fw.common.mithra.test.aggregate;

import com.gs.fw.common.mithra.AggregateBeanList;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.Seller;


public class TestAggregateBeanListOrderBy extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Sale.class,
                        Seller.class,
                        SalesLineItem.class
                };
    }

    public void testAggregateBeanListInvalidOrderByAttribute()
    {

        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("id", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        try
        {
            list.addOrderBy("invalid", true);
            fail("Order should be allowed on a attribute which is neither a group by nor a aggregate attribute");
        }
        catch (MithraBusinessException mbe)
        {
            assertNotNull(mbe.getMessage());
        }
    }

    public void testAggregateBeanListSimpleOrderBySellerName()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("id", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("saleDescription", SaleFinder.description().min());
        list.addOrderBy("sellerName", true);

        assertEquals(6, list.size());
        assertEquals(list.get(0).getSellerName(), "Jane Doe");
        assertEquals(list.get(1).getSellerName(), "John Doe");
        assertEquals(list.get(2).getSellerName(), "John Doe");
        assertEquals(list.get(3).getSellerName(), "Moh Rezaei");
        assertEquals(list.get(4).getSellerName(), "Moh Rezaei");
        assertEquals(list.get(5).getSellerName(), "Rafael Gonzalez");

    }

    public void testAggregateBeanListMultipleOrderBys()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("id", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("saleDescription", SaleFinder.description().min());
        list.addOrderBy("sellerName", true);
        list.addOrderBy("manufacturerCount", true);
        list.addOrderBy("id", false);
        assertEquals(6, list.size());

        assertEquals(list.get(0).getSellerName(), "Jane Doe");
        assertEquals(list.get(0).getId().intValue(), 1);
        assertEquals(list.get(0).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(1).getSellerName(), "John Doe");
        assertEquals(list.get(1).getId().intValue(), 1);
        assertEquals(list.get(1).getManufacturerCount().intValue(), 3);

        assertEquals(list.get(2).getSellerName(), "John Doe");
        assertEquals(list.get(2).getId().intValue(), 2);
        assertEquals(list.get(2).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(3).getSellerName(), "Moh Rezaei");
        assertEquals(list.get(3).getId().intValue(), 2);
        assertEquals(list.get(3).getManufacturerCount().intValue(), 3);

        assertEquals(list.get(4).getSellerName(), "Moh Rezaei");
        assertEquals(list.get(4).getId().intValue(), 1);
        assertEquals(list.get(4).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(5).getSellerName(), "Rafael Gonzalez");
        assertEquals(list.get(5).getId().intValue(), 1);
        assertEquals(list.get(5).getManufacturerCount().intValue(), 4);

    }


    public void testAggregateBeanListAscendingOrderForUnresolvedList()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("id", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("saleDescription", SaleFinder.description().min());
        list.setAscendingOrderBy(new String[]{"manufacturerCount"});
        assertEquals(6, list.size());

        assertEquals(list.get(0).getManufacturerCount().intValue(), 3);
        assertEquals(list.get(1).getManufacturerCount().intValue(), 3);
        assertEquals(list.get(2).getManufacturerCount().intValue(), 4);
        assertEquals(list.get(3).getManufacturerCount().intValue(), 4);
        assertEquals(list.get(4).getManufacturerCount().intValue(), 4);
        assertEquals(list.get(5).getManufacturerCount().intValue(), 4);


    }

    public void testAggregateBeanListAscendingAndDescendingOrderForResolvedList()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("id", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("saleDescription", SaleFinder.description().min());

        assertEquals(6, list.size());

        list.setDescendingOrderBy("id", "manufacturerCount");
        list.setAscendingOrderBy("sellerName");
        assertEquals(6, list.size());

        assertEquals(list.get(0).getSellerName(), "John Doe");
        assertEquals(list.get(0).getId().intValue(), 2);
        assertEquals(list.get(0).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(1).getSellerName(), "Moh Rezaei");
        assertEquals(list.get(1).getId().intValue(), 2);
        assertEquals(list.get(1).getManufacturerCount().intValue(), 3);

        assertEquals(list.get(2).getSellerName(), "Jane Doe");
        assertEquals(list.get(2).getId().intValue(), 1);
        assertEquals(list.get(2).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(3).getSellerName(), "Moh Rezaei");
        assertEquals(list.get(3).getId().intValue(), 1);
        assertEquals(list.get(3).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(4).getSellerName(), "Rafael Gonzalez");
        assertEquals(list.get(4).getId().intValue(), 1);
        assertEquals(list.get(4).getManufacturerCount().intValue(), 4);

        assertEquals(list.get(5).getSellerName(), "John Doe");
        assertEquals(list.get(5).getId().intValue(), 1);
        assertEquals(list.get(5).getManufacturerCount().intValue(), 3);
    }

}