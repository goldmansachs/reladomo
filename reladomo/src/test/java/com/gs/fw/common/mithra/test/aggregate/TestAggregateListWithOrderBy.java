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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.Seller;

import java.util.ArrayList;
import java.util.List;


public class TestAggregateListWithOrderBy extends MithraTestAbstract
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

    public void testInvalidOrderByAttribute()
    {

        Operation op = SaleFinder.all();
        AggregateList list = new AggregateList(op);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("man", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("cnt", SaleFinder.items().manufacturerId().count());
        try
        {
            list.addOrderBy("bogus", true);
            fail("Order should be allowed on a attribute which is neither a group by nor a aggregate attribute");
        }
        catch (MithraBusinessException mbe)
        {
            assertNotNull(mbe.getMessage());
        }
    }

    public void testSimpleOrderBySellerName()
    {
        Operation op = SaleFinder.all();
        AggregateList list = new AggregateList(op);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("man", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("cnt", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("description", SaleFinder.description().min());

        assertEquals(6, list.size());
        list.setAscendingOrderBy(new String[]{"sellerName"});
        assertAttribute(list, "sellerName", "Jane Doe", "John Doe", "John Doe", "Moh Rezaei", "Moh Rezaei", "Rafael Gonzalez");


    }

    public void testMultipleOrderBys()
    {
        Operation op = SaleFinder.all();
        AggregateList list = new AggregateList(op);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("man", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("cnt", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("description", SaleFinder.description().min());
        list.addOrderBy("sellerName", true);
        list.addOrderBy("cnt", true);
        list.addOrderBy("man", false);
        assertEquals(6, list.size());

        assertAttribute(list, "sellerName", "Jane Doe", "John Doe", "John Doe", "Moh Rezaei", "Moh Rezaei", "Rafael Gonzalez");
        assertAttribute(list, "man", 1, 1, 2, 2, 1, 1);
        assertAttribute(list, "cnt", 4, 3, 4, 3, 4, 4);
    }


    public void testAscendingOrderByCountForUnresolvedList()
    {
        Operation op = SaleFinder.all();
        AggregateList list = new AggregateList(op);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("man", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("cnt", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("description", SaleFinder.description().min());
        list.setAscendingOrderBy(new String[]{"cnt"});
        assertEquals(6, list.size());
        assertAttribute(list, "cnt", 3, 3, 4, 4, 4, 4);
    }

    public void testDescendingOrderByManufactureIdForResolvedList()
    {
        Operation op = SaleFinder.all();
        AggregateList list = new AggregateList(op);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addGroupBy("man", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("cnt", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("description", SaleFinder.description().min());

        assertEquals(6, list.size());

        list.setDescendingOrderBy("man", "cnt");
        assertEquals(6, list.size());

        assertAttribute(list, "sellerName", "John Doe", "Moh Rezaei", "Jane Doe", "Moh Rezaei", "Rafael Gonzalez", "John Doe");
        assertAttribute(list, "man", 2, 2, 1, 1, 1, 1);
        assertAttribute(list, "cnt", 4, 3, 4, 4, 4, 3);
    }

    private void assertAttribute(AggregateList aggList, String attributeName, Object... values)
    {
        assertEquals(values.length, aggList.size());
        for (int i = 0; i < aggList.size(); i++)
        {
            AggregateData aggregateData = aggList.getAggregateDataAt(i);
            assertEquals(values[i], aggregateData.getAttributeAsObject(attributeName));
        }
    }

}