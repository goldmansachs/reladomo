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

package com.gs.fw.common.mithra.test.aggregate;

import com.gs.fw.common.mithra.AggregateBeanList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TestAggregateBeanListImmutability extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Sale.class,
                        SalesLineItem.class
                };
    }


    public void testAddingAttributeToAggregateListAfterBeingResolved()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                MithraAggregateAttribute aggrAttr2 = SaleFinder.discountPercentage().avg();
                list.addAggregateAttribute("Avg", aggrAttr2);
            }
        });
    }


    public void testAddingElementToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                SimpleAggregateBean data = new SimpleAggregateBean();
                list.add(data);
            }
        });
    }

    public void testAddingIndexToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                SimpleAggregateBean data = new SimpleAggregateBean();
                list.add(1, data);
            }
        });
    }

    public void testSettingElementToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                SimpleAggregateBean data = new SimpleAggregateBean();
                list.set(0, data);
            }
        });
    }

    public void testAddingCollectionToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                List<SimpleAggregateBean> otherList = new ArrayList<SimpleAggregateBean>();
                for (int i = 0; i < 5; i++)
                {
                    SimpleAggregateBean data = new SimpleAggregateBean();
                    otherList.add(data);
                }
                list.addAll(otherList);
            }
        });
    }

    public void testRemovingElementByIndexFromAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                list.remove(0);
            }
        });
    }

    public void testRemovingElementUsingObjectFromAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                SimpleAggregateBean data = (SimpleAggregateBean) list.get(0);
                list.remove(data);
            }
        });
    }

    public void testClearingAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                list.clear();
            }
        });
    }

    public void testRetainingElementsInAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateBeanList list)
            {
                list.retainAll(Arrays.asList(new SimpleAggregateBean(), new SimpleAggregateBean()));
            }
        });
    }


    private void doMutationTest(ListMutationBlock block)
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr = SaleFinder.discountPercentage().sum();

        AggregateBeanList<SimpleAggregateBean> aggregateList1 = new AggregateBeanList(op, SimpleAggregateBean.class);
        aggregateList1.addAggregateAttribute("averageDiscount", aggrAttr);
        aggregateList1.addGroupBy("id", SaleFinder.sellerId());

        assertEquals(1, aggregateList1.size());
        AggregateBeanList aggregateList = aggregateList1;

        try
        {
            block.mutate(aggregateList);
            fail();
        }
        catch (MithraBusinessException mbe)
        {
            assertEquals(1, aggregateList.size());
        }
    }

    private interface ListMutationBlock
    {
        void mutate(AggregateBeanList list);
    }
}
