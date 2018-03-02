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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.AuditedOrder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatus;
import com.gs.fw.common.mithra.test.domain.BitemporalOrderStatusFinder;
import com.gs.fw.common.mithra.test.domain.ParaBalance;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SaleList;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.SalesLineItemFinder;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.TinyBalanceFinder;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



public class TestAggregateList extends MithraTestAbstract
{
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Sale.class,
            SalesLineItem.class,
            TinyBalance.class,
            AuditedOrder.class,
            BitemporalOrder.class,
            BitemporalOrderStatus.class,
            ParaBalance.class
        };
    }

    public void testAggregationWithOperationWithRelationshipInvolvingSource()
    {
        Operation op = TinyBalanceFinder.testRelationship().exists();
        op = op.and(TinyBalanceFinder.acmapCode().eq("A"));
        op = op.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2010-05-05 10:00:00")));
        AggregateList list = new AggregateList(op);
        list.addGroupBy("tinyIn", TinyBalanceFinder.processingDateFrom());
        list.addGroupBy("paraIn", TinyBalanceFinder.testRelationship().processingDateFrom());
        list.addAggregateAttribute("sum", TinyBalanceFinder.quantity().sum());

        list.forceResolve();
    }

    public void testAggregationWithRelationshipWithSource()
    {
        Operation op = TinyBalanceFinder.acmapCode().eq("A");
        op = op.and(TinyBalanceFinder.businessDate().eq(Timestamp.valueOf("2010-05-05 10:00:00")));
        AggregateList list = new AggregateList(op);
        list.addGroupBy("tinyIn", TinyBalanceFinder.processingDateFrom());
        list.addGroupBy("paraIn", TinyBalanceFinder.testRelationship().processingDateFrom());
        list.addAggregateAttribute("sum", TinyBalanceFinder.quantity().sum());

        list.forceResolve();
    }

    public void testCountWithoutGroupByForEmptyTable()
    {
        SaleFinder.findMany(SaleFinder.all()).deleteAll();
        AggregateList list = new AggregateList(SaleFinder.all());
        list.addAggregateAttribute("count", SaleFinder.description().count());
        list.addAggregateAttribute("max", SaleFinder.description().max());
        list.addAggregateAttribute("min", SaleFinder.description().max());

        assertEquals(1, list.size());
        assertEquals(0, list.get(0).getAttributeAsInt("count"));
        assertNull(list.get(0).getAttributeAsString("max"));
        assertNull(list.get(0).getAttributeAsString("min"));
    }

    public void testAggregateListWithUpdateAndRepeat()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                checkLeastId(2);
                checkLeastId(3);
                checkLeastId(6);
                return null;
            }
        });
    }

    private void checkLeastId(int expected)
    {
        AggregateList list = new AggregateList(SaleFinder.activeBoolean().eq(true));
        list.addAggregateAttribute("minSaleId", SaleFinder.saleId().min());
        assertEquals(1, list.size());

        Sale sale = SaleFinder.findOne(SaleFinder.saleId().eq(list.get(0).getAttributeAsInt("minSaleId")));
        sale.setActiveBoolean(false);

        assertEquals(expected, sale.getSaleId());
    }

    public void testAddingAttributeWithDifferentTopLevelPortal()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr1 = SalesLineItemFinder.quantity().sum();
        AggregateList aggregateList = new AggregateList(op);
        try
        {
            aggregateList.addAggregateAttribute("attr", aggrAttr1);
            aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
            fail();
        }
        catch(MithraBusinessException mbe)
        {
        }
    }

    public void testAddingGroupByAttributeWithDifferentTopLevelPortal()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr1 = SalesLineItemFinder.quantity().sum();
        AggregateList aggregateList = new AggregateList(op);
        try
        {
            aggregateList.addAggregateAttribute("attr", aggrAttr1);
            aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
            fail();
        }
        catch(MithraBusinessException mbe)
        {
        }
    }

    public void testAddingTwoAggregateAttributesWithTheSameName()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr1 = SaleFinder.discountPercentage().sum();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr2 = SaleFinder.discountPercentage().sum();
        AggregateList aggregateList = new AggregateList(op);
        try
        {
            aggregateList.addAggregateAttribute("attr", aggrAttr1);
            aggregateList.addAggregateAttribute("attr", aggrAttr2);
            aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
            fail();
        }
        catch(MithraBusinessException mbe)
        {
        }

    }

    public void testAddingTwoAttributeWithTheSameName()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.discountPercentage().sum();
        AggregateList aggregateList = new AggregateList(op);
        try
        {
            aggregateList.addAggregateAttribute("attr", aggrAttr);
            aggregateList.addGroupBy("attr", SaleFinder.sellerId());
            fail();
        }
        catch(MithraBusinessException mbe)
        {
        }

    }

    public void testAddingAttributeToAggregateListAfterBeingResolved()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr2 = SaleFinder.discountPercentage().avg();
                list.addAggregateAttribute("Avg", aggrAttr2);
            }
        });
    }


    public void testAddingElementToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                AggregateData data = new AggregateData();
                list.add(data);
            }
        });
    }

    public void testAddingIndexToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                AggregateData data = new AggregateData();
                list.add(1,data);
            }
        });
    }

    public void testSettingElementToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                AggregateData data = new AggregateData();
                list.set(0, data);
            }
        });
    }

    public void testAddingCollectionToAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                List<AggregateData> otherList = new ArrayList<AggregateData>();
                for(int i = 0; i < 5; i++)
                {
                    AggregateData data = new AggregateData();
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
            public void mutate(AggregateList list)
            {
                list.remove(0);
            }
        });
    }

    public void testRemovingElementUsingObjectFromAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                AggregateData data = list.get(0);
                list.remove(data);
            }
        });
    }

    public void testClearingAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                list.clear();
            }
        });
    }

    public void testRetainingElementsInAggregateList()
    {
        this.doMutationTest(new ListMutationBlock()
        {
            public void mutate(AggregateList list)
            {
                list.retainAll(Arrays.asList(new AggregateData(), new AggregateData()));
            }
        });
    }

    public void testGroupByDivdedBy()
    {
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("div", SalesLineItemFinder.quantity().dividedBy(10));

        assertEquals(2, list.size());
        AggregateData data = list.get(0);
        assertEquals(2, data.getAttributeAsInt("div"));
        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;

        data = list.get(1);
        assertEquals(2, data.getAttributeAsInt("div"));
        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testGroupByTimes()
    {
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("times", SalesLineItemFinder.quantity().times(2));

        assertEquals(2, list.size());
        AggregateData data = list.get(0);
        assertEquals(40, data.getAttributeAsInt("times"));
        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;

        data = list.get(1);
        assertEquals(40, data.getAttributeAsInt("times"));
        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testGroupByPlus()
    {
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("add", SalesLineItemFinder.quantity().plus(5));

        assertEquals(2, list.size());
        AggregateData data = list.get(0);
        assertEquals(25, data.getAttributeAsInt("add"));
        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;

        data = list.get(1);
        assertEquals(25, data.getAttributeAsInt("add"));
        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testSumWithLargeInClause()
    {
        IntHashSet set = new IntHashSet();
        set.add(1);
        for(int i=1000;i<2000;i++) set.add(i);
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2).and(SalesLineItemFinder.manufacturerId().in(set)));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addAggregateAttribute("quant", SalesLineItemFinder.quantity().sum());

        assertEquals(1, list.size());
        AggregateData data = list.get(0);
        assertEquals(40, data.getAttributeAsInt("quant"));
        assertEquals(1, data.getAttributeAsInt("manufacturer"));
    }

    public void testGroupByDividedByIntDouble()
    {
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("div", SalesLineItemFinder.quantity().dividedBy(10.0));

        assertEquals(2, list.size());
        AggregateData data = list.get(0);
        assertEquals(2.0, data.getAttributeAsDouble("div"), 0.0);
        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;

        data = list.get(1);
        assertEquals(2.0, data.getAttributeAsDouble("div"), 0.0);
        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testGroupByPlusIntDouble()
    {
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("add", SalesLineItemFinder.quantity().plus(5.0));

        assertEquals(2, list.size());
        AggregateData data = list.get(0);
        assertEquals(25.0, data.getAttributeAsDouble("add"), 0.0);
        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;

        data = list.get(1);
        assertEquals(25.0, data.getAttributeAsDouble("add"), 0.0);
        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testGroupByTimesIntDouble()
    {
        AggregateList list = new AggregateList(SalesLineItemFinder.saleId().eq(2));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("times", SalesLineItemFinder.quantity().times(2.0));

        assertEquals(2, list.size());
        AggregateData data = list.get(0);
        assertEquals(40.0, data.getAttributeAsDouble("times"), 0.0);
        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;

        data = list.get(1);
        assertEquals(40.0, data.getAttributeAsDouble("times"), 0.0);
        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testCountOnRelated()
    {
        Operation op = SaleFinder.all();
        AggregateList list = new AggregateList(op);
        list.addGroupBy("seller", SaleFinder.sellerId());
        list.addGroupBy("man", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("cnt", SaleFinder.items().manufacturerId().count());
        assertEquals(6, list.size());
    }

    public void testCountOnRelatedAfterCaching()
    {
        SaleList many = SaleFinder.findMany(SaleFinder.all());
        many.deepFetch(SaleFinder.items());
        many.forceResolve();
        testCountOnRelated();
    }

    public void testAggregateListWithLargeIn()
    {
        IntHashSet set = new IntHashSet();
        for(int i=0;i<2000;i++)
        {
            set.add(i);
        }
        AggregateList list = new AggregateList(SalesLineItemFinder.itemId().in(set));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addAggregateAttribute("quantity", SalesLineItemFinder.quantity().sum());

        assertEquals(2, list.size());
//        AggregateData data = list.get(0);
//        assertEquals(2, data.getAttributeAsInt("div"));
//        boolean haveOne = data.getAttributeAsInt("manufacturer") == 1;
//        boolean haveTwo = data.getAttributeAsInt("manufacturer") == 2;
//
//        data = list.get(1);
//        assertEquals(2, data.getAttributeAsInt("div"));
//        haveOne = haveOne || data.getAttributeAsInt("manufacturer") == 1;
//        haveTwo = haveTwo || data.getAttributeAsInt("manufacturer") == 2;
//
//        assertTrue(haveOne);
//        assertTrue(haveTwo);
    }

    public void testAggregateListOrderBy()
    {
        IntHashSet set = new IntHashSet();
        for(int i=0;i<2000;i++)
        {
            set.add(i);
        }
        AggregateList list = new AggregateList(SalesLineItemFinder.itemId().in(set));
        list.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());
        list.addAggregateAttribute("quantity", SalesLineItemFinder.quantity().sum());

        assertEquals(2, list.size());

        list.addOrderBy("quantity", true);
        assertEquals(89, list.get(0).getAttributeAsInteger("quantity"));
        assertEquals(241, list.get(1).getAttributeAsInteger("quantity"));
    }

    public void testAggregateUniToBitemporal() throws Exception
    {
        final Timestamp oldBusinessDate = new Timestamp(timestampFormat.parse("2006-11-29 00:00:00").getTime());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>() {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable {
                Operation op = BitemporalOrderStatusFinder.orderId().eq(1);
                op = op.and(BitemporalOrderStatusFinder.businessDate().eq(oldBusinessDate));
                BitemporalOrderStatusFinder.findOne(op).setLastUser("Barny");
                return null;
            }
        });

        Timestamp businessDate = new Timestamp(timestampFormat.parse("2012-11-29 00:00:00").getTime());
        Timestamp processingDate = new Timestamp(timestampFormat.parse("2013-11-29 10:00:00").getTime());

        Operation op = AuditedOrderFinder.processingDate().eq(processingDate);
        op = op.and(AuditedOrderFinder.bitemporalOrder(businessDate).orderStatus().status().eq(10));

        AggregateList list = new AggregateList(op);
        list.addAggregateAttribute("sum", AuditedOrderFinder.userId().sum());
        list.addGroupBy("group", AuditedOrderFinder.bitemporalOrder(businessDate).orderStatus().lastUser());

        assertEquals(1, list.size());
    }

    private void doMutationTest(ListMutationBlock block)
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr = SaleFinder.discountPercentage().sum();

        AggregateList aggregateList1 = new AggregateList(op);
        aggregateList1.addAggregateAttribute("DiscPctg", aggrAttr);
        aggregateList1.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(1, aggregateList1.size());
        AggregateList aggregateList = aggregateList1;

        try
        {
            block.mutate(aggregateList);
            fail();
        }
        catch(MithraBusinessException mbe)
        {
            assertEquals(1, aggregateList.size());
        }
    }

    private interface ListMutationBlock
    {
        void mutate(AggregateList list);
    }
}
