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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.AggregateData;

import java.sql.Timestamp;
import java.text.ParseException;




public class TestDatedAggregation extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                BitemporalOrder.class,
                Order.class,
                OrderItem.class,
                BitemporalOrderItem.class,
                BitemporalOrderItemStatus.class,
                BitemporalOrderStatus.class,
                Product.class,
                ProductSpecification.class,
                TestAsOfToTimestampJoinObjectA.class,
                TestAsOfToTimestampJoinObjectB.class
        };
    }

    public void testAggregateDatedToNonDated()
    {
        AggregateList list = new AggregateList(TestAsOfToTimestampJoinObjectBFinder.all());
        list.addAggregateAttribute("sum", TestAsOfToTimestampJoinObjectBFinder.a().id().sum());
        list.addGroupBy("id", TestAsOfToTimestampJoinObjectBFinder.id());

        assertEquals(2, list.size());
    }

    public void testSimulatedDistinct() throws Exception
    {
        Operation op = BitemporalOrderItemFinder.all();
        op = op.and(BitemporalOrderItemFinder.businessDate().eq(getBusinessDate()));
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addGroupBy("productId", BitemporalOrderItemFinder.productId());

        assertEquals(3, aggregateList.size());
    }

    private Timestamp getBusinessDate() throws ParseException
    {
        return new Timestamp(timestampFormat.parse("2010-11-29 00:00:00").getTime());
    }

    public void testAggregatingRelatedObjectAttributes() throws ParseException
    {
        Operation op = BitemporalOrderFinder.all();
        op = op.and(BitemporalOrderFinder.businessDate().eq(getBusinessDate()));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = BitemporalOrderFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("qty", aggrAttr);
        aggregateList.addGroupBy("orderId", BitemporalOrderFinder.orderId());

        assertEquals(2, aggregateList.size());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            if(data.getAttributeAsInt("orderId") == 1)
            {
                assertEquals(20.0, data.getAttributeAsDouble("qty"),0);

            }
            else if(data.getAttributeAsInt("orderId") == 2)
            {
                assertEquals(60.0, data.getAttributeAsDouble("qty"),0);
            }

            else
            {
                fail("Invalid orderId");
            }
        }
    }

    public void testAggregatingRelatedObjectAttributesGroupingByOtherRelatedObjectAttribute() throws ParseException
    {
        Operation op = BitemporalOrderFinder.items().quantity().greaterThan(10);
        op = op.and(BitemporalOrderFinder.businessDate().eq(getBusinessDate()));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = BitemporalOrderFinder.items().quantity().sum();
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("qty",aggrAttr);
        aggregateList.addGroupBy("status", BitemporalOrderFinder.orderStatus().status());
        assertEquals(1, aggregateList.size());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            if(data.getAttributeAsInt("status") == 10)
            {
                assertEquals(20.0, data.getAttributeAsDouble("qty"),0);

            }
            else
            {
                fail("Invalid statusId");
            }
        }
    }

    public void testSumGroupByRelatedObjectAttribute() throws ParseException
    {
        Operation op = BitemporalOrderItemFinder.all();
        op = op.and(BitemporalOrderItemFinder.businessDate().eq(getBusinessDate()));
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalSalePerProduct", BitemporalOrderItemFinder.quantity().times(BitemporalOrderItemFinder.originalPrice()).sum());
        aggregateList.addGroupBy("prodDesc", BitemporalOrderItemFinder.productInfo().productDescription());

        assertEquals(3, aggregateList.size());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            String prodName = data.getAttributeAsString("prodDesc");
            if("Product 1".equals(prodName))
            {
                assertEquals(420.0,data.getAttributeAsDouble("totalSalePerProduct"));
            }
            else if("Product 2".equals(prodName))
            {
                assertEquals(310.0,data.getAttributeAsDouble("totalSalePerProduct"));
            }
            else if("Product 3".equals(prodName))
            {
                assertEquals(410.0,data.getAttributeAsDouble("totalSalePerProduct"));
            }
            else
            {
                fail("Invalid product id");
            }
        }
    }

    public void testSumGroupByRelatedObjectAttribute2() throws ParseException
    {
        Operation op = BitemporalOrderItemFinder.all();
        op = op.and(BitemporalOrderItemFinder.businessDate().eq(getBusinessDate()));
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalSalePerProduct", BitemporalOrderItemFinder.quantity().times(BitemporalOrderItemFinder.productSpecs().originalPrice()).sum());
        aggregateList.addGroupBy("prodDesc", BitemporalOrderItemFinder.productSpecs().productName());

        assertEquals(3, aggregateList.size());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            String prodName = data.getAttributeAsString("prodDesc");
            if("PROD 1".equals(prodName))
            {
                assertEquals(500.0,data.getAttributeAsDouble("totalSalePerProduct"));
            }
            else if("PROD 2".equals(prodName))
            {
                assertEquals(399.79,data.getAttributeAsDouble("totalSalePerProduct"), 0.01);
            }
            else if("PROD 3".equals(prodName))
            {
                assertEquals(200.0,data.getAttributeAsDouble("totalSalePerProduct"));
            }
            else
            {
                fail("Invalid product id");
            }
        }
    }

    public void testSumGroupByRelatedObjectAttributeWithNullGroupBy() throws ParseException
    {
        Operation op = BitemporalOrderItemFinder.all();
        op = op.and(BitemporalOrderItemFinder.businessDate().eq(getBusinessDate()));
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalQtyPerManufacturer", BitemporalOrderItemFinder.quantity().sum());
        aggregateList.addGroupBy("manufacturerId", BitemporalOrderItemFinder.productSpecs().manufacturerId());

        assertEquals(3, aggregateList.size());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            if(data.isAttributeNull("manufacturerId"))
            {
                assertEquals(20.0,data.getAttributeAsDouble("totalQtyPerManufacturer"),0);
            }
            else if(data.getAttributeAsInt("manufacturerId") == 1)
            {
                assertEquals(40.0,data.getAttributeAsDouble("totalQtyPerManufacturer"),0);
            }
            else if(data.getAttributeAsInt("manufacturerId") == 2)
            {
                assertEquals(20.0,data.getAttributeAsDouble("totalQtyPerManufacturer"), 0);
            }
            else
            {
                fail("Invalid manufacturer id");
            }
        }

    }

    public void testSumGroupByRelatedObjectAttributeWithNullNonPrimitiveGroupBy() throws ParseException
    {
        Operation op = BitemporalOrderItemFinder.all();
        op = op.and(BitemporalOrderItemFinder.businessDate().eq(getBusinessDate()));
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalQtyPerManufacturer", BitemporalOrderItemFinder.quantity().sum());
        aggregateList.addGroupBy("manufacturerName", BitemporalOrderItemFinder.productSpecs().manufacturerName());

        assertEquals(3, aggregateList.size());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            if(data.getAttributeAsString("manufacturerName") == null)
            {
                assertEquals(20.0,data.getAttributeAsDouble("totalQtyPerManufacturer"),0);
            }
            else if(data.getAttributeAsString("manufacturerName").equals("M1"))
            {
                assertEquals(40.0,data.getAttributeAsDouble("totalQtyPerManufacturer"),0);
            }
            else if(data.getAttributeAsString("manufacturerName").equals("M2"))
            {
                assertEquals(20.0,data.getAttributeAsDouble("totalQtyPerManufacturer"), 0);
            }
            else
            {
                fail("Invalid manufacturer id");
            }
        }

    }

    public void testAggregateFromItemToOrderWithCondition() throws Exception
    {
        Operation op = BitemporalOrderItemFinder.order().userId().eq(1);
        op = op.and(BitemporalOrderItemFinder.businessDate().eq(getBusinessDate()));

        AggregateList list = new AggregateList(op);
        list.addGroupBy("state", BitemporalOrderItemFinder.order().state());
        list.addAggregateAttribute("count", BitemporalOrderItemFinder.id().count());

        list.forceResolve();
    }
}
