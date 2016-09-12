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

import com.gs.collections.api.set.primitive.MutableIntSet;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmCategory;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmCategoryFinder;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;


public class TestMax extends MithraTestAbstract
{
    private static final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");


    public Class[] getRestrictedClassList()
        {
            return new Class[]
                    {
                            AlarmCategory.class,
                            Sale.class,
                            Seller.class,
                            SalesLineItem.class,
                            WishListItem.class,
                            ProductSpecification.class,
                            OrderItem.class,
                            Order.class,
                            BigOrderItem.class,
                            BigOrder.class,
                            Manufacturer.class,
                            ParaDesk.class,
                            ParentNumericAttribute.class
                    };
        }

    public void testSimpleMax()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute maxDisc = SaleFinder.discountPercentage().max();

        AggregateList list = new AggregateList(op);
        list.addAggregateAttribute("MaxDisc", maxDisc);

        AggregateData data = list.get(0);
        assertEquals(0.1, data.getAttributeAsDouble("MaxDisc"));
        assertEquals(1, list.size());
    }

    public void testSimpleNullableMax()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute maxDisc = SaleFinder.nullableDouble().max();

        AggregateList list = new AggregateList(op);
        list.addAggregateAttribute("MaxDouble", maxDisc);

        AggregateData data = list.get(0);
        assertEquals(17.0, data.getAttributeAsDouble("MaxDouble"));
        assertEquals(1, list.size());
    }

    public void testAllOperationAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(10, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 2:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 3:
                    assertEquals(10, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 4:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 5:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 6:
                    assertEquals(15, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 10	:
                    assertEquals(12, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testNotMappedOperationAndAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(7, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 2:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 4:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 5:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);

        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 6:
                    assertEquals(15, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 10	:
                    assertEquals(12, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.06);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 2:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 3:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 4:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

    public void testMappedToManyOperationAndAggregateAttribute()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 2:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 3:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 4:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

    public void testOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.0);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 3:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 4:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

    public void testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.0);
        op = op.and(SaleFinder.discountPercentage().greaterThan(0.07));
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 3:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20);
        op = op.and(SaleFinder.seller().zipCode().notIn(zipcodeSet));

        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(16, data.getAttributeAsDouble("MaxItems"), 0);

    }

    public void testToOneOperationAndMappedToManyAttribute()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.seller().zipCode().notIn(zipcodeSet);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(16, data.getAttributeAsDouble("MaxItems"), 0);
    }

    public void testOperationWithLinkedToOneMapperAndToManyAggregateAttribute()
    {
        Operation op = SaleFinder.items().manufacturers().locationId().eq(2);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());
        MutableIntSet sellers = aggregateList.getAttributeAsGscIntSet("SellerId");
        assertEquals(4, sellers.size());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                 case 1:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 2:
                    assertEquals(23, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 3:
                    assertEquals(20, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                case 4:
                    assertEquals(16, data.getAttributeAsInt("MaxItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testAllOperationAndCalculatedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(600.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 2:
                    assertEquals(450.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

    public void testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(600.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 2:
                    assertEquals(425.00, data.getAttributeAsDouble("MaxPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(600, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 2:
                    assertEquals(450, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

    public void testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(600, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 2:
                    assertEquals(425, data.getAttributeAsDouble("MaxPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.07);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(425, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 3:
                    assertEquals(600, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }

    public void testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(399.80, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 2:
                    assertEquals(425.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                case 3:
                    assertEquals(600.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 4:
                    assertEquals(400.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(450.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                case 3:
                    assertEquals(600.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;
                case 4:
                    assertEquals(400.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id: "+data.getAttributeAsInt("SellerId"));
            }
        }

    }

    public void testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SaleFinder.discountPercentage().greaterThan(0.07));
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(425.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                case 3:
                    assertEquals(600.00, data.getAttributeAsDouble("MaxPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }

    public void testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SaleFinder.seller().zipCode().notIn(zipcodeSet));

        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(400, data.getAttributeAsDouble("MaxPrice"), 0);
    }


    public void testToOneRelationshipInBothOpAndAggregate()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItemPrice", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(25, data.getAttributeAsDouble("MaxItemPrice"), 0);
                    break;
                case 6:
                    assertEquals(30, data.getAttributeAsDouble("MaxItemPrice"), 0);
                    break;
                case 7:
                    assertEquals(30, data.getAttributeAsDouble("MaxItemPrice"), 0);
                    break;
                case 8:
                    assertEquals(33, data.getAttributeAsDouble("MaxItemPrice"), 0);
                    break;
                case 9:
                    assertEquals(25, data.getAttributeAsDouble("MaxItemPrice"), 0);
                    break;
                case 10	:
                    assertEquals(25, data.getAttributeAsDouble("MaxItemPrice"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    //todo: write a different test case for this
    public void testToManyRelationshipThatBecomesToOne()
    {
        Operation op = SaleFinder.items().itemId().eq(2);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());
        assertEquals(20, aggregateList.getAggregateDataAt(0).getAttributeAsInt("MaxItems"), 0);
    }

    //todo: add test cases for date, timestamp, boolean, string, float, char, byte, etc.

    public void testMaxTimestamp() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.saleDate().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxTs", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MaxTs"));
                    break;
                case 2:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MaxTs"));
                    break;
                case 3:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime()), data.getAttributeAsTimestamp("MaxTs"));
                    break;
                case 4:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 02:00:00.0").getTime()), data.getAttributeAsTimestamp("MaxTs"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMaxDate() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.settleDate().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDate", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(dateFormat.parse("2004-02-15"), data.getAttributeAsDate("MaxDate"));
                    break;
                case 2:
                    assertEquals(dateFormat.parse("2004-02-14"), data.getAttributeAsDate("MaxDate"));
                    break;
                case 3:
                    assertEquals(dateFormat.parse("2004-02-17"), data.getAttributeAsDate("MaxDate"));
                    break;
                case 4:
                    assertEquals(dateFormat.parse("2004-04-01"), data.getAttributeAsDate("MaxDate"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMaxTime() throws ParseException
    {
        Operation op = AlarmCategoryFinder.all();
        MithraAggregateAttribute aggrAttr = AlarmCategoryFinder.time().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxTime", aggrAttr);
        aggregateList.addGroupBy("CategoryId", AlarmCategoryFinder.category());

        assertEquals(3, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("CategoryId"))
            {
                case 1:
                    assertEquals(Time.withMillis(10, 30, 59, 11), data.getAttributeAsTime("MaxTime"));
                    break;
                case 2:
                    assertEquals(Time.withMillis(10, 30, 59, 11), data.getAttributeAsTime("MaxTime"));
                    break;
                case 3:
                    assertEquals(Time.withMillis(10, 30, 59, 11), data.getAttributeAsTime("MaxTime"));
                    break;
                case 4:
                    assertEquals(Time.withMillis(10, 30, 59, 11), data.getAttributeAsTime("MaxTime"));
                    break;
                default:
                    fail("Invalid category id");
            }
        }
    }

    public void testMaxDateYear()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().year().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        assertEquals(9999, aggregateList.get(0).getAttributeAsInteger("MaxMonth"));
        assertEquals(9999, aggregateList.get(1).getAttributeAsInteger("MaxMonth"));
    }

    public void testMaxDateMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().month().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        assertEquals(12, aggregateList.get(0).getAttributeAsInteger("MaxMonth"));
        assertEquals(12, aggregateList.get(1).getAttributeAsInteger("MaxMonth"));
    }

    public void testMaxDateDayOfMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().dayOfMonth().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDayOfMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        assertEquals(8, aggregateList.get(0).getAttributeAsInteger("MaxDayOfMonth"));
        assertEquals(8, aggregateList.get(1).getAttributeAsInteger("MaxDayOfMonth"));
    }

    public void testMaxTimestampYear()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().year().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxYear", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        assertEquals(2004, aggregateList.get(0).getAttributeAsInteger("MaxYear"));
    }

    public void testMaxTimestampMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().month().max();

        AggregateList aggregateList = new AggregateList(op);
            aggregateList.addAggregateAttribute("MaxMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("MaxMonth"));
    }

    public void testMaxTimestampDayOfMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().dayOfMonth().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDayOfMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(12, aggregateList.get(0).getAttributeAsInteger("MaxDayOfMonth"));
    }

    public void testMaxBoolean()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.activeBoolean().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxFlag", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(true, data.getAttributeAsBoolean("MaxFlag"));
                    break;
                case 2:
                    assertEquals(true, data.getAttributeAsBoolean("MaxFlag"));
                    break;
                case 3:
                    assertEquals(true, data.getAttributeAsBoolean("MaxFlag"));
                    break;
                case 4:
                    assertEquals(false, data.getAttributeAsBoolean("MaxFlag"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMaxMappedTimestamp() throws ParseException
    {
        Operation op = SellerFinder.all();
        MithraAggregateAttribute aggrAttr = SellerFinder.sales().saleDate().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxTs", aggrAttr);
        aggregateList.addGroupBy("activeFlag", SellerFinder.active());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            if(data.getAttributeAsBoolean("activeFlag"))
            {
                assertEquals(new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MaxTs"));
            }
            else
            {
                assertEquals(new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MaxTs"));
            }
        }
    }

    public void testMaxMappedDate() throws ParseException
    {
        Operation op = SellerFinder.all();
        MithraAggregateAttribute aggrAttr = SellerFinder.sales().settleDate().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxTs", aggrAttr);
        aggregateList.addGroupBy("activeFlag", SellerFinder.active());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            if(data.getAttributeAsBoolean("activeFlag"))
            {
                assertEquals(dateFormat.parse("2004-02-17"), data.getAttributeAsDate("MaxTs"));
            }
            else
            {
                assertEquals(dateFormat.parse("2004-04-01"), data.getAttributeAsDate("MaxTs"));
            }
        }
    }

    public void testMaxMappedBoolean()
    {
        Operation op = SellerFinder.all();
        MithraAggregateAttribute aggrAttr = SellerFinder.sales().activeBoolean().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxTs", aggrAttr);
        aggregateList.addGroupBy("activeFlag", SellerFinder.active());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            if(data.getAttributeAsBoolean("activeFlag"))
            {
                assertEquals(true, data.getAttributeAsBoolean("MaxTs"));
            }
            else
            {
                assertEquals(true, data.getAttributeAsBoolean("MaxTs"));
            }
        }
    }

    public void testMaxString() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.description().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDesc", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        Set set = aggregateList.getAttributeAsSet("MaxDesc");
        assertEquals(4, set.size());
        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals("Sale 0003", data.getAttributeAsString("MaxDesc"));
                    break;
                case 2:
                    assertEquals("Sale 0006",data.getAttributeAsString("MaxDesc"));
                    break;
                case 3:
                     assertEquals("Sale 0008", data.getAttributeAsString("MaxDesc"));
                    break;
                case 4:
                     assertEquals("Sale 0010", data.getAttributeAsString("MaxDesc"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMaxMappedString()
    {
        Operation op = SellerFinder.all();
        MithraAggregateAttribute aggrAttr = SellerFinder.sales().description().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDesc", aggrAttr);
        aggregateList.addGroupBy("activeFlag", SellerFinder.active());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            if(data.getAttributeAsBoolean("activeFlag"))
            {
                assertEquals("Sale 0008", data.getAttributeAsString("MaxDesc"));
            }
            else
            {
                assertEquals("Sale 0010", data.getAttributeAsString("MaxDesc"));
            }
        }
    }

    public void testMaxCharacter() throws ParseException
    {
        Operation op = ParentNumericAttributeFinder.all();
        MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.charAttr().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxChar", aggrAttr);
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);
            int userId = data.getAttributeAsInt("UserId");
            if(userId == 1)
            {
               assertEquals('c', data.getAttributeAsCharacter("MaxChar"));
            }
            else if(userId == 2)
            {
               assertEquals('f', data.getAttributeAsCharacter("MaxChar"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testMaxBigDecimal() throws Exception
    {
        Operation op = ParentNumericAttributeFinder.all();
        MithraAggregateAttribute aggrAttr1 = ParentNumericAttributeFinder.bigDecimalAttr().max();
        MithraAggregateAttribute aggrAttr2 = ParentNumericAttributeFinder.veryBigDecimalAttr().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("Attr1", aggrAttr1);
        aggregateList.addAggregateAttribute("Attr2", aggrAttr2);
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("UserId"))
            {
                case 1:
                    assertEquals(new BigDecimal("300.50"), data.getAttributeAsBigDecimal("Attr1"));
                    assertEquals(new BigDecimal("333333333333333333"), data.getAttributeAsBigDecimal("Attr2"));
                    break;
                case 2:

                    assertEquals(new BigDecimal("600.50"), data.getAttributeAsBigDecimal("Attr1"));
                    assertEquals(new BigDecimal("666666666666666666"), data.getAttributeAsBigDecimal("Attr2"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

    public void testMappedMaxBigDecimal() throws Exception
    {
        Operation op = BigOrderFinder.all();
        MithraAggregateAttribute aggrAttr1 = BigOrderFinder.items().quantity().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("Attr1", aggrAttr1);
        aggregateList.addGroupBy("UserId", BigOrderFinder.userId());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("UserId"))
            {
                case 1:
                    assertEquals(new BigDecimal("20.000"), data.getAttributeAsBigDecimal("Attr1"));
                    break;
                case 2:
                    assertEquals(new BigDecimal("1000000.999"), data.getAttributeAsBigDecimal("Attr1"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

}
