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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.BigOrder;
import com.gs.fw.common.mithra.test.domain.BigOrderFinder;
import com.gs.fw.common.mithra.test.domain.BigOrderItem;
import com.gs.fw.common.mithra.test.domain.Manufacturer;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.ParentNumericAttribute;
import com.gs.fw.common.mithra.test.domain.ParentNumericAttributeFinder;
import com.gs.fw.common.mithra.test.domain.ProductSpecification;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.SalesLineItemFinder;
import com.gs.fw.common.mithra.test.domain.Seller;
import com.gs.fw.common.mithra.test.domain.WishListItem;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.math.BigDecimal;
import java.util.Set;


public class TestAvg extends MithraTestAbstract
{


    public Class[] getRestrictedClassList()
        {
            return new Class[]
                    {
                            Sale.class,
                            Seller.class,
                            SalesLineItem.class,
                            WishListItem.class,
                            ProductSpecification.class,
                            OrderItem.class,
                            Order.class,
                            BigOrderItem.class,
                            BigOrder.class,
                            ParentNumericAttribute.class,
                            Manufacturer.class
                    };
        }


//CASE 1
//List Operation: SalesItemFinder.ALL()
//AggrAttr: SalesItemFinder.quantity().avg()
//
//SELECT t0.SALE_ID, AVG(t0.QTY)
//FROM SALES_LINE_ITEM t0
//GROUP BY t0.SALE_ID

    public void testAllOperationAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(10, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 2:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 3:
                    assertEquals(8, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 4:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 5:
                    assertEquals(18, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 6:
                    assertEquals(13, data.getAttributeAsDouble("AvgItems"), 1);
                    break;
                case 7:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 8:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 9:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 1);
                    break;
                case 10	:
                    assertEquals(11, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


// CASE 1.2
// List operation: SalesItemFinder.quantity().greaterThan(15)
// AGGR ATTR: SalesItemFinder.quantity().avg()
//
//select t0.SALE_ID , avg(t0.QTY )
//from SALES_LINE_ITEM t0
//WHERE t0.QTY > 15
//group by t0.SALE_ID

    public void testNotMappedOperationAndAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(7, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 2:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 4:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 5:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 1.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().sum()
//
//select t0.SALE_ID , sum(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.SALE_ID

    public void testMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);

        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 6:
                    assertEquals(13, data.getAttributeAsDouble("AvgItems"), 1);
                    break;
                case 7:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 8:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 10	:
                    assertEquals(12, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


// CASE 1.4
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().sum()
//
//select t0.SALE_ID , sum(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.QTY > 15
//AND t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.SALE_ID

    public void testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 2.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.06)
// AGGR ATTR: SaleFinder.items().quantity().avg()
//
//    select t0.USER_ID , avg(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.DISC_PCTG > 0.06
//    group by t0.USER_ID


    public void testNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.06);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 2:
                    assertEquals(16, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 3:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 4:
                    assertEquals(13, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.2
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().avg()
//
//    select t0.USER_ID , avg(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndAggregateAttribute()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 2:
                    assertEquals(17, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 3:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 4:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 1);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().avg()
//
//    select t0.USER_ID , sum(a1.QTY )
//    from SALE t0, SALES_LINE_ITEM a1
//    WHERE t0.SALE_ID = a1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//                WHERE t0.SALE_ID = t1.SALE_ID
//                AND t1.PROD_ID = t2.PROD_ID
//                AND t2.PRICE > 20)
//    group by t0.USER_ID


    public void testOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.0);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(16, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 3:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 4:
                    assertEquals(13, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }


// CASE 2.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().avg()
//
//    select t0.USER_ID , avg(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.DISC_PCTG > 0.07
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20)
//    group by t0.USER_ID

    public void testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.0);
        op = op.and(SaleFinder.discountPercentage().greaterThan(0.07));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(18, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 3:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }


// CASE 2.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().avg()
//    select t0.USER_ID , avg(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2, TEST_USER e3
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20
//                AND t0.USER_ID = e3.USER_ID
//                AND e3.ZIP_CODE NOT IN ('10001','10038'))
//    group by t0.USER_ID
//

    public void testAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20);
        op = op.and(SaleFinder.seller().zipCode().notIn(zipcodeSet));

        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(13, data.getAttributeAsDouble("AvgItems"), 0);
    }

// CASE 2.6
// List Operation: SaleFinder.seller().zipCode().notIn('10001', '10038')
// AGGR ATTR: SaleFinder.items().quantity().sum()
//
//    select t0.USER_ID , sum(t1.QTY )
//    FROM SALE t0, SALES_LINE_ITEM t1, TEST_USER t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.USER_ID = t2.USER_ID
//    AND t2.ZIP_CODE NOT IN ('10001', '10038')
//    group by t0.USER_ID


    public void testToOneOperationAndMappedToManyAttribute()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.seller().zipCode().notIn(zipcodeSet);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(13, data.getAttributeAsDouble("AvgItems"), 0);
    }

// CASE 2.7
// List Operation: SaleFinder.items().manufacturer().locationId().eq(100)
// AGGR ATTR: SaleFinder.items().quantity().avg()
//
//    select t0.USER_ID , avg(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, MANUFACTURER e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.MANUFACTURER_ID = e2.MANUFACTURER_ID
//                AND e2.LOCATION_ID = 100)
//    group by t0.USER_ID


    public void testOperationWithLinkedToOneMapperAndToManyAggregateAttribute()
    {
        Operation op = SaleFinder.items().manufacturers().locationId().eq(2);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                 case 1:
                    assertEquals(20, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 2:
                    assertEquals(17, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 3:
                    assertEquals(15, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                case 4:
                    assertEquals(13, data.getAttributeAsDouble("AvgItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 4.1
// Group by MANUFACTURER_ID
// List operation: SalesItemFinder.ALL()
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).avg()
//
//    SELECT t0.MANUFACTURER_ID, avg(t0.QTY * t1.PRICE)
//    FROM SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    group by t0.MANUFACTURER_ID


    public void testAllOperationAndCalculatedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(306.97, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 2:
                    assertEquals(242.85, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

//  --CASE 4.2
//-- Group by MANUFACTURER_ID
//-- List operation: SalesItemFinder.quantity().greaterThan(15)
//
//-- AGGR ATTR: SaleItemFinder.quantity().times(SaleItemFinder.productSpecification().originalPrice()).avg()
//
//select t0.MANUFACTURER_ID , avg(t0.QTY * t1.PRICE )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t0.QTY > 15
//group by t0.MANUFACTURER_ID
    public void testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(385.89, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 2:
                    assertEquals(312.50, data.getAttributeAsDouble("AvgPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 4.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.items().productSpecification().originalPrice()).avg()
//
//    select t0.MANUFACTURER_ID , avg(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(405.00, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 2:
                    assertEquals(391.67, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }


// CASE 4.4
// List operation: SaleItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).avg()
//
//    select t0.MANUFACTURER_ID , avg(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t0.QTY > 15
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(500, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 2:
                    assertEquals(425, data.getAttributeAsDouble("AvgPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 5.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.07)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).avg()
//
//    SELECT t0.USER_ID, avg(t1.QTY * t2.PRICE)
//    FROM SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND t0.DISC_PCTG > 0.07
//    group by t0.USER_ID

// SELECT T0.SELLER_ID , avg((A1.QUANTITY * A2.ORIGINAL_PRICE))
// FROM APP.SALE T0, APP.PRODUCT_SPECIFICATION A2, APP.SALES_LINE_ITEM A1
// WHERE  T0.DISC_PCTG > ?
// AND T0.SALE_ID = A1.SALE_ID
// A1[*].PRODUCT_ID = A2.PROD_ID
// GROUP BY T0.SELLER_ID  [42000-41]


    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.07);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(277.46, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 3:
                    assertEquals(432.25, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }




// CASE 5.2
// Group by USER_ID
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).avg()
//
//    select t0.USER_ID , avg(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(274.95, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 2:
                    assertEquals(277.46, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                case 3:
                    assertEquals(432.50, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 4:
                    assertEquals(293.75, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }


// CASE 5.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).avg()
//
//    select t0.USER_ID , avg(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1,TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20)
//    group by t0.USER_ID

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(322.50, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                case 3:
                    assertEquals(432.50, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;
                case 4:
                    assertEquals(253.125, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id: "+data.getAttributeAsInt("SellerId"));
            }
        }

    }

// CASE 5.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).avg()
//
//    select t0.USER_ID , avg(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND t0.DISC_PCTG > 0.07
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20)
//    group by t0.USER_ID


    public void testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SaleFinder.discountPercentage().greaterThan(0.07));
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(287.50, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                case 3:
                    assertEquals(432.50, data.getAttributeAsDouble("AvgPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }



// CASE 5.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).avg()
//
//    select t0.USER_ID , avg(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2, TEST_USER e3
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20
//                AND t0.USER_ID = e3.USER_ID
//                AND e3.ZIP_CODE NOT IN ('10001','10038'))
//    group by t0.USER_ID

    public void testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SaleFinder.seller().zipCode().notIn(zipcodeSet));

        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(253.125, data.getAttributeAsDouble("AvgPrice"), 0);
    }


    public void testToOneRelationshipInBothOpAndAggregate()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItemPrice", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(25, data.getAttributeAsDouble("AvgItemPrice"), 0);
                    break;
                case 6:
                    assertEquals(27.5, data.getAttributeAsDouble("AvgItemPrice"), 0);
                    break;
                case 7:
                    assertEquals(27.5, data.getAttributeAsDouble("AvgItemPrice"), 0);
                    break;
                case 8:
                    assertEquals(31.5, data.getAttributeAsDouble("AvgItemPrice"), 0);
                    break;
                case 9:
                    assertEquals(25, data.getAttributeAsDouble("AvgItemPrice"), 0);
                    break;
                case 10	:
                    assertEquals(25, data.getAttributeAsDouble("AvgItemPrice"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testToManyRelationshipThatBecomesToOne()
    {
        Operation op = SaleFinder.items().itemId().eq(2);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());
        assertEquals(20, aggregateList.getAggregateDataAt(0).getAttributeAsDouble("AvgItems"), 0);
    }

    public void testAvgBigDecimal() throws Exception
    {
        Operation op = ParentNumericAttributeFinder.all();
        MithraAggregateAttribute aggrAttr1 = ParentNumericAttributeFinder.bigDecimalAttr().avg();
        MithraAggregateAttribute aggrAttr2 = ParentNumericAttributeFinder.veryBigDecimalAttr().avg();

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
                    assertEquals(new BigDecimal("200.50"), data.getAttributeAsBigDecimal("Attr1"));
                    assertEquals(new BigDecimal("222222222222222222"), data.getAttributeAsBigDecimal("Attr2"));
                    break;
                case 2:

                    assertEquals(new BigDecimal("500.50"), data.getAttributeAsBigDecimal("Attr1"));
                    assertEquals(new BigDecimal("555555555555555555"), data.getAttributeAsBigDecimal("Attr2"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

    public void testMappedAvgBigDecimal() throws Exception
    {
        Operation op = BigOrderFinder.all();
        MithraAggregateAttribute aggrAttr1 = BigOrderFinder.items().quantity().avg();

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
                    assertEquals(new BigDecimal("13.000"), data.getAttributeAsBigDecimal("Attr1"));
                    break;
                case 2:
                    assertEquals(new BigDecimal("500028.666"), data.getAttributeAsBigDecimal("Attr1"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

}
