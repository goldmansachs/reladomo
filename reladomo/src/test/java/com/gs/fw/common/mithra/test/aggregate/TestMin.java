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
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParentNumericAttribute;
import com.gs.fw.common.mithra.test.domain.ParentNumericAttributeFinder;
import com.gs.fw.common.mithra.test.domain.ProductSpecification;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.SalesLineItemFinder;
import com.gs.fw.common.mithra.test.domain.Seller;
import com.gs.fw.common.mithra.test.domain.WishListItem;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmCategory;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmCategoryFinder;
import com.gs.fw.common.mithra.util.Time;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;


public class TestMin extends MithraTestAbstract
{
    private static final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public Class[] getRestrictedClassList()
        {
            return new Class[]
                    {
                            Sale.class,
                            AlarmCategory.class,
                            Seller.class,
                            ParaDesk.class,
                            SalesLineItem.class,
                            WishListItem.class,
                            ProductSpecification.class,
                            OrderItem.class,
                            Order.class,
                            BigOrderItem.class,
                            BigOrder.class,
                            Manufacturer.class,
                            ParentNumericAttribute.class
                    };
        }


    public void testSimpleMin()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute maxDisc = SaleFinder.discountPercentage().min();

        AggregateList list = new AggregateList(op);
        list.addAggregateAttribute("MinDisc", maxDisc);

        AggregateData data = list.get(0);
        assertEquals(0.03, data.getAttributeAsDouble("MinDisc"));
        assertEquals(1, list.size());
    }

    public void testSimpleNullableMin()
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute maxDisc = SaleFinder.nullableDouble().min();

        AggregateList list = new AggregateList(op);
        list.addAggregateAttribute("MinDouble", maxDisc);

        AggregateData data = list.get(0);
        assertEquals(10.0, data.getAttributeAsDouble("MinDouble"));
        assertEquals(1, list.size());
    }


    public void testAllOperationAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(10, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 2:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 3:
                    assertEquals(5, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 4:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 5:
                    assertEquals(15, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 6:
                    assertEquals(12, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 7:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 8:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 9:
                    assertEquals(15, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 10	:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


// CASE 1.2
// List operation: SalesItemFinder.quantity().greaterThan(15)
// AGGR ATTR: SalesItemFinder.quantity().min()
//
//select t0.SALE_ID , min(t0.QTY )
//from SALES_LINE_ITEM t0
//WHERE t0.QTY > 15
//group by t0.SALE_ID

    public void testNotMappedOperationAndAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(7, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 2:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 4:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 5:
                    assertEquals(17, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 1.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().min()
//
//select t0.SALE_ID , min(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.SALE_ID

    public void testMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);

        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 6:
                    assertEquals(12, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 7:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 8:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 10	:
                    assertEquals(12, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


// CASE 1.4
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().min()
//
//select t0.SALE_ID , min(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.QTY > 15
//AND t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.SALE_ID

    public void testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 2.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.06)
// AGGR ATTR: SaleFinder.items().quantity().min()
//
//    select t0.USER_ID , min(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.DISC_PCTG > 0.06
//    group by t0.USER_ID


    public void testNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.06);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 2:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 3:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 4:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.2
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().min()
//
//    select t0.USER_ID , min(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndAggregateAttribute()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 2:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 3:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 4:
                    assertEquals(15, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().min()
//
//    select t0.USER_ID , min(a1.QTY )
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
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(12, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 3:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 4:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }


// CASE 2.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().min()
//
//    select t0.USER_ID , min(t1.QTY )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(15, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 3:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }


// CASE 2.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().min()
//    select t0.USER_ID , min(t1.QTY )
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

        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(10, data.getAttributeAsDouble("MinItems"), 0);
    }

// CASE 2.6
// List Operation: SaleFinder.seller().zipCode().notIn('10001', '10038')
// AGGR ATTR: SaleFinder.items().quantity().min()
//
//    select t0.USER_ID , min(t1.QTY )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(10, data.getAttributeAsDouble("MinItems"), 0);

    }

// CASE 2.7
// List Operation: SaleFinder.items().manufacturer().locationId().eq(100)
// AGGR ATTR: SaleFinder.items().quantity().min()
//
//    select t0.USER_ID , min(t1.QTY )
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
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                 case 1:
                    assertEquals(20, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 2:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 3:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                case 4:
                    assertEquals(10, data.getAttributeAsInt("MinItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 4.1
// Group by MANUFACTURER_ID
// List operation: SalesItemFinder.ALL()
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).min()
//
//    SELECT t0.MANUFACTURER_ID, min(t0.QTY * t1.PRICE)
//    FROM SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    group by t0.MANUFACTURER_ID


    public void testAllOperationAndCalculatedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(125.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 2:
                    assertEquals(99.95, data.getAttributeAsDouble("MinPrice"), 2);
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
//-- AGGR ATTR: SaleItemFinder.quantity().times(SaleItemFinder.productSpecification().originalPrice()).min()
//
//select t0.MANUFACTURER_ID , min(t0.QTY * t1.PRICE )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t0.QTY > 15
//group by t0.MANUFACTURER_ID
    public void testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(250.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 2:
                    assertEquals(200.00, data.getAttributeAsDouble("MinPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 4.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.items().productSpecification().originalPrice()).min()
//
//    select t0.MANUFACTURER_ID , min(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(300, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 2:
                    assertEquals(300, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }


// CASE 4.4
// List operation: SaleItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).min()
//
//    select t0.MANUFACTURER_ID , min(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t0.QTY > 15
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(400, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 2:
                    assertEquals(425, data.getAttributeAsDouble("MinPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 5.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.07)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).min()
//
//    SELECT t0.USER_ID, min(t1.QTY * t2.PRICE)
//    FROM SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND t0.DISC_PCTG > 0.07
//    group by t0.USER_ID

// SELECT T0.SELLER_ID , min((A1.QUANTITY * A2.ORIGINAL_PRICE))
// FROM APP.SALE T0, APP.PRODUCT_SPECIFICATION A2, APP.SALES_LINE_ITEM A1
// WHERE  T0.DISC_PCTG > ?
// AND T0.SALE_ID = A1.SALE_ID
// A1[*].PRODUCT_ID = A2.PROD_ID
// GROUP BY T0.SELLER_ID  [42000-41]


    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.07);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(125, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 3:
                    assertEquals(300, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }




// CASE 5.2
// Group by USER_ID
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).min()
//
//    select t0.USER_ID , min(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(200.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 2:
                    assertEquals(125.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                case 3:
                    assertEquals(300.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 4:
                    assertEquals(187.50, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }


// CASE 5.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).min()
//
//    select t0.USER_ID , min(t1.QTY * t2.PRICE )
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
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(150.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                case 3:
                    assertEquals(300.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;
                case 4:
                    assertEquals(125.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id: "+data.getAttributeAsInt("SellerId"));
            }
        }

    }

// CASE 5.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).min()
//
//    select t0.USER_ID , min(t1.QTY * t2.PRICE )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(150.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                case 3:
                    assertEquals(300.00, data.getAttributeAsDouble("MinPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }



// CASE 5.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).min()
//
//    select t0.USER_ID , min(t1.QTY * t2.PRICE )
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

        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(125.00, data.getAttributeAsDouble("MinPrice"), 0);

    }


    public void testToOneRelationshipInBothOpAndAggregate()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItemPrice", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(25, data.getAttributeAsDouble("MinItemPrice"), 0);
                    break;
                case 6:
                    assertEquals(25, data.getAttributeAsDouble("MinItemPrice"), 0);
                    break;
                case 7:
                    assertEquals(25, data.getAttributeAsDouble("MinItemPrice"), 0);
                    break;
                case 8:
                    assertEquals(30, data.getAttributeAsDouble("MinItemPrice"), 0);
                    break;
                case 9:
                    assertEquals(25, data.getAttributeAsDouble("MinItemPrice"), 0);
                    break;
                case 10	:
                    assertEquals(25, data.getAttributeAsDouble("MinItemPrice"), 0);
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());
        assertEquals(20, aggregateList.getAggregateDataAt(0).getAttributeAsInt("MinItems"), 0);
    }


        //todo: add test cases for date, timestamp, string, float, char, byte, etc.

    public void testMinTimestamp() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.saleDate().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinTs", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MinTs"));
                    break;
                case 2:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MinTs"));
                    break;
                case 3:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MinTs"));
                    break;
                case 4:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()), data.getAttributeAsTimestamp("MinTs"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMinDate() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.settleDate().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDate", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(dateFormat.parse("2004-01-13"), data.getAttributeAsDate("MinDate"));
                    break;
                case 2:
                    assertEquals(dateFormat.parse("2004-02-12"), data.getAttributeAsDate("MinDate"));
                    break;
                case 3:
                    assertEquals(dateFormat.parse("2004-02-15"), data.getAttributeAsDate("MinDate"));
                    break;
                case 4:
                    assertEquals(dateFormat.parse("2004-03-01"), data.getAttributeAsDate("MinDate"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMinTime() throws ParseException
    {
        Operation op = AlarmCategoryFinder.all();
        MithraAggregateAttribute aggrAttr = AlarmCategoryFinder.time().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinTime", aggrAttr);
        aggregateList.addGroupBy("CategoryId", AlarmCategoryFinder.category());

        assertEquals(3, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("CategoryId"))
            {
                case 1:
                    assertEquals(Time.withMillis(3, 11, 23, 0), data.getAttributeAsTime("MinTime"));
                    break;
                case 2:
                    assertEquals(Time.withMillis(3, 11, 23, 0), data.getAttributeAsTime("MinTime"));
                    break;
                case 3:
                    assertEquals(Time.withMillis(1, 2, 2, 3), data.getAttributeAsTime("MinTime"));
                    break;
                case 4:
                    assertEquals(Time.withMillis(3, 11, 23, 0), data.getAttributeAsTime("MinTime"));
                    break;
                default:
                    fail("Invalid category id");
            }
        }
    }

    public void testMinBoolean()
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.activeBoolean().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinFlag", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(false, data.getAttributeAsBoolean("MinFlag"));
                    break;
                case 2:
                    assertEquals(false, data.getAttributeAsBoolean("MinFlag"));
                    break;
                case 3:
                    assertEquals(true, data.getAttributeAsBoolean("MinFlag"));
                    break;
                case 4:
                    assertEquals(false, data.getAttributeAsBoolean("MinFlag"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMinString() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.description().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDesc", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals("Sale 0001", data.getAttributeAsString("MinDesc"));
                    break;
                case 2:
                    assertEquals("Sale 0004",data.getAttributeAsString("MinDesc"));
                    break;
                case 3:
                     assertEquals("Sale 0007", data.getAttributeAsString("MinDesc"));
                    break;
                case 4:
                     assertEquals("Sale 0009", data.getAttributeAsString("MinDesc"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testMinCharacter() throws ParseException
    {
        Operation op = ParentNumericAttributeFinder.all();
        MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.charAttr().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinChar", aggrAttr);
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);
            int userId = data.getAttributeAsInt("UserId");
            if(userId == 1)
            {
               assertEquals('a', data.getAttributeAsCharacter("MinChar"));
            }
            else if(userId == 2)
            {
               assertEquals('d', data.getAttributeAsCharacter("MinChar"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testMinBigDecimal() throws Exception
    {
        Operation op = ParentNumericAttributeFinder.all();
        MithraAggregateAttribute aggrAttr1 = ParentNumericAttributeFinder.bigDecimalAttr().min();
        MithraAggregateAttribute aggrAttr2 = ParentNumericAttributeFinder.veryBigDecimalAttr().min();

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
                    assertEquals(new BigDecimal("100.50"), data.getAttributeAsBigDecimal("Attr1"));
                    assertEquals(new BigDecimal("111111111111111111"), data.getAttributeAsBigDecimal("Attr2"));
                    break;
                case 2:

                    assertEquals(new BigDecimal("400.50"), data.getAttributeAsBigDecimal("Attr1"));
                    assertEquals(new BigDecimal("444444444444444444"), data.getAttributeAsBigDecimal("Attr2"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

    public void testMappedMinBigDecimal() throws Exception
    {
        Operation op = BigOrderFinder.all();
        MithraAggregateAttribute aggrAttr1 = BigOrderFinder.items().price().min();

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
                    assertEquals(new BigDecimal("9.50"), data.getAttributeAsBigDecimal("Attr1"));
                    break;
                case 2:
                    assertEquals(new BigDecimal("1.00"), data.getAttributeAsBigDecimal("Attr1"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

    public void testMinDateYear()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().year().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinYear", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        assertEquals(1900, aggregateList.get(0).getAttributeAsInteger("MinYear"));
        assertEquals(1900, aggregateList.get(1).getAttributeAsInteger("MinYear"));
    }

    public void testMinDateMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().month().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        assertEquals(1, aggregateList.get(0).getAttributeAsInteger("MinMonth"));
        assertEquals(1, aggregateList.get(1).getAttributeAsInteger("MinMonth"));
    }

    public void testMinDateDayOfMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().dayOfMonth().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDayOfMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        assertEquals(1, aggregateList.get(0).getAttributeAsInteger("MinDayOfMonth"));
        assertEquals(1, aggregateList.get(1).getAttributeAsInteger("MinDayOfMonth"));
    }

    public void testMinTimestampYear()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().year().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinYear", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        assertEquals(2004, aggregateList.get(0).getAttributeAsInteger("MinYear"));
    }

    public void testMinTimestampMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().month().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(1, aggregateList.get(0).getAttributeAsInteger("MinMonth"));
    }

    public void testMinTimestampDayOfMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().dayOfMonth().min();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDayOfMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(12, aggregateList.get(0).getAttributeAsInteger("MinDayOfMonth"));
    }
}
