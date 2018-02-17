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
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;



public class TestCount extends MithraTestAbstract
{
    private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
        {
            return new Class[]
                    {
                            Sale.class,
                            Seller.class,
                            SalesLineItem.class,
                            WishListItem.class,
                            ParaDesk.class,
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


//CASE 1
//List Operation: SalesItemFinder.ALL()
//AggrAttr: SalesItemFinder.quantity().count()
//
//SELECT t0.SALE_ID, count(t0.QTY)
//FROM SALES_LINE_ITEM t0
//GROUP BY t0.SALE_ID

    public void testAllOperationAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(10, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 2:
                    assertEquals(3, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 3:
                    assertEquals(3, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 4:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 5:
                    assertEquals(3, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 6:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 7:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 8:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 9:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 10	:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


// CASE 1.2
// List operation: SalesItemFinder.quantity().greaterThan(15)
// AGGR ATTR: SalesItemFinder.quantity().count()
//
//select t0.SALE_ID , count(t0.QTY )
//from SALES_LINE_ITEM t0
//WHERE t0.QTY > 15
//group by t0.SALE_ID

    public void testNotMappedOperationAndAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(7, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 2:
                    assertEquals(3, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 4:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 5:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 7:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 8:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 9:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 1.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().count()
//
//select t0.SALE_ID , count(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.SALE_ID

    public void testMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);

        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 6:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 7:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 8:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 9:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 10	:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


// CASE 1.4
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().count()
//
//select t0.SALE_ID , count(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.QTY > 15
//AND t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.SALE_ID

    public void testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 7:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 8:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 9:
                    assertEquals(1, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 2.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.06)
// AGGR ATTR: SaleFinder.items().quantity().count()
//
//    select t0.USER_ID , count(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.DISC_PCTG > 0.06
//    group by t0.USER_ID


    public void testNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.06);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(3, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 2:
                    assertEquals(7, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 3:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 4:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.2
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().count()
//
//    select t0.USER_ID , count(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndAggregateAttribute()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 2:
                    assertEquals(5, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 3:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 4:
                    assertEquals(2, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().count()
//
//    select t0.USER_ID , count(a1.QTY )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(5, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 3:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 4:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }


// CASE 2.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().count()
//
//    select t0.USER_ID , count(t1.QTY )
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
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(3, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 3:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }


// CASE 2.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().count()
//    select t0.USER_ID , count(t1.QTY )
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

        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(4, data.getAttributeAsDouble("CountItems"), 0);
    }

// CASE 2.6
// List Operation: SaleFinder.seller().zipCode().notIn('10001', '10038')
// AGGR ATTR: SaleFinder.items().quantity().count()
//
//    select t0.USER_ID , count(t1.QTY )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(4, data.getAttributeAsDouble("CountItems"), 0);
    }

// CASE 2.7
// List Operation: SaleFinder.items().manufacturer().locationId().eq(100)
// AGGR ATTR: SaleFinder.items().quantity().count()
//
//    select t0.USER_ID , count(t1.QTY )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                 case 1:
                    assertEquals(4, data.getAttributeAsInt("CountItems"));
                    assertEquals(4, ((Integer)data.getAttributeAsObject("CountItems")).intValue());
                    break;
                case 2:
                    assertEquals(5, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 3:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                case 4:
                    assertEquals(4, data.getAttributeAsInt("CountItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 4.1
// Group by MANUFACTURER_ID
// List operation: SalesItemFinder.ALL()
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).count()
//
//    SELECT t0.MANUFACTURER_ID, count(t0.QTY * t1.PRICE)
//    FROM SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    group by t0.MANUFACTURER_ID


    public void testAllOperationAndCalculatedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(15, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 2:
                    assertEquals(7, data.getAttributeAsDouble("CountPrice"), 2);
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
//-- AGGR ATTR: SaleItemFinder.quantity().times(SaleItemFinder.productSpecification().originalPrice()).count()
//
//select t0.MANUFACTURER_ID , count(t0.QTY * t1.PRICE )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t0.QTY > 15
//group by t0.MANUFACTURER_ID
    public void testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(8, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 2:
                    assertEquals(2, data.getAttributeAsDouble("CountPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 4.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.items().productSpecification().originalPrice()).count()
//
//    select t0.MANUFACTURER_ID , count(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(6, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 2:
                    assertEquals(3, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }


// CASE 4.4
// List operation: SaleItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).count()
//
//    select t0.MANUFACTURER_ID , count(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t0.QTY > 15
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(3, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 2:
                    assertEquals(1, data.getAttributeAsDouble("CountPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 5.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.07)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).count()
//
//    SELECT t0.USER_ID, count(t1.QTY * t2.PRICE)
//    FROM SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND t0.DISC_PCTG > 0.07
//    group by t0.USER_ID

// SELECT T0.SELLER_ID , count((A1.QUANTITY * A2.ORIGINAL_PRICE))
// FROM APP.SALE T0, APP.PRODUCT_SPECIFICATION A2, APP.SALES_LINE_ITEM A1
// WHERE  T0.DISC_PCTG > ?
// AND T0.SALE_ID = A1.SALE_ID
// A1[*].PRODUCT_ID = A2.PROD_ID
// GROUP BY T0.SELLER_ID  [42000-41]


    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.07);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(5, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 3:
                    assertEquals(4, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }




// CASE 5.2
// Group by USER_ID
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).count()
//
//    select t0.USER_ID , count(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(4, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 2:
                    assertEquals(5, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                case 3:
                    assertEquals(4, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 4:
                    assertEquals(2, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }


// CASE 5.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).count()
//
//    select t0.USER_ID , count(t1.QTY * t2.PRICE )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(5, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                case 3:
                    assertEquals(4, data.getAttributeAsDouble("CountPrice"), 2);
                    break;
                case 4:
                    assertEquals(4, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id: "+data.getAttributeAsInt("SellerId"));
            }
        }

    }

// CASE 5.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).count()
//
//    select t0.USER_ID , count(t1.QTY * t2.PRICE )
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
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(3, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                case 3:
                    assertEquals(4, data.getAttributeAsDouble("CountPrice"), 2);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }



// CASE 5.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).count()
//
//    select t0.USER_ID , count(t1.QTY * t2.PRICE )
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

        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(4, data.getAttributeAsDouble("CountPrice"), 0);
    }


    public void testToOneRelationshipInBothOpAndAggregate()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItemPrice", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(1, data.getAttributeAsDouble("CountItemPrice"), 0);
                    break;
                case 6:
                    assertEquals(2, data.getAttributeAsDouble("CountItemPrice"), 0);
                    break;
                case 7:
                    assertEquals(2, data.getAttributeAsDouble("CountItemPrice"), 0);
                    break;
                case 8:
                    assertEquals(2, data.getAttributeAsDouble("CountItemPrice"), 0);
                    break;
                case 9:
                    assertEquals(1, data.getAttributeAsDouble("CountItemPrice"), 0);
                    break;
                case 10	:
                    assertEquals(1, data.getAttributeAsDouble("CountItemPrice"), 0);
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());
        assertEquals(3, aggregateList.getAggregateDataAt(0).getAttributeAsInt("CountItems"), 0);
    }


        //todo: add test cases for date, timestamp, string, float, char, byte, etc.

    public void testCountTimestamp() throws ParseException
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.saleDate().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TsCount", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(3, data.getAttributeAsInt("TsCount"));
                    break;
                case 2:
                    assertEquals(3, data.getAttributeAsInt("TsCount"));
                    break;
                case 3:
                    assertEquals(2, data.getAttributeAsInt("TsCount"));
                    break;
                case 4:
                    assertEquals(2, data.getAttributeAsInt("TsCount"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testCountDate() throws ParseException
    {
        Operation op = ParaDeskFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("DateCount", aggrAttr);
        aggregateList.addGroupBy("activityFlag", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            if(data.getAttributeAsBoolean("activityFlag"))
            {
                assertEquals(17, data.getAttributeAsInt("DateCount"));
            }
            else
            {
               assertEquals(3, data.getAttributeAsInt("DateCount"));
            }
        }
    }

    public void testCountBoolean() throws ParseException
    {
        Operation op = ParaDeskFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = ParaDeskFinder.activeBoolean().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("ActiveCount", aggrAttr);
        aggregateList.addGroupBy("activityFlag", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            if(data.getAttributeAsBoolean("activityFlag"))
            {
                assertEquals(17, data.getAttributeAsInt("ActiveCount"));
            }
            else
            {
               assertEquals(3, data.getAttributeAsInt("ActiveCount"));
            }
        }
    }

    public void testCountString() throws ParseException
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.description().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("DescCount", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(3, data.getAttributeAsInt("DescCount"));
                    break;
                case 2:
                    assertEquals(3,data.getAttributeAsInt("DescCount"));
                    break;
                case 3:
                     assertEquals(2, data.getAttributeAsInt("DescCount"));
                    break;
                case 4:
                     assertEquals(2, data.getAttributeAsInt("DescCount"));
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

    public void testCountCharacter() throws ParseException
    {
        Operation op = ParentNumericAttributeFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.charAttr().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CharCount", aggrAttr);
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());

        assertEquals(2, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);
            int userId = data.getAttributeAsInt("UserId");
            if(userId == 1)
            {
               assertEquals(3, data.getAttributeAsInt("CharCount"));
            }
            else if(userId == 2)
            {
               assertEquals(3, data.getAttributeAsInt("CharCount"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCountBigDecimal() throws Exception
    {
        Operation op = ParentNumericAttributeFinder.all();
        MithraAggregateAttribute aggrAttr1 = ParentNumericAttributeFinder.bigDecimalAttr().count();
        MithraAggregateAttribute aggrAttr2 = ParentNumericAttributeFinder.veryBigDecimalAttr().count();

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
                    assertEquals(3, data.getAttributeAsInt("Attr1"));
                    assertEquals(3, data.getAttributeAsInt("Attr2"));
                    break;
                case 2:

                    assertEquals(3, data.getAttributeAsInt("Attr1"));
                    assertEquals(3, data.getAttributeAsInt("Attr2"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

    public void testMappedCountBigDecimal() throws Exception
    {
        Operation op = BigOrderFinder.all();
        MithraAggregateAttribute aggrAttr1 = BigOrderFinder.items().quantity().count();

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
                    assertEquals(5, data.getAttributeAsInt("Attr1"));
                    break;
                case 2:
                    assertEquals(6, data.getAttributeAsInt("Attr1"));
                    break;
                default:
                    fail("Invalid user id");
            }
        }
    }

    public void testCountDateYear()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().year().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountYear", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        aggregateList.setAscendingOrderBy("ActiveBoolean");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("CountYear"));
    }

    public void testCountDateMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().month().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        aggregateList.setAscendingOrderBy("ActiveBoolean");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("CountMonth"));
    }

    public void testCountDateDayOfMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().dayOfMonth().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountDayOfMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        aggregateList.setAscendingOrderBy("ActiveBoolean");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("CountDayOfMonth"));
    }

    public void testCountTimestampYear()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().year().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountYear", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("CountYear"));
        assertEquals(1, aggregateList.get(1).getAttributeAsInteger("CountYear"));
        assertEquals(3, aggregateList.get(2).getAttributeAsInteger("CountYear"));
    }

    public void testCountTimestampMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().month().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());
        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("CountMonth"));
        assertEquals(1, aggregateList.get(1).getAttributeAsInteger("CountMonth"));
        assertEquals(3, aggregateList.get(2).getAttributeAsInteger("CountMonth"));
    }

    public void testCountTimestampDayOfMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().dayOfMonth().count();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("CountDayOfMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());
        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(3, aggregateList.get(0).getAttributeAsInteger("CountDayOfMonth"));
        assertEquals(1, aggregateList.get(1).getAttributeAsInteger("CountDayOfMonth"));
        assertEquals(3, aggregateList.get(2).getAttributeAsInteger("CountDayOfMonth"));
    }
}

