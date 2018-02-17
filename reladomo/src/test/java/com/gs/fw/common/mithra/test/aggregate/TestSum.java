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
import com.gs.fw.common.mithra.test.domain.Account;
import com.gs.fw.common.mithra.test.domain.BigOrder;
import com.gs.fw.common.mithra.test.domain.BigOrderItem;
import com.gs.fw.common.mithra.test.domain.BigOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.Manufacturer;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ProductSpecification;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.SalesLineItemFinder;
import com.gs.fw.common.mithra.test.domain.Seller;
import com.gs.fw.common.mithra.test.domain.SellerFinder;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.WishListItem;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistory;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigDecimal;
import java.util.Set;


public class TestSum extends MithraTestAbstract
{
    public static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Sale.class,
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
                        Account.class,
                        PositionAdjustmentHistory.class,
                        TinyBalance.class
                };
    }

//List Operation: SalesItemFinder.ALL()
//AggrAttr: SalesItemFinder.quantity().sum()
//
//SELECT t0.SALE_ID, sum(t0.QTY)
//FROM SALES_LINE_ITEM t0
//GROUP BY t0.SALE_ID


    public void testAllOperationAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(10, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 3:
                    assertEquals(25, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 4:
                    assertEquals(30, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 5:
                    assertEquals(55, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 6:
                    assertEquals(27, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 7:
                    assertEquals(30, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 8:
                    assertEquals(30, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 9:
                    assertEquals(31, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 10:
                    assertEquals(22, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 1.2
// List operation: SalesItemFinder.quantity().greaterThan(15)
// AGGR ATTR: SalesItemFinder.quantity().sum()
//
//select t0.SALE_ID , sum(t0.QTY )
//from SALES_LINE_ITEM t0
//WHERE t0.QTY > 15
//group by t0.SALE_ID


    public void testRegularOperationAndAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        assertEquals(7, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 4:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 5:
                    assertEquals(40, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsDouble("TotalItems"), 0);
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

    public void testMappedToOneOpAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);

        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 6:
                    assertEquals(27, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 7:
                    assertEquals(30, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 8:
                    assertEquals(30, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 10:
                    assertEquals(12, data.getAttributeAsDouble("TotalItems"), 0);
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

    public void testAndOperationWithToOneOpAndRegularAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 5:
                    assertEquals(17, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 7:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 8:
                    assertEquals(20, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 9:
                    assertEquals(16, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 2.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.06)
// AGGR ATTR: SaleFinder.items().quantity().sum()
//
//    select t0.USER_ID , sum(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.DISC_PCTG > 0.06
//    group by t0.USER_ID


    public void testRegularOpAndMappedToManyAggregateAttribute()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.06);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(112, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 3:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 4:
                    assertEquals(53, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 2.2
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().sum()
//
//    select t0.USER_ID , sum(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testToManyOpAndAggregateAttribute()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(80, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(85, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 3:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 4:
                    assertEquals(31, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 2.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().sum()
//
//    select t0.USER_ID , sum(a1.QTY )
//    from SALE t0, SALES_LINE_ITEM a1
//    WHERE t0.SALE_ID = a1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//                WHERE t0.SALE_ID = t1.SALE_ID
//                AND t1.PROD_ID = t2.PROD_ID
//                AND t2.PRICE > 20)
//    group by t0.USER_ID


    public void testOpWithLinkedToManyAndMappedToManyAttribute()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.0);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(82, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 3:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 4:
                    assertEquals(53, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 2.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().sum()
//
//    select t0.USER_ID , sum(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.DISC_PCTG > 0.07
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20)
//    group by t0.USER_ID

    public void testAndOperationWithLinkedToManyMapperAndMappedToManyAttribute()
    {
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20.0);
        op = op.and(SaleFinder.discountPercentage().greaterThan(0.07));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(55, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 3:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 2.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().sum()
//    --  Expected results
//    --	USER_ID		SUM(QTY)
//    --	-------		-----
//    --	4			53
//    select t0.USER_ID , sum(t1.QTY )
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
//
//    select t0.USER_ID , sum(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_USER t3
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t0.USER_ID = t3.USER_ID
//    AND t3.ZIP_CODE NOT IN ('10001','10038')
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, TEST_PROD_INFO e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.PROD_ID = e2.PROD_ID
//                AND e2.PRICE > 20
//                )
//    group by t0.USER_ID


    public void testAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        Set<String> zipcodeSet = new UnifiedSet(2);
        zipcodeSet.add("10001");
        zipcodeSet.add("10038");
        Operation op = SaleFinder.items().productSpecs().originalPrice().greaterThan(20);
        op = op.and(SaleFinder.seller().zipCode().notIn(zipcodeSet));

        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(53, data.getAttributeAsDouble("TotalItems"), 0);
    }

// CASE 2.6
// List Operation: SaleFinder.seller().zipCode().notIn('10001', '10038')
// AGGR ATTR: SaleFinder.items().quantity().sum()
//
//    --  Expected results
//    --	USER_ID		SUM(QTY)
//    --	-------		-----
//    --	4			53
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(53, data.getAttributeAsDouble("TotalItems"), 0);

    }

// CASE 2.7
// List Operation: SaleFinder.items().manufacturer().locationId().eq(100)
// AGGR ATTR: SaleFinder.items().quantity().sum()
//    --  Expected results
//    --	USER_ID		SUM(QTY)
//    --	-------		-----
//    --	1			80
//    --	2			85
//    --  3			60
//    --	4 			53
//
//    select t0.USER_ID , sum(t1.QTY )
//    from SALE t0, SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1, MANUFACTURER e2
//                WHERE t0.SALE_ID = e1.SALE_ID
//                AND e1.MANUFACTURER_ID = e2.MANUFACTURER_ID
//                AND e2.LOCATION_ID = 100)
//    group by t0.USER_ID


    public void testOpWithLinkedToManyMapperAndToManyAggregateAttribute()
    {
        Operation op = SaleFinder.items().manufacturers().locationId().eq(2);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(80, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(85, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 3:
                    assertEquals(60, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 4:
                    assertEquals(53, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 4.1
// Group by MANUFACTURER_ID
// List operation: SalesItemFinder.ALL()
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).sum()
//
//    SELECT t0.MANUFACTURER_ID, sum(t0.QTY * t1.PRICE)
//    FROM SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    group by t0.MANUFACTURER_ID


    public void testAllOperationAndCalculatedAggregateAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(4604.60, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 2:
                    assertEquals(1699.95, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

    public void testAllOperationAndCalculatedAggregateAttributeWithBigDecimal()
    {
        Operation op = BigOrderItemFinder.all();
        MithraAggregateAttribute aggrAttr = BigOrderItemFinder.quantity().times(BigOrderItemFinder.price()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("OrderId", BigOrderItemFinder.orderId());
        assertEquals(5, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("OrderId"))
            {
                case 100:
                    assertEquals(new BigDecimal("47.50000"), data.getAttributeAsBigDecimal("TotalPrice"));
                    break;
                case 101:
                    assertEquals(new BigDecimal("260.00000"), data.getAttributeAsBigDecimal("TotalPrice"));
                    break;
                case 102:
                    assertEquals(new BigDecimal("620.00000"), data.getAttributeAsBigDecimal("TotalPrice"));
                    break;
                case 103:
                    assertEquals(new BigDecimal("199.80000"), data.getAttributeAsBigDecimal("TotalPrice"));
                    break;
                case 104:
                    assertEquals(new BigDecimal("3000926.99700"), data.getAttributeAsBigDecimal("TotalPrice"));
                    break;
                default:
                    fail("Invalid order id");
            }
        }
    }

//  --CASE 4.2
//-- Group by MANUFACTURER_ID
//-- List operation: SalesItemFinder.quantity().greaterThan(15)
//
//-- AGGR ATTR: SaleItemFinder.quantity().times(SaleItemFinder.productSpecification().originalPrice()).sum()
//
//select t0.MANUFACTURER_ID , sum(t0.QTY * t1.PRICE )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t0.QTY > 15

    //group by t0.MANUFACTURER_ID
    public void testRegularOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(3087.10, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 2:
                    assertEquals(625, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 4.3
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.items().productSpecification().originalPrice()).sum()
//
//    select t0.MANUFACTURER_ID , sum(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(2430.00, data.getAttributeAsDouble("TotalPrice"), 0);
                    break;
                case 2:
                    assertEquals(1175.00, data.getAttributeAsDouble("TotalPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }

    }

// CASE 4.4
// List operation: SaleItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().times(SalesItemFinder.productSpecification().originalPrice()).sum()
//    select t0.MANUFACTURER_ID , sum(t0.QTY * t1.PRICE )
//    FROM SALES_LINE_ITEM t0,TEST_PROD_INFO t1
//    WHERE t0.PROD_ID = t1.PROD_ID
//    AND t0.QTY > 15
//    AND t1.PRICE > 20
//    group by t0.MANUFACTURER_ID

    public void testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("ManufacturerId", SalesLineItemFinder.manufacturerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("ManufacturerId"))
            {
                case 1:
                    assertEquals(1500.00, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 2:
                    assertEquals(425, data.getAttributeAsDouble("TotalPrice"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

// CASE 5.1
// Group by USER_ID
// List operation: SaleFinder.discountPercentage.greaterThan(0.07)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).sum()
//
//    SELECT t0.USER_ID, sum(t1.QTY * t2.PRICE)
//    FROM SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND t0.DISC_PCTG > 0.07
//    group by t0.USER_ID

// SELECT T0.SELLER_ID , SUM((A1.QUANTITY * A2.ORIGINAL_PRICE))
// FROM APP.SALE T0, APP.PRODUCT_SPECIFICATION A2, APP.SALES_LINE_ITEM A1
// WHERE  T0.DISC_PCTG > ?
// AND T0.SALE_ID = A1.SALE_ID
// A1[*].PRODUCT_ID = A2.PROD_ID
// GROUP BY T0.SELLER_ID  [42000-41]


    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.discountPercentage().greaterThan(0.07);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().productSpecs().originalPrice().times(SaleFinder.items().quantity()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(1387.30, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 3:
                    assertEquals(1730.00, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
    }


    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper2()
    {
        Operation op = SalesLineItemFinder.sale().discountPercentage().greaterThan(0.07);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().times(SalesLineItemFinder.quantity()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SalesLineItemFinder.sale().sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(1387.30, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 3:
                    assertEquals(1730.00, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
    }

// CASE 5.2
// Group by USER_ID
// List operation: SaleFinder.items().quantity().greaterThan(15)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).sum()
//
//    select t0.USER_ID , sum(t1.QTY * t2.PRICE )
//    from SALE t0, SALES_LINE_ITEM t1, TEST_PROD_INFO t2
//    WHERE t0.SALE_ID = t1.SALE_ID
//    AND t1.PROD_ID = t2.PROD_ID
//    AND EXISTS (SELECT 1 FROM SALES_LINE_ITEM e1 WHERE t0.SALE_ID = e1.SALE_ID AND e1.QTY > 15)
//    group by t0.USER_ID

    public void testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        Operation op = SaleFinder.items().quantity().greaterThan(15);
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1099.80, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 2:
                    assertEquals(1387.30, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                case 3:
                    assertEquals(1730.00, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 4:
                    assertEquals(587.50, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 5.3
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20)
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).sum()
//    select t0.USER_ID , sum(t1.QTY * t2.PRICE )
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
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {

                case 2:
                    assertEquals(1612.50, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                case 3:
                    assertEquals(1730.00, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 4:
                    assertEquals(1012.50, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                default:
                    fail("Invalid seller id: " + data.getAttributeAsInt("SellerId"));
            }
        }

    }

// CASE 5.4
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.discountPercentage().greaterThan(0.07))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).sum()
//    select t0.USER_ID , sum(t1.QTY * t2.PRICE )
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
        MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.getAggregateDataAt(i);
            switch (data.getAttributeAsInt("SellerId"))
            {
                case 2:
                    assertEquals(862.50, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                case 3:
                    assertEquals(1730.00, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }

    }

// CASE 5.5
// List operation: SaleFinder.items().prodSpecs().price().greaterThan(20).AND(SaleFinder.users().zipCode().notIn('10001', '10038'))
// AGGR ATTR: SaleFinder.items().quantity().times(SaleFinder.items().productSpecification().originalPrice()).sum()
//    select t0.USER_ID , sum(t1.QTY * t2.PRICE )
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

        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().times(SaleFinder.items().productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());

        AggregateData data = aggregateList.getAggregateDataAt(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(1012.50, data.getAttributeAsDouble("TotalPrice"));
    }


    public void testToOneRelationshipInBothOpAndAggregate()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {
                case 5:
                    assertEquals(25, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 6:
                    assertEquals(55, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 7:
                    assertEquals(55, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 8:
                    assertEquals(63, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 9:
                    assertEquals(25, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 10:
                    assertEquals(25, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testToManyRelationshipThatBecomesToOne()
    {
        Operation op = SaleFinder.items().itemId().eq(2);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(1, aggregateList.size());
        assertEquals(60, aggregateList.getAggregateDataAt(0).getAttributeAsDouble("TotalItems"), 0);
    }

//    -- CASE 3.0
// List operation: SalesFinder.ALL()
// AGGR ATTR: SaleFinder.discountPercentage().times( (SaleFinder.items().quantity().sum()).sum()
//
//    SELECT t0.SALE_ID, (t0.DISC_PCTG * sum(t1.QTY))
//    FROM eod.SALE t0, eod.SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    group by t0.SALE_ID, t0.DISC_PCTG

// This is not posible in DB2 so it needs to be rewritten
// AGGR ATTR: SaleFinder.discountPercentage().times( (SaleFinder.items().quantity()).sum()
// SELECT  t0.SALE_ID, sum(t0.DISC_PCTG * t1.QTY)
//    FROM eod.SALE t0, eod.SALES_LINE_ITEM t1
//    WHERE t0.SALE_ID = t1.SALE_ID
//    group by t0.SALE_ID, t0.DISC_PCTG

    public void test3()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.discountPercentage().times(SaleFinder.items().quantity()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("SaleId", SaleFinder.saleId());
        assertEquals(10, aggregateList.size());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {
                case 1:
                    assertEquals(1, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 2:
                    assertEquals(4.2, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 3:
                    assertEquals(0.75, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 4:
                    assertEquals(3, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 5:
                    assertEquals(4.4, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 6:
                    assertEquals(1.89, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 7:
                    assertEquals(2.4, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 8:
                    assertEquals(2.7, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 9:
                    assertEquals(2.17, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 10:
                    assertEquals(1.54, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

// CASE 7.1
// Group by USER_ID
//
// List operation: SaleFinder.discountPercentage.greaterThan(0.07)
// AGGR ATTR: SaleFinder.discountPercentage().times( (SaleFinder.items().quantity() * SaleFinder.items().prodSpecs().price()).sum()).sum()
//
//SELECT t0.USER_ID, sum(t0.DISC_PCTG *
//                      (SELECT sum(t1.QTY * t2.PRICE)
//					   FROM eod.SALES_LINE_ITEM t1, eod.TEST_PROD_INFO t2
//					   WHERE t0.SALE_ID = t1.SALE_ID
//					   AND t1.PROD_ID = t2.PROD_ID
//					   ))
//FROM eod.SALE t0
//WHERE t0.DISC_PCTG > 0.07
//GROUP BY t0.USER_ID

    public void test7()
    {
        Operation op = SalesLineItemFinder.sale().discountPercentage().greaterThan(0.07);
        MithraAggregateAttribute aggrAttr =
                SalesLineItemFinder.quantity().times
                        (SalesLineItemFinder.productSpecs().originalPrice()).times
                        (SalesLineItemFinder.sale().discountPercentage()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalPrice", aggrAttr);
        aggregateList.addGroupBy("UserId", SalesLineItemFinder.sale().sellerId());
        assertEquals(2, aggregateList.size());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("UserId"))
            {
                case 2:
                    assertEquals(121.48, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;
                case 3:
                    assertEquals(147.7, data.getAttributeAsDouble("TotalPrice"), 0.01);
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }


    public void testTestTest()
    {
        Operation op = SellerFinder.all();
        MithraAggregateAttribute attr = SellerFinder.sales().items().quantity().times(SellerFinder.sales().items().sale().discountPercentage()).sum();


        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("Total", attr);
        aggregateList.addGroupBy("SellerId", SellerFinder.sellerId());
        assertEquals(4, aggregateList.size());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(5.95, data.getAttributeAsDouble("Total"), 0.01);
                    break;
                case 2:
                    assertEquals(9.29, data.getAttributeAsDouble("Total"), 0.01);
                    break;

                case 3:
                    assertEquals(5.1, data.getAttributeAsDouble("Total"), 0.01);
                    break;
                case 4:
                    assertEquals(3.71, data.getAttributeAsDouble("Total"), 0.01);
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testAllOpWithCalculatedAggrAttrWithLinkedMapperWithMoreThanOneToManyMappers()
    {
        Operation op = SellerFinder.all();

        com.gs.fw.common.mithra.MithraAggregateAttribute attr = SellerFinder.sales().items().productSpecs().originalPrice().times(SellerFinder.sales().items().quantity()).sum();


        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("Total", attr);
        aggregateList.addGroupBy("SellerId", SellerFinder.sellerId());
        assertEquals(4, aggregateList.size());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1424.75, data.getAttributeAsDouble("Total"), 0.01);
                    break;
                case 2:
                    assertEquals(2137.3, data.getAttributeAsDouble("Total"), 0.01);
                    break;

                case 3:
                    assertEquals(1730.00, data.getAttributeAsDouble("Total"), 0.01);
                    break;
                case 4:
                    assertEquals(1012.50, data.getAttributeAsDouble("Total"), 0.01);
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }


    public void testAllOpWithCalculatedAggrAttrWithSingleAttributeAndLinkedMapper()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute attr = SaleFinder.discountPercentage().times(SaleFinder.items().productSpecs().originalPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("DiscountPrice", attr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        assertEquals(4, aggregateList.size());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(4.874, data.getAttributeAsDouble("DiscountPrice"), 0.01);
                    break;
                case 2:
                    assertEquals(10.899, data.getAttributeAsDouble("DiscountPrice"), 0.01);
                    break;

                case 3:
                    assertEquals(10.07, data.getAttributeAsDouble("DiscountPrice"), 0.01);
                    break;
                case 4:
                    assertEquals(5.25, data.getAttributeAsDouble("DiscountPrice"), 0.01);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
    }

//
//SELECT t0.MANUFACTURER_ID, sum(t0.QTY)
//FROM SALES_LINE_ITEM t0
//GROUP BY t0.MANUFACTURER_ID , t0.SALE_ID

    public void testMultipleGroupByAttributes()
    {
        Operation op = SalesLineItemFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("Manufacturer", SalesLineItemFinder.manufacturerId());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(12, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("Manufacturer"))
            {

                case 1:
                    switch (data.getAttributeAsInt("SaleId"))
                    {
                        case 1:
                            assertEquals(20, data.getAttributeAsInt("TotalItems"));
                            break;

                        case 2:
                            assertEquals(40, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 4:
                            assertEquals(30, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 5:
                            assertEquals(38, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 7:
                            assertEquals(30, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 8:
                            assertEquals(30, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 9:
                            assertEquals(31, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 10:
                            assertEquals(22, data.getAttributeAsInt("TotalItems"));
                            break;
                        default:
                            fail("Valid ManufacturerId but invalid SaleId");
                    }
                    break;
                case 2:
                    switch (data.getAttributeAsInt("SaleId"))
                    {
                        case 2:
                            assertEquals(20, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 3:
                            assertEquals(25, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 5:
                            assertEquals(17, data.getAttributeAsInt("TotalItems"));
                            break;
                        case 6:
                            assertEquals(27, data.getAttributeAsInt("TotalItems"));
                            break;
                        default:
                            fail("Valid ManufacturerId but invalid SaleId");
                    }

                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

    public void testMultipleAggregateattributes()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThan(15);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr1 = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().originalPrice()).sum();
        MithraAggregateAttribute aggrAttr2 = SalesLineItemFinder.quantity().times(SalesLineItemFinder.productSpecs().discountPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalSalePrice", aggrAttr1);
        aggregateList.addAggregateAttribute("TotalDiscountedPrice", aggrAttr2);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(7, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 1:
                    assertEquals(250.00, data.getAttributeAsDouble("TotalSalePrice"), 0);
                    assertEquals(230.00, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;
                case 2:
                    assertEquals(849.8, data.getAttributeAsDouble("TotalSalePrice"), 0);
                    assertEquals(729.8, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;

                case 4:
                    assertEquals(399.80, data.getAttributeAsDouble("TotalSalePrice"), 0.01);
                    assertEquals(300, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;
                case 5:
                    assertEquals(712.5, data.getAttributeAsDouble("TotalSalePrice"), 0);
                    assertEquals(689.5, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;

                case 7:
                    assertEquals(500, data.getAttributeAsDouble("TotalSalePrice"), 0);
                    assertEquals(500, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;

                case 8:
                    assertEquals(600, data.getAttributeAsDouble("TotalSalePrice"), 0);
                    assertEquals(500, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;

                case 9:
                    assertEquals(400, data.getAttributeAsDouble("TotalSalePrice"), 0);
                    assertEquals(400, data.getAttributeAsDouble("TotalDiscountedPrice"), 0);
                    break;

                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

// CASE 1.7
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20)
// AGGR ATTR: SalesItemFinder.quantity().sum()
//
//select t0.MANUFACTURER_ID , sum(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.MANUFACTURER_ID

    public void testcase1_7()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("Manufacturer", SalesLineItemFinder.manufacturerId());

        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("Manufacturer"))
            {

                case 1:
                    assertEquals(88, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(44, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid manufacturer id");
            }
        }
    }

// CASE 1.8
// List operation: SalesItemFinder.prodSpecs().price().greaterThan(20).AND(SaleItemFinder.quantity().greaterThan(15))
// AGGR ATTR: SalesItemFinder.quantity().sum()
//
//select t0.MANUFACTURER_ID , sum(t0.QTY )
//from SALES_LINE_ITEM t0, TEST_PROD_INFO t1
//WHERE t0.QTY > 15
//AND t0.PROD_ID = t1.PROD_ID
//AND t1.PRICE > 20
//group by t0.MANUFACTURER_ID

    public void testcase1_8()
    {
        Operation op = SalesLineItemFinder.productSpecs().originalPrice().greaterThan(20.00);
        op = op.and(SalesLineItemFinder.quantity().greaterThan(15));
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.quantity().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalItems", aggrAttr);
        aggregateList.addGroupBy("Manufacturer", SalesLineItemFinder.manufacturerId());

        assertEquals(2, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("Manufacturer"))
            {

                case 1:
                    assertEquals(56, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                case 2:
                    assertEquals(17, data.getAttributeAsDouble("TotalItems"), 0);
                    break;
                default:
                    fail("Invalid maunfacturer id");
            }
        }
    }


    public void testSubstraction()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThanEquals(20);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().minus(SalesLineItemFinder.productSpecs().discountPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalDiscount", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 1:
                    assertEquals(1, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 2:
                    assertEquals(5.99, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 4:
                    assertEquals(4.98, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 5:
                    assertEquals(1.00, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 7:
                    assertEquals(0.0, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 8:
                    assertEquals(5.0, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testSubstractionWithMappedToManyAggregateAttribute()
    {
        Operation op = SaleFinder.items().quantity().greaterThanEquals(20);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.items().productSpecs().originalPrice().minus(SaleFinder.items().productSpecs().discountPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalDiscount", aggrAttr);
        aggregateList.addGroupBy("SaleId", SaleFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 1:
                    assertEquals(6.99, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 2:
                    assertEquals(6.99, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 3:
                    assertEquals(15.00, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }


    public void testAddition()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThanEquals(20);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().plus(SalesLineItemFinder.productSpecs().discountPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalDiscount", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 1:
                    assertEquals(24.00, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 2:
                    assertEquals(78.97, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 4:
                    assertEquals(34.98, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 5:
                    assertEquals(24.00, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 7:
                    assertEquals(50.0, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 8:
                    assertEquals(55.0, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testAdditionWithMappedToManyAttributes()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.discountPercentage().plus(SaleFinder.items().quantity()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalDiscount", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {

                case 1:
                    assertEquals(105.35, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 2:
                    assertEquals(112.58, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 3:
                    assertEquals(60.34, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 4:
                    assertEquals(53.28, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
    }

//    public void testMultiplication()
//    {
//        fail();
//    }
//
//    public void testMultiplicationWithMappedToManyAttribute()
//    {
//        fail();
//    }

    public void testDoubleDivision()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThanEquals(20);
        MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().originalPrice().dividedBy(SalesLineItemFinder.productSpecs().discountPrice()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalDiscount", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SaleId"))
            {

                case 1:
                    assertEquals(1.08, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 2:
                    assertEquals(3.42, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 4:
                    assertEquals(1.33, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                case 5:
                    assertEquals(1.08, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 7:
                    assertEquals(1.0, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;

                case 8:
                    assertEquals(1.2, data.getAttributeAsDouble("TotalDiscount"), 0.01);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }


    public void testIntDivision()
    {
        Operation op = SalesLineItemFinder.quantity().greaterThanEquals(20);
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SalesLineItemFinder.productSpecs().minQuantity().dividedBy(SalesLineItemFinder.quantity()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("Attr", aggrAttr);
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());

        assertEquals(6, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SaleId"))
            {

                case 1:
                    assertEquals(7, data.getAttributeAsDouble("Attr"), 0);
                    break;
                case 2:
                    assertEquals(35, data.getAttributeAsDouble("Attr"), 0);
                    break;

                case 4:
                    assertEquals(11, data.getAttributeAsDouble("Attr"), 0);
                    break;
                case 5:
                    assertEquals(6, data.getAttributeAsDouble("Attr"), 0);
                    break;

                case 7:
                    assertEquals(7, data.getAttributeAsDouble("Attr"), 0);
                    break;

                case 8:
                    assertEquals(25, data.getAttributeAsDouble("Attr"), 0);
                    break;
                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testSumAbsoluteValue()
    {
        Operation op = SalesLineItemFinder.all();
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("sumDeltaPriceAbs", SalesLineItemFinder.productSpecs().deltaPrice().absoluteValue().sum());
        aggregateList.addGroupBy("manufacturer", SalesLineItemFinder.manufacturerId());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            int manufacturerId = data.getAttributeAsInt("manufacturer");

            if (manufacturerId == 1)
            {
                assertEquals(35.05, data.getAttributeAsDouble("sumDeltaPriceAbs"), 0.01);
            }
            else if (manufacturerId == 2)
            {
                assertEquals(12.13, data.getAttributeAsDouble("sumDeltaPriceAbs"),0.01);
            }
            else
            {
                fail("Invalid manufacturer");
            }
        }
    }

    public void testSumModulo()
    {
        Operation op = SaleFinder.all();
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalMod", SaleFinder.saleId().mod(3).sum());
        aggregateList.addGroupBy("userId", SaleFinder.sellerId());
        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            int userId = data.getAttributeAsInt("userId");

            if (userId == 1)
            {
                assertEquals(3, data.getAttributeAsInt("totalMod"));
            }
            else if (userId == 2)
            {
                assertEquals(3, data.getAttributeAsInt("totalMod"));
            }
            else if (userId == 3)
            {
                assertEquals(3, data.getAttributeAsInt("totalMod"));
            }
            else if (userId == 4)
            {
                assertEquals(1, data.getAttributeAsInt("totalMod"));
            }
            else
            {
                fail("Invalid account id");
            }
        }
    }

    public void testSumModuloOnRelatedToManyObjects()
    {
        Operation op = SaleFinder.all();
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalMod", SaleFinder.items().itemId().mod(3).sum());
        aggregateList.addGroupBy("sellerId", SaleFinder.sellerId());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            int sellerId = data.getAttributeAsInt("sellerId");

            if (sellerId == 1)
            {

                assertEquals(7, data.getAttributeAsInt("totalMod"));
            }
            else if (sellerId == 2)
            {

                assertEquals(8, data.getAttributeAsInt("totalMod"));
            }
            else if (sellerId == 3)
            {

                assertEquals(3, data.getAttributeAsInt("totalMod"));
            }
            else if (sellerId == 4)
            {

                assertEquals(4, data.getAttributeAsInt("totalMod"));
            }
            else
            {
                fail("Invalid account id");
            }
        }
    }

    public void testSumGroupByRelatedObjectAttribute()
    {
        Operation op = SalesLineItemFinder.all();
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("totalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("prodName", SalesLineItemFinder.productSpecs().productName());

        for(int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);
            String prodName = data.getAttributeAsString("prodName");
            if("PROD 1".equals(prodName))
            {
                assertEquals(108,data.getAttributeAsInt("totalQty"));
            }
            else if("PROD 2".equals(prodName))
            {
                assertEquals(45,data.getAttributeAsInt("totalQty"));
            }
            else if("PROD 3".equals(prodName))
            {
                assertEquals(45,data.getAttributeAsInt("totalQty"));
            }
            else if("PROD 4".equals(prodName))
            {
                assertEquals(77,data.getAttributeAsInt("totalQty"));
            }
            else if("PROD 5".equals(prodName))
            {
                assertEquals(45,data.getAttributeAsInt("totalQty"));
            }
            else if("PROD 6".equals(prodName))
            {
                assertEquals(10,data.getAttributeAsInt("totalQty"));
            }
        }
    }

    public void testSumDateYear()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().year().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumYear", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        aggregateList.setAscendingOrderBy("ActiveBoolean");
        assertEquals((1981 + 9999 + 1900), aggregateList.get(0).getAttributeAsInteger("SumYear"));
    }

    public void testSumDateMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().month().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        aggregateList.setAscendingOrderBy("ActiveBoolean");
        assertEquals(19, aggregateList.get(0).getAttributeAsInteger("SumMonth"));
    }

    public void testSumDateDayOfMonth()
    {
        Operation op = ParaDeskFinder.all();
        MithraAggregateAttribute aggrAttr = ParaDeskFinder.closedDate().dayOfMonth().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumDayOfMonth", aggrAttr);
        aggregateList.addGroupBy("ActiveBoolean", ParaDeskFinder.activeBoolean());

        assertEquals(2, aggregateList.size());
        aggregateList.setAscendingOrderBy("ActiveBoolean");
        assertEquals(10, aggregateList.get(0).getAttributeAsInteger("SumDayOfMonth"));
    }

    public void testSumTimestampYear()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().year().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumYear", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());

        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(2004 + 2004 + 2004, aggregateList.get(0).getAttributeAsInteger("SumYear"));
        assertEquals(2004, aggregateList.get(1).getAttributeAsInteger("SumYear"));
        assertEquals(2004 + 2004 + 2004, aggregateList.get(2).getAttributeAsInteger("SumYear"));
    }

    public void testSumTimestampMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().month().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());
        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(6, aggregateList.get(0).getAttributeAsInteger("SumMonth"));
        assertEquals(4, aggregateList.get(1).getAttributeAsInteger("SumMonth"));
        assertEquals(12, aggregateList.get(2).getAttributeAsInteger("SumMonth"));
    }

    public void testSumTimestampDayOfMonth()
    {
        Operation op = OrderFinder.all();
        MithraAggregateAttribute aggregateAttribute = OrderFinder.orderDate().dayOfMonth().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumDayOfMonth", aggregateAttribute);
        aggregateList.addGroupBy("UserId", OrderFinder.userId());
        assertEquals(3, aggregateList.size());
        aggregateList.setAscendingOrderBy("UserId");
        assertEquals(36, aggregateList.get(0).getAttributeAsInteger("SumDayOfMonth"));
        assertEquals(12, aggregateList.get(1).getAttributeAsInteger("SumDayOfMonth"));
        assertEquals(36, aggregateList.get(2).getAttributeAsInteger("SumDayOfMonth"));
    }
}
