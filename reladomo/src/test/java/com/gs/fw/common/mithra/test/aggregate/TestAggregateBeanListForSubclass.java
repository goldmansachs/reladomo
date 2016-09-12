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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class TestAggregateBeanListForSubclass extends MithraTestAbstract
{
    private static final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Sale.class,
                        Seller.class,
                        SalesLineItem.class,
                        ProductSpecification.class,
                        NullTest.class
                };
    }

    public void testCanCreateAggregateBeanListForOnlyExtendedMethods()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        assertEquals(4, list.size());
    }

    public void testCanCreateAggregateBeanListFromImplementedMethods()
    {
        Operation op = SalesLineItemFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("productId", SalesLineItemFinder.productId());
        list.addAggregateAttribute("manufacturerCount", SalesLineItemFinder.manufacturerId().count());
        list.addAggregateAttribute("salesCount", SalesLineItemFinder.quantity().count());
        assertEquals(6, list.size());
    }

    public void testCanCreateAggregateBeanListForOnlyDeclaredMethods()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("totalItemsForManufacturer", SaleFinder.items().manufacturerId().count());
        assertEquals(4, list.size());
    }

    public void testCanCreateAggregateBeanListForExtenedAndDeclaredMethods()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        list.addAggregateAttribute("totalItemsForManufacturer", SaleFinder.items().manufacturerId().count());
        assertEquals(4, list.size());
    }

    public void testSubclassBehaviourForNullResultSet()
    {
        Operation op = ProductSpecificationFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("sellerName", ProductSpecificationFinder.manufacturerName());
        list.addAggregateAttribute("manufacturerCount", ProductSpecificationFinder.discountPrice().count());
        list.addOrderBy("sellerName", true);
        assertEquals(3, list.size());
        assertNull(list.get(0).getSellerName());
        assertEquals("M1", list.get(1).getSellerName());
        assertEquals("M2", list.get(2).getSellerName());
        assertEquals(3, list.get(0).getManufacturerCount().intValue());
        assertEquals(2, list.get(1).getManufacturerCount().intValue());
        assertEquals(1, list.get(2).getManufacturerCount().intValue());

    }


    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeString()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("descriptionCount", SaleFinder.description().count());
        list.addAggregateAttribute("descriptionMax", SaleFinder.description().max());
        list.addAggregateAttribute("descriptionMin", SaleFinder.description().min());

        assertEquals(4, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            ExtendedAggregateBean currentBean = list.get(i);
            if (currentBean.getSellerName().equals("Moh Rezaei"))
            {
                assertEquals(currentBean.getDescriptionCount().intValue(), 3);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0006");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0004");
            }
            else if (currentBean.getSellerName().equals("Rafael Gonzalez"))
            {
                assertEquals(currentBean.getDescriptionCount().intValue(), 2);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0008");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0007");
            }
            else if (currentBean.getSellerName().equals("Jane Doe"))
            {
                assertEquals(currentBean.getDescriptionCount().intValue(), 2);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0010");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0009");
            }
            else if (currentBean.getSellerName().equals("John Doe"))
            {
                assertEquals(currentBean.getDescriptionCount().intValue(), 3);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0003");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0001");
            }
            else
            {
                fail("Invalid bean in the list");
            }
        }

    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeInteger()
    {
        Operation op = SalesLineItemFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("productId", SalesLineItemFinder.productId());
        list.addAggregateAttribute("totalManufacturers", SalesLineItemFinder.manufacturerId().count());
        list.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().count());

        assertEquals(6, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            ExtendedAggregateBean currentBean = list.get(i);
            if (currentBean.getProductId().equals(1))
            {
                assertEquals(currentBean.getTotalManufacturers().intValue(), 7);
                assertEquals(currentBean.getTotalQuantity().intValue(), 7);

            }
            else if (currentBean.getProductId().equals(2))
            {
                assertEquals(currentBean.getTotalManufacturers().intValue(), 3);
                assertEquals(currentBean.getTotalQuantity().intValue(), 3);

            }
            else if (currentBean.getProductId().equals(3))
            {
                assertEquals(currentBean.getTotalManufacturers().intValue(), 3);
                assertEquals(currentBean.getTotalQuantity().intValue(), 3);

            }
            else if (currentBean.getProductId().equals(4))
            {
                assertEquals(currentBean.getTotalManufacturers().intValue(), 5);
                assertEquals(currentBean.getTotalQuantity().intValue(), 5);

            }
            else if (currentBean.getProductId().equals(5))
            {
                assertEquals(currentBean.getTotalManufacturers().intValue(), 3);
                assertEquals(currentBean.getTotalQuantity().intValue(), 3);

            }
            else if (currentBean.getProductId().equals(6))
            {
                assertEquals(currentBean.getTotalManufacturers().intValue(), 1);
                assertEquals(currentBean.getTotalQuantity().intValue(), 1);

            }
            else
            {
                fail("Unexpected bean in the aggregate bean list.");
            }
        }
    }


    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeDouble()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("averageDiscount", SaleFinder.discountPercentage().avg());
        list.addAggregateAttribute("maxDiscount", SaleFinder.discountPercentage().max());
        list.addAggregateAttribute("minDiscount", SaleFinder.discountPercentage().min());
        assertEquals(6, list.size());
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeBoolean()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSellerActive", SaleFinder.seller().active().max());
        list.addAggregateAttribute("minSellerActive", SaleFinder.seller().active().min());
        assertEquals(6, list.size());
    }


    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeDate() throws ParseException
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSettleDate", SaleFinder.settleDate().max());
        list.addAggregateAttribute("leastSettleDate", SaleFinder.settleDate().min());
        assertEquals(6, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            ExtendedAggregateBean currentBean = list.get(i);
            if (currentBean.getProductId().equals(1))
            {
                assertEquals(currentBean.getMaxSettleDate(), dateFormat.parse("2004-04-01"));
                assertEquals(currentBean.getLeastSettleDate(), dateFormat.parse("2004-01-13"));

            }
            else if (currentBean.getProductId().equals(2))
            {
                assertEquals(currentBean.getMaxSettleDate(), dateFormat.parse("2004-02-15"));
                assertEquals(currentBean.getLeastSettleDate(), dateFormat.parse("2004-02-12"));
            }
            else if (currentBean.getProductId().equals(3))
            {
                assertEquals(currentBean.getMaxSettleDate(), dateFormat.parse("2004-02-15"));
                assertEquals(currentBean.getLeastSettleDate(), dateFormat.parse("2004-02-13"));

            }
            else if (currentBean.getProductId().equals(4))
            {
                assertEquals(currentBean.getMaxSettleDate(), dateFormat.parse("2004-04-01"));
                assertEquals(currentBean.getLeastSettleDate(), dateFormat.parse("2004-02-13"));
            }
            else if (currentBean.getProductId().equals(5))
            {
                assertEquals(currentBean.getMaxSettleDate(), dateFormat.parse("2004-02-17"));
                assertEquals(currentBean.getLeastSettleDate(), dateFormat.parse("2004-02-14"));
            }
            else if (currentBean.getProductId().equals(6))
            {
                assertEquals(currentBean.getMaxSettleDate(), dateFormat.parse("2004-02-17"));
                assertEquals(currentBean.getLeastSettleDate(), dateFormat.parse("2004-02-17"));
            }
            else
            {
                fail("Invalid bean in aggregate bean list.");
            }
        }
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeTimestamp()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<ExtendedAggregateBean> list = new AggregateBeanList<ExtendedAggregateBean>(op, ExtendedAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSaleDate", SaleFinder.saleDate().max());
        list.addAggregateAttribute("minSaleDate", SaleFinder.saleDate().min());
        assertEquals(6, list.size());
        try
        {
            for (int i = 0; i < list.size(); i++)
            {
                ExtendedAggregateBean currentBean = list.get(i);
                if (currentBean.getProductId().equals(1))
                {
                    assertEquals(currentBean.getMaxSaleDate(), new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime()));
                    assertEquals(currentBean.getMinSaleDate(), new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime()));

                }
                else if (currentBean.getProductId().equals(2))
                {
                    assertEquals(currentBean.getMaxSaleDate(), new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime()));
                    assertEquals(currentBean.getMinSaleDate(), new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()));

                }
                else if (currentBean.getProductId().equals(3))
                {
                    assertEquals(currentBean.getMaxSaleDate(), new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime()));
                    assertEquals(currentBean.getMinSaleDate(), new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()));

                }
                else if (currentBean.getProductId().equals(4))
                {
                    assertEquals(currentBean.getMaxSaleDate(), new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime()));
                    assertEquals(currentBean.getMinSaleDate(), new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()));

                }
                else if (currentBean.getProductId().equals(5))
                {
                    assertEquals(currentBean.getMaxSaleDate(), new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime()));
                    assertEquals(currentBean.getMinSaleDate(), new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()));
                }
                else if (currentBean.getProductId().equals(6))
                {
                    assertEquals(currentBean.getMaxSaleDate(), new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime()));
                    assertEquals(currentBean.getMinSaleDate(), new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime()));

                }
                else
                {
                    fail("Invalid bean in aggregate bean list.");
                }
            }
        }
        catch (ParseException e)
        {
            fail("Error gettting timetsmap");
        }
    }
}
