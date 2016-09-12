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
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;



public class TestAggregateBeanListWithPrimitives extends MithraTestAbstract
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

    public void testSimpleAggregationOnPrimitive()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        assertEquals(4, list.size());
    }


    public void testAggregateBeanListCanHandleNullResultSetForString()
    {
        Operation op = ProductSpecificationFinder.all();
        AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
        list.addGroupBy("sellerName", ProductSpecificationFinder.manufacturerName());
        list.addAggregateAttribute("manufacturerCount", ProductSpecificationFinder.discountPrice().count());
        list.addOrderBy("sellerName", true);
        assertEquals(3, list.size());
        assertNull(list.get(0).getSellerName());
        assertEquals("M1", list.get(1).getSellerName());
        assertEquals("M2", list.get(2).getSellerName());
        assertEquals(3, list.get(0).getManufacturerCount());
        assertEquals(2, list.get(1).getManufacturerCount());
        assertEquals(1, list.get(2).getManufacturerCount());
    }

    public void testMustThrowForNullPrimitiveIntResultSet()
    {
        try
        {
            Operation op = ProductSpecificationFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("id", ProductSpecificationFinder.manufacturerId());
            list.addAggregateAttribute("manufacturerCount", ProductSpecificationFinder.discountPrice().count());
            list.addOrderBy("id", true);
            assertEquals(3, list.size());
            fail("Cannot set null value for int type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage()); // if any other exception - test fails.
        }
    }


    public void testMustThrowForNullPrimitiveDoubleResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("avgPrice", NullTestFinder.noDefaultNullDouble());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
            fail("Cannot set null value for double type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }


    public void testMustThrowForNullPrimitiveBooleanResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("minSellerActive", NullTestFinder.noDefaultNullBoolean());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
            fail("Cannot set null value for boolean type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }


    public void testMustThrowForNullPrimitiveCharResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("minChar", NullTestFinder.noDefaultNullChar());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
            fail("Cannot set null value for char type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }

    public void testMustThrowForNullPrimitiveShortResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("testShort", NullTestFinder.noDefaultNullShort());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
            fail("Cannot set null value for short type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }

    public void testMustThrowForNullPrimitiveByteResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("testByte", NullTestFinder.noDefaultNullByte());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
            fail("Cannot set null value for byte type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }


    public void testMustThrowForNullPrimitiveFloatResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
            list.addGroupBy("testFloat", NullTestFinder.noDefaultNullFloat());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
            fail("Cannot set null value for float type");
        }
        catch (MithraNullPrimitiveException mnpe)
        {
            assertTrue(mnpe.getMessage().contains("Aggregate result returned null"));
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }


    public void testCanDoAggregateAndGroupByOperationsOnString()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("descriptionCount", SaleFinder.description().count());
        list.addAggregateAttribute("descriptionMax", SaleFinder.description().max());
        list.addAggregateAttribute("descriptionMin", SaleFinder.description().min());

        assertEquals(4, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            PrimitiveAggregateBean currentBean = list.get(i);
            if (currentBean.getSellerName().equals("Moh Rezaei"))
            {
                assertEquals(currentBean.getDescriptionCount(), 3);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0006");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0004");
            }
            else if (currentBean.getSellerName().equals("Rafael Gonzalez"))
            {
                assertEquals(currentBean.getDescriptionCount(), 2);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0008");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0007");
            }
            else if (currentBean.getSellerName().equals("Jane Doe"))
            {
                assertEquals(currentBean.getDescriptionCount(), 2);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0010");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0009");
            }
            else if (currentBean.getSellerName().equals("John Doe"))
            {
                assertEquals(currentBean.getDescriptionCount(), 3);
                assertEquals(currentBean.getDescriptionMax(), "Sale 0003");
                assertEquals(currentBean.getDescriptionMin(), "Sale 0001");
            }
            else
            {
                fail("Invalid bean in the list");
            }
        }

    }

    public void testCanDoAggregateAndGroupByOperationsOnPrimitiveInteger()
    {
        Operation op = SalesLineItemFinder.all();
        AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
        list.addGroupBy("productId", SalesLineItemFinder.productId());
        list.addAggregateAttribute("totalManufacturers", SalesLineItemFinder.manufacturerId().count());
        list.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().count());

        assertEquals(6, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            PrimitiveAggregateBean currentBean = list.get(i);
            if (currentBean.getProductId() == 1)
            {
                assertEquals(currentBean.getTotalManufacturers(), 7);
                assertEquals(currentBean.getTotalQuantity(), 7);

            }
            else if (currentBean.getProductId() == 2)
            {
                assertEquals(currentBean.getTotalManufacturers(), 3);
                assertEquals(currentBean.getTotalQuantity(), 3);

            }
            else if (currentBean.getProductId() == 3)
            {
                assertEquals(currentBean.getTotalManufacturers(), 3);
                assertEquals(currentBean.getTotalQuantity(), 3);

            }
            else if (currentBean.getProductId() == 4)
            {
                assertEquals(currentBean.getTotalManufacturers(), 5);
                assertEquals(currentBean.getTotalQuantity(), 5);

            }
            else if (currentBean.getProductId() == 5)
            {
                assertEquals(currentBean.getTotalManufacturers(), 3);
                assertEquals(currentBean.getTotalQuantity(), 3);

            }
            else if (currentBean.getProductId() == 6)
            {
                assertEquals(currentBean.getTotalManufacturers(), 1);
                assertEquals(currentBean.getTotalQuantity(), 1);

            }
            else
            {
                fail("Unexpected bean in the aggregate bean list.");
            }
        }
    }


    public void testCanDoAggregateAndGroupByOperationsOnPrimitiveDouble()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("averageDiscount", SaleFinder.discountPercentage().avg());
        list.addAggregateAttribute("maxDiscount", SaleFinder.discountPercentage().max());
        list.addAggregateAttribute("minDiscount", SaleFinder.discountPercentage().min());
        assertEquals(6, list.size());
    }

    public void testCanDoAggregateAndGroupByOperationsOnPrimitiveBoolean()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<PrimitiveAggregateBean> list = new AggregateBeanList<PrimitiveAggregateBean>(op, PrimitiveAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSellerActive", SaleFinder.seller().active().max());
        list.addAggregateAttribute("minSellerActive", SaleFinder.seller().active().min());
        assertEquals(6, list.size());
    }
}
