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
import com.gs.fw.common.mithra.HavingOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;



public class TestAggregateBeanListWithHavingClause extends MithraTestAbstract
{
    private static final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

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
                        SalesLineItem.class,
                        Sale.class,
                        ProductSpecification.class,
                        ParentNumericAttribute.class
                };
    }

    public void testStringHavingEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMin", SaleFinder.description().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().min().eq("Sale 0009"));

        assertEquals(1, aggregateBeanList.size());
        assertEquals("Sale 0009", aggregateBeanList.get(0).getDescriptionMin());
    }

    public void testStringHavingEqWithDifferentAttributeForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMin", SaleFinder.description().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().max().eq("Sale 0010"));

        assertEquals(1, aggregateBeanList.size());
        assertEquals("Sale 0009", aggregateBeanList.get(0).getDescriptionMin());
    }

    public void testStringHavingNotEqForAggregateBeanListForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMin", SaleFinder.description().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().min().notEq("Sale 0009"));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals("Sale 0001", data.getDescriptionMin());
            }

            else if (data.getId() == 2)
            {
                assertEquals("Sale 0004", data.getDescriptionMin());
            }

            else if (data.getId() == 3)
            {
                assertEquals("Sale 0007", data.getDescriptionMin());
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testStringHavingGreaterThanForAggregateBeanListForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMin", SaleFinder.description().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().min().greaterThan("Sale 0004"));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 3)
            {
                assertEquals("Sale 0007", data.getDescriptionMin());
            }
            else if (data.getId() == 4)
            {
                assertEquals("Sale 0009", data.getDescriptionMin());
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testStringHavingGreaterThanEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMin", SaleFinder.description().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().min().greaterThanEquals("Sale 0004"));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 2)
            {
                assertEquals("Sale 0004", data.getDescriptionMin());
            }
            else if (data.getId() == 3)
            {
                assertEquals("Sale 0007", data.getDescriptionMin());
            }
            else if (data.getId() == 4)
            {
                assertEquals("Sale 0009", data.getDescriptionMin());
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testStringHavingLessThanForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMax", SaleFinder.description().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().max().lessThan("Sale 0004"));

        assertEquals(1, aggregateBeanList.size());
        assertEquals("Sale 0003", aggregateBeanList.get(0).getDescriptionMax());
    }

    public void testStringHavingLessThanEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("descriptionMax", SaleFinder.description().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.description().max().lessThanEquals("Sale 0006"));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals("Sale 0003", data.getDescriptionMax());
            }
            else if (data.getId() == 2)
            {
                assertEquals("Sale 0006", data.getDescriptionMax());
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testTimestampHavingEqForAggregateBeanList() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSaleDate", SaleFinder.saleDate().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.saleDate().min().eq(ts));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            assertEquals(ts, data.getMinSaleDate());
            assertTrue(data.getId() != 1);
        }
    }

    public void testTimestampHavingNotEqForAggregateBeanList() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime());
        Timestamp ts2 = new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSaleDate", SaleFinder.saleDate().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.saleDate().min().notEq(ts));

        assertEquals(1, aggregateBeanList.size());
        assertEquals(ts2, aggregateBeanList.get(0).getMinSaleDate());
        assertTrue(aggregateBeanList.get(0).getId() == 1);

    }

    public void testTimestampHavingGreaterThanForAggregateBeanList() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-12 12:00:00.0").getTime());
        Timestamp ts2 = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts3 = new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSaleDate", SaleFinder.saleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.saleDate().max().greaterThan(ts));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(ts2, data.getMaxSaleDate());
            }
            else if (data.getId() == 2)
            {
                assertEquals(ts3, data.getMaxSaleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testTimestampHavingGreaterThanEqForAggregateBeanList() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts2 = new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSaleDate", SaleFinder.saleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.saleDate().max().greaterThanEquals(ts));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(ts, data.getMaxSaleDate());
            }
            else if (data.getId() == 2)
            {
                assertEquals(ts2, data.getMaxSaleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testTimestampHavingLessThanForAggregateBeanList() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts3 = new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime());
        Timestamp ts4 = new Timestamp(timestampFormat.parse("2004-02-12 02:00:00.0").getTime());

        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSaleDate", SaleFinder.saleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.saleDate().max().lessThan(ts));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 3)
            {
                assertEquals(ts3, data.getMaxSaleDate());
            }
            else if (data.getId() == 4)
            {
                assertEquals(ts4, data.getMaxSaleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testTimestampHavingLessThanEqForAggregateBeanList() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts3 = new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime());
        Timestamp ts4 = new Timestamp(timestampFormat.parse("2004-02-12 02:00:00.0").getTime());

        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSaleDate", SaleFinder.saleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.saleDate().max().lessThanEquals(ts));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(ts, data.getMaxSaleDate());
            }
            else if (data.getId() == 3)
            {
                assertEquals(ts3, data.getMaxSaleDate());
            }
            else if (data.getId() == 4)
            {
                assertEquals(ts4, data.getMaxSaleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }


    public void testDateHavingEqForAggregateBeanList() throws ParseException
    {
        Date ts = dateFormat.parse("2004-02-12");
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("leastSettleDate", SaleFinder.settleDate().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.settleDate().min().eq(ts));

        assertEquals(1, aggregateBeanList.size());

        assertEquals(ts, aggregateBeanList.get(0).getLeastSettleDate());
        assertTrue(aggregateBeanList.get(0).getId() == 2);
    }

    public void testDateHavingNotEqForAggregateBeanList() throws ParseException
    {
        Date ts = dateFormat.parse("2004-02-12");
        Date ts2 = dateFormat.parse("2004-01-13");
        Date ts3 = dateFormat.parse("2004-02-15");
        Date ts4 = dateFormat.parse("2004-03-01");
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("leastSettleDate", SaleFinder.settleDate().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.settleDate().min().notEq(ts));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(ts2, data.getLeastSettleDate());
            }
            else if (data.getId() == 3)
            {
                assertEquals(ts3, data.getLeastSettleDate());
            }
            else if (data.getId() == 4)
            {
                assertEquals(ts4, data.getLeastSettleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }

    }

    public void testDateHavingGreaterThanForAggregateBeanList() throws ParseException
    {
        Date ts = dateFormat.parse("2004-03-01");
        Date ts2 = dateFormat.parse("2004-04-01");
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSettleDate", SaleFinder.settleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.settleDate().max().greaterThan(ts));

        assertEquals(1, aggregateBeanList.size());
        //  assertEquals(4 , aggregateBeanList.get(0).getId());
        assertEquals(ts2, aggregateBeanList.get(0).getMaxSettleDate());
    }

    public void testDateHavingGreaterThanEqForAggregateBeanList() throws ParseException
    {
        Date ts = dateFormat.parse("2004-02-17");
        Date ts2 = dateFormat.parse("2004-04-01");
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSettleDate", SaleFinder.settleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.settleDate().max().greaterThanEquals(ts));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 3)
            {
                assertEquals(ts, data.getMaxSettleDate());
            }
            else if (data.getId() == 4)
            {
                assertEquals(ts2, data.getMaxSettleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testDateHavingLessThanForAggregateBeanList() throws ParseException
    {
        Date ts = dateFormat.parse("2004-02-17");
        Date ts2 = dateFormat.parse("2004-02-14");
        Date ts3 = dateFormat.parse("2004-02-15");
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSettleDate", SaleFinder.settleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.settleDate().max().lessThan(ts));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(ts3, data.getMaxSettleDate());
            }
            else if (data.getId() == 2)
            {
                assertEquals(ts2, data.getMaxSettleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testDateHavingLessThanEqForAggregateBeanList() throws ParseException
    {
        Date ts = dateFormat.parse("2004-02-17");
        Date ts2 = dateFormat.parse("2004-02-14");
        Date ts3 = dateFormat.parse("2004-02-15");
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxSettleDate", SaleFinder.settleDate().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.settleDate().max().lessThanEquals(ts));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(ts3, data.getMaxSettleDate());
            }
            else if (data.getId() == 2)
            {
                assertEquals(ts2, data.getMaxSettleDate());
            }
            else if (data.getId() == 3)
            {
                assertEquals(ts, data.getMaxSettleDate());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }


    public void testBooleanHavingEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSellerActive", SaleFinder.activeBoolean().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.activeBoolean().min().eq(true));

        assertEquals(1, aggregateBeanList.size());

        assertEquals(new Integer(3), aggregateBeanList.get(0).getId());
        assertEquals(Boolean.TRUE, aggregateBeanList.get(0).getMinSellerActive());
    }

    public void testBooleanHavingNotEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSellerActive", SaleFinder.activeBoolean().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.activeBoolean().min().notEq(true));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            assertFalse(3 == data.getId());
            assertFalse(data.getMinSellerActive());
        }
    }

    public void testBooleanHavingGreaterThanForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSellerActive", SaleFinder.activeBoolean().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.activeBoolean().min().greaterThan(false));

        assertEquals(1, aggregateBeanList.size());
        SimpleAggregateBean data = aggregateBeanList.get(0);
        assertEquals(3, data.getId().intValue());
        assertEquals(Boolean.TRUE, data.getMinSellerActive());
    }

    public void testBooleanHavingGreaterThanEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSellerActive", SaleFinder.activeBoolean().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.activeBoolean().min().greaterThanEquals(false));

        assertEquals(4, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals(Boolean.FALSE, data.getMinSellerActive());
            }
            else if (data.getId() == 2)
            {
                assertEquals(Boolean.FALSE, data.getMinSellerActive());
            }
            else if (data.getId() == 3)
            {
                assertEquals(Boolean.TRUE, data.getMinSellerActive());
            }
            else if (data.getId() == 4)
            {
                assertEquals(Boolean.FALSE, data.getMinSellerActive());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testBooleanHavingLessThanForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSellerActive", SaleFinder.activeBoolean().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.activeBoolean().max().lessThan(true));

        assertEquals(1, aggregateBeanList.size());
        SimpleAggregateBean data = aggregateBeanList.get(0);
        assertEquals(4, data.getId().intValue());
        assertEquals(Boolean.FALSE, data.getMinSellerActive());
    }

    public void testBooleanHavingLessThanEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minSellerActive", SaleFinder.activeBoolean().min());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());
        aggregateBeanList.setHavingOperation(SaleFinder.activeBoolean().min().lessThanEquals(true));

        assertEquals(4, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertFalse(data.getMinSellerActive());
            }
            else if (data.getId() == 2)
            {
                assertFalse(data.getMinSellerActive());
            }
            else if (data.getId() == 3)
            {
                assertTrue(data.getMinSellerActive());
            }
            else if (data.getId() == 4)
            {
                assertFalse(data.getMinSellerActive());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }


    public void testCharHavingEqForAggregateBeanList()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minChar", ParentNumericAttributeFinder.charAttr().min());
        aggregateBeanList.addGroupBy("id", ParentNumericAttributeFinder.userId());
        aggregateBeanList.setHavingOperation(ParentNumericAttributeFinder.charAttr().min().eq('a'));

        assertEquals(1, aggregateBeanList.size());
        SimpleAggregateBean data = aggregateBeanList.get(0);
        assertEquals(1, data.getId().intValue());
        assertEquals('a', data.getMinChar().charValue());
    }

    public void testCharHavingNotEqForAggregateBeanList()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("minChar", ParentNumericAttributeFinder.charAttr().min());
        aggregateBeanList.addGroupBy("id", ParentNumericAttributeFinder.userId());
        aggregateBeanList.setHavingOperation(ParentNumericAttributeFinder.charAttr().min().notEq('t'));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals('a', data.getMinChar().charValue());
            }
            else if (data.getId() == 2)
            {
                assertEquals('d', data.getMinChar().charValue());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCharHavingGreaterThanForAggregateBeanList()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxChar", ParentNumericAttributeFinder.charAttr().min());
        aggregateBeanList.addGroupBy("id", ParentNumericAttributeFinder.userId());
        aggregateBeanList.setHavingOperation(ParentNumericAttributeFinder.charAttr().min().greaterThan('a'));

        assertEquals(1, aggregateBeanList.size());
        SimpleAggregateBean data = aggregateBeanList.get(0);
        assertEquals(2, data.getId().intValue());
        assertEquals('d', data.getMaxChar().charValue());
    }

    public void testCharHavingGreaterThanEqForAggregateBeanList()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxChar", ParentNumericAttributeFinder.charAttr().max());
        aggregateBeanList.addGroupBy("id", ParentNumericAttributeFinder.userId());
        aggregateBeanList.setHavingOperation(ParentNumericAttributeFinder.charAttr().max().greaterThanEquals('a'));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals('c', data.getMaxChar().charValue());
            }
            else if (data.getId() == 2)
            {
                assertEquals('f', data.getMaxChar().charValue());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCharHavingLessThanForAggregateBeanList()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxChar", ParentNumericAttributeFinder.charAttr().max());
        aggregateBeanList.addGroupBy("id", ParentNumericAttributeFinder.userId());
        aggregateBeanList.setHavingOperation(ParentNumericAttributeFinder.charAttr().max().lessThan('t'));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals('c', data.getMaxChar().charValue());
            }
            else if (data.getId() == 2)
            {
                assertEquals('f', data.getMaxChar().charValue());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCharHavingLessThanEqForAggregateBeanList()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxChar", ParentNumericAttributeFinder.charAttr().max());
        aggregateBeanList.addGroupBy("id", ParentNumericAttributeFinder.userId());
        aggregateBeanList.setHavingOperation(ParentNumericAttributeFinder.charAttr().max().greaterThanEquals('c'));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (data.getId() == 1)
            {
                assertEquals('c', data.getMaxChar().charValue());
            }
            else if (data.getId() == 2)
            {
                assertEquals('f', data.getMaxChar().charValue());
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testIntegerHavingEqForAggregateBeanList() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("avgQuantity", SalesLineItemFinder.quantity().avg());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().avg().eq(20));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getAvgQuantity().intValue());
            }
            else if (2 == data.getId())
            {
                assertEquals(20, data.getAvgQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testIntegerHavingNotEqForAggregateBeanList() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().notEq(30));

        assertEquals(7, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getTotalQuantity().intValue());
            }
            else if (2 == data.getId())
            {
                assertEquals(60, data.getTotalQuantity().intValue());
            }
            else if (3 == data.getId())
            {
                assertEquals(25, data.getTotalQuantity().intValue());
            }
            else if (5 == data.getId())
            {
                assertEquals(55, data.getTotalQuantity().intValue());
            }
            else if (6 == data.getId())
            {
                assertEquals(27, data.getTotalQuantity().intValue());
            }
            else if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testIntegerHavingGreaterThanForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());

        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThan(30));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {

            if (2 == data.getId())
            {
                assertEquals(60, data.getTotalQuantity().intValue());
            }
            else if (5 == data.getId())
            {
                assertEquals(55, data.getTotalQuantity().intValue());
            }
            else if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testIntegerHavingGreaterThanEqForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThanEquals(30));

        assertEquals(6, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {

            if (2 == data.getId())
            {
                assertEquals(60, data.getTotalQuantity().intValue());
            }
            else if (4 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (5 == data.getId())
            {
                assertEquals(55, data.getTotalQuantity().intValue());
            }
            else if (7 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (8 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testIntegerHavingLessThanForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThan(30));

        assertEquals(4, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getTotalQuantity().intValue());
            }
            else if (3 == data.getId())
            {
                assertEquals(25, data.getTotalQuantity().intValue());
            }
            else if (6 == data.getId())
            {
                assertEquals(27, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testIntegerHavingLessThanEqForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThanEquals(30));

        assertEquals(7, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getTotalQuantity().intValue());
            }
            else if (3 == data.getId())
            {
                assertEquals(25, data.getTotalQuantity().intValue());
            }
            else if (4 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (6 == data.getId())
            {
                assertEquals(27, data.getTotalQuantity().intValue());
            }
            else if (7 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (8 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testDoubleHavingEqForAggregateBeanList() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("avgQuantity", SalesLineItemFinder.quantity().avg());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().avg().eq(20));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getAvgQuantity().intValue());
            }
            else if (2 == data.getId())
            {
                assertEquals(20, data.getAvgQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testDoubleHavingNotEqForAggregateBeanList() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().notEq(30));

        assertEquals(7, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getTotalQuantity().intValue());
            }
            else if (2 == data.getId())
            {
                assertEquals(60, data.getTotalQuantity().intValue());
            }
            else if (3 == data.getId())
            {
                assertEquals(25, data.getTotalQuantity().intValue());
            }
            else if (5 == data.getId())
            {
                assertEquals(55, data.getTotalQuantity().intValue());
            }
            else if (6 == data.getId())
            {
                assertEquals(27, data.getTotalQuantity().intValue());
            }
            else if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingGreaterThanForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThan(30));

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {

            if (2 == data.getId())
            {
                assertEquals(60, data.getTotalQuantity().intValue());
            }
            else if (5 == data.getId())
            {
                assertEquals(55, data.getTotalQuantity().intValue());
            }
            else if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testDoubleHavingGreaterThanEqForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThanEquals(30));

        assertEquals(6, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {

            if (2 == data.getId())
            {
                assertEquals(60, data.getTotalQuantity().intValue());
            }
            else if (4 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (5 == data.getId())
            {
                assertEquals(55, data.getTotalQuantity().intValue());
            }
            else if (7 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (8 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingLessThanForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThan(30));

        assertEquals(4, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getTotalQuantity().intValue());
            }
            else if (3 == data.getId())
            {
                assertEquals(25, data.getTotalQuantity().intValue());
            }
            else if (6 == data.getId())
            {
                assertEquals(27, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingLessThanEqForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThanEquals(30));

        assertEquals(7, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(20, data.getTotalQuantity().intValue());
            }
            else if (3 == data.getId())
            {
                assertEquals(25, data.getTotalQuantity().intValue());
            }
            else if (4 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (6 == data.getId())
            {
                assertEquals(27, data.getTotalQuantity().intValue());
            }
            else if (7 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (8 == data.getId())
            {
                assertEquals(30, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testCountAggregationWithHavingForAggregateBeanList()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SalesLineItemFinder.itemId().count());
        aggregateBeanList.addGroupBy("id", SalesLineItemFinder.saleId());
        aggregateBeanList.setHavingOperation(SalesLineItemFinder.itemId().count().eq(1));

        assertEquals(1, aggregateBeanList.size());
        assertEquals(1, aggregateBeanList.get(0).getId().intValue());
        assertEquals(1, aggregateBeanList.get(0).getTotalQuantity().intValue());
    }

    public void testMappedDoubleHavingEqForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("avgPrice", SaleFinder.items().productSpecs().originalPrice().avg());
        aggregateBeanList.addGroupBy("id", SaleFinder.saleId());
        aggregateBeanList.setHavingOperation(SaleFinder.items().productSpecs().originalPrice().avg().eq(12.5));

        assertEquals(1, aggregateBeanList.size());

        assertEquals(1, aggregateBeanList.get(0).getId().intValue());
        assertEquals(12.5, aggregateBeanList.get(0).getAvgPrice().doubleValue());
    }

    public void testMappedDoubleHavingEqUsingDifferentAttributeInHavingForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantity", SaleFinder.items().quantity().sum());
        aggregateBeanList.addGroupBy("id", SaleFinder.saleId());
        aggregateBeanList.setHavingOperation(SaleFinder.items().productSpecs().originalPrice().avg().eq(18.75));

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (9 == data.getId())
            {
                assertEquals(31, data.getTotalQuantity().intValue());
            }
            else if (10 == data.getId())
            {
                assertEquals(22, data.getTotalQuantity().intValue());
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testDatedMappedDoubleHavingEqUsingDifferentAttributeInHavingForAggregateBeanList()
    {
        Operation op = BitemporalOrderFinder.all();
        op = op.and(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("totalQuantityDouble", BitemporalOrderFinder.items().quantity().sum());
        aggregateBeanList.addGroupBy("id", BitemporalOrderFinder.orderId());
        aggregateBeanList.setHavingOperation(BitemporalOrderFinder.items().originalPrice().avg().greaterThan(11.0));

        assertEquals(1, aggregateBeanList.size());
        assertEquals(2, aggregateBeanList.get(0).getId().intValue());
        assertEquals(60.0, aggregateBeanList.get(0).getTotalQuantityDouble().doubleValue(), 0);

    }


    public void testAndHavingOperationForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxDiscount", SaleFinder.discountPercentage().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());

        HavingOperation having = SaleFinder.discountPercentage().avg().greaterThanEquals(0.05);
        having = having.and(SaleFinder.discountPercentage().max().lessThan(0.1));
        aggregateBeanList.setHavingOperation(having);

        assertEquals(3, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(0.07, data.getMaxDiscount().doubleValue(), 0);
            }
            else if (3 == data.getId())
            {
                assertEquals(0.09, data.getMaxDiscount().doubleValue(), 0);
            }
            else if (4 == data.getId())
            {
                assertEquals(0.07, data.getMaxDiscount().doubleValue(), 0);
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testOrHavingOperationForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxDiscount", SaleFinder.discountPercentage().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());

        HavingOperation having = SaleFinder.discountPercentage().min().lessThan(0.05);
        having = having.or(SaleFinder.discountPercentage().max().greaterThanEquals(0.1));
        aggregateBeanList.setHavingOperation(having);

        assertEquals(2, aggregateBeanList.size());
        for (SimpleAggregateBean data : aggregateBeanList)
        {
            if (1 == data.getId())
            {
                assertEquals(0.07, data.getMaxDiscount().doubleValue(), 0);
            }
            else if (2 == data.getId())
            {
                assertEquals(0.1, data.getMaxDiscount().doubleValue(), 0);
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testComplexHavingOperationForAggregateBeanList()
    {
        Operation op = SaleFinder.all();

        AggregateBeanList<SimpleAggregateBean> aggregateBeanList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        aggregateBeanList.addAggregateAttribute("maxDiscount", SaleFinder.discountPercentage().max());
        aggregateBeanList.addGroupBy("id", SaleFinder.sellerId());

        HavingOperation having = SaleFinder.discountPercentage().min().lessThan(0.05);
        having = having.or(SaleFinder.discountPercentage().max().greaterThanEquals(0.1));
        having = having.and(SaleFinder.discountPercentage().avg().greaterThan(0.07));
        aggregateBeanList.setHavingOperation(having);

        assertEquals(1, aggregateBeanList.size());
        assertEquals(2, aggregateBeanList.get(0).getId().intValue());
        assertEquals(0.1, aggregateBeanList.get(0).getMaxDiscount().doubleValue(), 0);

    }

}
