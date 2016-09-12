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

import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.HavingOperation;
import com.gs.fw.common.mithra.util.Pair;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.sql.Timestamp;
import java.util.Date;



public class TestAggregationWithHavingClause extends MithraTestAbstract
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

    public void testStringHavingEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = createAggregateList(op, new Pair("MinDesc",SaleFinder.description().min()),
                new Pair("SellerId",SaleFinder.sellerId()),SaleFinder.description().min().eq("Sale 0009") );

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals("Sale 0009", data.getAttributeAsString("MinDesc"));
    }

    private AggregateList createAggregateList(Operation op, Pair<String, com.gs.fw.common.mithra.MithraAggregateAttribute> aggregateAttribute,
                                              Pair<String, Attribute> groupByAttribute, HavingOperation havingClause)
    {
        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute(aggregateAttribute.getOne(), aggregateAttribute.getTwo());
        aggregateList.addGroupBy(groupByAttribute.getOne(), groupByAttribute.getTwo());
        aggregateList.setHavingOperation(havingClause);
        return aggregateList;
    }

    public void testStringHavingEqWithDifferentAttribute()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDesc", SaleFinder.description().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.description().max().eq("Sale 0010"));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals("Sale 0009", data.getAttributeAsString("MinDesc"));
    }

    public void testStringHavingNotEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDesc", SaleFinder.description().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.description().min().notEq("Sale 0009"));

        assertEquals(3, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals("Sale 0001",data.getAttributeAsString("MinDesc"));
            }

            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals("Sale 0004",data.getAttributeAsString("MinDesc"));
            }

            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals("Sale 0007",data.getAttributeAsString("MinDesc"));
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testStringHavingGreaterThan()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDesc", SaleFinder.description().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.description().min().greaterThan("Sale 0004"));

        assertEquals(2, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals("Sale 0007",data.getAttributeAsString("MinDesc"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals("Sale 0009",data.getAttributeAsString("MinDesc"));
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testStringHavingGreaterThanEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinDesc", SaleFinder.description().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.description().min().greaterThanEquals("Sale 0004"));

        assertEquals(3, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals("Sale 0004",data.getAttributeAsString("MinDesc"));
            }
            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals("Sale 0007",data.getAttributeAsString("MinDesc"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals("Sale 0009",data.getAttributeAsString("MinDesc"));
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testStringHavingLessThan()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDesc", SaleFinder.description().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.description().max().lessThan("Sale 0004"));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals("Sale 0003", data.getAttributeAsString("MaxDesc"));
    }

    public void testStringHavingLessThanEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDesc", SaleFinder.description().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.description().max().lessThanEquals("Sale 0006"));

        assertEquals(2, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals("Sale 0003",data.getAttributeAsString("MaxDesc"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals("Sale 0006",data.getAttributeAsString("MaxDesc"));
            }
            else
            {
                fail("invalid seller id");
            }
        }
    }

    public void testTimestampHavingEq() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinSaleDate", SaleFinder.saleDate().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.saleDate().min().eq(ts));

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            assertEquals(ts, data.getAttributeAsTimestamp("MinSaleDate"));
             assertTrue(data.getAttributeAsInt("SellerId") != 1 );
        }
    }

    public void testTimestampHavingNotEq() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime());
        Timestamp ts2 = new Timestamp(timestampFormat.parse("2004-01-12 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinSaleDate", SaleFinder.saleDate().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.saleDate().min().notEq(ts));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(ts2, data.getAttributeAsTimestamp("MinSaleDate"));
        assertTrue(data.getAttributeAsInt("SellerId") == 1 );

    }

    public void testTimestampHavingGreaterThan() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-12 12:00:00.0").getTime());
        Timestamp ts2 = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts3 = new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSaleDate", SaleFinder.saleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.saleDate().max().greaterThan(ts));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(ts2, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals(ts3, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testTimestampHavingGreaterThanEq() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts2 = new Timestamp(timestampFormat.parse("2004-02-14 00:00:00.0").getTime());
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSaleDate", SaleFinder.saleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.saleDate().max().greaterThanEquals(ts));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(ts, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals(ts2, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testTimestampHavingLessThan() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts3 = new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime());
        Timestamp ts4 = new Timestamp(timestampFormat.parse("2004-02-12 02:00:00.0").getTime());

        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSaleDate", SaleFinder.saleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.saleDate().max().lessThan(ts));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(ts3, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals(ts4, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testTimestampHavingLessThanEq() throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2004-02-13 00:00:00.0").getTime());
        Timestamp ts3 = new Timestamp(timestampFormat.parse("2004-02-12 01:00:00.0").getTime());
        Timestamp ts4 = new Timestamp(timestampFormat.parse("2004-02-12 02:00:00.0").getTime());

        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSaleDate", SaleFinder.saleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.saleDate().max().lessThanEquals(ts));

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(ts, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(ts3, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals(ts4, data.getAttributeAsTimestamp("MaxSaleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }


    public void testDateHavingEq() throws ParseException
    {
        Date ts = dateFormat.parse("2004-02-12");
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinSettleDate", SaleFinder.settleDate().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.settleDate().min().eq(ts));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(ts, data.getAttributeAsDate("MinSettleDate"));
        assertTrue(data.getAttributeAsInt("SellerId") == 2 );
    }

    public void testDateHavingNotEq() throws ParseException
    {
        Date ts =  dateFormat.parse("2004-02-12");
        Date ts2 = dateFormat.parse("2004-01-13");
        Date ts3 = dateFormat.parse("2004-02-15");
        Date ts4 = dateFormat.parse("2004-03-01");
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinSettleDate", SaleFinder.settleDate().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.settleDate().min().notEq(ts));

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(ts2, data.getAttributeAsDate("MinSettleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(ts3, data.getAttributeAsDate("MinSettleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals(ts4, data.getAttributeAsDate("MinSettleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }

    }

    public void testDateHavingGreaterThan() throws ParseException
    {
        Date ts =  dateFormat.parse("2004-03-01");
        Date ts2 = dateFormat.parse("2004-04-01");
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSettleDate", SaleFinder.settleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.settleDate().max().greaterThan(ts));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(ts2, data.getAttributeAsDate("MaxSettleDate"));
    }

    public void testDateHavingGreaterThanEq() throws ParseException
    {
        Date ts =  dateFormat.parse("2004-02-17");
        Date ts2 = dateFormat.parse("2004-04-01");
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSettleDate", SaleFinder.settleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.settleDate().max().greaterThanEquals(ts));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(ts, data.getAttributeAsDate("MaxSettleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals(ts2, data.getAttributeAsDate("MaxSettleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testDateHavingLessThan() throws ParseException
    {
        Date ts =  dateFormat.parse("2004-02-17");
        Date ts2 = dateFormat.parse("2004-02-14");
        Date ts3 = dateFormat.parse("2004-02-15");
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSettleDate", SaleFinder.settleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.settleDate().max().lessThan(ts));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(ts3, data.getAttributeAsDate("MaxSettleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals(ts2, data.getAttributeAsDate("MaxSettleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testDateHavingLessThanEq() throws ParseException
    {
        Date ts =  dateFormat.parse("2004-02-17");
        Date ts2 = dateFormat.parse("2004-02-14");
        Date ts3 = dateFormat.parse("2004-02-15");
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxSettleDate", SaleFinder.settleDate().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.settleDate().max().lessThanEquals(ts));

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(ts3, data.getAttributeAsDate("MaxSettleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals(ts2, data.getAttributeAsDate("MaxSettleDate"));
            }
            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(ts, data.getAttributeAsDate("MaxSettleDate"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }


    public void testBooleanHavingEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinActiveBoolean", SaleFinder.activeBoolean().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.activeBoolean().min().eq(true));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(3, data.getAttributeAsInt("SellerId"));
        assertEquals(true, data.getAttributeAsBoolean("MinActiveBoolean"));
    }

    public void testBooleanHavingNotEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinActiveBoolean", SaleFinder.activeBoolean().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.activeBoolean().min().notEq(true));

        assertEquals(3, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            assertFalse(3 == data.getAttributeAsInt("SellerId"));
            assertFalse(data.getAttributeAsBoolean("MinActiveBoolean"));
        }
    }

    public void testBooleanHavingGreaterThan()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinActiveBoolean", SaleFinder.activeBoolean().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.activeBoolean().min().greaterThan(false));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(3, data.getAttributeAsInt("SellerId"));
        assertEquals(true, data.getAttributeAsBoolean("MinActiveBoolean"));
    }

    public void testBooleanHavingGreaterThanEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinActiveBoolean", SaleFinder.activeBoolean().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.activeBoolean().min().greaterThanEquals(false));

        assertEquals(4, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(true, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testBooleanHavingLessThan()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinActiveBoolean", SaleFinder.activeBoolean().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.activeBoolean().max().lessThan(true));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(4, data.getAttributeAsInt("SellerId"));
        assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
    }

    public void testBooleanHavingLessThanEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinActiveBoolean", SaleFinder.activeBoolean().min());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());
        aggregateList.setHavingOperation(SaleFinder.activeBoolean().min().lessThanEquals(true));

        assertEquals(4, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("SellerId") == 1)
            {
                assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else if(data.getAttributeAsInt("SellerId") == 2)
            {
                assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else if(data.getAttributeAsInt("SellerId") == 3)
            {
                assertEquals(true, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else if(data.getAttributeAsInt("SellerId") == 4)
            {
                assertEquals(false, data.getAttributeAsBoolean("MinActiveBoolean"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }


    public void testCharHavingEq()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinChar", ParentNumericAttributeFinder.charAttr().min());
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
        aggregateList.setHavingOperation(ParentNumericAttributeFinder.charAttr().min().eq('a'));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(1, data.getAttributeAsInt("UserId"));
        assertEquals('a', data.getAttributeAsCharacter("MinChar"));
    }

    public void testCharHavingNotEq()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MinChar", ParentNumericAttributeFinder.charAttr().min());
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
        aggregateList.setHavingOperation(ParentNumericAttributeFinder.charAttr().min().notEq('t'));

        assertEquals(2, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("UserId") == 1)
            {
                assertEquals('a', data.getAttributeAsCharacter("MinChar"));
            }
            else if(data.getAttributeAsInt("UserId") == 2)
            {
                assertEquals('d', data.getAttributeAsCharacter("MinChar"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCharHavingGreaterThan()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxChar", ParentNumericAttributeFinder.charAttr().min());
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
        aggregateList.setHavingOperation(ParentNumericAttributeFinder.charAttr().min().greaterThan('a'));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(2, data.getAttributeAsInt("UserId"));
        assertEquals('d', data.getAttributeAsCharacter("MaxChar"));
    }

    public void testCharHavingGreaterThanEq()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxChar", ParentNumericAttributeFinder.charAttr().max());
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
        aggregateList.setHavingOperation(ParentNumericAttributeFinder.charAttr().max().greaterThanEquals('a'));

        assertEquals(2, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("UserId") == 1)
            {
                assertEquals('c', data.getAttributeAsCharacter("MaxChar"));
            }
            else if(data.getAttributeAsInt("UserId") == 2)
            {
                assertEquals('f', data.getAttributeAsCharacter("MaxChar"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCharHavingLessThan()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxChar", ParentNumericAttributeFinder.charAttr().max());
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
        aggregateList.setHavingOperation(ParentNumericAttributeFinder.charAttr().max().lessThan('t'));

        assertEquals(2, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("UserId") == 1)
            {
                assertEquals('c', data.getAttributeAsCharacter("MaxChar"));
            }
            else if(data.getAttributeAsInt("UserId") == 2)
            {
                assertEquals('f', data.getAttributeAsCharacter("MaxChar"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testCharHavingLessThanEq()
    {
        Operation op = ParentNumericAttributeFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxChar", ParentNumericAttributeFinder.charAttr().max());
        aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
        aggregateList.setHavingOperation(ParentNumericAttributeFinder.charAttr().max().greaterThanEquals('c'));

        assertEquals(2, aggregateList.size());
        for(AggregateData data:aggregateList)
        {
            if(data.getAttributeAsInt("UserId") == 1)
            {
                assertEquals('c', data.getAttributeAsCharacter("MaxChar"));
            }
            else if(data.getAttributeAsInt("UserId") == 2)
            {
                assertEquals('f', data.getAttributeAsCharacter("MaxChar"));
            }
            else
            {
                fail("Invalid seller id");
            }
        }
    }

    public void testIntegerHavingEq() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItemQty", SalesLineItemFinder.quantity().avg());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().avg().eq(20));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("AvgItemQty"));
            }
            else if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("AvgItemQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testIntegerHavingNotEq() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().notEq(30));

        assertEquals(7, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("TotalQty"));
            }
            else if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(60, data.getAttributeAsInt("TotalQty"));
            }
            else if(3 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(25, data.getAttributeAsInt("TotalQty"));
            }
            else if(5 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(55, data.getAttributeAsInt("TotalQty"));
            }
            else if(6 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(27, data.getAttributeAsInt("TotalQty"));
            }
            else if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testIntegerHavingGreaterThan()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThan(30));

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {

            if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(60, data.getAttributeAsInt("TotalQty"));
            }
            else if(5 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(55, data.getAttributeAsInt("TotalQty"));
            }
            else if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testIntegerHavingGreaterThanEq()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThanEquals(30));

        assertEquals(6, aggregateList.size());
        for(AggregateData data: aggregateList)
        {

            if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(60, data.getAttributeAsInt("TotalQty"));
            }
            else if(4 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(5 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(55, data.getAttributeAsInt("TotalQty"));
            }
            else if(7 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(8 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testIntegerHavingLessThan()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThan(30));

        assertEquals(4, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("TotalQty"));
            }
            else if(3 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(25, data.getAttributeAsInt("TotalQty"));
            }
            else if(6 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(27, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testIntegerHavingLessThanEq()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThanEquals(30));

        assertEquals(7, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("TotalQty"));
            }
            else if(3 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(25, data.getAttributeAsInt("TotalQty"));
            }
            else if(4 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(6 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(27, data.getAttributeAsInt("TotalQty"));
            }
            else if(7 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(8 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingEq() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItemQty", SalesLineItemFinder.quantity().avg());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().avg().eq(20));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("AvgItemQty"));
            }
            else if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("AvgItemQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testDoubleHavingNotEq() throws ParseException
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().notEq(30));

        assertEquals(7, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("TotalQty"));
            }
            else if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(60, data.getAttributeAsInt("TotalQty"));
            }
            else if(3 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(25, data.getAttributeAsInt("TotalQty"));
            }
            else if(5 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(55, data.getAttributeAsInt("TotalQty"));
            }
            else if(6 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(27, data.getAttributeAsInt("TotalQty"));
            }
            else if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingGreaterThan()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThan(30));

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {

            if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(60, data.getAttributeAsInt("TotalQty"));
            }
            else if(5 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(55, data.getAttributeAsInt("TotalQty"));
            }
            else if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testDoubleHavingGreaterThanEq()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().greaterThanEquals(30));

        assertEquals(6, aggregateList.size());
        for(AggregateData data: aggregateList)
        {

            if(2 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(60, data.getAttributeAsInt("TotalQty"));
            }
            else if(4 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(5 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(55, data.getAttributeAsInt("TotalQty"));
            }
            else if(7 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(8 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingLessThan()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThan(30));

        assertEquals(4, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("TotalQty"));
            }
            else if(3 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(25, data.getAttributeAsInt("TotalQty"));
            }
            else if(6 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(27, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testDoubleHavingLessThanEq()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.quantity().sum());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.quantity().sum().lessThanEquals(30));

        assertEquals(7, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(20, data.getAttributeAsInt("TotalQty"));
            }
            else if(3 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(25, data.getAttributeAsInt("TotalQty"));
            }
            else if(4 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(6 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(27, data.getAttributeAsInt("TotalQty"));
            }
            else if(7 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(8 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(30, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }




    public void testCountAggregationWithHaving()
    {
        Operation op = SalesLineItemFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SalesLineItemFinder.itemId().count());
        aggregateList.addGroupBy("SaleId", SalesLineItemFinder.saleId());
        aggregateList.setHavingOperation(SalesLineItemFinder.itemId().count().eq(1));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(1, data.getAttributeAsInt("SaleId"));
        assertEquals(1, data.getAttributeAsInt("TotalQty"));
    }

    public void testMappedDoubleHavingEq()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("AvgItemPrice", SaleFinder.items().productSpecs().originalPrice().avg());
        aggregateList.addGroupBy("SaleId", SaleFinder.saleId());
        aggregateList.setHavingOperation(SaleFinder.items().productSpecs().originalPrice().avg().eq(12.5));

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(1, data.getAttributeAsInt("SaleId"));
        assertEquals(12.5, data.getAttributeAsDouble("AvgItemPrice"));
    }

    public void testMappedDoubleHavingEqUsingDifferentAttributeInHaving()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", SaleFinder.items().quantity().sum());
        aggregateList.addGroupBy("SaleId", SaleFinder.saleId());
        aggregateList.setHavingOperation(SaleFinder.items().productSpecs().originalPrice().avg().eq(18.75));

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(9 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(31, data.getAttributeAsInt("TotalQty"));
            }
            else if(10 == data.getAttributeAsInt("SaleId"))
            {
                assertEquals(22, data.getAttributeAsInt("TotalQty"));
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    public void testDatedMappedDoubleHavingEqUsingDifferentAttributeInHaving()
    {
        Operation op = BitemporalOrderFinder.all();
        op = op.and(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("TotalQty", BitemporalOrderFinder.items().quantity().sum());
        aggregateList.addGroupBy("OrderId", BitemporalOrderFinder.orderId());
        aggregateList.setHavingOperation(BitemporalOrderFinder.items().originalPrice().avg().greaterThan(11.0));

        assertEquals(1, aggregateList.size());
        for(AggregateData data: aggregateList)
        {

            if(2 == data.getAttributeAsInt("OrderId"))
            {
                assertEquals(60.0, data.getAttributeAsDouble("TotalQty"),0);
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }


    // We still need to undertsand how to do this case. In this case we are going from Order to Items,we are aggregating
    // Order but the having operation is aggregating on items
    public void xtestDatedHavingEqUsingDifferentMappedAttributeInHaving()
    {
        Operation op = BitemporalOrderFinder.all();
        op = op.and(BitemporalOrderFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())));

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("OrderIdSum", BitemporalOrderFinder.orderId().sum());
        aggregateList.addGroupBy("UserId", BitemporalOrderFinder.userId());
        aggregateList.setHavingOperation(BitemporalOrderFinder.items().originalPrice().avg().greaterThan(11.0));

        assertEquals(1, aggregateList.size());
        for(AggregateData data: aggregateList)
        {

            if(2 == data.getAttributeAsInt("UserId"))
            {
                assertEquals(60.0, data.getAttributeAsDouble("OrderIdSum"),0);
            }
            else
            {
                fail("Invalid sale id");
            }
        }
    }

    public void testAndHavingOperation()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDiscount", SaleFinder.discountPercentage().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        HavingOperation having = SaleFinder.discountPercentage().avg().greaterThan(0.05);
        having = having.and(SaleFinder.discountPercentage().max().lessThan(0.1));
        aggregateList.setHavingOperation(having);

        assertEquals(3, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SellerId"))
            {
                assertEquals(0.07, data.getAttributeAsDouble("MaxDiscount"),0);
            }
            else if(3 == data.getAttributeAsInt("SellerId"))
            {
                assertEquals(0.09, data.getAttributeAsDouble("MaxDiscount"),0);
            }
            else if(4 == data.getAttributeAsInt("SellerId"))
            {
                assertEquals(0.07, data.getAttributeAsDouble("MaxDiscount"),0);
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testOrHavingOperation()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDiscount", SaleFinder.discountPercentage().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        HavingOperation having = SaleFinder.discountPercentage().min().lessThan(0.05);
        having = having.or(SaleFinder.discountPercentage().max().greaterThanEquals(0.1));
        aggregateList.setHavingOperation(having);

        assertEquals(2, aggregateList.size());
        for(AggregateData data: aggregateList)
        {
            if(1 == data.getAttributeAsInt("SellerId"))
            {
                assertEquals(0.07, data.getAttributeAsDouble("MaxDiscount"),0);
            }
            else if(2 == data.getAttributeAsInt("SellerId"))
            {
                assertEquals(0.1, data.getAttributeAsDouble("MaxDiscount"),0);
            }
            else
            {
                fail("Invalid sale id");
            }
        }

    }

    public void testComplexHavingOperation()
    {
        Operation op = SaleFinder.all();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxDiscount", SaleFinder.discountPercentage().max());
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        HavingOperation having = SaleFinder.discountPercentage().min().lessThan(0.05);
        having = having.or(SaleFinder.discountPercentage().max().greaterThanEquals(0.1));
        having = having.and(SaleFinder.discountPercentage().avg().greaterThan(0.07));
        aggregateList.setHavingOperation(having);

        assertEquals(1, aggregateList.size());
        AggregateData data = aggregateList.get(0);
        assertEquals(2 ,data.getAttributeAsInt("SellerId"));
        assertEquals(0.1, data.getAttributeAsDouble("MaxDiscount"),0);
           
    }

}
