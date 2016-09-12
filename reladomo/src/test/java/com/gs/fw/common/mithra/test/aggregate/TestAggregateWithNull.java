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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;


public class TestAggregateWithNull extends MithraTestAbstract
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
                        WishListItem.class,
                        ProductSpecification.class,
                        OrderItem.class,
                        Order.class,
                        Manufacturer.class,
                        ParaDesk.class
                };
    }

    public void testDoubleSumWithNull()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableDouble().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumND", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(21, data.getAttributeAsDouble("SumND"), 0);
                    break;
                case 2:
                    assertEquals(13, data.getAttributeAsDouble("SumND"), 0);
                    break;
                case 3:
                    assertEquals(33, data.getAttributeAsDouble("SumND"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumND"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testIntSumWithNull()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableInt().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumND", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(6, data.getAttributeAsInt("SumND"), 0);
                    break;
                case 2:
                    assertEquals(6, data.getAttributeAsInt("SumND"), 0);
                    break;
                case 3:
                    assertEquals(15, data.getAttributeAsInt("SumND"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumND"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testDoubleAvgWithNull()
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.nullableDouble().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumND", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(10.5, data.getAttributeAsDouble("SumND"), 0);
                    break;
                case 2:
                    assertEquals(13.0, data.getAttributeAsDouble("SumND"), 0);
                    break;
                case 3:
                    assertEquals(16.5, data.getAttributeAsDouble("SumND"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumND"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testIntAvgWithNull()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableInt().avg();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumND", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(2, data.getAttributeAsInt("SumND"), 0);
                    break;
                case 2:
                    assertEquals(6, data.getAttributeAsInt("SumND"), 0);
                    break;
                case 3:
                    assertEquals(7, data.getAttributeAsInt("SumND"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumND"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }


    public void testDateMaxWithNull() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableDate().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxND", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(dateFormat.parse("2004-02-14"), data.getAttributeAsDate("MaxND"));
                    break;
                case 2:
                    assertEquals(dateFormat.parse("2004-02-13"), data.getAttributeAsDate("MaxND"));
                    break;
                case 3:
                    assertTrue(data.isAttributeNull("MaxND"));
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("MaxND"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testTimestampMaxWithNull() throws ParseException
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.nullableTimestamp().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxNT", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 00:00:00.0").getTime()), data.getAttributeAsDate("MaxNT"));
                    break;
                case 2:
                    assertTrue(data.isAttributeNull("MaxNT"));
                    break;
                case 3:
                    assertTrue(data.isAttributeNull("MaxNT"));
                    break;
                case 4:
                    assertEquals(new Timestamp(timestampFormat.parse("2004-02-12 00:00:0.0").getTime()), data.getAttributeAsDate("MaxNT"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testStringMaxWithNull() throws ParseException
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableString().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxNS", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals("Sale 003", data.getAttributeAsString("MaxNS"));
                    break;
                case 2:
                    assertEquals("Sale 006", data.getAttributeAsString("MaxNS"));
                    break;
                case 3:
                    assertTrue(data.isAttributeNull("MaxNS"));
                    break;
                case 4:
                    assertEquals("Sale 010", data.getAttributeAsString("MaxNS"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testBooleanMaxWithNull()
    {
        Operation op = SaleFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SaleFinder.nullableBoolean().max();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("MaxNB", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(true, data.getAttributeAsBoolean("MaxNB"));
                    break;
                case 2:
                    assertEquals(true, data.getAttributeAsBoolean("MaxNB"));
                    break;
                case 3:
                    assertTrue(data.isAttributeNull("MaxNB"));
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("MaxNB"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testMappedDoubleSumWithNull()
    {
        Operation op = SellerFinder.all();
        com.gs.fw.common.mithra.MithraAggregateAttribute aggrAttr = SellerFinder.sales().nullableDouble().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumND", aggrAttr);
        aggregateList.addGroupBy("ActiveFlag", SellerFinder.active());

        assertEquals(2, aggregateList.size());


        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);
             if(data.getAttributeAsBoolean("ActiveFlag"))
             {
                 assertEquals(46, data.getAttributeAsDouble("SumND"), 0);
             }
             else
             {
                 assertEquals(21, data.getAttributeAsDouble("SumND"), 0);
             }

        }
    }

//    public void testMappedToManyDoubleSumWithNull()
//    {
//        fail();
//    }

    public void testAdditionCalculatedDoubleSumWithNull()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableDouble().plus(SaleFinder.nullableInt()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(24.0, data.getAttributeAsDouble("SumCD"), 0);
                    break;
                case 2:
                    assertTrue(data.isAttributeNull("SumCD"));
                    break;
                case 3:
                    assertEquals(48.0, data.getAttributeAsDouble("SumCD"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testSubstractionCalculatedDoubleSumWithNull()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableDouble().minus(SaleFinder.nullableInt()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(18.0, data.getAttributeAsDouble("SumCD"), 0);
                    break;
                case 2:
                    assertTrue(data.isAttributeNull("SumCD"));
                    break;
                case 3:
                    assertEquals(18.0, data.getAttributeAsDouble("SumCD"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

    public void testMultiplicationCalculatedDoubleSumWithNull()
    {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableDouble().times(SaleFinder.nullableInt()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(32.0, data.getAttributeAsDouble("SumCD"), 0);
                    break;
                case 2:
                    assertTrue(data.isAttributeNull("SumCD"));
                    break;
                case 3:
                    assertEquals(248.0, data.getAttributeAsDouble("SumCD"), 0);
                    break;
                case 4:
                    assertTrue(data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid sale id");
            }
        }
    }

//    public void testDivisionCalculatedDoubleSumWithNull()
//    {
//        fail();
//    }
//
//    public void testMappedAdditionCalculatedDoubleSumWithNull()
//    {
//        fail();
//    }
//
//    public void testMappedDateMaxWithNull()
//    {
//        fail();
//    }
//
//    public void testMappedTimestampMaxWithNull()
//    {
//        fail();
//    }
//
//     public void testMappedStringMaxWithNull()
//    {
//        fail();
//    }
//
//    public void testMappedBooleanMaxWithNull()
//    {
//        fail();
//    }

   public void testAggregatingAdditionCalculatedBigDecimalAttribute()
   {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableBigDecimal().plus(SaleFinder.nullableVeryBigDecimal()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(new BigDecimal("43003.49898"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 2:
                    assertEquals(new BigDecimal("103558.99898"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 3:
                    assertEquals(new BigDecimal("80778.99999"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 4:
                    assertTrue( data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
   }
   public void testAggregatingSubstractionCalculatedBigDecimalAttribute()
   {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableBigDecimal().minus(SaleFinder.nullableVeryBigDecimal()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(new BigDecimal("-37000.50098"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 2:
                    assertEquals(new BigDecimal("-96445.00098"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 3:
                    assertEquals(new BigDecimal("-79222.99999"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 4:
                    assertTrue( data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
   }
   public void testAggregatingDivisionCalculatedBigDecimalAttribute()
   {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableBigDecimal().dividedBy(SaleFinder.nullableVeryBigDecimal()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.166737740, data.getAttributeAsBigDecimal("SumCD").doubleValue(),0.00000001);
                    break;
                case 2:
                    assertEquals(0.084289612, data.getAttributeAsBigDecimal("SumCD").doubleValue(),0.00000001);
                    break;
                case 3:
                    assertEquals(0.009724878, data.getAttributeAsBigDecimal("SumCD").doubleValue(),0.00000001);
                    break;
                case 4:
                    assertTrue( data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
   }

   public void testAggregatingDivisionCalculatedBigDecimalAttribute2()
   {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableVeryBigDecimal().dividedBy(SaleFinder.nullableBigDecimal()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {                                                  
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(24.9890129750, data.getAttributeAsBigDecimal("SumCD").doubleValue(),0.00000001);
                    break;
                case 2:
                    assertEquals(121.2446956383, data.getAttributeAsBigDecimal("SumCD").doubleValue(),0.00000001);
                    break;
                case 3:
                    assertEquals(102.82904883, data.getAttributeAsBigDecimal("SumCD").doubleValue(),0.00000001);
                    break;
                case 4:
                    assertTrue( data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
   }

   public void testAggregatingMultiplicationCalculatedBigDecimalAttribute()
   {
        Operation op = SaleFinder.all();
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableBigDecimal().times(SaleFinder.nullableVeryBigDecimal()).sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumCD", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(4, aggregateList.size());

        for(int i = 0 ; i < aggregateList.size(); i ++)
        {
            AggregateData data = aggregateList.get(i);

            switch(data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(new BigDecimal("70037971.46898501"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 2:
                    assertEquals(new BigDecimal("153403516.96343001"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 3:
                    assertEquals(new BigDecimal("62240777.99222000"), data.getAttributeAsBigDecimal("SumCD"));
                    break;
                case 4:
                    assertTrue( data.isAttributeNull("SumCD"));
                    break;

                default:
                    fail("Invalid seller id");
            }
        }
   }

    public void testNullException()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr = SaleFinder.nullableDouble().sum();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("SumND", aggrAttr);
        aggregateList.addGroupBy("SellerId", SaleFinder.sellerId());

        assertEquals(1, aggregateList.size());
        try
        {
            AggregateData data = aggregateList.get(0);
            data.getAttributeAsDouble("SumND");
            fail();
        }
        catch(Exception e)
        {
            getLogger().info("Expected Exception: "+e.getMessage());
        }
    }
}
