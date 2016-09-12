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
import java.math.BigDecimal;


public class TestNumericAttribute extends MithraTestAbstract
 {

     public Class[] getRestrictedClassList()
     {
         return new Class[]
                 {
                         ParentNumericAttribute.class,
                         ChildNumericAttribute.class,
                         BigOrder.class,
                         BigOrderItem.class
                 };
     }

     public void testBigDecimalSum()
     {

         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr1 = ParentNumericAttributeFinder.bigDecimalAttr().sum();
         MithraAggregateAttribute aggrAttr2 = ParentNumericAttributeFinder.veryBigDecimalAttr().sum();

         AggregateList aggregateList = new AggregateList(op);
         aggregateList.addAggregateAttribute("Attr1", aggrAttr1);
         aggregateList.addAggregateAttribute("Attr2", aggrAttr2);
         aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());

         assertEquals(2, aggregateList.size());

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {
                 case 1:
                     assertEquals(new BigDecimal("601.50"), data.getAttributeAsBigDecimal("Attr1"));
                     assertEquals(new BigDecimal("666666666666666666"), data.getAttributeAsBigDecimal("Attr2"));
                     break;
                 case 2:

                     assertEquals(new BigDecimal("1501.50"), data.getAttributeAsBigDecimal("Attr1"));
                     assertEquals(new BigDecimal("1666666666666666665"), data.getAttributeAsBigDecimal("Attr2"));
                     break;
                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testByteSum()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.byteAttr().sum();

         AggregateList aggregateList = new AggregateList(op);
         aggregateList.addAggregateAttribute("Attr", aggrAttr);
         aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());

         assertEquals(2, aggregateList.size());

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals((byte) 6, data.getAttributeAsByte("Attr"), 2);
                     break;
                 case 2:
                     assertEquals((byte) 15, data.getAttributeAsByte("Attr"), 2);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testShortSum()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.shortAttr().sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {
                 case 1:
                     assertEquals((short) 600, data.getAttributeAsShort("Attr"));
                     break;
                 case 2:
                     assertEquals((short) 1500, data.getAttributeAsShort("Attr"));
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testLongSum()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.longAttr().sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {
                 case 1:
                     assertEquals(320000000, data.getAttributeAsLong("Attr"));
                     break;
                 case 2:
                     assertEquals(420000000, data.getAttributeAsLong("Attr"));
                     break;
                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testGroupByDifferentTypes()
     {
         Operation op = ParentNumericAttributeFinder.all();

         AggregateList aggregateList = new AggregateList(op);
         aggregateList.addAggregateAttribute("UserId", ParentNumericAttributeFinder.userId().sum());
         aggregateList.addGroupBy("byteAttr", ParentNumericAttributeFinder.byteAttr());
         aggregateList.addGroupBy("shortAttr", ParentNumericAttributeFinder.shortAttr());
         aggregateList.addGroupBy("charAttr", ParentNumericAttributeFinder.charAttr());
         aggregateList.addGroupBy("intAttr", ParentNumericAttributeFinder.intAttr());
         aggregateList.addGroupBy("longAttr", ParentNumericAttributeFinder.longAttr());
         aggregateList.addGroupBy("doubleAttr", ParentNumericAttributeFinder.doubleAttr());
         aggregateList.addGroupBy("floatAttr", ParentNumericAttributeFinder.floatAttr());
         assertEquals(6, aggregateList.size());

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);
             assertFalse(data.isAttributeNull("byteAttr"));
             assertFalse(data.isAttributeNull("shortAttr"));
             assertFalse(data.isAttributeNull("charAttr"));
             assertFalse(data.isAttributeNull("intAttr"));
             assertFalse(data.isAttributeNull("longAttr"));
             assertFalse(data.isAttributeNull("doubleAttr"));
             assertFalse(data.isAttributeNull("floatAttr"));
             ParentNumericAttribute pna = ParentNumericAttributeFinder.findOne(ParentNumericAttributeFinder.longAttr().eq(data.getAttributeAsLong("longAttr")));

             assertEquals(pna.getByteAttr(), data.getAttributeAsByte("byteAttr"));
             assertEquals(pna.getShortAttr(), data.getAttributeAsShort("shortAttr"));
             assertEquals(pna.getCharAttr(), data.getAttributeAsCharacter("charAttr"));
             assertEquals(pna.getIntAttr(), data.getAttributeAsInt("intAttr"));
             assertEquals(pna.getDoubleAttr(), data.getAttributeAsDouble("doubleAttr"));
             assertEquals(pna.getFloatAttr(), data.getAttributeAsFloat("floatAttr"));
         }
     }

     public void testFloatSum()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.floatAttr().sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {
                 case 1:
                     assertEquals(33.3f, data.getAttributeAsFloat("Attr"), 0.01f);
                     break;
                 case 2:
                     assertEquals(42.3f, data.getAttributeAsFloat("Attr"), 0.01f);
                     break;
                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testIntDivisionPromotedToDouble()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.intAttr().dividedBy(ParentNumericAttributeFinder.doubleAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {
                 case 1:
                     assertEquals(299.08, data.getAttributeAsDouble("Attr"), 2);
                     assertEquals(299.08, ((Double) data.getAttributeAsObject("Attr")).doubleValue(), 2);
                     break;
                 case 2:
                     assertEquals(299.69, data.getAttributeAsDouble("Attr"), 2);
                     assertEquals(299.69, ((Double) data.getAttributeAsObject("Attr")).doubleValue(), 2);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testIntDivisionPromotedToLong()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.intAttr().dividedBy(ParentNumericAttributeFinder.longAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {
                 case 1:
                     assertEquals(0, data.getAttributeAsLong("Attr"));
                     break;
                 case 2:
                     assertEquals(0, data.getAttributeAsLong("Attr"));
                     break;
                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testIntDivisionPromotedToFloat()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.intAttr().dividedBy(ParentNumericAttributeFinder.floatAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(5271.23f, data.getAttributeAsFloat("Attr"), 0.01f);
                     break;
                 case 2:
                     assertEquals(10573.04f, data.getAttributeAsFloat("Attr"), 0.01f);
                     break;
                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testLongDivisionPromotedToFloat()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.longAttr().dividedBy(ParentNumericAttributeFinder.floatAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(2.8738156E7f, data.getAttributeAsFloat("Attr"), 0.01f);
                     break;
                 case 2:
                     assertEquals(2.9786516E7f, data.getAttributeAsFloat("Attr"), 0.01f);
                     break;
                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testLongDivisionPromotedToDouble()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.longAttr().dividedBy(ParentNumericAttributeFinder.doubleAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(1843485.25, data.getAttributeAsDouble("Attr"), 0.01);
                     break;
                 case 2:
                     assertEquals(854314.53, data.getAttributeAsDouble("Attr"), 0.01);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testFloatDivisionPromotedToDouble()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.floatAttr().dividedBy(ParentNumericAttributeFinder.doubleAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(0.19, data.getAttributeAsDouble("Attr"), 0.01);
                     break;
                 case 2:
                     assertEquals(0.08, data.getAttributeAsDouble("Attr"), 0.01);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testByteDivisionPromotedToDouble()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.byteAttr().dividedBy(ParentNumericAttributeFinder.doubleAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(0.0299, data.getAttributeAsDouble("Attr"), 0.0001);
                     break;
                 case 2:
                     assertEquals(0.0299, data.getAttributeAsDouble("Attr"), 0.0001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }


     public void testDoubleDivisionPromotedToBigDecimal()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.doubleAttr().dividedBy(ParentNumericAttributeFinder.bigDecimalAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(3.00, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.001);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(3.00, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testSumOfAggregatingAdditionCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().plus(ParentNumericAttributeFinder.bigDecimalAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(6.6666666666666726E17, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(1.66666666666666829E18, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testSumOfSubstractionCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().minus(ParentNumericAttributeFinder.bigDecimalAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(6.6666666666666611E17, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(1.66666666666666522E18, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testSumOfDivisionCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().dividedBy(ParentNumericAttributeFinder.bigDecimalAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(3323185796138933.693038, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(3329911022314668.461395, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testSumOfDivisionCalculatedMappedBigDecimalAttribute()
     {
         Operation op = BigOrderFinder.all();
         AggregateList aggregateList = new AggregateList(op);
         MithraAggregateAttribute aggrAttr = BigOrderFinder.items().price().dividedBy(BigOrderFinder.items().quantity()).sum();
         aggregateList.addAggregateAttribute("SumOfWeirdValue",aggrAttr);
         aggregateList.addGroupBy("UserId", BigOrderFinder.userId());

         assertEquals(2, aggregateList.size());
         for (int i = 0; i < aggregateList.size(); i++)
           {
               AggregateData data = aggregateList.get(i);

               switch (data.getAttributeAsInt("UserId"))
               {

                   case 1:
                       assertEquals(6.0500000000, data.getAttributeAsBigDecimal("SumOfWeirdValue").doubleValue(), 0.000001);
                       break;
                   case 2:
                       //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                       assertEquals(0.8245030000, data.getAttributeAsBigDecimal("SumOfWeirdValue").doubleValue(), 0.000001);
                       break;


                   default:
                       fail("Invalid user id");
               }
           }
      }

     public void testSumOfMultiplicationCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().times(ParentNumericAttributeFinder.bigDecimalAttr()).sum();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(1.558888888888889E20, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(8.563888888888889E20, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.00001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }


     public void testAvgOfAggregatingAdditionCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().plus(ParentNumericAttributeFinder.bigDecimalAttr()).avg();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(222222222222222422.50, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.01);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(555555555555556055.50, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.01);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testAvgOfSubstractionCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().minus(ParentNumericAttributeFinder.bigDecimalAttr().times(10000.0)).avg();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(222222222220217222.0, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.01);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(5.5555555555055053E17, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.01);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testAvgOfDivisionCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().dividedBy(ParentNumericAttributeFinder.bigDecimalAttr()).avg();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(1107728598712977.9, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.000001);
                     break;
                 case 2:
                     assertEquals(1109970340771556.1, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.000001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }

     public void testAvgOfDivisionCalculatedMappedBigDecimalAttribute()
     {
         Operation op = BigOrderFinder.all();
         AggregateList aggregateList = new AggregateList(op);
         MithraAggregateAttribute aggrAttr = BigOrderFinder.items().price().dividedBy(BigOrderFinder.items().quantity()).avg();
         aggregateList.addAggregateAttribute("SumOfWeirdValue",aggrAttr);
         aggregateList.addGroupBy("UserId", BigOrderFinder.userId());

         assertEquals(2, aggregateList.size());
         for (int i = 0; i < aggregateList.size(); i++)
           {
               AggregateData data = aggregateList.get(i);

               switch (data.getAttributeAsInt("UserId"))
               {

                   case 1:
                       assertEquals(1.2100000000, data.getAttributeAsBigDecimal("SumOfWeirdValue").doubleValue(), 0.000001);
                       break;
                   case 2:
                       //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                       assertEquals(0.1374171667, data.getAttributeAsBigDecimal("SumOfWeirdValue").doubleValue(), 0.000001);
                       break;


                   default:
                       fail("Invalid user id");
               }
           }
      }

     public void testAvgOfMultiplicationCalculatedBigDecimalAttribute()
     {
         Operation op = ParentNumericAttributeFinder.all();
         MithraAggregateAttribute aggrAttr = ParentNumericAttributeFinder.veryBigDecimalAttr().times(ParentNumericAttributeFinder.bigDecimalAttr()).avg();

         AggregateList aggregateList = constructAndValidateAggregateList(op, aggrAttr);

         for (int i = 0; i < aggregateList.size(); i++)
         {
             AggregateData data = aggregateList.get(i);

             switch (data.getAttributeAsInt("UserId"))
             {

                 case 1:
                     assertEquals(5.1962962962962964E19, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.000001);
                     break;
                 case 2:
                     //assertEquals(new BigDecimal("3.00"), data.getAttributeAsBigDecimal("Attr"));
                     assertEquals(2.8546296296296297E20, data.getAttributeAsBigDecimal("Attr").doubleValue(), 0.000001);
                     break;


                 default:
                     fail("Invalid user id");
             }
         }
     }


     private AggregateList constructAndValidateAggregateList(Operation op, MithraAggregateAttribute aggrAttr)
     {
         AggregateList aggregateList = new AggregateList(op);
         aggregateList.addAggregateAttribute("Attr", aggrAttr);
         aggregateList.addGroupBy("UserId", ParentNumericAttributeFinder.userId());
         assertEquals(2, aggregateList.size());
         return aggregateList;
     }
 }
