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

import java.text.ParseException;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.SaleAggregateDataFinder;


public class TestVariance extends MithraTestAbstract
{

    public double DELTA = 0.000000001;

    public TestVariance()
    {
    }

    public TestVariance(double delta)
    {
        this.DELTA = delta;
    }

    public void testVarianceDouble() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceDouble().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceDouble().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceDouble().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceDouble().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceDouble().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceDouble().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceDouble().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceDouble().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("VariancePrice");
                        fail("should have thrown Runtime Exception Null Value");
                    }
                    catch (RuntimeException e)
                    {
                        //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopDouble() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceDouble().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceDouble().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceDouble().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceDouble().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceDouble().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceDouble().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceDouble().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceDouble().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVarianceInt() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceInt().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceInt().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceInt().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceInt().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceInt().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceInt().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceInt().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceInt().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("VariancePrice");
                        fail("should have thrown Runtime Exception Null Value");
                    }
                    catch (RuntimeException e)
                    {
                        //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopInt() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceInt().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceInt().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceInt().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceInt().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceInt().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceInt().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceInt().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceInt().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVarianceLong() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceLong().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceLong().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceLong().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceLong().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceLong().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceLong().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceLong().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceLong().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("VariancePrice");
                        fail("should have thrown Runtime Exception Null Value");
                    }
                    catch (RuntimeException e)
                    {
                        //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopLong() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceLong().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceLong().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceLong().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceLong().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceLong().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceLong().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceLong().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceLong().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVarianceShort() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceShort().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceShort().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceShort().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceShort().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceShort().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceShort().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceShort().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceShort().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("VariancePrice");
                        fail("should have thrown Runtime Exception Null Value");
                    }
                    catch (RuntimeException e)
                    {
                        //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopShort() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceShort().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceShort().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceShort().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceShort().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceShort().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceShort().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceShort().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceShort().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVarianceByte() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceByte().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceByte().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceByte().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceByte().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceByte().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceByte().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceByte().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceByte().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("VariancePrice");
                        fail("should have thrown Runtime Exception Null Value");
                    }
                    catch (RuntimeException e)
                    {
                        //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopByte() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceByte().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceByte().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceByte().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceByte().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceByte().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceByte().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceByte().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceByte().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVarianceFloat() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceFloat().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceFloat().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceFloat().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceFloat().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceFloat().varianceSample());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceFloat().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceFloat().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceFloat().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("VariancePrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("VariancePrice");
                        fail("should have thrown Runtime Exception Null Value");
                    }
                    catch (RuntimeException e)
                    {
                        //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopFloat() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceFloat().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceFloat().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceFloat().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceFloat().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceFloat().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceFloat().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceFloat().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceFloat().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(0.6666666666666666, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVarianceBigDecimal() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceBigDecimal().varianceSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("variancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("variancePrice1", SaleAggregateDataFinder.priceBigDecimal().varianceSample());
        aggregateList.addAggregateAttribute("variancePrice2", SaleAggregateDataFinder.priceBigDecimal().varianceSample());
        aggregateList.addAggregateAttribute("variancePrice3", SaleAggregateDataFinder.priceBigDecimal().varianceSample());
        aggregateList.addAggregateAttribute("variancePrice4", SaleAggregateDataFinder.priceBigDecimal().varianceSample());
        aggregateList.addAggregateAttribute("variancePrice5", SaleAggregateDataFinder.priceBigDecimal().varianceSample());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceBigDecimal().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceBigDecimal().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(2.333333333333333, data.getAttributeAsDouble("variancePrice"), DELTA);
                    assertEquals(2.333333333333333, data.getAttributeAsDouble("variancePrice1"), DELTA);
                    assertEquals(2.333333333333333, data.getAttributeAsDouble("variancePrice2"), DELTA);
                    assertEquals(2.333333333333333, data.getAttributeAsDouble("variancePrice3"), DELTA);
                    assertEquals(2.333333333333333, data.getAttributeAsDouble("variancePrice4"), DELTA);
                    assertEquals(2.333333333333333, data.getAttributeAsDouble("variancePrice5"), DELTA);
                    assertEquals(2.45, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("variancePrice"), DELTA);
                    assertEquals(1.12, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 3:
                    try
                        {
                            data.getAttributeAsDouble("VariancePrice");
                            fail("should have thrown Runtime Exception Null Value");
                        }
                        catch (RuntimeException e)
                        {
                            //good
                    }
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testVariancePopBigDecimal() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceBigDecimal().variancePopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("VariancePrice", aggrAttr);
        aggregateList.addAggregateAttribute("VariancePrice1", SaleAggregateDataFinder.priceBigDecimal().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice2", SaleAggregateDataFinder.priceBigDecimal().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice3", SaleAggregateDataFinder.priceBigDecimal().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice4", SaleAggregateDataFinder.priceBigDecimal().variancePopulation());
        aggregateList.addAggregateAttribute("VariancePrice5", SaleAggregateDataFinder.priceBigDecimal().variancePopulation());
        aggregateList.addAggregateAttribute("avg", SaleAggregateDataFinder.priceBigDecimal().avg());
        aggregateList.addAggregateAttribute("savg2", SaleAggregateDataFinder.priceBigDecimal().avg());
        aggregateList.addGroupBy("SellerId", SaleAggregateDataFinder.sellerId());

        assertEquals(3, aggregateList.size());

        for (int i = 0; i < aggregateList.size(); i++)
        {
            AggregateData data = aggregateList.get(i);

            switch (data.getAttributeAsInt("SellerId"))
            {
                case 1:
                    assertEquals(1.5555555555555554, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(1.5555555555555554, data.getAttributeAsDouble("VariancePrice1"), DELTA);
                    assertEquals(1.5555555555555554, data.getAttributeAsDouble("VariancePrice2"), DELTA);
                    assertEquals(1.5555555555555554, data.getAttributeAsDouble("VariancePrice3"), DELTA);
                    assertEquals(1.5555555555555554, data.getAttributeAsDouble("VariancePrice4"), DELTA);
                    assertEquals(1.5555555555555554, data.getAttributeAsDouble("VariancePrice5"), DELTA);
                    assertEquals(2.45, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    assertEquals(1.12, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("VariancePrice"), DELTA);
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }
}
