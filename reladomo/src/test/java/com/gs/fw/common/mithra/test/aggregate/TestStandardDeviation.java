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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.SaleAggregateData;
import com.gs.fw.common.mithra.test.domain.SaleAggregateDataFinder;


public class TestStandardDeviation extends MithraTestAbstract
{

    public double DELTA = 0.000000001;

    public TestStandardDeviation()
    {
    }

    public TestStandardDeviation(double delta)
    {
        this.DELTA = delta;
    }

    public void testStdDevDouble() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceDouble().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceDouble().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceDouble().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceDouble().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceDouble().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceDouble().standardDeviationSample());
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
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopDouble() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceDouble().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceDouble().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceDouble().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceDouble().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceDouble().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceDouble().standardDeviationPopulation());
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
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testStdDevInt() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceInt().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceInt().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceInt().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceInt().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceInt().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceInt().standardDeviationSample());
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
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopInt() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceInt().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceInt().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceInt().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceInt().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceInt().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceInt().standardDeviationPopulation());
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
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testStdDevLong() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceLong().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceLong().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceLong().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceLong().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceLong().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceLong().standardDeviationSample());
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
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopLong() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceLong().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceLong().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceLong().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceLong().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceLong().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceLong().standardDeviationPopulation());
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
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testStdDevShort() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceShort().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceShort().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceShort().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceShort().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceShort().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceShort().standardDeviationSample());
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
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopShort() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceShort().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceShort().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceShort().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceShort().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceShort().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceShort().standardDeviationPopulation());
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
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testStdDevByte() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceByte().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceByte().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceByte().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceByte().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceByte().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceByte().standardDeviationSample());
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
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopByte() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceByte().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceByte().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceByte().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceByte().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceByte().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceByte().standardDeviationPopulation());
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
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testStdDevFloat() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceFloat().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceFloat().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceFloat().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceFloat().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceFloat().standardDeviationSample());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceFloat().standardDeviationSample());
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
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice1"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice2"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice3"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice4"));
                    assertEquals(1.0, data.getAttributeAsDouble("StdDevPrice5"));
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    try
                    {
                        data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopFloat() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceFloat().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceFloat().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceFloat().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceFloat().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceFloat().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceFloat().standardDeviationPopulation());
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
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(0.816496580927726, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.0, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    assertEquals(1.0, data.getAttributeAsDouble("savg2"));
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"));
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }

    public void testStdDevBigDecimal() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("stdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("stdDevPrice1", SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample());
        aggregateList.addAggregateAttribute("stdDevPrice2", SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample());
        aggregateList.addAggregateAttribute("stdDevPrice3", SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample());
        aggregateList.addAggregateAttribute("stdDevPrice4", SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample());
        aggregateList.addAggregateAttribute("stdDevPrice5", SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample());
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
                    assertEquals(1.5275252316519468, data.getAttributeAsDouble("stdDevPrice"), DELTA);
                    assertEquals(1.5275252316519468, data.getAttributeAsDouble("stdDevPrice1"), DELTA);
                    assertEquals(1.5275252316519468, data.getAttributeAsDouble("stdDevPrice2"), DELTA);
                    assertEquals(1.5275252316519468, data.getAttributeAsDouble("stdDevPrice3"), DELTA);
                    assertEquals(1.5275252316519468, data.getAttributeAsDouble("stdDevPrice4"), DELTA);
                    assertEquals(1.5275252316519468, data.getAttributeAsDouble("stdDevPrice5"), DELTA);
                    assertEquals(2.45, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("stdDevPrice"), DELTA);
                    assertEquals(1.12, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 3:
                    try
                        {
                            data.getAttributeAsDouble("StdDevPrice");
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

    public void testStdDevPopBigDecimal() throws ParseException
    {
        Operation op = SaleAggregateDataFinder.all();
        MithraAggregateAttribute aggrAttr = SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation();

        AggregateList aggregateList = new AggregateList(op);
        aggregateList.addAggregateAttribute("StdDevPrice", aggrAttr);
        aggregateList.addAggregateAttribute("StdDevPrice1", SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice2", SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice3", SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice4", SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation());
        aggregateList.addAggregateAttribute("StdDevPrice5", SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation());
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
                    assertEquals(1.247219128924647, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(1.247219128924647, data.getAttributeAsDouble("StdDevPrice1"), DELTA);
                    assertEquals(1.247219128924647, data.getAttributeAsDouble("StdDevPrice2"), DELTA);
                    assertEquals(1.247219128924647, data.getAttributeAsDouble("StdDevPrice3"), DELTA);
                    assertEquals(1.247219128924647, data.getAttributeAsDouble("StdDevPrice4"), DELTA);
                    assertEquals(1.247219128924647, data.getAttributeAsDouble("StdDevPrice5"), DELTA);
                    assertEquals(2.45, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 2:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    assertEquals(1.12, data.getAttributeAsDouble("savg2"), DELTA);
                    break;
                case 3:
                    assertEquals(0.0, data.getAttributeAsDouble("StdDevPrice"), DELTA);
                    break;
                default:
                    fail("Invalid id");
            }
        }
    }
}
