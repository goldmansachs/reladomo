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

import com.gs.fw.common.mithra.AggregateBeanList;
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.NullTest;
import com.gs.fw.common.mithra.test.domain.NullTestFinder;
import com.gs.fw.common.mithra.test.domain.ProductSpecification;
import com.gs.fw.common.mithra.test.domain.ProductSpecificationFinder;
import com.gs.fw.common.mithra.test.domain.Sale;
import com.gs.fw.common.mithra.test.domain.SaleAggregateData;
import com.gs.fw.common.mithra.test.domain.SaleAggregateDataFinder;
import com.gs.fw.common.mithra.test.domain.SaleFinder;
import com.gs.fw.common.mithra.test.domain.SaleList;
import com.gs.fw.common.mithra.test.domain.SalesLineItem;
import com.gs.fw.common.mithra.test.domain.SalesLineItemFinder;
import com.gs.fw.common.mithra.test.domain.Seller;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class TestAggregateBeanList extends MithraTestAbstract
{
    private static final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Sale.class,
                        SaleAggregateData.class,
                        Seller.class,
                        SalesLineItem.class,
                        ProductSpecification.class,
                        NullTest.class
                };
    }

    public void testCanCreateAggregateBeanList()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        assertEquals(4, list.size());
    }

    public void testCanCreateAggregateBeanListStdDevSampleDouble()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        list.addAggregateAttribute("sampleDiscount", SaleAggregateDataFinder.priceInt().standardDeviationSample());
        list.addAggregateAttribute("averageDiscount", SaleAggregateDataFinder.priceDouble().avg());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getAverageDiscount();
        assertEquals(2.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevPopDouble()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        list.addAggregateAttribute("sampleDiscount", SaleAggregateDataFinder.priceDouble().standardDeviationPopulation());
        list.addAggregateAttribute("averageDiscount", SaleAggregateDataFinder.priceDouble().avg());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(0.816496580927726, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getAverageDiscount();
        assertEquals(2.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevSampleFloat()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceFloat().standardDeviationSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceFloat().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevPopFloat()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceFloat().standardDeviationPopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceFloat().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(0.816496580927726, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double count = list.get(0).getManufacturerCount();
        assertEquals(3.0, count);
    }

    public void testCanCreateAggregateBeanListStdDevSampleInt()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceInt().standardDeviationSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceInt().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevPopInt()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceInt().standardDeviationPopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceInt().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(0.816496580927726, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double count = list.get(0).getManufacturerCount();
        assertEquals(3.0, count);
    }

    public void testCanCreateAggregateBeanListStdDevSampleShort()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceShort().standardDeviationSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceShort().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevPopShort()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceShort().standardDeviationPopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceShort().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(0.816496580927726, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double count = list.get(0).getManufacturerCount();
        assertEquals(3.0, count);
    }

    public void testCanCreateAggregateBeanListStdDevSampleByte()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceByte().standardDeviationSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceByte().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevPopByte()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceByte().standardDeviationPopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceByte().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(0.816496580927726, discountStdDev);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double count = list.get(0).getManufacturerCount();
        assertEquals(3.0, count);
    }

    public void testCanCreateAggregateBeanListStdDevSampleBigDecimal()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceBigDecimal().standardDeviationSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceBigDecimal().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.5275252316519465, discountStdDev, 0.0000000000001);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListStdDevPopBigDecimal()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceBigDecimal().standardDeviationPopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceBigDecimal().count());

        assertEquals(3, list.size());
        double discountStdDev = list.get(0).getSampleDiscount();
        assertEquals(1.247219128924647, discountStdDev, 0.0000000000001);
        double discountStdDev2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountStdDev2);
        double discountStdDev3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountStdDev3);
    }

    public void testCanCreateAggregateBeanListVarianceSampleDouble()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        list.addAggregateAttribute("sampleDiscount", SaleAggregateDataFinder.priceInt().varianceSample());
        list.addAggregateAttribute("averageDiscount", SaleAggregateDataFinder.priceDouble().avg());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getAverageDiscount();
        assertEquals(2.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVarianceSampleFloat()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceFloat().varianceSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceFloat().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVarianceSampleInt()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceInt().varianceSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceInt().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVarianceSampleShort()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceShort().varianceSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceShort().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVarianceSampleByte()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceByte().varianceSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceByte().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(1.0, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVarianceSampleBigDecimal()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceBigDecimal().varianceSample();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceBigDecimal().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(2.333333333333333, discountVariance, 0.0000000000001);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVariancePopDouble()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        list.addAggregateAttribute("sampleDiscount", SaleAggregateDataFinder.priceInt().variancePopulation());
        list.addAggregateAttribute("averageDiscount", SaleAggregateDataFinder.priceDouble().avg());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(0.6666666666666666, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getAverageDiscount();
        assertEquals(2.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVariancePopFloat()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceFloat().variancePopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceFloat().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(0.6666666666666666, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVariancePopInt()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceInt().variancePopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceInt().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(0.6666666666666666, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVariancePopShort()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceShort().variancePopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceShort().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(0.6666666666666666, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVariancePopByte()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceByte().variancePopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceByte().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(0.6666666666666666, discountVariance);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testCanCreateAggregateBeanListVariancePopBigDecimal()
    {
        Operation op = SaleAggregateDataFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerId", SaleAggregateDataFinder.sellerId());
        DoubleAggregateAttribute aggregateAttribute = SaleAggregateDataFinder.priceBigDecimal().variancePopulation();
        list.addAggregateAttribute("sampleDiscount", aggregateAttribute);
        list.addAggregateAttribute("manufacturerCount", SaleAggregateDataFinder.priceBigDecimal().count());

        assertEquals(3, list.size());
        double discountVariance = list.get(0).getSampleDiscount();
        assertEquals(1.5555555555555554, discountVariance, 0.0000000000001);
        double discountVariance2 = list.get(1).getSampleDiscount();
        assertEquals(0.0, discountVariance2);
        double discountVariance3 = list.get(0).getManufacturerCount();
        assertEquals(3.0, discountVariance3);
    }

    public void testAggregateBeanListCanHandleNullResultSet()
    {
        Operation op = ProductSpecificationFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
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

    public void testAggregateBeanListCanHandleNullResultSetForInteger()
    {
        Operation op = ProductSpecificationFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("id", ProductSpecificationFinder.manufacturerId());
        list.addAggregateAttribute("manufacturerCount", ProductSpecificationFinder.discountPrice().count());
        list.addOrderBy("id", true);
        assertEquals(3, list.size());
        assertNull(list.get(0).getId());
        assertEquals(1, list.get(1).getId().intValue());
        assertEquals(2, list.get(2).getId().intValue());
        assertEquals(3, list.get(0).getManufacturerCount().intValue());
        assertEquals(2, list.get(1).getManufacturerCount().intValue());
        assertEquals(1, list.get(2).getManufacturerCount().intValue());

    }
    
    public void testAggregateBeanListCanHandleNullResultSetForInteger2()
      {
          try
          {
              Operation op = ProductSpecificationFinder.all();
              AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
              list.addGroupBy("id", ProductSpecificationFinder.manufacturerId());
              list.addAggregateAttribute("manufacturerCount", ProductSpecificationFinder.discountPrice().count());
              list.addOrderBy("id", true);
              assertEquals(3, list.size());
          }
          catch (MithraNullPrimitiveException mnpe)
          {
              fail(mnpe.getMessage());
          }
          catch (MithraBusinessException mbe)
          {
              fail(mbe.getMessage());
          }
      }


      public void testAggregateBeanListCanHandleNullResultSetForDouble()
      {
          try
          {
              Operation op = NullTestFinder.all();
              AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
              list.addGroupBy("avgPrice", NullTestFinder.noDefaultNullDouble());
              list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
              assertEquals(2, list.size());
          }
          catch (MithraNullPrimitiveException mnpe)
          {
              fail(mnpe.getMessage());
          }
          catch (MithraBusinessException mbe)
          {
              fail(mbe.getMessage());
          }
      }


      public void testAggregateBeanListCanHandleNullResultSetForBoolean()
      {
          try
          {
              Operation op = NullTestFinder.all();
              AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
              list.addGroupBy("minSellerActive", NullTestFinder.noDefaultNullBoolean());
              list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
              assertEquals(2, list.size());
          }
          catch (MithraNullPrimitiveException mnpe)
          {
              fail(mnpe.getMessage());
          }
          catch (MithraBusinessException mbe)
          {
              fail(mbe.getMessage());
          }
      }


      public void testAggregateBeanListCanHandleNullResultSetForCharacter()
      {
          try
          {
              Operation op = NullTestFinder.all();
              AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
              list.addGroupBy("minChar", NullTestFinder.noDefaultNullChar());
              list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
              assertEquals(2, list.size());
          }
          catch (MithraNullPrimitiveException mnpe)
          {
              fail(mnpe.getMessage());
          }
          catch (MithraBusinessException mbe)
          {
              fail(mbe.getMessage());
          }
      }

      public void testAggregateBeanListCanHandleNullResultSetForShort()
      {
          try
          {
              Operation op = NullTestFinder.all();
              AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
              list.addGroupBy("testShort", NullTestFinder.noDefaultNullShort());
              list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
              assertEquals(2, list.size());
          }
          catch (MithraNullPrimitiveException mnpe)
          {
             fail(mnpe.getMessage());
          }
          catch (MithraBusinessException mbe)
          {
              fail(mbe.getMessage());
          }
      }

       public void testAggregateBeanListCanHandleNullByteResultSet()
    {
        try
        {
            Operation op = NullTestFinder.all();
            AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
            list.addGroupBy("testByte", NullTestFinder.noDefaultNullByte());
            list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
            assertEquals(2, list.size());
        }
        catch (MithraNullPrimitiveException mnpe)
        {
           fail(mnpe.getMessage());
        }
        catch (MithraBusinessException mbe)
        {
            fail(mbe.getMessage());
        }
    }



      public void testAggregateBeanListCanHandleNullResultSetForFloat()
      {
          try
          {
              Operation op = NullTestFinder.all();
              AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
              list.addGroupBy("testFloat", NullTestFinder.noDefaultNullFloat());
              list.addAggregateAttribute("manufacturerCount", NullTestFinder.notNullString().count());
              assertEquals(2, list.size());
          }
          catch (MithraNullPrimitiveException mnpe)
          {
             fail(mnpe.getMessage());
          }
          catch (MithraBusinessException mbe)
          {
              fail(mbe.getMessage());
          }
      }
    

    public void testAggregateBeanListCountWithoutGroupByForEmptyTable()
    {
        SaleFinder.findMany(SaleFinder.all()).deleteAll();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SaleFinder.all(), SimpleAggregateBean.class);
        list.addAggregateAttribute("descriptionCount", SaleFinder.description().count());
        list.addAggregateAttribute("descriptionMax", SaleFinder.description().max());
        list.addAggregateAttribute("descriptionMin", SaleFinder.description().max());

        assertEquals(1, list.size());
        assertEquals(0, list.get(0).getDescriptionCount().intValue());
        assertNull(list.get(0).getDescriptionMax());
        assertNull(list.get(0).getDescriptionMin());
    }

    public void testAggregateBeanListAddingAttributeWithDifferentTopLevelPortal()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr1 = SalesLineItemFinder.quantity().sum();
        AggregateBeanList<SimpleAggregateBean> aggregateList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        try
        {
            aggregateList.addAggregateAttribute("totalQuantity", aggrAttr1);
            aggregateList.addGroupBy("id", SaleFinder.sellerId());
            fail();
        }
        catch (MithraBusinessException mbe)
        {
        }
    }

    public void testAggregateBeanListAddingGroupByAttributeWithDifferentTopLevelPortal()
    {
        Operation op = SalesLineItemFinder.all();
        MithraAggregateAttribute aggrAttr1 = SalesLineItemFinder.quantity().sum();
        AggregateBeanList<SimpleAggregateBean> aggregateList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        try
        {
            aggregateList.addAggregateAttribute("totalQuantity", aggrAttr1);
            aggregateList.addGroupBy("id", SaleFinder.sellerId());
            fail();
        }
        catch (MithraBusinessException mbe)
        {
        }
    }

    public void testAggregateBeanListAddingTwoAggregateAttributesWithTheSameName()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr1 = SaleFinder.discountPercentage().sum();
        MithraAggregateAttribute aggrAttr2 = SaleFinder.discountPercentage().sum();
        AggregateBeanList<SimpleAggregateBean> aggregateList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        try
        {
            aggregateList.addAggregateAttribute("attr", aggrAttr1);
            aggregateList.addAggregateAttribute("attr", aggrAttr2);
            aggregateList.addGroupBy("id", SaleFinder.sellerId());
            fail();
        }
        catch (MithraBusinessException mbe)
        {
        }

    }

    public void testAggregateBeanListAddingTwoAttributeWithTheSameName()
    {
        Operation op = SaleFinder.sellerId().eq(4);
        MithraAggregateAttribute aggrAttr = SaleFinder.discountPercentage().sum();
        AggregateBeanList<SimpleAggregateBean> aggregateList = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        try
        {
            aggregateList.addAggregateAttribute("attr", aggrAttr);
            aggregateList.addGroupBy("id", SaleFinder.sellerId());
            fail();
        }
        catch (MithraBusinessException mbe)
        {
        }

    }

    public void testAggregateBeanListGroupByDivdedBy()
    {
         AggregateBeanList<SimpleAggregateBean>  list = new  AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("totalQuantity", SalesLineItemFinder.quantity().dividedBy(10));

        assertEquals(2, list.size());
        assertEquals(2, list.get(0).getTotalQuantity().intValue());
        boolean haveOne = list.get(0).getId() == 1;
        boolean haveTwo = list.get(0).getId() == 2;


        assertEquals(2, list.get(1).getTotalQuantity().intValue());
        haveOne = haveOne || list.get(1).getId() == 1;
        haveTwo = haveTwo || list.get(1).getId() == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testAggregateBeanListGroupByTimes()
    {
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("totalQuantity", SalesLineItemFinder.quantity().times(2));

        assertEquals(2, list.size());

        assertEquals(40, list.get(0).getTotalQuantity().intValue());
        boolean haveOne = list.get(0).getId() == 1;
        boolean haveTwo = list.get(0).getId() == 2;


        assertEquals(40, list.get(1).getTotalQuantity().intValue());
        haveOne = haveOne || list.get(1).getId() == 1;
        haveTwo = haveTwo || list.get(1).getId() == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testAggregateBeanListGroupByPlus()
    {
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("totalQuantity", SalesLineItemFinder.quantity().plus(5));

        assertEquals(2, list.size());

        assertEquals(25, list.get(0).getTotalQuantity().intValue());
        boolean haveOne = list.get(0).getId() == 1;
        boolean haveTwo = list.get(0).getId() == 2;


        assertEquals(25, list.get(1).getTotalQuantity().intValue());
        haveOne = haveOne || list.get(1).getId() == 1;
        haveTwo = haveTwo || list.get(1).getId() == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testAggregateBeanListSumWithLargeInClause()
    {
        IntHashSet set = new IntHashSet();
        set.add(1);
        for (int i = 1000; i < 2000; i++) set.add(i);
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2).and(SalesLineItemFinder.manufacturerId().in(set)), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());

        assertEquals(1, list.size());

        assertEquals(40, list.get(0).getTotalQuantity().intValue());
        assertEquals(1, list.get(0).getId().intValue());
    }

    public void testAggregateBeanListGroupByDivdedByIntDouble()
    {
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("totalQuantityDouble", SalesLineItemFinder.quantity().dividedBy(10.0));

        assertEquals(2, list.size());

        assertEquals(2.0, list.get(0).getTotalQuantityDouble());
        boolean haveOne = list.get(0).getId() == 1;
        boolean haveTwo = list.get(0).getId() == 2;


        assertEquals(2.0, list.get(0).getTotalQuantityDouble(), 0.0);
        haveOne = haveOne || list.get(1).getId() == 1;
        haveTwo = haveTwo || list.get(1).getId() == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testAggregateBeanListGroupByPlusIntDouble()
    {
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("totalQuantityDouble", SalesLineItemFinder.quantity().plus(5.0));

        assertEquals(2, list.size());

        assertEquals(25.0, list.get(0).getTotalQuantityDouble(), 0.0);
        boolean haveOne = list.get(0).getId() == 1;
        boolean haveTwo = list.get(0).getId() == 2;


        assertEquals(25.0, list.get(1).getTotalQuantityDouble(), 0.0);
        haveOne = haveOne || list.get(1).getId() == 1;
        haveTwo = haveTwo || list.get(1).getId() == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testAggregateBeanListGroupByTimesIntDouble()
    {
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.saleId().eq(2), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addGroupBy("totalQuantityDouble", SalesLineItemFinder.quantity().times(2.0));

        assertEquals(2, list.size());
        assertEquals(40.0, list.get(0).getTotalQuantityDouble(), 0.0);
        boolean haveOne = list.get(0).getId() == 1;
        boolean haveTwo = list.get(0).getId() == 2;

        assertEquals(40.0, list.get(1).getTotalQuantityDouble(), 0.0);
        haveOne = haveOne || list.get(1).getId() == 1;
        haveTwo = haveTwo || list.get(1).getId() == 2;

        assertTrue(haveOne);
        assertTrue(haveTwo);
    }

    public void testAggregateBeanListCountOnRelated()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("id", SaleFinder.sellerId());
        list.addGroupBy("productId", SaleFinder.items().manufacturerId());
        list.addAggregateAttribute("manufacturerCount", SaleFinder.items().manufacturerId().count());
        assertEquals(6, list.size());
    }

    public void testAggregateBeanListCountOnRelatedAfterCaching()
    {
        SaleList many = SaleFinder.findMany(SaleFinder.all());
        many.deepFetch(SaleFinder.items());
        many.forceResolve();
        testAggregateBeanListCountOnRelated();
    }

    public void testAggregateBeanListWithLargeIn()
    {
        IntHashSet set = new IntHashSet();
        for (int i = 0; i < 2000; i++)
        {
            set.add(i);
        }
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(SalesLineItemFinder.itemId().in(set), SimpleAggregateBean.class);
        list.addGroupBy("id", SalesLineItemFinder.manufacturerId());
        list.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().sum());

        assertEquals(2, list.size());

    }


    public void testCanDoAggregateAndGroupByOperationsOnString()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("sellerName", SaleFinder.seller().name());
        list.addAggregateAttribute("descriptionCount", SaleFinder.description().count());
        list.addAggregateAttribute("descriptionMax", SaleFinder.description().max());
        list.addAggregateAttribute("descriptionMin", SaleFinder.description().min());

        assertEquals(4, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            SimpleAggregateBean currentBean = list.get(i);
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

    public void testCanDoAggregateAndGroupByOperationsOnInteger()
    {
        Operation op = SalesLineItemFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("productId", SalesLineItemFinder.productId());
        list.addAggregateAttribute("totalManufacturers", SalesLineItemFinder.manufacturerId().count());
        list.addAggregateAttribute("totalQuantity", SalesLineItemFinder.quantity().count());

        assertEquals(6, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            SimpleAggregateBean currentBean = list.get(i);
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


    public void testCanDoAggregateAndGroupByOperationsOnDouble()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("averageDiscount", SaleFinder.discountPercentage().avg());
        list.addAggregateAttribute("maxDiscount", SaleFinder.discountPercentage().max());
        list.addAggregateAttribute("minDiscount", SaleFinder.discountPercentage().min());
        assertEquals(6, list.size());
    }

    public void testCanDoAggregateAndGroupByOperationsOnBoolean()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSellerActive", SaleFinder.seller().active().max());
        list.addAggregateAttribute("minSellerActive", SaleFinder.seller().active().min());
        assertEquals(6, list.size());
    }


    public void testCanDoAggregateAndGroupByOperationsOnDate() throws ParseException
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSettleDate", SaleFinder.settleDate().max());
        list.addAggregateAttribute("leastSettleDate", SaleFinder.settleDate().min());
        assertEquals(6, list.size());
        for (int i = 0; i < list.size(); i++)
        {
            SimpleAggregateBean currentBean = list.get(i);
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

    public void testCanDoAggregateAndGroupByOperationsOnTimestamp()
    {
        Operation op = SaleFinder.all();
        AggregateBeanList<SimpleAggregateBean> list = new AggregateBeanList<SimpleAggregateBean>(op, SimpleAggregateBean.class);
        list.addGroupBy("productId", SaleFinder.items().productId());
        list.addAggregateAttribute("maxSaleDate", SaleFinder.saleDate().max());
        list.addAggregateAttribute("minSaleDate", SaleFinder.saleDate().min());
        assertEquals(6, list.size());
        try
        {
            for (int i = 0; i < list.size(); i++)
            {
                SimpleAggregateBean currentBean = list.get(i);
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
