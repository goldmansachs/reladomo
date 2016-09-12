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

import java.sql.Timestamp;
import java.util.Date;


public class SimpleAggregateBean
{
    private String sellerName;
    private Integer manufacturerCount;
    private Integer descriptionCount;
    private String descriptionMax;
    private String descriptionMin;
    private Integer productId;
    private Integer totalManufacturers;
    private Integer totalQuantity;
    private String saleDescription;
    private Double averageDiscount;
    private Integer sellerId;
    private Double sampleDiscount;
    private Double maxDiscount;
    private Double minDiscount;
    private Boolean maxSellerActive;
    private Boolean minSellerActive;
    private java.util.Date leastSettleDate;
    private java.util.Date maxSettleDate;
    private Timestamp maxSaleDate;
    private Timestamp minSaleDate;
    private Integer id;
    private Character minChar;
    private Character maxChar;
    private Integer avgQuantity;
    private Double avgPrice;
    private Double totalQuantityDouble;
    private Short testShort;
    private Float testFloat;
    private Byte testByte;

    public String getSellerName()
    {
        return sellerName;
    }

    public void setSellerName(String name)
    {
        this.sellerName = name;
    }

    public Integer getManufacturerCount()
    {
        return manufacturerCount;
    }

    public void setManufacturerCount(Integer manufacturerCount)
    {
        this.manufacturerCount = manufacturerCount;
    }

    public Integer getDescriptionCount()
    {
        return descriptionCount;
    }

    public void setDescriptionCount(Integer descriptionCount)
    {
        this.descriptionCount = descriptionCount;
    }

    public String getDescriptionMax()
    {
        return descriptionMax;
    }

    public void setDescriptionMax(String descriptionMax)
    {
        this.descriptionMax = descriptionMax;
    }

    public String getDescriptionMin()
    {
        return descriptionMin;
    }

    public void setDescriptionMin(String descriptionMin)
    {
        this.descriptionMin = descriptionMin;
    }

    public Integer getProductId()
    {
        return productId;
    }

    public void setProductId(Integer productId)
    {
        this.productId = productId;
    }

    public Integer getTotalManufacturers()
    {
        return totalManufacturers;
    }

    public void setTotalManufacturers(Integer totalManufacturers)
    {
        this.totalManufacturers = totalManufacturers;
    }

    public Integer getTotalQuantity()
    {
        return totalQuantity;
    }

    public void setTotalQuantity(Integer totalQuantity)
    {
        this.totalQuantity = totalQuantity;
    }

    public String getSaleDescription()
    {
        return saleDescription;
    }

    public void setSaleDescription(String saleDescription)
    {
        this.saleDescription = saleDescription;
    }

    public Double getAverageDiscount()
    {
        return averageDiscount;
    }

    public void setAverageDiscount(Double averageDiscount)
    {
        this.averageDiscount = averageDiscount;
    }

    public Integer getSellerId()
    {
        return sellerId;
    }

    public void setSellerId(Integer sellerId)
    {
        this.sellerId = sellerId;
    }

    public Double getSampleDiscount()
    {
        return sampleDiscount;
    }

    public void setSampleDiscount(Double sampleDiscount)
    {
        this.sampleDiscount = sampleDiscount;
    }

    public Double getMaxDiscount()
    {
        return maxDiscount;
    }

    public void setMaxDiscount(Double maxDiscount)
    {
        this.maxDiscount = maxDiscount;
    }

    public Double getMinDiscount()
    {
        return minDiscount;
    }

    public void setMinDiscount(Double minDiscount)
    {
        this.minDiscount = minDiscount;
    }

    public Boolean getMaxSellerActive()
    {
        return maxSellerActive;
    }

    public void setMaxSellerActive(Boolean maxSellerActive)
    {
        this.maxSellerActive = maxSellerActive;
    }

    public Boolean getMinSellerActive()
    {
        return minSellerActive;
    }

    public void setMinSellerActive(Boolean minSellerActive)
    {
        this.minSellerActive = minSellerActive;
    }

    public Date getLeastSettleDate()
    {
        return leastSettleDate;
    }

    public void setLeastSettleDate(Date leastSettleDate)
    {
        this.leastSettleDate = leastSettleDate;
    }

    public Date getMaxSettleDate()
    {
        return maxSettleDate;
    }

    public void setMaxSettleDate(Date maxSettleDate)
    {
        this.maxSettleDate = maxSettleDate;
    }

    public Timestamp getMaxSaleDate()
    {
        return maxSaleDate;
    }

    public void setMaxSaleDate(Timestamp maxSaleDate)
    {
        this.maxSaleDate = maxSaleDate;
    }

    public Timestamp getMinSaleDate()
    {
        return minSaleDate;
    }

    public void setMinSaleDate(Timestamp minSaleDate)
    {
        this.minSaleDate = minSaleDate;
    }

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public Character getMinChar()
    {
        return minChar;
    }

    public void setMinChar(Character minChar)
    {
        this.minChar = minChar;
    }

    public Character getMaxChar()
    {
        return maxChar;
    }

    public void setMaxChar(Character maxChar)
    {
        this.maxChar = maxChar;
    }

    public Integer getAvgQuantity()
    {
        return avgQuantity;
    }

    public void setAvgQuantity(Integer avgQuantity)
    {
        this.avgQuantity = avgQuantity;
    }

    public Double getAvgPrice()
    {
        return avgPrice;
    }

    public void setAvgPrice(Double avgPrice)
    {
        this.avgPrice = avgPrice;
    }

    public Double getTotalQuantityDouble()
    {
        return totalQuantityDouble;
    }

    public void setTotalQuantityDouble(Double totalQuantityDouble)
    {
        this.totalQuantityDouble = totalQuantityDouble;
    }

    public Short getTestShort()
    {
        return testShort;
    }

    public void setTestShort(Short testShort)
    {
        this.testShort = testShort;
    }

    public Float getTestFloat()
    {
        return testFloat;
    }

    public void setTestFloat(Float testFloat)
    {
        this.testFloat = testFloat;
    }

    public Byte getTestByte()
    {
        return testByte;
    }

    public void setTestByte(Byte testByte)
    {
        this.testByte = testByte;
    }
}

