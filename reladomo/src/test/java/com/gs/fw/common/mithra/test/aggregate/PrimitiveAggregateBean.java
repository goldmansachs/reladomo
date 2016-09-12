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



public class PrimitiveAggregateBean
{
    private String sellerName;
    private int manufacturerCount;
    private int descriptionCount;
    private String descriptionMax;
    private String descriptionMin;
    private int productId;
    private int totalManufacturers;
    private int totalQuantity;
    private String saleDescription;
    private double averageDiscount;
    private double maxDiscount;
    private double minDiscount;
    private boolean maxSellerActive;
    private boolean minSellerActive;
    private java.util.Date leastSettleDate;
    private java.util.Date maxSettleDate;
    private Timestamp maxSaleDate;
    private Timestamp minSaleDate;
    private int id;
    private char minChar;
    private char maxChar;
    private int avgQuantity;
    private double avgPrice;
    private double totalQuantityDouble;
    private short testShort;
    private float testFloat;
    private byte testByte;

    public String getSellerName()
    {
        return sellerName;
    }

    public void setSellerName(String sellerName)
    {
        this.sellerName = sellerName;
    }

    public int getManufacturerCount()
    {
        return manufacturerCount;
    }

    public void setManufacturerCount(int manufacturerCount)
    {
        this.manufacturerCount = manufacturerCount;
    }

    public int getDescriptionCount()
    {
        return descriptionCount;
    }

    public void setDescriptionCount(int descriptionCount)
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

    public int getProductId()
    {
        return productId;
    }

    public void setProductId(int productId)
    {
        this.productId = productId;
    }

    public int getTotalManufacturers()
    {
        return totalManufacturers;
    }

    public void setTotalManufacturers(int totalManufacturers)
    {
        this.totalManufacturers = totalManufacturers;
    }

    public int getTotalQuantity()
    {
        return totalQuantity;
    }

    public void setTotalQuantity(int totalQuantity)
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

    public double getAverageDiscount()
    {
        return averageDiscount;
    }

    public void setAverageDiscount(double averageDiscount)
    {
        this.averageDiscount = averageDiscount;
    }

    public double getMaxDiscount()
    {
        return maxDiscount;
    }

    public void setMaxDiscount(double maxDiscount)
    {
        this.maxDiscount = maxDiscount;
    }

    public double getMinDiscount()
    {
        return minDiscount;
    }

    public void setMinDiscount(double minDiscount)
    {
        this.minDiscount = minDiscount;
    }

    public boolean isMaxSellerActive()
    {
        return maxSellerActive;
    }

    public void setMaxSellerActive(boolean maxSellerActive)
    {
        this.maxSellerActive = maxSellerActive;
    }

    public boolean isMinSellerActive()
    {
        return minSellerActive;
    }

    public void setMinSellerActive(boolean minSellerActive)
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

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public char getMinChar()
    {
        return minChar;
    }

    public void setMinChar(char minChar)
    {
        this.minChar = minChar;
    }

    public char getMaxChar()
    {
        return maxChar;
    }

    public void setMaxChar(char maxChar)
    {
        this.maxChar = maxChar;
    }

    public int getAvgQuantity()
    {
        return avgQuantity;
    }

    public void setAvgQuantity(int avgQuantity)
    {
        this.avgQuantity = avgQuantity;
    }

    public double getAvgPrice()
    {
        return avgPrice;
    }

    public void setAvgPrice(double avgPrice)
    {
        this.avgPrice = avgPrice;
    }

    public double getTotalQuantityDouble()
    {
        return totalQuantityDouble;
    }

    public void setTotalQuantityDouble(double totalQuantityDouble)
    {
        this.totalQuantityDouble = totalQuantityDouble;
    }

    public short getTestShort()
    {
        return testShort;
    }

    public void setTestShort(short testShort)
    {
        this.testShort = testShort;
    }

    public float getTestFloat()
    {
        return testFloat;
    }

    public void setTestFloat(float testFloat)
    {
        this.testFloat = testFloat;
    }

    public byte getTestByte()
    {
        return testByte;
    }

    public void setTestByte(byte testByte)
    {
        this.testByte = testByte;
    }
}
