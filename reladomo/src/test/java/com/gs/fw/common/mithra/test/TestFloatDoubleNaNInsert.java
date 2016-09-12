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

package com.gs.fw.common.mithra.test;
import com.gs.fw.common.mithra.test.domain.Book;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.TinyBalance;
import com.gs.fw.common.mithra.test.domain.InfinityTimestamp;
import com.gs.fw.common.mithra.MithraBusinessException;


public class TestFloatDoubleNaNInsert extends MithraTestAbstract
{

    protected Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Book.class,
            Product.class,
            TinyBalance.class
        };
    }

    public void testSetNaNDoubleValue()
    {
        try
        {
            Book book = new Book();
            book.setUnitPrice(Double.NaN);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetNaNFloatValueInDoubleAttribute()
    {
        try
        {
            Book book = new Book();
            book.setUnitPrice(Double.NaN);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetNaNFloatValue()
    {
        try
        {
            Product product = new Product();
            product.setDailyProductionRate(Float.NaN);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetNaNValue()
    {
        try
        {
            TinyBalance balance = new TinyBalance(InfinityTimestamp.getParaInfinity());
            balance.setQuantity(Double.NaN);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }

    }

        public void testSetDoubleInfinityValue()
    {
        try
        {
            Book book = new Book();
            book.setUnitPrice(Double.POSITIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetFloatInfinityValueInDoubleAttribute()
    {
        try
        {
            Book book = new Book();
            book.setUnitPrice(Double.POSITIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetFloatInfinityValue()
    {
        try
        {
            Product product = new Product();
            product.setDailyProductionRate(Float.POSITIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetDoubleInfinityValueInDatedObject()
    {
        try
        {
            TinyBalance balance = new TinyBalance(InfinityTimestamp.getParaInfinity());
            balance.setQuantity(Double.POSITIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }

    }


    public void testSetDoubleNegInfinityValue()
    {
        try
        {
            Book book = new Book();
            book.setUnitPrice(Double.NEGATIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetFloatNegInfinityValueInDoubleAttribute()
    {
        try
        {
            Book book = new Book();
            book.setUnitPrice(Double.NEGATIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetFloatNegInfinityValue()
    {
        try
        {
            Product product = new Product();
            product.setDailyProductionRate(Float.NEGATIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }
    }

    public void testSetDoubleNegInfinityValueInDatedObject()
    {
        try
        {
            TinyBalance balance = new TinyBalance(InfinityTimestamp.getParaInfinity());
            balance.setQuantity(Double.NEGATIVE_INFINITY);
            fail("Should not get here!!");
        }
        catch(MithraBusinessException e)
        {
            getLogger().info("Expected exception: ",e);
        }

    }


}
