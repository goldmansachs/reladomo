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

package com.gs.fw.common.mithra.util;

import java.math.BigDecimal;



public class MutableBigDecimal extends MutableNumber
{

    private BigDecimal value = BigDecimal.ZERO;
    public MutableBigDecimal()
    {
    }

    public MutableBigDecimal(BigDecimal value)
    {
        this.value = value;
        if(value == null)
        {
            this.setInitializedAndNull();
        }
        else
        {
           this.setInitializedAndNotNull();
        }
    }

    @Override
    public double doubleValue()
    {
        checkForNull();
        return this.value.doubleValue();
    }

    @Override
    public float floatValue()
    {
        checkForNull();
        return this.value.floatValue();
    }

    @Override
    public int intValue()
    {
        checkForNull();
        return this.value.intValue();
    }

    @Override
    public long longValue()
    {
        checkForNull();
        return  this.value.longValue();
    }

    @Override
    public BigDecimal bigDecimalValue()
    {
        checkForNull();
        return this.value;
    }

    public void add(BigDecimal value)
    {
        this.value = this.value.add(value);
        this.setInitializedAndNotNull();
    }

    public void replace(BigDecimal value)
    {
        this.value = value;
        this.setInitializedAndNotNull();
    }

    public int compareTo(MutableBigDecimal other)
    {
        boolean leftNull = this.isNull();
        boolean rightNull = other.isNull();
        int result = 0;
        if (leftNull)
        {
            if (rightNull)
            {
                result = 0;
            }
            else
            {
                result = -1;
            }
        }
        else if (rightNull)
        {
            result = 1;
        }
        if (result == 0) result = this.bigDecimalValue().compareTo(other.bigDecimalValue());
        return result;
    }

    public BigDecimal getBigDecimalValue()
    {
        return value;
    }

    public int bigDecimalCompare(BigDecimal otherBigDecimal)
    {
        if (isNull())
        {
           return -1;
        }
        else
        {
           return value.compareTo(otherBigDecimal);
        }
    }

    @Override
    public int hashCode()
    {
        if (this.isNull()) return 0;
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableBigDecimal that = (MutableBigDecimal) o;

        return this.isNull() ?  that.isNull() : this.value.equals(that.value);
    }
}
