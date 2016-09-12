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



public class MutableBigDecimalAvg extends MutableBigDecimal implements Nullable
{

    private int mutableCount;

    public MutableBigDecimalAvg()
    {

    }

    public MutableBigDecimalAvg(BigDecimal value)
    {
        super(value);
        this.mutableCount = 1;
        this.setInitializedAndNotNull();
    }

    private BigDecimal calculateAverage()
    {
        BigDecimal divisor = new BigDecimal(this.mutableCount);
        return BigDecimalUtil.divide(this.getBigDecimalValue().scale(), this.getBigDecimalValue(), divisor);
    }

    @Override
    public double doubleValue()
    {
        checkForNull();
        return calculateAverage().doubleValue();
    }

    @Override
    public float floatValue()
    {
        checkForNull();
        return calculateAverage().floatValue();
    }

    @Override
    public int intValue()
    {
        checkForNull();
        return calculateAverage().intValue();
    }

    @Override
    public long longValue()
    {
        checkForNull();
        return calculateAverage().longValue();
    }

    @Override
    public BigDecimal bigDecimalValue()
    {
        checkForNull();
        return calculateAverage();
    }


    @Override
    public void add(BigDecimal value)
    {
        super.add(value);
        this.mutableCount++;
    }

    @Override
    public int compareTo(MutableBigDecimal o)
    {
        boolean leftNull = this.isNull();
        boolean rightNull = o.isNull();
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
        if (result == 0) result = this.bigDecimalValue().compareTo(o.bigDecimalValue());
        return result;
    }

    @Override
    public int hashCode()
    {
        if (this.isNull()) return 0;
        return HashUtil.hash(this.doubleValue());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableBigDecimalAvg that = (MutableBigDecimalAvg) o;

        return this.isNull() ?  that.isNull() : this.bigDecimalValue().equals(that.bigDecimalValue());
    }
}
