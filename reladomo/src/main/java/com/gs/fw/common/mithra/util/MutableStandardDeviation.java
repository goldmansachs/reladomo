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


public class MutableStandardDeviation extends MutableNumber
        implements Nullable
{
    private int count = 0;
    private double mean = 0.0;
    private double m2 = 0.0;
    private double m2Compensation = 0.0;
    private double meanCompensation = 0.0;

    public MutableStandardDeviation()
    {
    }

    public MutableStandardDeviation(double mutableDouble)
    {
        this.count = 1;
        this.mean = mutableDouble;
        this.m2 = mutableDouble * mutableDouble;
        this.setInitializedAndNotNull();
    }

    @Override
    public Object getAsObject()
    {
        if (isNull())
        {
            return null;
        }
        return stddev();
    }

    @Override
    public double doubleValue()
    {
        return stddevWithNullCheck();
    }

    private double stddevWithNullCheck()
    {
        checkForNull();
        return stddev();
    }

    @Override
    public float floatValue()
    {
        return (float) stddevWithNullCheck();
    }

    private double stddev()
    {
        return Math.sqrt(m2 / (count - 1));
    }

    @Override
    public int intValue()
    {
        return (int) stddevWithNullCheck();
    }

    @Override
    public long longValue()
    {
        return (long) stddevWithNullCheck();
    }

    public void add(double value)
    {
        this.count++;
        this.adjustValue(value);
        this.setInitializedAndNotNull();
    }

    private void adjustValue(double value)
    {
        double delta = value - this.mean;
        this.mean = this.addToMean(this.mean, (delta / this.count));
        this.m2 = this.addToM2(this.m2, delta * (value - mean));
    }

    private double addToM2(double sumDouble, double newValue)
    {
        double adjustedValue = newValue - this.m2Compensation;
        double nextSum = sumDouble + adjustedValue;
        this.m2Compensation = (nextSum - sumDouble) - adjustedValue;
        return nextSum;
    }

    private double addToMean(double sumDouble, double newValue)
    {
        double adjustedValue = newValue - this.meanCompensation;
        double nextSum = sumDouble + adjustedValue;
        this.meanCompensation = (nextSum - sumDouble) - adjustedValue;
        return nextSum;
    }

    public int compareValues(MutableNumber other)
    {
        return Double.compare(this.stddev(), other.doubleValue());
    }

    @Override
    public int hashCode()
    {
        if (this.isNull())
        {
            return 0;
        }
        return HashUtil.hash(this.doubleValue());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        MutableStandardDeviation that = (MutableStandardDeviation) o;

        return this.isNull() ? that.isNull() : this.doubleValue() == that.doubleValue();
    }

    @Override
    public void checkForNull()
    {
        if (this.isNull() || this.count == 0 || this.count == 1)
        {
            throw new RuntimeException("Null Value");
        }
    }
}
