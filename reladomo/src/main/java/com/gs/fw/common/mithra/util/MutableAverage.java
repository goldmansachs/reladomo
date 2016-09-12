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


public class MutableAverage extends MutableNumber
implements Nullable
{

    private double mutableDouble;
    private int mutableCount;

    public MutableAverage()
    {
        
    }

    public MutableAverage(double mutableDouble)
    {
        this.mutableDouble = mutableDouble;
        this.mutableCount = 1;
        this.setInitializedAndNotNull();
    }

    @Override
    public Object getAsObject()
    {
        if (isNull()) return null;
        return Double.valueOf(mutableDouble/mutableCount);
    }

    @Override
    public double doubleValue()
    {
        checkForNull();
        return this.mutableDouble/this.mutableCount;
    }

    @Override
    public float floatValue()
    {
        checkForNull();
        return (float) (this.mutableDouble/this.mutableCount);
    }

    @Override
    public int intValue()
    {
        checkForNull();
        return (int) (this.mutableDouble/this.mutableCount);
    }

    @Override
    public long longValue()
    {
        checkForNull();
        return (long) ( this.mutableDouble/this.mutableCount);
    }

    public void add(double value)
    {
        this.mutableDouble += value;
        this.mutableCount++;
        this.setInitializedAndNotNull();
    }

    @Override
    public int compareValues(MutableNumber o)
    {
        return Double.compare((this.mutableDouble / this.mutableCount), o.doubleValue());
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

        MutableAverage that = (MutableAverage) o;

        return this.isNull() ?  that.isNull() : this.doubleValue() == that.doubleValue();
    }
}
