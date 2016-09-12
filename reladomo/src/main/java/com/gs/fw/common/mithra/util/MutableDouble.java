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



public class MutableDouble extends MutableNumber
{
    private double value;

    public MutableDouble()
    {
    }

    public MutableDouble(double value)
    {
        this.value = value;
        this.setInitializedAndNotNull();
    }

    @Override
    public Object getAsObject()
    {
        if (isNull()) return null;
        return Double.valueOf(value);
    }

    @Override
    public double doubleValue()
    {
        checkForNull();
        return this.value;
    }

    @Override
    public float floatValue()
    {
        checkForNull();
        return (float) this.value;
    }

    @Override
    public int intValue()
    {
        checkForNull();
        return (int) this.value;
    }

    @Override
    public long longValue()
    {
        checkForNull();
        return (long) this.value;
    }

    public void add(double value)
    {
        this.value += value;
        this.setInitializedAndNotNull();
    }

    public void replace(double value)
    {
        this.value = value;
        this.setInitializedAndNotNull();
    }

    public int compareValues(MutableNumber other)
    {
        return doubleCompare(other.doubleValue());
    }

    public int doubleCompare(double otherDouble)
    {
        if (isNull() || value < otherDouble)
            return -1;
        if (value > otherDouble)
            return 1;

        long thisBits = Double.doubleToLongBits(value);
        long anotherBits = Double.doubleToLongBits(otherDouble);

        return (thisBits == anotherBits ?  0 :
                (thisBits < anotherBits ? -1 :
                 1));
    }

    @Override
    public int hashCode()
    {
        if (this.isNull()) return 0;
        return HashUtil.hash(this.value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableDouble that = (MutableDouble) o;

        return this.isNull() ?  that.isNull() : this.value == that.value;
    }
}
