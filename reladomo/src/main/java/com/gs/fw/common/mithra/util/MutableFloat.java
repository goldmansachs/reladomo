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


public class MutableFloat extends MutableNumber implements Nullable
{

    private float value;

    public MutableFloat()
    {
    }

    public MutableFloat(float value)
    {
        this.value = value;
        this.setInitializedAndNotNull();
    }

    @Override
    public Object getAsObject()
    {
        if (isNull()) return null;
        return Float.valueOf(value);
    }

    @Override
    public double doubleValue()
    {
        checkForNull();
        return (double)this.value;
    }

    @Override
    public float floatValue()
    {
        checkForNull();
        return this.value;
    }

    @Override
    public int intValue()
    {
        checkForNull();
        return (int)this.value;
    }

    @Override
    public long longValue()
    {
        checkForNull();
        return (long) this.value;
    }

    public void add(float value)
    {
        this.value += value;
        this.setInitializedAndNotNull();
    }

    public void replace(float value)
    {
        this.value = value;
        this.setInitializedAndNotNull();
    }

    public int compareValues(MutableNumber other)
    {
        return floatCompare(other.floatValue());
    }

    public int floatCompare(float otherFloat)
    {
        if (isNull() || value < otherFloat)
            return -1;
        if (value > otherFloat)
            return 1;
        return 0;
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

        MutableFloat that = (MutableFloat) o;

        return this.isNull() ?  that.isNull() : this.value == that.value;
    }
}

