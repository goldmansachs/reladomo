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


public class MutableIntAverage extends MutableNumber implements Nullable
{

    private long mutableInt;
    private int mutableCount;

    public MutableIntAverage()
    {

    }

    public MutableIntAverage(int mutableInt)
    {
        this.mutableInt = mutableInt;
        this.mutableCount = 1;
        this.setInitializedAndNotNull();
    }

    @Override
    public Object getAsObject()
    {
        if (isNull()) return null;
        return Integer.valueOf((int)(mutableInt/mutableCount));
    }

    @Override
    public double doubleValue()
    {
        checkForNull();
        return (double) (this.mutableInt / this.mutableCount);
    }

    @Override
    public float floatValue()
    {
        checkForNull();
        return (float) (this.mutableInt / this.mutableCount);
    }

    @Override
    public int intValue()
    {
        checkForNull();
        return (int) this.mutableInt / this.mutableCount;
    }

    @Override
    public long longValue()
    {
        checkForNull();
        return (this.mutableInt / this.mutableCount);
    }

    public void add(long value)
    {
        this.mutableInt += value;
        this.mutableCount++;
        this.setInitializedAndNotNull();
    }

    public int compareValues(MutableNumber other)
    {
        return (int) (this.mutableInt / this.mutableCount) - other.intValue();
    }

    @Override
    public int hashCode()
    {
        if (this.isNull()) return 0;
        return HashUtil.hash(this.intValue());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableIntAverage that = (MutableIntAverage) o;

        return this.isNull() ?  that.isNull() : this.intValue() == that.intValue();
    }
}
