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


public final class MutableBoolean implements java.io.Serializable,
                                      Comparable<MutableBoolean>, Nullable
{
    private boolean value;
    private boolean nullFlag = true;
    private boolean initialized = false;

    public MutableBoolean()
    {
    }

    public MutableBoolean(boolean value)
    {
        this.value = value;
        setInitializedAndNotNull();
    }

    public Object getAsObject()
    {
        if (isNull()) return null;
        return Boolean.valueOf(value);
    }

    public boolean booleanValue()
    {
       checkForNull();
       return value;
    }

    public boolean getValue()
    {
        checkForNull();
        return value;
    }

    public int compareTo(MutableBoolean b)
    {
        return booleanCompare(b.value);
    }

    public int booleanCompare(boolean b)
    {
        if(nullFlag)
        {
            return -1;
        }
        else
        {
            return (b == value ? 0 : (value ? 1 : -1));
        }
    }

    public void replace(boolean value)
    {
        this.value = value;
        this.setInitializedAndNotNull();
    }

    public void checkForNull()
    {
        if(nullFlag)
        {
            throw new RuntimeException("Null Value");
        }
    }

    public boolean isNull()
    {
        return nullFlag;
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    protected void setInitializedAndNotNull()
    {
        this.nullFlag = false;
        this.initialized = true;
    }
    protected void setNullFlag(boolean nullFlag)
    {
        this.nullFlag = nullFlag;
    }

    protected void setInitialized(boolean initialized)
    {
        this.initialized = initialized;
    }

    public void setValueNull()
    {
        if(nullFlag)
            this.nullFlag = true;
        this.initialized = true;
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

        MutableBoolean that = (MutableBoolean) o;

        return this.isNull() ?  that.isNull() : this.value == that.value;
    }
}
