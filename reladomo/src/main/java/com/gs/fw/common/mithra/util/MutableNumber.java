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


public class MutableNumber extends Number implements Nullable, Comparable<MutableNumber>
{
    private boolean nullFlag = true;
    private boolean initialized = false;

    public Object getAsObject()
    {
        if (isNull()) return null;
        return Integer.valueOf(0);
    }

    public void setValueNull()
    {
        if(nullFlag)
           this.nullFlag = true;
        this.initialized = true;
    }

    public int intValue()
    {
        checkForNull();
        return 0;
    }

    public long longValue()
    {
        checkForNull();
        return 0;
    }

    public float floatValue()
    {
        checkForNull();
        return 0;
    }

    public double doubleValue()
    {
        checkForNull();
        return 0;
    }

    public BigDecimal bigDecimalValue()
    {
        return BigDecimal.ZERO;
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

    protected void setInitializedAndNull()
    {
        this.nullFlag = true;
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

    public void checkForNull()
    {
        if(nullFlag)
        {
            throw new RuntimeException("Null Value");
        }
    }

    public int compareTo(MutableNumber o)
    {
        if(nullFlag)
        {
            return -1;
        }
        return this.compareValues(o);
    }

    public int compareValues(MutableNumber o)
    {
        return 0;
    }
}
