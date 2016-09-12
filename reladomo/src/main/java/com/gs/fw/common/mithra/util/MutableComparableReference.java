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

import java.io.Serializable;


public class MutableComparableReference<T extends Comparable<? super T>> implements Serializable, Comparable<MutableComparableReference<T>>, Nullable
{
    private T value;
    private boolean initialized = true;

    public MutableComparableReference()
    {
    }

    public MutableComparableReference(T value)
    {
        this.value = value;
        this.initialized = true;
    }

    public Object getAsObject()
    {
        if (isNull()) return null;
        return value;
    }

    public void replace(T value)
    {
        this.value = value;
        this.initialized = true;
    }

    public boolean isNull()
    {
        return (value == null);
    }

    public void checkForNull()
    {
        //do nothing
    }

    public T getValue()
    {
        return value;
    }

    public int compareTo(MutableComparableReference<T> o)
    {
        boolean leftNull = this.value == null;
        boolean rightNull = o.getValue() == null;
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
        if (!(leftNull || rightNull)) result = this.value.compareTo(o.getValue());
        return result; 
    }

    @Override
    public int hashCode()
    {
        if (this.value == null) return 0;
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableComparableReference that = (MutableComparableReference) o;

        return !(value == null ? that.value != null : !value.equals(that.value));
    }

    public boolean isInitialized()
    {
        return initialized;
    }

    public void setValueNull()
    {
        this.initialized = true;
    }
}
