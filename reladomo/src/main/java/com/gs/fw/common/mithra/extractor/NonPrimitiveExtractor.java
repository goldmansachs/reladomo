
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

package com.gs.fw.common.mithra.extractor;

import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.Timestamp;

public abstract class NonPrimitiveExtractor<T,V> implements Extractor<T,V>
{

    public boolean isAttributeNull(T o)
    {
        return this.valueOf(o) == null;
    }

    public void setValueNull(T o)
    {
        this.setValue(o, null);
    }

    public void setValueUntil(T o, V newValue, Timestamp exclusiveUntil)
    {
        throw new UnsupportedOperationException("This method should only be called on objects with as of attributes");
    }

    public void setValueNullUntil(T o, Timestamp exclusiveUntil)
    {
        throw new UnsupportedOperationException("This method should only be called on objects with as of attributes");
    }

    public boolean valueEquals(T first, T second)
    {
        if (first == second)
        {
            return true;
        }
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull)
        {
            return secondNull;
        }
        return this.valueOf(first).equals(this.valueOf(second));
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O,V> secondExtractor)
    {
        Object firstValue = this.valueOf(first);
        Object secondValue = secondExtractor.valueOf(second);
        if (firstValue == secondValue)
        {
            return true;
        }
        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public int valueHashCode(T o)
    {
        Object value = this.valueOf(o);
        if (value == null)
        {
            return HashUtil.NULL_HASH;
        }
        return value.hashCode();
    }
}
