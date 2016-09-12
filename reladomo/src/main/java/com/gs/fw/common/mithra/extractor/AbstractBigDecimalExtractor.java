
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
import java.math.BigDecimal;

public abstract class AbstractBigDecimalExtractor<T> implements BigDecimalExtractor<T, BigDecimal>
{
    public BigDecimal valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return this.bigDecimalValueOf(o);
    }

    public void setValue(T o, BigDecimal newValue)
    {
        this.setBigDecimalValue(o, newValue);
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o)) return HashUtil.NULL_HASH;
        return HashUtil.hash(this.bigDecimalValueOf(o));
    }

    public void setValueUntil(T o, BigDecimal newValue, Timestamp exclusiveUntil)
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
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.bigDecimalValueOf(first).equals(this.bigDecimalValueOf(second));
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, BigDecimal> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.bigDecimalValueOf(first).equals(((BigDecimalExtractor) secondExtractor).bigDecimalValueOf(second));
        }
        return true;
    }

    public void increment(T o, BigDecimal increment)
    {
        this.setBigDecimalValue(o, this.bigDecimalValueOf(o).add(increment));
    }

    public void incrementUntil(T o, BigDecimal increment, Timestamp exclusiveUntil)
    {
        throw new UnsupportedOperationException("This method should only be called on objects with as of attributes");
    }
}
