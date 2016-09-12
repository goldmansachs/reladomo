
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

public abstract class AbstractDoubleExtractor<T> extends PrimitiveExtractor<T, Double> implements DoubleExtractor<T, Double>
{

    public Double valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return this.doubleValueOf(o);
    }

    public void setValue(T o, Double newValue)
    {
        this.setDoubleValue(o, newValue);
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o)) return HashUtil.NULL_HASH;
        return HashUtil.hash(this.doubleValueOf(o));
    }

    public void increment(T o, double increment)
    {
        this.setDoubleValue(o, this.doubleValueOf(o) + increment);
    }

    public void incrementUntil(T o, double increment, Timestamp exclusiveUntil)
    {
        throw new UnsupportedOperationException("This method should only be called on objects with as of attributes");
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.doubleValueOf(first) == this.doubleValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Double> secondExtractor)
    {
        return this.doubleValueOf(first) == ((DoubleExtractor) secondExtractor).doubleValueOf(second);
    }
}
