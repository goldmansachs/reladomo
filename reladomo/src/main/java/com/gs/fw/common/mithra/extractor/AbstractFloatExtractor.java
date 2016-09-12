
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

public abstract class AbstractFloatExtractor<T> extends PrimitiveExtractor<T, Float> implements FloatExtractor<T, Float>
{

    public Float valueOf(T o)
    {
        return this.isAttributeNull(o) ? null : this.floatValueOf(o);
    }

    public void setValue(T o, Float newValue)
    {
        this.setFloatValue(o, newValue);
    }

    public int valueHashCode(T o)
    {
        return (this.isAttributeNull(o)) ? HashUtil.NULL_HASH : HashUtil.hash(this.floatValueOf(o));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.floatValueOf(first) == this.floatValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Float> secondExtractor)
    {
        return this.floatValueOf(first) == ((FloatExtractor) secondExtractor).floatValueOf(second);
    }
}
