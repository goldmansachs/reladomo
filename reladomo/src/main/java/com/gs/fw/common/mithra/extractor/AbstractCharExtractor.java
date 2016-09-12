
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

public abstract class AbstractCharExtractor<T> extends PrimitiveExtractor<T, Character> implements CharExtractor<T>
{

    public Character valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return this.charValueOf(o);
    }

    public void setValue(T o, Character newValue)
    {
        this.setCharValue(o, newValue);
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o)) return HashUtil.NULL_HASH;
        return HashUtil.hash(this.charValueOf(o));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.charValueOf(first) == this.charValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Character> secondExtractor)
    {
        return this.charValueOf(first) == ((CharExtractor) secondExtractor).charValueOf(second);
    }
}
