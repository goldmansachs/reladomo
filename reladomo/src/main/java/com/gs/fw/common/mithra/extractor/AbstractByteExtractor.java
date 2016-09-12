
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

public abstract class AbstractByteExtractor<T> extends PrimitiveExtractor<T, Byte> implements ByteExtractor<T, Byte>, IntExtractor<T, Byte>
{

    public int intValueOf(T o)
    {
        return (int) this.byteValueOf(o);
    }

    public void setIntValue(T o, int newValue)
    {
        this.setByteValue(o, (byte) newValue);
    }

    public Byte valueOf(T o)
    {
        return this.isAttributeNull(o) ? null : this.byteValueOf(o);
    }

    public void setValue(T o, Byte newValue)
    {
        this.setByteValue(o, newValue);
    }

    public int valueHashCode(T o)
    {
        return this.isAttributeNull(o) ? HashUtil.NULL_HASH : HashUtil.hash(this.byteValueOf(o));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.byteValueOf(first) == this.byteValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Byte> secondExtractor)
    {
        return this.byteValueOf(first) == ((ByteExtractor) secondExtractor).byteValueOf(second);
    }
}
