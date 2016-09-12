
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
import java.util.Arrays;

public abstract class AbstractByteArrayExtractor<T> implements ByteArrayExtractor<T, byte[]>
{
    public byte[] valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return this.byteArrayValueOf(o);
    }

    public void setValue(T o, byte[] newValue)
    {
        this.setByteArrayValue(o, newValue);
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o)) return HashUtil.NULL_HASH;
        return HashUtil.hash(this.byteArrayValueOf(o));
    }

    public void setValueUntil(T o, byte[] newValue, Timestamp exclusiveUntil)
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
            return Arrays.equals(this.byteArrayValueOf(first), this.byteArrayValueOf(second));
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, byte[]> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return Arrays.equals(this.byteArrayValueOf(first), ((ByteArrayExtractor) secondExtractor).byteArrayValueOf(second));
        }
        return true;
    }
}