
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

import java.sql.Timestamp;

public abstract class PrimitiveExtractor<T,V> implements Extractor<T,V>
{

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
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.primitiveValueEquals(first, second);
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O,V> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.primitiveValueEquals(first, second, secondExtractor);
        }
        return true;
    }

    protected abstract boolean primitiveValueEquals(T first, T second);

    protected abstract <O> boolean primitiveValueEquals(T first, O second, Extractor<O,V> secondExtractor);
}
