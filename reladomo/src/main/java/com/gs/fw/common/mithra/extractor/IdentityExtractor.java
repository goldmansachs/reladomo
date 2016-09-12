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

import com.gs.fw.common.mithra.finder.orderby.OrderBy;

import java.sql.Timestamp;


public class IdentityExtractor implements Extractor
{

    private static final IdentityExtractor instance = new IdentityExtractor();

    private static final Extractor[] arrayInstance = new Extractor[] { instance };

    public static IdentityExtractor getInstance()
    {
        return instance;
    }

    public static Extractor[] getArrayInstance()
    {
        return arrayInstance;
    }

    public void setValue(Object o, Object newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
    {
        return first == secondExtractor.valueOf(second);
    }

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(Object o)
    {
        return System.identityHashCode(o);
    }

    public boolean valueEquals(Object first, Object second)
    {
        return first == second;
    }

    public Object valueOf(Object anObject)
    {
        return anObject;
    }
}
