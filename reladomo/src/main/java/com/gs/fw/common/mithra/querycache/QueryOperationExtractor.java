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

package com.gs.fw.common.mithra.querycache;

import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;

import java.sql.Timestamp;



public class QueryOperationExtractor implements Extractor
{

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public Object valueOf(Object o)
    {
        return ((CachedQuery)o).getOperation();
    }

    public void setValue(Object o, Object newValue)
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

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

   public int valueHashCode(Object o)
    {
        return this.valueOf(o).hashCode();
    }

    public boolean valueEquals(Object first, Object second)
    {
        return valueOf(first).equals(valueOf(second));
    }

    public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
    {
        return this.valueOf(first).equals(secondExtractor.valueOf(second));
    }

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }
}
