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
import java.math.BigDecimal;



public class OperationParameterExtractor
{

    public void setTimestampValue(Object o, Timestamp newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setShortValue(Object o, short newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setLongValue(Object o, long newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setBooleanValue(Object o, boolean newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setCharValue(Object o, char newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setDoubleValue(Object o, double newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setIntValue(Object o, int newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setByteValue(Object o, byte newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setFloatValue(Object o, float newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setBigDecimalValue(Object o, BigDecimal newValue)
    {
        throw new RuntimeException("not implemented");
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

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public void increment(Object o, double d)
    {
        throw new RuntimeException("not implemented");
    }

    public void incrementUntil(Object o, double d, Timestamp t)
    {
        throw new RuntimeException("not implemented");
    }

    public void increment(Object o, BigDecimal d)
    {
        throw new RuntimeException("not implemented");
    }

    public void incrementUntil(Object o, BigDecimal d, Timestamp t)
    {
        throw new RuntimeException("not implemented");
    }
    
    public boolean isAttributeNull(Object o)
    {
        return false;
    }

}
