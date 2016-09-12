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

package com.gs.fw.common.mithra.attribute.calculator;

import com.gs.fw.common.mithra.attribute.FloatAttribute;

import java.math.BigDecimal;



public class AbsoluteValueCalculatorFloat extends AbstractAbsoluteValueCalculator
{

    public AbsoluteValueCalculatorFloat(FloatAttribute attribute)
    {
        super(attribute);
    }

    public double doubleValueOf(Object o)
    {
        return Math.abs((double)((FloatAttribute)this.attribute).floatValueOf(o));
    }

    public float floatValueOf(Object o)
    {
        return Math.abs(((FloatAttribute)this.attribute).floatValueOf(o));
    }

    public long longValueOf(Object o)
    {
        return Math.abs((long)((FloatAttribute)this.attribute).floatValueOf(o));
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return BigDecimal.valueOf(floatValueOf(o));
    }

    public int intValueOf(Object o)
    {
        return Math.abs((int)((FloatAttribute)this.attribute).floatValueOf(o));
    }
}
