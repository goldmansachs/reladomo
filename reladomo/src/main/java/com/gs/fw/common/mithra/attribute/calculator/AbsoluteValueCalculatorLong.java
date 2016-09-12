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

import com.gs.fw.common.mithra.attribute.LongAttribute;

import java.math.BigDecimal;



public class AbsoluteValueCalculatorLong extends AbstractAbsoluteValueCalculator
{

    public AbsoluteValueCalculatorLong(LongAttribute attribute)
    {
        super(attribute);
    }

    public double doubleValueOf(Object o)
    {
        return Math.abs((double)((LongAttribute)this.attribute).longValueOf(o));
    }

    public float floatValueOf(Object o)
    {
        return Math.abs((float)((LongAttribute)this.attribute).longValueOf(o));
    }

    public long longValueOf(Object o)
    {
        return Math.abs(((LongAttribute)this.attribute).longValueOf(o));
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return BigDecimal.valueOf(longValueOf(o));
    }

    public int intValueOf(Object o)
    {
        return Math.abs((int)((LongAttribute)this.attribute).longValueOf(o));
    }
}
