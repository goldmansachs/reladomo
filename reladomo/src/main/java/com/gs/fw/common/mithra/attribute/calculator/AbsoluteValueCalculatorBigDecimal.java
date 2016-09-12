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

import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;

import java.math.BigDecimal;



public class AbsoluteValueCalculatorBigDecimal extends AbstractAbsoluteValueCalculator implements BigDecimalAttributeCalculator
{
    public AbsoluteValueCalculatorBigDecimal(NumericAttribute attribute)
    {
        super(attribute);
    }

    public int intValueOf(Object o)
    {
        return Math.abs(((BigDecimalAttribute)this.attribute).bigDecimalValueOf(o).intValue());
    }

//    public int intValueOf(Object o)
//    {
//        return this.bigDecimalValueOf(o).intValue();
//    }

    public double doubleValueOf(Object o)
    {
        return Math.abs(((BigDecimalAttribute)this.attribute).bigDecimalValueOf(o).doubleValue());
    }

    public float floatValueOf(Object o)
    {
        return Math.abs(((BigDecimalAttribute)this.attribute).bigDecimalValueOf(o).floatValue());
    }

    public long longValueOf(Object o)
    {
        return Math.abs(((BigDecimalAttribute)this.attribute).bigDecimalValueOf(o).longValue());
    }

    public int getScale()
    {
        return this.attribute.getScale();
    }

    public int getPrecision()
    {
        return this.attribute.getPrecision();
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return ((BigDecimalAttribute)this.attribute).bigDecimalValueOf(o).abs();
    }
}
