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

package com.gs.fw.common.mithra.attribute.calculator.aggregateFunction;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.util.*;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.math.BigDecimal;



public class MaxCalculatorNumeric extends AbstractAggregateAttributeCalculator implements DoubleProcedure, BigDecimalProcedure,FloatProcedure, LongProcedure, IntegerProcedure
{
    private static final long serialVersionUID = -1006474383125254184L;

    private NumericAttribute attribute;

    public MaxCalculatorNumeric(NumericAttribute attribute)
    {
        super("max");
        this.attribute = attribute;
    }

    public MaxCalculatorNumeric()
    {
    }

    public Attribute getAttribute()
    {
        return (Attribute) this.attribute;
    }

    public Object aggregate(Object previousValue, Object newValue)
    {
        if (previousValue == null)
        {
            previousValue = ((NumericAttribute)getAttribute()).getNumericType().createMutableNumber();
        }
        ((NumericAttribute)getAttribute()).getNumericType().executeForEach(this, (NumericAttribute)getAttribute(),previousValue, newValue );
        return previousValue;
    }


    public boolean execute(int object, Object context)
    {
        MutableInteger result = (MutableInteger) context;
        if(result.intCompare(object) < 0)
        {
            result.replace(object);
        }
        return false;
    }

    public boolean execute(double object, Object context)
    {
       MutableDouble result = (MutableDouble) context;
        if(result.doubleCompare(object) < 0)
        {
            result.replace(object);
        }
        return false;
    }

   public boolean execute(BigDecimal object, Object context)
    {
        MutableBigDecimal result = (MutableBigDecimal)context;
        if(result.bigDecimalCompare(object) < 0)
        {
            result.replace(object);
        }
        return false;
    }

    public boolean execute(float object, Object context)
    {
        MutableFloat result = (MutableFloat) context;
        if(result.floatCompare(object) < 0)
        {
            result.replace(object);
        }
        return false;
    }

    public boolean execute(long object, Object context)
    {
        MutableLong result = (MutableLong) context;
        if(result.longCompare(object) < 0)
        {
            result.replace(object);
        }
        return false;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(attribute);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        attribute = (NumericAttribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return this.attribute.getNumericType().createMutableNumber();
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0x98AF10EB;
    }

    @Override
    public Class valueType()
    {
        return this.attribute.getNumericType().valueType();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MaxCalculatorNumeric)) return false;

        MaxCalculatorNumeric that = (MaxCalculatorNumeric) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
}
