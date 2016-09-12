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
import com.gs.fw.common.mithra.attribute.BooleanAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BooleanProcedure;
import com.gs.fw.common.mithra.util.*;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;


public class MinCalculatorBoolean extends AbstractAggregateAttributeCalculator implements BooleanProcedure
{
    private static final long serialVersionUID = -3375949404022497789L;

    private BooleanAttribute attribute;

    public MinCalculatorBoolean(BooleanAttribute attribute)
    {
        super("min");
        this.attribute = attribute;
    }

    public MinCalculatorBoolean()
    {
    }

    public Attribute getAttribute()
    {
        return this.attribute;
    }

    public Object aggregate(Object previousValue, Object newValue)
    {
        if(previousValue == null)
        {
            previousValue = new MutableBoolean();
        }
        this.attribute.forEach(this, newValue, previousValue);
        return previousValue;
    }

    public boolean execute(boolean object, Object context)
    {
        MutableBoolean result = (MutableBoolean) context;
        if(result.isNull() || result.booleanCompare(object) > 0)
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
        attribute = (BooleanAttribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return new MutableBoolean();
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0xEB10AF98;
    }

    @Override
    public Class valueType()
    {
        return Boolean.class;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MinCalculatorBoolean)) return false;

        MinCalculatorBoolean that = (MinCalculatorBoolean) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
}
