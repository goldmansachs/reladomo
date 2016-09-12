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
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.StringProcedure;
import com.gs.fw.common.mithra.util.*;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;


public class MaxCalculatorString extends AbstractAggregateAttributeCalculator implements StringProcedure
{
    private static final long serialVersionUID = 1159092325093624169L;
    private StringAttribute attribute;

    public MaxCalculatorString(StringAttribute attribute)
    {
        super("max");
        this.attribute = attribute;
    }

    public MaxCalculatorString()
    {
    }

    public Attribute getAttribute()
    {
        return attribute;
    }

    public Object aggregate(Object previousValue, Object newValue)
    {
        if(previousValue == null)
        {
             previousValue = new MutableComparableReference<String>();
        }
        this.attribute.forEach(this, newValue, previousValue);
        return previousValue;
    }

    public boolean execute(String object, Object context)
    {
        MutableComparableReference<String> result = (MutableComparableReference<String>) context;
        if(result.isNull() || object.compareTo(result.getValue()) > 0)
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
        attribute = (StringAttribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return new MutableComparableReference<String>();
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0x98AF10EB;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MaxCalculatorString)) return false;

        MaxCalculatorString that = (MaxCalculatorString) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
     public Class valueType()
    {
        return String.class;
    }
}
