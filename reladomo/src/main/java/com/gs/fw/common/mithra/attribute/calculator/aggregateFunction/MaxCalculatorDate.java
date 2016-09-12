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
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DateProcedure;
import com.gs.fw.common.mithra.util.*;

import java.util.Date;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;


public class MaxCalculatorDate extends AbstractAggregateAttributeCalculator implements DateProcedure
{
    private static final long serialVersionUID = 4800691534161185577L;
    private DateAttribute attribute;

    public MaxCalculatorDate(DateAttribute attribute)
    {
        super("max");
        this.attribute = attribute;
    }

    public MaxCalculatorDate()
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
            previousValue = new MutableComparableReference<Date>();
        }
        this.attribute.forEach(this, newValue, previousValue);
        return previousValue;
    }

    public boolean execute(Date object, Object context)
    {
        MutableComparableReference<Date> result = (MutableComparableReference<Date>) context;
        if(result.isNull() || (object.compareTo(result.getValue()) > 0))
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
        attribute = (DateAttribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return new MutableComparableReference<Date>();
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0x98AF10EB;
    }

    @Override
    public Class valueType()
    {
        return Date.class;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MaxCalculatorDate)) return false;

        MaxCalculatorDate that = (MaxCalculatorDate) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
}
