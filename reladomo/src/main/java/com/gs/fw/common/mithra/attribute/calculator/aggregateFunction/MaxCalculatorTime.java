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
import com.gs.fw.common.mithra.attribute.TimeAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.TimeProcedure;
import com.gs.fw.common.mithra.util.MutableComparableReference;
import com.gs.fw.common.mithra.util.Nullable;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class MaxCalculatorTime extends AbstractAggregateAttributeCalculator implements TimeProcedure
{
    private static final long serialVersionUID = 0L;
    private TimeAttribute attribute;

    public MaxCalculatorTime(TimeAttribute attribute)
    {
        super("max");
        this.attribute = attribute;
    }

    public MaxCalculatorTime()
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
            previousValue = new MutableComparableReference<Time>();
        }
        this.attribute.forEach(this, newValue, previousValue);
        return previousValue;
    }

    public boolean execute(Time object, Object context)
    {
        MutableComparableReference<Time> result = (MutableComparableReference<Time>) context;
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
        attribute = (TimeAttribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return new MutableComparableReference<Time>();
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0x1E6222;
    }

    @Override
    public Class valueType()
    {
        return Time.class;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MaxCalculatorTime)) return false;

        MaxCalculatorTime that = (MaxCalculatorTime) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
}
