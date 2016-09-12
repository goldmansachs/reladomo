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

package com.gs.fw.common.mithra.finder.timestamp;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.finder.NonPrimitiveEqOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;



public class TimestampEqOperation extends NonPrimitiveEqOperation implements Externalizable
{

    public TimestampEqOperation(TimestampAttribute attribute, Timestamp parameter)
    {
        super(attribute, parameter);
    }

    public TimestampEqOperation()
    {
        // for externalizable
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        TimestampAttribute attribute = (TimestampAttribute) in.readObject();
        this.setAttribute(attribute);
        this.setParameter(attribute.readFromStream(in));
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        TimestampAttribute timestampAttribute = (TimestampAttribute) this.getAttribute();
        timestampAttribute.writeToStream(out, (Timestamp)this.getParameterAsObject());
    }

    public Operation susbtituteOtherAttribute(Attribute other)
    {
        if (other instanceof TimestampAttribute)
        {
            return new TimestampEqOperation((TimestampAttribute)other, (Timestamp) this.getParameterAsObject());
        }
        if (other instanceof AsOfAttribute)
        {
            return new AsOfEqOperation((AsOfAttribute)other, (Timestamp) this.getParameterAsObject());
        }
        return null;
    }

    public Operation zFindEquality(TimestampAttribute attr)
    {
        return (attr.equals(this.getAttribute())) ? this : null;
    }

    @Override
    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements TimestampExtractor
    {
        public Object valueOf(Object o)
        {
            return getParameterAsObject();
        }

        public boolean isAttributeNull(Object o)
        {
            return this.valueOf(o) == null;
        }

        @Override
        public Timestamp timestampValueOf(Object o)
        {
            return (Timestamp) getParameterAsObject();
        }

        @Override
        public long timestampValueOfAsLong(Object o)
        {
            return timestampValueOf(o).getTime();
        }

        public int valueHashCode(Object o)
        {
            return valueOf(o).hashCode();
        }

        public boolean valueEquals(Object first, Object second)
        {
            if (first == second) return true;
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = this.isAttributeNull(second);
            if (firstNull) return secondNull;
            return this.valueOf(first).equals(this.valueOf(second));
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            Object firstValue = this.valueOf(first);
            Object secondValue = secondExtractor.valueOf(second);
            if (firstValue == secondValue) return true; // takes care of both null

            return (firstValue != null) && firstValue.equals(secondValue);
        }
    }

}
