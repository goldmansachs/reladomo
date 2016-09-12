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

import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.finder.NonPrimitiveInOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Set;


public class TimestampInOperation extends NonPrimitiveInOperation
{

    public TimestampInOperation(NonPrimitiveAttribute attribute, Set set)
    {
        super(attribute, set);
    }

    public TimestampInOperation()
    {
        // for Externalizable
    }

    protected void writeParameter(ObjectOutput out, Object o) throws IOException
    {
        ((TimestampAttribute)this.getAttribute()).writeToStream(out, (Timestamp) o);
    }

    protected Object readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return ((TimestampAttribute)this.getAttribute()).readFromStream(in);
    }

    @Override
    protected Extractor createParameterExtractor()
    {
        return new ParameterExtractor();
    }

    private class ParameterExtractor extends PositionBasedOperationParameterExtractor implements TimestampExtractor
    {
        public int getSetSize()
        {
            return TimestampInOperation.this.getSetSize();
        }

        public boolean isAttributeNull(Object o)
        {
            return this.valueOf(o) == null;
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

        public Object valueOf(Object anObject)
        {
            return copiedArray[this.getPosition()];
        }

        @Override
        public Timestamp timestampValueOf(Object o)
        {
            return (Timestamp) valueOf(o);
        }

        @Override
        public long timestampValueOfAsLong(Object o)
        {
            return timestampValueOf(o).getTime();
        }
    }

}
