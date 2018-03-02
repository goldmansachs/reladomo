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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;


public class LazyTuple extends TupleImpl
{

    private transient Object delegate;
    private transient Extractor[] delegateAttributes;

    public static Function createFactory(final Extractor[] delegateAttributes)
    {
        return new Function()
        {
            @Override
            public Object valueOf(Object delegate)
            {
                return new LazyTuple(delegate, delegateAttributes);
            }
        };
    }

    public LazyTuple(Object delegate, Extractor[] delegateAttributes)
    {
        this.delegate = delegate;
        this.delegateAttributes = delegateAttributes;
    }

    public Object getAttribute(int index)
    {
        return delegateAttributes[index].valueOf(delegate);
    }

    public boolean isAttributeNull(int index)
    {
        return delegateAttributes[index].isAttributeNull(delegate);
    }

    public boolean getAttributeAsBoolean(int index)
    {
        return ((BooleanExtractor)delegateAttributes[index]).booleanValueOf(delegate);
    }

    public byte getAttributeAsByte(int index)
    {
        return ((ByteExtractor)delegateAttributes[index]).byteValueOf(delegate);
    }

    public short getAttributeAsShort(int index)
    {
        return ((ShortExtractor)delegateAttributes[index]).shortValueOf(delegate);
    }

    public char getAttributeAsChar(int index)
    {
        return ((CharExtractor)delegateAttributes[index]).charValueOf(delegate);
    }

    public int getAttributeAsInt(int index)
    {
        return ((IntExtractor)delegateAttributes[index]).intValueOf(delegate);
    }

    public long getAttributeAsLong(int index)
    {
        return ((LongExtractor)delegateAttributes[index]).longValueOf(delegate);
    }

    public float getAttributeAsFloat(int index)
    {
        return ((FloatExtractor)delegateAttributes[index]).floatValueOf(delegate);
    }

    public double getAttributeAsDouble(int index)
    {
        return ((DoubleExtractor)delegateAttributes[index]).doubleValueOf(delegate);
    }

    public String getAttributeAsString(int index)
    {
        return ((StringExtractor)delegateAttributes[index]).stringValueOf(delegate);
    }

    public Date getAttributeAsDate(int index)
    {
        return ((DateExtractor)delegateAttributes[index]).dateValueOf(delegate);
    }

    public Time getAttributeAsTime(int index)
    {
        return ((TimeExtractor)delegateAttributes[index]).timeValueOf(delegate);
    }

    public Timestamp getAttributeAsTimestamp(int index)
    {
        return ((TimestampExtractor)delegateAttributes[index]).timestampValueOf(delegate);
    }

    public byte[] getAttributeAsByteArray(int index)
    {
        return ((ByteArrayExtractor)delegateAttributes[index]).byteArrayValueOf(delegate);
    }

    public BigDecimal getAttributeAsBigDecimal(int index)
    {
        return ((BigDecimalExtractor)delegateAttributes[index]).bigDecimalValueOf(delegate);
    }

    public void writeToStream(ObjectOutput os) throws IOException
    {
        for(int i=0;i<this.delegateAttributes.length;i++)
        {
            os.writeObject(this.delegateAttributes[i].valueOf(delegate));
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        Object[] values = new Object[this.delegateAttributes.length];
        for(int i=0;i<this.delegateAttributes.length;i++)
        {
            values[i] = this.delegateAttributes[i].valueOf(delegate);
        }
        return new ArrayTuple(values);
    }

    public String zGetPrintablePrimaryKey()
    {
        StringBuffer buf = new StringBuffer(this.delegateAttributes.length * 8);
        for(int i=0;i<this.delegateAttributes.length;i++)
        {
            if (i > 0) buf.append(',');
            buf.append(delegateAttributes[i].valueOf(delegate));
        }
        return buf.toString();
    }
}
