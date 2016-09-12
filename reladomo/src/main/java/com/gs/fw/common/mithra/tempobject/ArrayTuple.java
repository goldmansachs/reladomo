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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;


public class ArrayTuple extends TupleImpl
{

    private Object[] values;

    public ArrayTuple(Object[] values)
    {
        this.values = values;
    }

    public ArrayTuple(int size, ObjectInput in) throws IOException, ClassNotFoundException
    {
        values = new Object[size];
        for(int i=0;i<values.length;i++)
        {
            values[i] = in.readObject();
        }
    }

    public void setAttribute(int index, Object val)
    {
        this.values[index] = val;
    }

    public Object getAttribute(int index)
    {
        return values[index];
    }

    public boolean isAttributeNull(int index)
    {
        return values[index] == null;
    }

    public boolean getAttributeAsBoolean(int index)
    {
        return (Boolean) values[index];
    }

    public byte getAttributeAsByte(int index)
    {
        return (Byte) values[index];
    }

    public short getAttributeAsShort(int index)
    {
        return (Short) values[index];
    }

    public char getAttributeAsChar(int index)
    {
        return (Character) values[index];
    }

    public int getAttributeAsInt(int index)
    {
        return (Integer) values[index];
    }

    public long getAttributeAsLong(int index)
    {
        return (Long) values[index];
    }

    public float getAttributeAsFloat(int index)
    {
        return (Float) values[index];
    }

    public double getAttributeAsDouble(int index)
    {
        return (Double) values[index];
    }

    public String getAttributeAsString(int index)
    {
        return (String) values[index];
    }

    public Date getAttributeAsDate(int index)
    {
        return (Date) values[index];
    }

    public Time getAttributeAsTime(int index)
    {
        return (Time) values[index];
    }

    public Timestamp getAttributeAsTimestamp(int index)
    {
        return (Timestamp) values[index];
    }

    public byte[] getAttributeAsByteArray(int index)
    {
        return (byte[]) values[index];
    }

    public void writeToStream(ObjectOutput os) throws IOException
    {
        for(int i=0;i<values.length;i++)
        {
            os.writeObject(values[i]);
        }
    }

    public BigDecimal getAttributeAsBigDecimal(int index)
    {
        return (BigDecimal) values[index];
    }

    public String zGetPrintablePrimaryKey()
    {
        StringBuffer buf = new StringBuffer(this.values.length * 8);
        for(int i=0;i<this.values.length;i++)
        {
            if (i > 0) buf.append(',');
            buf.append(values[i]);
        }
        return buf.toString();
    }
}
