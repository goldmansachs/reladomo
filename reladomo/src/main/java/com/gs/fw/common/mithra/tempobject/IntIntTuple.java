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
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;


public class IntIntTuple extends TupleImpl
{
    private int first;
    private int second;

    public IntIntTuple(int first, int second)
    {
        this.first = first;
        this.second = second;
    }

    @Override
    public String zGetPrintablePrimaryKey()
    {
        return first+","+second;
    }

    @Override
    public Object getAttribute(int index)
    {
        switch(index)
        {
            case 0:
                return Integer.valueOf(first);
            case 1:
                return Integer.valueOf(second);
            default:
                throw new ArrayIndexOutOfBoundsException(index);
        }
    }

    @Override
    public boolean isAttributeNull(int index)
    {
        return false;
    }

    @Override
    public boolean getAttributeAsBoolean(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to boolean");
    }

    @Override
    public byte getAttributeAsByte(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to byte");
    }

    @Override
    public short getAttributeAsShort(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to short");
    }

    @Override
    public char getAttributeAsChar(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to char");
    }

    @Override
    public int getAttributeAsInt(int index)
    {
        switch(index)
        {
            case 0:
                return first;
            case 1:
                return second;
            default:
                throw new ArrayIndexOutOfBoundsException(index);
        }
    }

    @Override
    public long getAttributeAsLong(int index)
    {
        return getAttributeAsInt(index);
    }

    @Override
    public float getAttributeAsFloat(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to float");
    }

    @Override
    public double getAttributeAsDouble(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to double");
    }

    @Override
    public String getAttributeAsString(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to String");
    }

    @Override
    public Date getAttributeAsDate(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to Date");
    }

    @Override
    public Time getAttributeAsTime(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to Time");
    }

    @Override
    public Timestamp getAttributeAsTimestamp(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to Timestamp");
    }

    @Override
    public byte[] getAttributeAsByteArray(int index)
    {
        throw new UnsupportedOperationException("integers cannot be converted to byte[]");
    }

    @Override
    public BigDecimal getAttributeAsBigDecimal(int pos)
    {
        throw new UnsupportedOperationException("integers cannot be converted to BigDecimal");
    }

    @Override
    public void writeToStream(ObjectOutput os) throws IOException
    {
        //todo: optimize this with a check in ExternalizableTupleList & MithraArrayTupleTupleSet
        os.writeObject(Integer.valueOf(first));
        os.writeObject(Integer.valueOf(second));
    }
}
