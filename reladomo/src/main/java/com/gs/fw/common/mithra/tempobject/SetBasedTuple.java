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

import com.gs.fw.common.mithra.finder.SetBasedAtomicOperation;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;


public class SetBasedTuple extends TupleImpl
{
    private SetBasedAtomicOperation setBasedOp;
    private int index;

    public SetBasedTuple(SetBasedAtomicOperation setBasedOp, int index)
    {
        this.setBasedOp = setBasedOp;
        this.index = index;
    }

    public boolean getAttributeAsBoolean(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsBoolean(this.index);
    }

    public byte getAttributeAsByte(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsByte(this.index);
    }

    public byte[] getAttributeAsByteArray(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsByteArray(this.index);
    }

    public char getAttributeAsChar(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsChar(this.index);
    }

    public Date getAttributeAsDate(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsDate(this.index);
    }

    public Time getAttributeAsTime(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsTime(this.index);
    }

    public double getAttributeAsDouble(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsDouble(this.index);
    }

    public float getAttributeAsFloat(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsFloat(this.index);
    }

    public int getAttributeAsInt(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsInt(this.index);
    }

    public long getAttributeAsLong(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsLong(this.index);
    }

    public short getAttributeAsShort(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsShort(this.index);
    }

    public String getAttributeAsString(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsString(this.index);
    }

    public Timestamp getAttributeAsTimestamp(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsTimestamp(this.index);
    }

    public boolean isAttributeNull(int index)
    {
        return false;
    }

    public Object getAttribute(int index)
    {
        throw new RuntimeException("should not get here");
    }

    public void writeToStream(ObjectOutput os) throws IOException
    {
        throw new RuntimeException("should not get here");
    }

    public BigDecimal getAttributeAsBigDecimal(int unusedShouldBeZero)
    {
        return setBasedOp.getSetValueAsBigDecimal(this.index);
    }

    public String zGetPrintablePrimaryKey()
    {
        return "Not implemented: set based tuple";
    }
}
