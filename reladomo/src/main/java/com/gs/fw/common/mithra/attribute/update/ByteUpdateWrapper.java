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

package com.gs.fw.common.mithra.attribute.update;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.ByteAttribute;
import com.gs.fw.common.mithra.extractor.ByteExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;



public class ByteUpdateWrapper extends AttributeUpdateWrapper implements ByteExtractor
{

    private byte newValue;

    public ByteUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, byte newValue)
    {
        super(attribute, dataToUpdate);
        this.newValue = newValue;
    }

    public ByteUpdateWrapper()
    {
    }

    @Override
    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof ByteUpdateWrapper && this.newValue == ((ByteUpdateWrapper)other).newValue;
    }

    @Override
    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setByte(index, newValue);
        return 1;
    }

    @Override
    public int getNewValueHashCode()
    {
        return HashUtil.hash(this.newValue);
    }

    public Object valueOf(Object anObject)
    {
        return Byte.valueOf(newValue);
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public int valueHashCode(Object o)
    {
        return HashUtil.hash(this.newValue);
    }

    public int intValueOf(Object o)
    {
        return (int) this.byteValueOf(o);
    }

    public void setIntValue(Object o, int newValue)
    {
        this.setByteValue(o, (byte) newValue);
    }

    public byte byteValueOf(Object o)
    {
        return this.newValue;
    }

    @Override
    public void updateData(MithraDataObject data)
    {
        ((ByteAttribute)this.getAttribute()).setByteValue(data, newValue);
    }

    public void setByteValue(Object o, byte newValue)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        out.writeByte(this.newValue);
    }

    @Override
    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.newValue = in.readByte();
    }
}
