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
import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.extractor.LongExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;



public class LongUpdateWrapper extends AttributeUpdateWrapper implements LongExtractor
{

    private long newValue;

    public LongUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, long newValue)
    {
        super(attribute, dataToUpdate);
        this.newValue = newValue;
    }

    public LongUpdateWrapper()
    {
    }

    @Override
    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof LongUpdateWrapper && this.newValue == ((LongUpdateWrapper)other).newValue;
    }

    @Override
    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setLong(index, newValue);
        return 1;
    }

    @Override
    public int getNewValueHashCode()
    {
        return HashUtil.hash(this.newValue);
    }

    public Object valueOf(Object anObject)
    {
        return Long.valueOf(newValue);
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public int valueHashCode(Object o)
    {
        return HashUtil.hash(this.newValue);
    }

    public long longValueOf(Object o)
    {
        return this.newValue;
    }

    @Override
    public void updateData(MithraDataObject data)
    {
        ((LongAttribute)this.getAttribute()).setLongValue(data, newValue);
    }

    public void setLongValue(Object o, long newValue)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        out.writeLong(this.newValue);
    }

    @Override
    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.newValue = in.readLong();
    }
}
