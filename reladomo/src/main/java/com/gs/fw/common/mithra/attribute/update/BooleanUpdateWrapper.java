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
import com.gs.fw.common.mithra.attribute.BooleanAttribute;
import com.gs.fw.common.mithra.extractor.BooleanExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;



public class BooleanUpdateWrapper extends AttributeUpdateWrapper implements BooleanExtractor
{

    private boolean newValue;

    public BooleanUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, boolean newValue)
    {
        super(attribute, dataToUpdate);
        this.newValue = newValue;
    }

    public BooleanUpdateWrapper()
    {
    }

    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof BooleanUpdateWrapper && this.newValue == ((BooleanUpdateWrapper)other).newValue;
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setBoolean(index, newValue);
        return 1;
    }

    public int getNewValueHashCode()
    {
        return HashUtil.hash(this.newValue);
    }

    public Object valueOf(Object anObject)
    {
        return Boolean.valueOf(newValue);
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public int valueHashCode(Object o)
    {
        return HashUtil.hash(this.newValue);
    }

    public boolean booleanValueOf(Object o)
    {
        return this.newValue;
    }

    public void updateData(MithraDataObject data)
    {
        ((BooleanAttribute)this.getAttribute()).setBooleanValue(data, newValue);
    }

    public void setBooleanValue(Object o, boolean newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        out.writeBoolean(this.newValue);
    }

    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.newValue = in.readBoolean();
    }
}
