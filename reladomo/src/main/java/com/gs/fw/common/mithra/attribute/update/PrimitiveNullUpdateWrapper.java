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
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;



public abstract class PrimitiveNullUpdateWrapper extends AttributeUpdateWrapper
{

    protected PrimitiveNullUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate)
    {
        super(attribute, dataToUpdate);
    }

    public PrimitiveNullUpdateWrapper()
    {
    }

    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof PrimitiveNullUpdateWrapper;
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setNull(index, this.getSqlType());
        return 1;
    }

    public int getNewValueHashCode()
    {
        return HashUtil.NULL_HASH;
    }

    public void updateData(MithraDataObject data)
    {
        this.getAttribute().setValueNull(data);
    }

    public boolean isAttributeNull(Object o)
    {
        return true;
    }

    public int valueHashCode(Object o)
    {
        return HashUtil.NULL_HASH;
    }

    public Object valueOf(Object anObject)
    {
        return null;
    }

    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        //nothing to do
    }

    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        // nothing to do
    }

    public abstract int getSqlType();
}
