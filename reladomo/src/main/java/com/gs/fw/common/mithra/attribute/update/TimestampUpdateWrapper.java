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
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;



public class TimestampUpdateWrapper extends ObjectUpdateWrapper implements TimestampExtractor, Externalizable
{

    public TimestampUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, Object newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public TimestampUpdateWrapper()
    {
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        TimestampAttribute timestampAttribute  = (TimestampAttribute) this.getAttribute();
        timestampAttribute.setSqlParameter(index, ps, this.getValue(), timeZone, databaseType);
        return 1;
    }

    public Timestamp timestampValueOf(Object o)
    {
        return (Timestamp) this.getValue();
    }

    @Override
    public long timestampValueOfAsLong(Object o)
    {
        return this.timestampValueOf(o).getTime();
    }

    public void setTimestampValue(Object o, Timestamp newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(this.getAttribute());
        TimestampAttribute timestampAttribute = (TimestampAttribute) this.getAttribute();
        timestampAttribute.writeToStream(out, (Timestamp) this.getValue());
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setAttribute((Attribute)in.readObject());
        TimestampAttribute timestampAttribute = (TimestampAttribute) this.getAttribute();
        this.setNewValue(timestampAttribute.readFromStream(in));
    }

    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        TimestampAttribute timestampAttribute = (TimestampAttribute) this.getAttribute();
        timestampAttribute.writeToStream(out, (Timestamp) this.getValue());
    }

    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        TimestampAttribute timestampAttribute = (TimestampAttribute) this.getAttribute();
        this.setNewValue(timestampAttribute.readFromStream(in));
    }
}
