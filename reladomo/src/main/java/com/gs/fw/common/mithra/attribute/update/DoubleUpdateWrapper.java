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
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;



public class DoubleUpdateWrapper extends AttributeUpdateWrapper implements DoubleExtractor
{

    private double newValue;

    public DoubleUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, double newValue)
    {
        super(attribute, dataToUpdate);
        this.newValue = newValue;
    }

    public DoubleUpdateWrapper()
    {
    }

    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof DoubleUpdateWrapper && this.newValue == ((DoubleUpdateWrapper)other).newValue;
    }

    public int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setDouble(index, newValue);
        return 1;
    }

    public int getNewValueHashCode()
    {
        return HashUtil.hash(this.newValue);
    }

    public Object valueOf(Object anObject)
    {
        return new Double(newValue);
    }

    public boolean isAttributeNull(Object o)
    {
        return false;
    }

    public int valueHashCode(Object o)
    {
        return HashUtil.hash(this.newValue);
    }

    public double doubleValueOf(Object o)
    {
        return this.newValue;
    }

    public double getNewValue()
    {
        return newValue;
    }

    public void setNewValue(double newValue)
    {
        this.newValue = newValue;
    }

    public void updateData(MithraDataObject data)
    {
        ((DoubleAttribute)this.getAttribute()).setDoubleValue(data, newValue);
    }

    public void setDoubleValue(Object o, double newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void increment(Object o, double d)
    {
        throw new RuntimeException("not implemented");
    }

    public void incrementUntil(Object o, double d, Timestamp t)
    {
        throw new RuntimeException("not implemented");
    }

    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        out.writeDouble(this.newValue);
    }

    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.newValue = in.readDouble();
    }
}
