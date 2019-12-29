
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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.util.MutableBoolean;
import com.gs.fw.common.mithra.util.MutableCharacter;
import com.gs.fw.common.mithra.util.MutableComparableReference;
import com.gs.fw.common.mithra.util.MutableNumber;
import com.gs.fw.common.mithra.util.Nullable;
import com.gs.fw.common.mithra.util.Time;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class AggregateData implements Externalizable
{
    private static final long serialVersionUID = -352045255078317877L;

    private Object[] values;
    private AggregateDataConfig config;

    public AggregateData()
    {
        // for serialization
    }

    public AggregateData(int attributeCount)
    {
        values = new Object[attributeCount];
        config = new AggregateDataConfig();
    }

    public AggregateData(AggregateDataConfig config)
    {
        values = new Object[config.getAttributeCount()];
        this.config = config;
    }

    public Object[] getValues()
    {
        return this.config.getValuesAsArray(this.values);
    }

    public void setValueAt(int position, Object value)
    {
        this.config.setValueAt(position, value, values);
    }

    public Object getValueAt(int position)
    {
        return values[position];
    }

    public void setValues(Object[] values)
    {
        this.values = values;
    }

    public void setNameToPositionMap(ObjectIntMap map)
    {
        if (this.config == null)
        {
            this.config = new AggregateDataConfig();
        }
        this.config.setNameToPositionMap(map);
    }

    public Object getValue(String name)
    {
        return this.config.getObjectValueForName(name, values);
    }

    public Object getAttributeAsObject(String attributeName)
    {
        Nullable result = (Nullable) getValueByAttributeName(attributeName);
        return result.getAsObject();
    }

    public int getAttributeAsInt(String s)
    {
        Object value = this.config.getObjectValueForName(s, values);
        if (((Nullable)value).isNull())
        {
            throw new MithraNullPrimitiveException("attribute ' "+s+" ' is null. Call isAttributeNull before getAttributeAs");
        }
        return ((Number) value).intValue();
    }

    public int getAttributeAsInteger(String s)
    {
        return this.getAttributeAsInt(s);
    }

    public double getAttributeAsDouble(String s)
    {
        return ((Number) this.config.getObjectValueForName(s, values)).doubleValue();
    }

    public float getAttributeAsFloat(String s)
    {
        return ((Number) this.config.getObjectValueForName(s, values)).floatValue();
    }

    public byte getAttributeAsByte(String s)
    {
        return ((Number) this.config.getObjectValueForName(s, values)).byteValue();
    }

    public long getAttributeAsLong(String s)
    {
        return ((Number) this.config.getObjectValueForName(s, values)).longValue();
    }

    public short getAttributeAsShort(String s)
    {
        return ((Number) this.config.getObjectValueForName(s, values)).shortValue();
    }

    public boolean getAttributeAsBoolean(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableBoolean) result).booleanValue();
    }

    public String getAttributeAsString(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableComparableReference<String>) result).getValue();
    }

    public char getAttributeAsCharacter(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableCharacter) result).getValue();
    }

    public Timestamp getAttributeAsTimestamp(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableComparableReference<Timestamp>) result).getValue();
    }

    public Date getAttributeAsDate(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableComparableReference<Date>) result).getValue();
    }

    public Time getAttributeAsTime(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableComparableReference<Time>) result).getValue();
    }

    public BigDecimal getAttributeAsBigDecimal(String s)
    {
        Object result = getValueByAttributeName(s);
        return ((MutableNumber) result).bigDecimalValue();
    }

    private Object getValueByAttributeName(String s)
    {
        return this.config.getObjectValueForName(s, values);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(config);
        config.writeToStream(out, this);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.config = (AggregateDataConfig) in.readObject();
        values = new Object[this.config.getAttributeCount()];
        config.readFromStream(in, this);
    }

    public boolean isAttributeNull(String attributeName)
    {
        Nullable result = (Nullable) getValueByAttributeName(attributeName);
        return result.isNull();
    }
}
