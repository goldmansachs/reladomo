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

package com.gs.fw.common.mithra.attribute.calculator.aggregateFunction;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.AttributeMetaData;
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;



public class CountCalculator extends AbstractAggregateAttributeCalculator implements ObjectProcedure
{
    private static final long serialVersionUID = -6744643716498358949L;
    private Attribute attribute;

    public CountCalculator(Attribute attribute)
    {
        super("count");
        this.attribute = attribute;
    }

    public CountCalculator()
    {
    }


    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        String arg = this.getAttribute().getFullyQualifiedLeftHandExpression(query);
        AttributeMetaData metaData = this.getAttribute().getMetaData();
        if (metaData != null && !metaData.isNullable())
        {
            arg = "1";
        }
        return "count("+arg+")";
    }

    public Attribute getAttribute()
    {
        return this.attribute;
    }

     public Object aggregate(Object previousValue, Object newValue)
    {
        if (previousValue == null)
        {
            previousValue = new MutableInteger();
        }
        this.attribute.forEach(this, newValue, previousValue);
        return previousValue;
    }

    public boolean execute(Object object, Object context)
    {
        MutableInteger result = (MutableInteger) context;
        result.add(1);
        return false;
    }

    @Override
    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt) throws SQLException
    {
        data.setValueAt((resultSetPosition -1) , new MutableInteger(rs.getInt(resultSetPosition)));
        return 1;
    }

    @Override
    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        Integer integer = rs.getInt(resultSetPosition);
        tempArray[0] = integer;
        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + " on instance of class" + object.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + " on instance of class" + object.getClass().getName(), e);
        }
        return 1;
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeInt(((MutableInteger)valueWrappedInNullable).intValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableInteger(in.readInt());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(attribute);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        attribute = (Attribute)in.readObject();
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return new MutableInteger(0);
    }

    @Override
    public int hashCode()
    {
        return this.attribute.hashCode() ^ 0x12FF34EC;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof CountCalculator)) return false;

        CountCalculator that = (CountCalculator) o;

        if (!attribute.equals(that.attribute)) return false;

        return true;
    }
    public Class valueType()
    {
        return Integer.class;
    }
}
