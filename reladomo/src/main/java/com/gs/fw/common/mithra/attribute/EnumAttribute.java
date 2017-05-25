
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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.EnumExtractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.io.*;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public abstract class EnumAttribute<Owner, E extends Enum<E>> extends NonPrimitiveAttribute<Owner, E> implements com.gs.fw.finder.attribute.EnumerationAttribute<Owner, E>, EnumExtractor<Owner, E>
{

    private final Attribute delegate;

    public EnumAttribute(Attribute delegate)
    {
        this.delegate = delegate;
    }

    protected Attribute getDelegate()
    {
        return this.delegate;
    }

    public E valueOf(Owner o)
    {
        return this.enumValueOf(o);
    }

    public void setValue(Owner o, E newValue)
    {
        this.setEnumValue(o, newValue);
    }

    public void setValueUntil(Owner o, E newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    public void setUntil(Object o, E newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public abstract Operation eq(E other);

    public abstract Operation notEq(E other);

    public abstract Operation in(Set<E> enumSet);
    
    public abstract Operation notIn(Set<E> enumSet);

    public abstract <Owner2> Operation eq(EnumAttribute<Owner2, E> other);

    public abstract <Owner2> Operation notEq(EnumAttribute<Owner2, E> other);

    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq((E) other);
    }

    // delegate sql and file parser methods

    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        ((NonPrimitiveAttribute) this.delegate).setSqlParameter(index, ps, o, databaseTimeZone, databaseType);
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        this.delegate.zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, object, method, databaseTimezone, dt, tempArray);
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        this.delegate.zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, data, databaseTimezone, dt);
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        this.delegate.zPopulateAggregateDataValue(position, value, data);
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        this.delegate.serializeNonNullAggregateDataValue(valueWrappedInNullable, out);
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return this.delegate.deserializeNonNullAggregateDataValue(in);
    }

    public void parseNumberAndSet(double value, Owner data, int lineNumber) throws ParseException
    {
        this.delegate.parseNumberAndSet(value, data, lineNumber);
    }

    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format) throws ParseException
    {
        this.delegate.parseStringAndSet(value, data, lineNumber, format);
    }

    public void parseWordAndSet(String word, Owner data, int lineNumber) throws ParseException
    {
        this.delegate.parseWordAndSet(word, data, lineNumber);
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return this.delegate.zGetSqlForDatabaseType(databaseType);
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return this.delegate.zConstructNullUpdateWrapper(data);
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, Owner reladomoObject) throws IOException
    {
        throw new RuntimeException("not implemented");
    }
}
