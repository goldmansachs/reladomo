
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

import com.gs.fw.common.mithra.util.*;
import org.slf4j.Logger;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.aggregate.attribute.IntegerAggregateAttribute;
import com.gs.fw.common.mithra.aggregate.attribute.TimestampAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.TimestampProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;


public class TimestampAttributeAsOfAttributeToInfiniteNull<Owner> extends SingleColumnTimestampAttribute<Owner>
{

    private SingleColumnTimestampAttribute timestampAttribute;

    protected TimestampAttributeAsOfAttributeToInfiniteNull(TimestampAttribute t)
    {
        this.timestampAttribute = (SingleColumnTimestampAttribute)t;
    }


    public void setColumnName(String columnName)
    {
        timestampAttribute.setColumnName(columnName);
    }

    public boolean isSourceAttribute()
    {
        return timestampAttribute.isSourceAttribute();
    }

    public String getColumnName()
    {
        return timestampAttribute.getColumnName();
    }

    public Operation eq(Timestamp other)
    {
        return timestampAttribute.eq(other);
    }

    public Operation notEq(Timestamp other)
    {
        return timestampAttribute.notEq(other);
    }

    public Operation in(Set set)
    {
        return (Operation) timestampAttribute.in(set);
    }

    public Operation notIn(Set set)
    {
        return (Operation) timestampAttribute.notIn(set);
    }

    public Operation eq(AsOfAttribute other)
    {
        return timestampAttribute.eq(other);
    }

    public Operation eq(TimestampAttribute other)
    {
        return timestampAttribute.eq(other);
    }

    public Operation joinEq(TimestampAttribute other)
    {
        return timestampAttribute.joinEq(other);
    }

    public Operation filterEq(TimestampAttribute other)
    {
        return timestampAttribute.filterEq(other);
    }

    public Operation notEq(TimestampAttribute other)
    {
        return timestampAttribute.notEq(other);
    }

    public Operation greaterThan(Timestamp target)
    {
        return timestampAttribute.greaterThan(target);
    }

    public Operation greaterThanEquals(Timestamp target)
    {
        return timestampAttribute.greaterThanEquals(target);
    }

    public Operation lessThan(Timestamp target)
    {
        return timestampAttribute.lessThan(target);
    }

    public Operation lessThanEquals(Timestamp target)
    {
        return timestampAttribute.lessThanEquals(target);
    }

    public Operation eq(Date other)
    {
        return timestampAttribute.eq(other);
    }

    public Operation notEq(Date other)
    {
        return timestampAttribute.notEq(other);
    }

    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        return timestampAttribute.getFullyQualifiedLeftHandExpression(query);
    }

    public void forEach(TimestampProcedure proc, Owner o, Object context)
    {
        timestampAttribute.forEach(proc, o, context);
    }

    public void setVersionAttributeSqlParameters(PreparedStatement pps, MithraDataObject mdo, int pos, TimeZone databaseTimeZone)
    throws SQLException
    {
        timestampAttribute.setVersionAttributeSqlParameters(pps, mdo, pos, databaseTimeZone);
    }

    public Object writeReplace() throws ObjectStreamException
    {
        return timestampAttribute.writeReplace();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        timestampAttribute.appendColumnDefinition(sb, dt, sqlLogger, false);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        return timestampAttribute.verifyColumn(info);
    }

    public void setTimestampProperties(byte conversionType, boolean setAsString, boolean isAsOfAttributeTo, Timestamp infinity, byte precision)
    {
        timestampAttribute.setTimestampProperties(conversionType, setAsString, isAsOfAttributeTo, infinity, precision);
    }

    public boolean isSetAsString()
    {
        return timestampAttribute.isSetAsString();
    }

    public boolean isAsOfAttributeTo()
    {
        return timestampAttribute.isAsOfAttributeTo();
    }

    public Timestamp getAsOfAttributeInfinity()
    {
        return timestampAttribute.getAsOfAttributeInfinity();
    }

    public Operation nonPrimitiveEq(Object other)
    {
        return timestampAttribute.nonPrimitiveEq(other);
    }

    public EqualityMapper constructEqualityMapper(AsOfAttribute right)
    {
        return timestampAttribute.constructEqualityMapper(right);
    }

    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType)
    throws SQLException
    {
        timestampAttribute.setSqlParameter(index, ps, o, databaseTimeZone, databaseType);
    }

    public Timestamp zConvertTimezoneIfNecessary(Timestamp timestamp, TimeZone databaseTimeZone)
    {
        return timestampAttribute.zConvertTimezoneIfNecessary(timestamp, databaseTimeZone);
    }

    public Timestamp zFixPrecisionAndInfinityIfNecessary(Timestamp timestamp, TimeZone databaseTimeZone)
    {
        return timestampAttribute.zFixPrecisionAndInfinityIfNecessary(timestamp, databaseTimeZone);
    }

    public boolean requiresConversionFromUtc()
    {
        return timestampAttribute.requiresConversionFromUtc();
    }

    public boolean requiresConversionFromDatabaseTime()
    {
        return timestampAttribute.requiresConversionFromDatabaseTime();
    }

    public boolean requiresNoTimezoneConversion()
    {
        return timestampAttribute.requiresNoTimezoneConversion();
    }

    public Class valueType()
    {
        return timestampAttribute.valueType();
    }

    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format)
    throws ParseException
    {
        timestampAttribute.parseStringAndSet(value, data, lineNumber, format);
    }

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
    throws SQLException
    {
        timestampAttribute.setSqlParameters(pps, mithraDataObject, pos, databaseTimeZone, databaseType);
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray)
            throws SQLException
    {
       timestampAttribute.zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, object, method, databaseTimezone, dt, tempArray);
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
    throws SQLException
    {
        timestampAttribute.zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, data, databaseTimezone, dt);
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        timestampAttribute.zPopulateAggregateDataValue(position, value, data);
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        timestampAttribute.serializeNonNullAggregateDataValue(valueWrappedInNullable, out);
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return timestampAttribute.deserializeNonNullAggregateDataValue(in);
    }

    public void serializedNonNullValue(Owner o, ObjectOutput out)
    throws IOException
    {
        timestampAttribute.serializedNonNullValue(o, out);
    }

    public void deserializedNonNullValue(Owner o, ObjectInput in)
    throws IOException, ClassNotFoundException
    {
        timestampAttribute.deserializedNonNullValue(o, in);
    }

    public Timestamp readFromStream(ObjectInput in)
    throws IOException
    {
        return timestampAttribute.readFromStream(in);
    }

    public void writeToStream(ObjectOutput out, Timestamp date)
    throws IOException
    {
        timestampAttribute.writeToStream(out, date);
    }

    public void setValueUntil(Owner o, Timestamp newValue, Timestamp exclusiveUntil)
    {
        timestampAttribute.setValueUntil(o, newValue, exclusiveUntil);
    }

    public void setUntil(Owner o, Timestamp timestamp, Timestamp exclusiveUntil)
    {
        timestampAttribute.setUntil(o, timestamp, exclusiveUntil);
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        return timestampAttribute.valueOfAsString(object, formatter);
    }

    public Timestamp getDataSpecificValue(MithraDataObject data)
    {
        return timestampAttribute.getDataSpecificValue(data);
    }

    public boolean dataMatches(Object data, Timestamp asOfDate, AsOfAttribute asOfAttribute)
    {
        return timestampAttribute.dataMatches(data, asOfDate, asOfAttribute);
    }

    public boolean matchesMoreThanOne()
    {
        return timestampAttribute.matchesMoreThanOne();
    }

    public int valueHashCode(Owner o)
    {
        return timestampAttribute.valueHashCode(o);
    }

    public boolean valueEquals(Owner first, Owner second)
    {
        return timestampAttribute.valueEquals(first, second);
    }

    public <O> boolean valueEquals(Owner first, O second, Extractor<O, Timestamp> secondExtractor)
    {
        return timestampAttribute.valueEquals(first, second, secondExtractor);
    }

    public TimestampAggregateAttribute min()
    {
        return timestampAttribute.min();
    }

    public TimestampAggregateAttribute max()
    {
        return timestampAttribute.max();
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return timestampAttribute.zGetSqlForDatabaseType(databaseType);
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return timestampAttribute.zConstructNullUpdateWrapper(data);
    }

    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        return timestampAttribute.constructEqualityMapper(right);
    }

    public boolean isAttributeNull(Owner o)
    {
        return timestampAttribute.isAttributeNull(o);
    }

    public OrderBy ascendingOrderBy()
    {
        return timestampAttribute.ascendingOrderBy();
    }

    public OrderBy descendingOrderBy()
    {
        return timestampAttribute.descendingOrderBy();
    }

    public void setValueNull(Owner o)
    {
        timestampAttribute.setValueNull(o);
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        return timestampAttribute.in(objects, extractor);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        return timestampAttribute.in(objects, extractor);
    }

    public Set newSetForInClause()
    {
        return timestampAttribute.newSetForInClause();
    }

    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        return timestampAttribute.zInWithMax(maxInClause, objects, extractor);
    }

    public void setValueNullUntil(Owner o, Timestamp exclusiveUntil)
    {
        timestampAttribute.setValueNullUntil(o, exclusiveUntil);
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        return timestampAttribute.zCountUniqueInstances(dataObjects);
    }

//todo:goldmaa: how to deal with name classh: have the same erasure
/*
    public Operation eq(Object other)
    {
        return timestampAttribute.eq(other);
    }

    public Operation notEq(Object other)
    {
        return timestampAttribute.notEq(other);
    }
*/

    public void setAll(String attributeName, String busClassNameWithDots, String busClassName, boolean isNullable, RelatedFinder relatedFinder, Map properties, boolean transactional)
    {
        timestampAttribute.setAll(attributeName, busClassNameWithDots, busClassName, isNullable, relatedFinder, properties, transactional);
    }

    public Object getProperty(String key)
    {
        return timestampAttribute.getProperty(key);
    }

    public String getAttributeName()
    {
        return timestampAttribute.getAttributeName();
    }

    public Operation isNull()
    {
        return timestampAttribute.isNull();
    }

    public Operation isNotNull()
    {
        return timestampAttribute.isNotNull();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return timestampAttribute.getOwnerPortal();
    }

    public void serializeValue(Owner o, ObjectOutput out)
    throws IOException
    {
        timestampAttribute.serializeValue(o, out);
    }

    public void deserializeValue(Owner o, ObjectInput in)
    throws IOException, ClassNotFoundException
    {
        timestampAttribute.deserializeValue(o, in);
    }

    public int getUpdateCount()
    {
        return timestampAttribute.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return timestampAttribute.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        timestampAttribute.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        timestampAttribute.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        timestampAttribute.rollbackUpdateCount();
    }

    public SourceAttributeType getSourceAttributeType()
    {
        return timestampAttribute.getSourceAttributeType();
    }

    public Attribute getSourceAttribute()
    {
        return timestampAttribute.getSourceAttribute();
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return timestampAttribute.getAsOfAttributes();
    }

    public Operation joinEqWithSourceAndAsOfCheck(Attribute other)
    {
        return timestampAttribute.joinEqWithSourceAndAsOfCheck(other);
    }

    public Mapper constructEqualityMapperWithAsOfCheck(Attribute other)
    {
        return timestampAttribute.constructEqualityMapperWithAsOfCheck(other);
    }

    public boolean isAsOfAttribute()
    {
        return timestampAttribute.isAsOfAttribute();
    }

    public void parseNumberAndSet(double value, Owner data, int lineNumber)
    throws ParseException
    {
        timestampAttribute.parseNumberAndSet(value, data, lineNumber);
    }

    public void parseWordAndSet(String word, Owner data, int lineNumber)
    throws ParseException
    {
        timestampAttribute.parseWordAndSet(word, data, lineNumber);
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return timestampAttribute.getTopLevelPortal();
    }

    public Operation zGetMapperOperation()
    {
        return timestampAttribute.zGetMapperOperation();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        timestampAttribute.generateMapperSql(query);
    }

    public Operation zCreateMappedOperation()
    {
        return timestampAttribute.zCreateMappedOperation();
    }

    public boolean zFindDeepRelationshipInMemory(Operation op)
    {
        return timestampAttribute.zFindDeepRelationshipInMemory(op);
    }

    public void forEach(ObjectProcedure proc, Object newValue, Object context)
    {
        timestampAttribute.forEach(proc, newValue, context);
    }

    public NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder, NumericAttribute wrappedAttribute)
    {
        return timestampAttribute.createMappedAttributeWithMapperRemainder(mapperRemainder, wrappedAttribute);
    }

    public NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder)
    {
        return timestampAttribute.createMappedAttributeWithMapperRemainder(mapperRemainder);
    }

    public NumericAttribute createOtherMappedAttributeWithMapperRemainder(MappedAttribute mappedAttribute, Mapper otherMapperRemainder)
    {
        return timestampAttribute.createOtherMappedAttributeWithMapperRemainder(mappedAttribute, otherMapperRemainder);
    }

    public IntegerAggregateAttribute count()
    {
        return timestampAttribute.count();
    }

    public Rep getSerializationReplacement()
    {
        return timestampAttribute.getSerializationReplacement();
    }

    public String getBusClassName()
    {
        return timestampAttribute.getBusClassName();
    }

    public String getDataClassName()
    {
        return timestampAttribute.getBusClassNameWithDots();
    }

    public boolean isNullable()
    {
        return timestampAttribute.isNullable();
    }

    public String toString()
    {
        return timestampAttribute.toString();
    }

    public void appendNullable(StringBuilder sb, DatabaseType dt)
    {
        timestampAttribute.appendNullable(sb, dt);
    }

    public boolean hasSameVersion(MithraDataObject first, MithraDataObject second)
    {
        return timestampAttribute.hasSameVersion(first, second);
    }


    public Timestamp timestampValueOf(Owner o)
    {
        Timestamp result = timestampAttribute.timestampValueOf(o);
        if (result == null)
        {
            return NullDataTimestamp.getInstance();
        }
        return result;
    }

    public void setTimestampValue(Owner o, Timestamp newValue)
    {
        if (newValue == NullDataTimestamp.getInstance())
        {
            newValue = null;
        }
        timestampAttribute.setTimestampValue(o, newValue);
    }

    public Timestamp valueOf(Owner o)
    {
        Timestamp result =  timestampAttribute.valueOf(o);
        if (result == null)
        {
            return NullDataTimestamp.getInstance();
        }
        return result;
    }

    public void setValue(Owner o, Timestamp newValue)
    {
        if (newValue == NullDataTimestamp.getInstance())
        {
            newValue = null;
        }
        timestampAttribute.setValue(o, newValue);
    }

    public boolean isInfiniteNull()
    {
        return true;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        final TimestampAttributeAsOfAttributeToInfiniteNull that = (TimestampAttributeAsOfAttributeToInfiniteNull) o;

        if (!timestampAttribute.equals(that.timestampAttribute)) return false;

        return true;
    }

    public int hashCode()
    {
        int result = super.hashCode();
        result = 29 * result + timestampAttribute.hashCode();
        return result;
    }

}
