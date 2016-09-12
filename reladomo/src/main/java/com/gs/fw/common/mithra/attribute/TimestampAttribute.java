
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
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.aggregate.attribute.TimestampAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorTimestamp;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorTimestamp;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.TimestampDayOfMonthCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.TimestampMonthCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.TimestampYearCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.TimestampProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.TimestampUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.asofop.AsOfExtractor;
import com.gs.fw.common.mithra.finder.timestamp.TimestampAsOfEqualityMapper;
import com.gs.fw.common.mithra.util.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public abstract class TimestampAttribute<Owner> extends NonPrimitiveAttribute<Owner, Timestamp> implements com.gs.fw.finder.attribute.TimestampAttribute<Owner>, AsOfExtractor<Owner>, TemporalAttribute
{
    public static final TimeZone NO_CONVERSION_TIMEZONE = new SimpleTimeZone(0,"NO_CONVERSION");

    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
    public static final byte CONVERT_TO_DATABASE = 10;
    public static final byte CONVERT_NONE = 20;
    public static final byte CONVERT_TO_UTC = 30;
    public static final byte NANOSECOND_PRECISION = 0;
    public static final byte MILLISECOND_PRECISION = 1;
    private static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();

    private transient byte conversionType = CONVERT_NONE;
    private transient boolean setAsString = false;
    private transient boolean isAsOfAttributeTo;
    private transient byte precision;
    private transient Timestamp infinity;

    private static final long serialVersionUID = -7152457756671643121L;

    protected TimestampAttribute()
    {
    }

    protected void setTimestampProperties(byte conversionType, boolean setAsString, boolean isAsOfAttributeTo, Timestamp infinity, byte precision)
    {
        this.conversionType = conversionType;
        this.setAsString = setAsString;
        this.isAsOfAttributeTo = isAsOfAttributeTo;
        this.infinity = infinity;
        this.precision = precision;
    }

    protected byte getConversionType()
    {
        return conversionType;
    }

    protected byte getPrecision()
    {
        return this.precision;
    }

    public boolean isSetAsString()
    {
        return setAsString;
    }

    @Override
    public long timestampValueOfAsLong(Owner o)
    {
        Timestamp timestamp = this.timestampValueOf(o);
        return timestamp == null ? TimestampPool.OFF_HEAP_NULL : timestamp.getTime();
    }

    public boolean isAsOfAttributeTo()
    {
        return isAsOfAttributeTo;
    }

    public Timestamp getAsOfAttributeInfinity()
    {
        return infinity;
    }

    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq((Timestamp) other);
    }

    public abstract Operation eq(Timestamp other);

    public abstract Operation notEq(Timestamp other);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(TimestampAttribute other);

    public abstract Operation joinEq(TimestampAttribute other);

    public abstract Operation filterEq(TimestampAttribute other);

    public abstract Operation notEq(TimestampAttribute other);

    public Operation eq(AsOfAttribute other)
    {
        return new MappedOperation(new TimestampAsOfEqualityMapper(this, other, true), new All(other));
    }

    public abstract Operation greaterThan(Timestamp target);

    public abstract Operation greaterThanEquals(Timestamp target);

    public abstract Operation lessThan(Timestamp target);

    public abstract Operation lessThanEquals(Timestamp target);

    public abstract Operation eq(Date other);

    public abstract Operation notEq(Date other);

    public Timestamp valueOf(Owner o)
    {
        return this.timestampValueOf(o);
    }

    public void setValue(Owner o, Timestamp newValue)
    {
        this.setTimestampValue(o, newValue);
    }

    public EqualityMapper constructEqualityMapper(AsOfAttribute right)
    {
        return new TimestampAsOfEqualityMapper(this, right);
    }

    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        Timestamp timestamp = (Timestamp) o;
        if (o == null)
        {
            ps.setNull(index, java.sql.Types.TIMESTAMP);
        }
        else
        {
            setTimestampOnPreparedStatement(ps, index, timestamp, databaseType, databaseTimeZone);
        }
    }

    public Timestamp zConvertTimezoneIfNecessary(Timestamp timestamp, TimeZone databaseTimeZone)
    {
        if (this.isAsOfAttributeTo() && timestamp.getTime() == this.getAsOfAttributeInfinity().getTime())
        {
            // nothing to do
        }
        else if (databaseTimeZone != NO_CONVERSION_TIMEZONE)
        {
            if (this.requiresConversionFromUtc())
            {
                timestamp = MithraTimestamp.createUtcTime(timestamp);
            }
            else if (databaseTimeZone != null && this.requiresConversionFromDatabaseTime())
            {
                timestamp = MithraTimestamp.createDatabaseTime(timestamp, databaseTimeZone);
            }
        }
        return timestamp;
    }

    public Timestamp zFixPrecisionAndInfinityIfNecessary(Timestamp timestamp, TimeZone databaseTimeZone)
    {
        if (this.precision == MILLISECOND_PRECISION)
        {
            int nanos = timestamp.getNanos();
            nanos = nanos / 1000000 * 1000000; // truncate the nanos down to millis
            timestamp.setNanos(nanos);
        }
        if (this.isAsOfAttributeTo())
        {
            timestamp = MithraTimestamp.zFixInfinity(timestamp, this.getConversionTimeZone(databaseTimeZone),  this.getAsOfAttributeInfinity());
        }
        return timestamp;
    }

    private void setTimestampOnPreparedStatement(PreparedStatement ps, int index, Timestamp timestamp, DatabaseType databaseType, TimeZone databaseTimeZone)
            throws SQLException
    {
        TimeZone timeZone = getConversionTimeZone(databaseTimeZone);
        if (databaseTimeZone != NO_CONVERSION_TIMEZONE && this.isAsOfAttributeTo() && timestamp != null && timestamp.getTime() == this.getAsOfAttributeInfinity().getTime())
        {
            timeZone = MithraTimestamp.DefaultTimeZone;
        }
        databaseType.setTimestamp(ps, index, timestamp, this.setAsString, timeZone);
    }

    public boolean requiresConversionFromUtc()
    {
        return this.conversionType == CONVERT_TO_UTC;
    }

    public boolean requiresConversionFromDatabaseTime()
    {
        return this.conversionType == CONVERT_TO_DATABASE;
    }

    public boolean requiresNoTimezoneConversion()
    {
        return this.conversionType == CONVERT_NONE;
    }

    public Class valueType()
    {
        return Timestamp.class;
    }

    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format) throws ParseException
    {
        Date date = (Date)format.parseObject(value);
        this.setTimestampValue(data, new Timestamp(date.getTime()));
    }

      @Override
      public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
      {
          tempArray[0] = zReadTimestampFromResultSet(resultSetPosition, rs, databaseTimezone, dt);
          try
          {
              method.invoke(object, tempArray);
          }
          catch (IllegalArgumentException e)
          {
              throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + " of class " + object.getClass().getName(), e);
          }
          catch (IllegalAccessException e)
          {
              throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
          }
          catch (InvocationTargetException e)
          {
              throw new MithraBusinessException("Error invoking method " + method.getName() + " of class " + object.getClass().getName(), e);
          }
      }

    public Timestamp zReadTimestampFromResultSet(int position, ResultSet rs, TimeZone databaseTimezone, DatabaseType dt) throws SQLException
    {
        return zFixPrecisionAndInfinityIfNecessary(dt.getTimestampFromResultSet(rs, position, this.getConversionTimeZone(databaseTimezone)), databaseTimezone);
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        data.setValueAt(dataPosition, new MutableComparableReference<Timestamp>(
                zReadTimestampFromResultSet(resultSetPosition, rs, databaseTimezone, dt)));
    }

    protected TimeZone getConversionTimeZone(TimeZone databaseTimeZone)
    {
        TimeZone timeZone = DEFAULT_TIMEZONE;
        if (databaseTimeZone != NO_CONVERSION_TIMEZONE)
        {
            if (this.requiresConversionFromUtc())
            {
                timeZone = MithraTimestamp.UtcTimeZone;
            }
            else if (databaseTimeZone != null && this.requiresConversionFromDatabaseTime())
            {
                timeZone = databaseTimeZone;
            }
        }
        return timeZone;
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableComparableReference<Timestamp>((Timestamp) value));
    }

    protected void serializedNonNullValue(Owner o, ObjectOutput out) throws IOException
    {
        writeToStream(out, this.timestampValueOf(o));
    }

    protected void deserializedNonNullValue(Owner o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setTimestampValue(o, this.readFromStream(in));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        writeToStream(out, ((MutableComparableReference<Timestamp>) valueWrappedInNullable).getValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableComparableReference<Timestamp>(this.readFromStream(in));
    }

    public Timestamp readFromStream(ObjectInput in) throws IOException
    {
        Timestamp result;
        Timestamp infinityDate = this.getAsOfAttributeInfinity();
        if (infinityDate != null)
        {
            if (this.requiresNoTimezoneConversion())
            {
                result = MithraTimestamp.readTimezoneInsensitiveTimestampWithInfinity(in, infinityDate);
            }
            else
            {
                result = MithraTimestamp.readTimestampWithInfinity(in, infinityDate);
            }
        }
        else
        {
            if (this.requiresNoTimezoneConversion())
            {
                result = MithraTimestamp.readTimezoneInsensitiveTimestamp(in);
            }
            else
            {
                result = MithraTimestamp.readTimestamp(in);
            }
        }
        return result;
    }

    public void writeToStream(ObjectOutput out, Timestamp date) throws IOException
    {
        Timestamp infinityDate = this.getAsOfAttributeInfinity();
        if (infinityDate != null)
        {
            if (this.requiresNoTimezoneConversion())
            {
                MithraTimestamp.writeTimezoneInsensitiveTimestampWithInfinity(out, date, infinityDate);
            }
            else
            {
                MithraTimestamp.writeTimestampWithInfinity(out, date, infinityDate);
            }
        }
        else
        {
            if (this.requiresNoTimezoneConversion())
            {
                MithraTimestamp.writeTimezoneInsensitiveTimestamp(out, date);
            }
            else
            {
                MithraTimestamp.writeTimestamp(out, date);
            }
        }
    }

    public void setValueUntil(Owner o, Timestamp newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    protected void setUntil(Owner o, Timestamp timestamp, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        return formatter.format(this.timestampValueOf(object));
    }

    public Timestamp getDataSpecificValue(MithraDataObject data)
    {
        throw new RuntimeException("Not implemented");
    }

    public boolean dataMatches(Object data, Timestamp asOfDate, AsOfAttribute asOfAttribute)
    {
        return asOfAttribute.dataMatches(data, asOfDate);
    }

    public boolean matchesMoreThanOne()
    {
        return false;
    }

    public int valueHashCode(Owner o)
    {
        long val = this.timestampValueOfAsLong(o);
        if (val == TimestampPool.OFF_HEAP_NULL) return HashUtil.NULL_HASH;
        return HashUtil.hash(val);
    }

    public boolean valueEquals(Owner first, Owner second)
    {
        if (first == second) return true;
        Timestamp firstValue = this.timestampValueOf(first);
        Timestamp secondValue = this.timestampValueOf(second);
        if (firstValue == secondValue) return true; // takes care of both null

        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public <O> boolean valueEquals(Owner first, O second, Extractor<O, Timestamp> secondExtractor)
    {
        Timestamp firstValue = this.timestampValueOf(first);
        Object secondValue = secondExtractor.valueOf(second);
        if (firstValue == secondValue) return true; // takes care of both null

        return (firstValue != null) && firstValue.equals(secondValue);
    }

    public TimestampAggregateAttribute min()
    {
        return new TimestampAggregateAttribute(new MinCalculatorTimestamp(this));
    }

    public TimestampAggregateAttribute max()
    {
        return new TimestampAggregateAttribute(new MaxCalculatorTimestamp(this));
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForTimestamp();
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new TimestampUpdateWrapper(this, data, null);
    }

    public boolean isInfiniteNull()
    {
        return false;
    }

    public abstract void forEach(final TimestampProcedure proc, Owner o, Object context);

    public IntegerAttribute year()
    {
        return new CalculatedIntegerAttribute(new TimestampYearCalculator(this));
    }

    public IntegerAttribute month()
    {
        return new CalculatedIntegerAttribute(new TimestampMonthCalculator(this));
    }

    public IntegerAttribute dayOfMonth()
    {
        return new CalculatedIntegerAttribute(new TimestampDayOfMonthCalculator(this));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(ImmutableTimestamp.ZERO);
    }
}
