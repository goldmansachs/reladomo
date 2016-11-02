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
import com.gs.fw.common.mithra.aggregate.attribute.TimeAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorTime;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorTime;
import com.gs.fw.common.mithra.attribute.calculator.procedure.TimeProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.TimeUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.TimeExtractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.MutableComparableReference;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

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
import java.util.Map;
import java.util.TimeZone;

public abstract class TimeAttribute<Owner> extends NonPrimitiveAttribute<Owner, Time> implements com.gs.fw.finder.attribute.TimeAttribute<Owner>, TimeExtractor<Owner>
{
    private static final long serialVersionUID = 0L;

    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq((Time) other);
    }

    public abstract Operation eq(Time other);

    public abstract Operation notEq(Time other);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(TimeAttribute other);

    public abstract Operation joinEq(TimeAttribute other);

    public abstract Operation filterEq(TimeAttribute other);

    public abstract Operation notEq(TimeAttribute other);

    public abstract Operation greaterThan(Time target);

    public abstract Operation greaterThanEquals(Time target);

    public abstract Operation lessThan(Time target);

    public abstract Operation lessThanEquals(Time target);

    public Time valueOf(Owner o)
    {
        return this.timeValueOf(o);
    }

    public void setValue(Owner o, Time newValue)
    {
        this.setTimeValue(o, newValue);
    }

    public Class valueType()
    {
        return Time.class;
    }

    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        if (o == null)
        {
            databaseType.setTimeNull(ps, index);
        }
        else
        {
            databaseType.setTime(ps, index, (Time) o);
        }
    }

    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format) throws ParseException
    {
        int hour = 0;
        int start = 0;
        int pos = start;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "hour", pos, c, ':')) break;
            hour = parseDigit(value, "hour", pos, hour, c);
            pos++;
        }
        start = pos + 1;
        pos = start;
        int min = 0;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "minutes", pos, c, ':')) break;
            min = parseDigit(value, "minutes", pos, min, c);
            pos++;
        }

        start = pos + 1;
        pos = start;
        int sec = 0;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            if (checkEnd(value, start, "seconds", pos, c, '.')) break;
            sec = parseDigit(value, "seconds", pos, sec, c);
            pos++;
        }
        int milli = 0;
        pos++;
        while(pos < value.length())
        {
            char c = value.charAt(pos);
            milli = parseDigit(value, "milliseconds", pos, milli, c);
            pos++;
        }
        if (hour > 23)
        {
            throw new ParseException("Hour too large in "+value, 0);
        }
        if(milli > 999)
        {
            throw new ParseException("Milli too large (must be three digits) in "+value, 0);
        }
        this.setTimeValue(data, Time.withMillis(hour, min, sec, milli));
    }

    private int parseDigit(String value, String timePartName, int pos, int sec, char c) throws ParseException
    {
        if (c >= '0' && c <= '9')
        {
            sec *= 10;
            sec += (c - '0');
        }
        else
        {
            throw new ParseException("Could not parse " + timePartName + " in " +value, pos);
        }
        return sec;
    }

    private boolean checkEnd(String value, int start, String timePartName, int pos, char c, char end) throws ParseException
    {
        if (c == end)
        {
            if (pos == start)
            {
                throw new ParseException("Could not parse " + timePartName + " in " +value, pos);
            }
            return true;
        }
        return false;
    }

    public void setValueUntil(Owner o, Time newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    protected void setUntil(Owner o, Time time, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        tempArray[0] = dt.getTime(rs, resultSetPosition);
        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalArgumentException e)
        {
            throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + "  of class " + object.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + "of class " + object.getClass().getName(), e);
        }
    }

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        data.setValueAt(dataPosition, new MutableComparableReference<Time>(dt.getTime(rs, resultSetPosition)));
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableComparableReference<Time>((Time) value));
    }

    public TimeAggregateAttribute min()
    {
        return new TimeAggregateAttribute(new MinCalculatorTime(this));
    }

    public TimeAggregateAttribute max()
    {
        return new TimeAggregateAttribute(new MaxCalculatorTime(this));
    }

    public Time readFromStream(ObjectInput in) throws IOException
    {
        return Time.readFromStream(in);
    }

    public void writeToStream(ObjectOutput out, Time time) throws IOException
    {
        Time.writeToStream(out, time);
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForTime();
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new TimeUpdateWrapper(this, data, null);
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        return "";
    }

    @Override
    public long offHeapTimeValueOfAsLong(Owner valueHolder)
    {
        return timeValueOf(valueHolder).getOffHeapTime();
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(Time.withMillis(0, 0, 0, 0));
    }

    public abstract void forEach(final TimeProcedure proc, Owner o, Object context);

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, Owner reladomoObject) throws IOException
    {
        writer.writeTime(context, this.getAttributeName(), this.timeValueOf(reladomoObject));
    }
}
