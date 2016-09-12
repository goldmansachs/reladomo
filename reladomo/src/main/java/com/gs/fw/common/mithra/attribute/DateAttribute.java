
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
import com.gs.fw.common.mithra.aggregate.attribute.DateAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorDate;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorDate;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.DateDayOfMonthCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.DateMonthCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.DateYearCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DateProcedure;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.DateUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.DateExtractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.util.MutableComparableReference;

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
import java.util.*;

public abstract class DateAttribute<Owner> extends NonPrimitiveAttribute<Owner, Date> implements com.gs.fw.finder.attribute.DateAttribute<Owner>, DateExtractor<Owner>
{

    private static SimpleDateFormat parserDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final long serialVersionUID = -4807319830338599755L;

    private transient boolean setAsString = false;

    protected void setDateProperties(boolean setAsString)
    {
        this.setAsString = setAsString;
    }

    protected boolean isSetAsString()
    {
        return setAsString;
    }

    protected Date normalizeDate(Date incoming)
    {
        Calendar cal = new GregorianCalendar();
        cal.setTime(incoming);
        cal.set(Calendar.AM_PM, Calendar.AM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq((Date) other);
    }

    public abstract Operation eq(Date other);

    public abstract Operation notEq(Date other);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(DateAttribute other);

    public abstract Operation joinEq(DateAttribute other);

    public abstract Operation filterEq(DateAttribute other);

    public abstract Operation notEq(DateAttribute other);

    public abstract Operation greaterThan(Date target);

    public abstract Operation greaterThanEquals(Date target);

    public abstract Operation lessThan(Date target);

    public abstract Operation lessThanEquals(Date target);

    public Date valueOf(Owner o)
    {
        return this.dateValueOf(o);
    }

    public void setValue(Owner o, Date newValue)
    {
        this.setDateValue(o, newValue);
    }

    public Class valueType()
    {
        return Date.class;
    }

    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        if (o == null)
        {
            ps.setNull(index, java.sql.Types.DATE);
        }
        else
        {
            databaseType.setDate(ps, index, (Date) o, this.setAsString);
        }
    }

    public void parseStringAndSet(String value, Owner data, int lineNumber, Format format) throws ParseException
    {
        Date date = parserDateFormat.parse(value);
        this.setDateValue(data, date);
    }

    public void setValueUntil(Owner o, Date newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    protected void setUntil(Owner o, Date date, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }
    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        tempArray[0] = rs.getDate(resultSetPosition);
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
        data.setValueAt(dataPosition, new MutableComparableReference<Date>(rs.getDate(resultSetPosition)));
    }

    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableComparableReference<Date>((Date) value));
    }

    public String valueOfAsString(Owner object, Formatter formatter)
    {
        return formatter.format(this.dateValueOf(object));
    }

    public DateAggregateAttribute min()
    {
        return new DateAggregateAttribute(new MinCalculatorDate(this));
    }

    public DateAggregateAttribute max()
    {
        return new DateAggregateAttribute(new MaxCalculatorDate(this));
    }

    public Date readFromStream(ObjectInput in) throws IOException
    {
        return MithraTimestamp.readTimezoneInsensitiveDate(in);
    }

    public void writeToStream(ObjectOutput out, Date date) throws IOException
    {
        MithraTimestamp.writeTimezoneInsensitiveDate(out, date);
    }

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForDateTime();
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new DateUpdateWrapper(this, data, null);
    }

    @Override
    public long dateValueOfAsLong(Owner valueHolder)
    {
        return dateValueOf(valueHolder).getTime();
    }

    public abstract void forEach(final DateProcedure proc, Owner o, Object context);

    public IntegerAttribute year()
    {
        return new CalculatedIntegerAttribute(new DateYearCalculator(this));
    }

    public IntegerAttribute month()
    {
        return new CalculatedIntegerAttribute(new DateMonthCalculator(this));
    }

    public IntegerAttribute dayOfMonth()
    {
        return new CalculatedIntegerAttribute(new DateDayOfMonthCalculator(this));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(new Date(0));
    }
}
