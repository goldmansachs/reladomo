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

package com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator;

import com.gs.fw.common.mithra.attribute.CalculatedIntegerAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbstractAbsoluteValueCalculator;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;

public class TimestampYearCalculator extends SingleAttributeNumericCalculator<TimestampAttribute>
{
    public TimestampYearCalculator(TimestampAttribute timestampAttribute)
    {
        super(timestampAttribute);
    }

    @Override
    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        int conversion;
        if(attribute.requiresConversionFromUtc())
        {
            conversion = TimestampAttribute.CONVERT_TO_UTC;
        }
        else if(attribute.requiresConversionFromDatabaseTime())
        {
            conversion = TimestampAttribute.CONVERT_TO_DATABASE;
        }
        else
        {
            conversion = TimestampAttribute.CONVERT_NONE;
        }

        return "(" + query.getDatabaseType().getSqlExpressionForTimestampYear(attribute.getFullyQualifiedLeftHandExpression(query), conversion, query.getTimeZone()) + ")";
    }

    @Override
    public int intValueOf(Object o)
    {
        Timestamp timestamp = this.attribute.valueOf(o);
        DateTime time = new DateTime(timestamp.getTime());
        return time.getYear();
    }

    @Override
    public int getScale()
    {
        return 0;
    }

    @Override
    public int getPrecision()
    {
        return 4;
    }

    @Override
    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("year(");
        this.attribute.zAppendToString(toStringContext);
        toStringContext.append(")");
    }

    @Override
    public Operation optimizedIntegerEq(int value, CalculatedIntegerAttribute intAttribute)
    {
        DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(MithraTimestamp.DefaultTimeZone);
        DateTime dateBefore = new DateTime().withZone(dateTimeZone).withYear(value).withDayOfMonth(1).withMonthOfYear(1).withTimeAtStartOfDay();
        DateTime dateAfter = new DateTime().withZone(dateTimeZone).withYear(value + 1).withDayOfMonth(1).withMonthOfYear(1).withTimeAtStartOfDay();

        Timestamp timestampBefore = new Timestamp(dateBefore.getMillis());
        Timestamp timestampAfter = new Timestamp(dateAfter.getMillis());

        return this.attribute.greaterThanEquals(timestampBefore).and(attribute.lessThan(timestampAfter));
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj.getClass().equals(this.getClass()))
        {
            return this.attribute.equals(((TimestampYearCalculator)obj).attribute);
        }
        return false;
    }

    public int hashCode()
    {
        return 0x78123456 ^ this.attribute.hashCode();
    }
}
