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
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import java.util.Date;

public class DateYearCalculator extends SingleAttributeNumericCalculator<DateAttribute>
{
    public DateYearCalculator(DateAttribute dateAttribute)
    {
        super(dateAttribute);
    }

    @Override
    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return "(" + query.getDatabaseType().getSqlExpressionForDateYear(attribute.getFullyQualifiedLeftHandExpression(query)) + ")";
    }

    @Override
    public int intValueOf(Object o)
    {
        Date date = this.attribute.valueOf(o);
        DateTime time = new DateTime(date);
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
        LocalDate localDateBefore = new LocalDate().withYear(value).withDayOfMonth(1).withMonthOfYear(1);
        LocalDate localDateAfter = new LocalDate().withYear(value + 1).withDayOfMonth(1).withMonthOfYear(1);

        return this.attribute.greaterThanEquals(localDateBefore.toDate()).and(attribute.lessThan(localDateAfter.toDate()));
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj.getClass().equals(this.getClass()))
        {
            return this.attribute.equals(((DateYearCalculator)obj).attribute);
        }
        return false;
    }

    public int hashCode()
    {
        return 0x45678123 ^ this.attribute.hashCode();
    }
}
