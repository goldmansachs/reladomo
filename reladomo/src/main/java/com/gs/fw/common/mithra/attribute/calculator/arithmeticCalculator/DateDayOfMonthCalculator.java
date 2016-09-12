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
import com.gs.fw.common.mithra.finder.integer.IntegerEqOperation;
import org.joda.time.DateTime;

import java.util.Date;

public class DateDayOfMonthCalculator extends SingleAttributeNumericCalculator<DateAttribute>
{

    public DateDayOfMonthCalculator(DateAttribute dateAttribute)
    {
        super(dateAttribute);
    }

    @Override
    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return "(" + query.getDatabaseType().getSqlExpressionForDateDayOfMonth(attribute.getFullyQualifiedLeftHandExpression(query)) + ")";
    }

    @Override
    public int intValueOf(Object o)
    {
        Date date = this.attribute.valueOf(o);
        DateTime time = new DateTime(date);
        return time.getDayOfMonth();
    }

    @Override
    public int getScale()
    {
        return 0;
    }

    @Override
    public int getPrecision()
    {
        return 2;
    }

    @Override
    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append(attribute.getAttributeName()).append(".dayOfMonth");
    }

    @Override
    public Operation optimizedIntegerEq(int value, CalculatedIntegerAttribute intAttribute)
    {
        return new IntegerEqOperation(intAttribute, value);
    }
}
