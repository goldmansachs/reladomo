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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.attribute.calculator.AbstractSingleAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.WrappedProcedureAndContext;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.BigDecimalUtil;

import java.math.BigDecimal;



public class ConstDivisionCalculatorInteger extends AbstractSingleAttributeCalculator
{

    private int divisor;

    public ConstDivisionCalculatorInteger(IntegerAttribute attribute, int divisor)
    {
        super(attribute);
        this.divisor = divisor;
    }

    public int intValueOf(Object o)
    {
        return ((IntegerAttribute)this.attribute).intValueOf(o) / divisor;
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return BigDecimal.valueOf(intValueOf(o));
    }

    public float floatValueOf(Object o)
    {
        return intValueOf(o);
    }

    public long longValueOf(Object o)
    {
        return intValueOf(o);
    }

    public double doubleValueOf(Object o)
    {
        return intValueOf(o);
    }

    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return this.attribute.getFullyQualifiedLeftHandExpression(query) + " / " + divisor;
    }

    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("(");
        ((Attribute)this.attribute).zAppendToString(toStringContext);
        toStringContext.append("/");
        toStringContext.append(""+divisor);
        toStringContext.append(")");
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj instanceof ConstDivisionCalculatorInteger)
        {
            ConstDivisionCalculatorInteger other = (ConstDivisionCalculatorInteger) obj;
            return this.attribute.equals(other.attribute) && this.divisor == other.divisor;
        }
        return false;
    }

    public int hashCode()
    {
        return HashUtil.combineHashes(0x3742A2AA ^ this.attribute.hashCode(), divisor);
    }

    public boolean execute(double object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((int)object) / divisor, realContext.getWrappedContext());
    }

    public boolean executeForNull(Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((NullHandlingProcedure)realContext.getWrappedProcedure()).executeForNull(realContext.getWrappedContext());
    }

    public boolean execute(int object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((IntegerProcedure)realContext.getWrappedProcedure()).execute(object / divisor, realContext.getWrappedContext());
    }

    public boolean execute(float object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((FloatProcedure)realContext.getWrappedProcedure()).execute(((int)object) / divisor, realContext.getWrappedContext());
    }

    public boolean execute(long object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((LongProcedure)realContext.getWrappedProcedure()).execute(((int)object) / divisor, realContext.getWrappedContext());
    }

    public boolean execute(BigDecimal object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        BigDecimal bigDivisor = BigDecimal.valueOf(divisor);
        int scale = BigDecimalUtil.calculateQuotientScale(object.precision(), object.scale(), bigDivisor.precision(), bigDivisor.scale());
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute(BigDecimalUtil.divide(scale, object,bigDivisor), realContext.getWrappedContext());
    }
}
