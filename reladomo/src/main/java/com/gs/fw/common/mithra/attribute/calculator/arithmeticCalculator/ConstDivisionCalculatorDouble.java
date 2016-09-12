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
import com.gs.fw.common.mithra.attribute.PrimitiveNumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbstractSingleAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.WrappedProcedureAndContext;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.NullHandlingProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.BigDecimalUtil;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;

import java.math.BigDecimal;



public class ConstDivisionCalculatorDouble extends AbstractSingleAttributeCalculator
{

    private double divisor;

    public ConstDivisionCalculatorDouble(PrimitiveNumericAttribute attribute, double divisor)
    {
        super(attribute);
        this.divisor = divisor;
    }

    public int intValueOf(Object o)
    {
        return (int) doubleValueOf(o);
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return BigDecimal.valueOf(doubleValueOf(o));
    }

    public float floatValueOf(Object o)
    {
        return (float) doubleValueOf(o);
    }

    public long longValueOf(Object o)
    {
        return (long) doubleValueOf(o);
    }

    public double doubleValueOf(Object o)
    {
        return ((DoubleExtractor)this.attribute).doubleValueOf(o) / divisor;
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
        if (obj instanceof ConstDivisionCalculatorDouble)
        {
            ConstDivisionCalculatorDouble other = (ConstDivisionCalculatorDouble) obj;
            return this.attribute.equals(other.attribute) && this.divisor == other.divisor;
        }
        return false;
    }

    public int hashCode()
    {
        return HashUtil.combineHashes(0x3742A2C4 ^ this.attribute.hashCode(), HashUtil.hash(divisor));
    }

    public boolean execute(double object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(object / divisor, realContext.getWrappedContext());
    }

    public boolean executeForNull(Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((NullHandlingProcedure)realContext.getWrappedProcedure()).executeForNull(realContext.getWrappedContext());
    }

    public boolean execute(int object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((double)object) / divisor, realContext.getWrappedContext());
    }

    public boolean execute(float object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((double)object) / divisor, realContext.getWrappedContext());
    }

    public boolean execute(long object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((double)object) / divisor, realContext.getWrappedContext());
    }

    public boolean execute(BigDecimal object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        BigDecimal bigDivisor = BigDecimal.valueOf(divisor);
        int scale = BigDecimalUtil.calculateQuotientScale(object.precision(), object.scale(), bigDivisor.precision(), bigDivisor.scale());
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute(BigDecimalUtil.divide(scale, object,bigDivisor), realContext.getWrappedContext());
    }
}
