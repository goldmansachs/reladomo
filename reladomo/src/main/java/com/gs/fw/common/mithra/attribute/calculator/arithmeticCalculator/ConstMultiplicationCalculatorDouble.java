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
import com.gs.fw.common.mithra.attribute.DoubleAttribute;import com.gs.fw.common.mithra.attribute.PrimitiveNumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbstractSingleAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.WrappedProcedureAndContext;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.NullHandlingProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.HashUtil;import com.gs.fw.common.mithra.extractor.DoubleExtractor;

import java.math.BigDecimal;



public class ConstMultiplicationCalculatorDouble extends AbstractSingleAttributeCalculator
{

    private double multiplicand;

    public ConstMultiplicationCalculatorDouble(PrimitiveNumericAttribute attribute, double multiplicand)
    {
        super(attribute);
        this.multiplicand = multiplicand;
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
        return ((DoubleExtractor)this.attribute).doubleValueOf(o) * multiplicand;
    }

    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return this.attribute.getFullyQualifiedLeftHandExpression(query) + " * " + multiplicand;
    }

    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("(");
        ((Attribute)this.attribute).zAppendToString(toStringContext);
        toStringContext.append("*");
        toStringContext.append(""+multiplicand);
        toStringContext.append(")");
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj instanceof ConstMultiplicationCalculatorDouble)
        {
            ConstMultiplicationCalculatorDouble other = (ConstMultiplicationCalculatorDouble) obj;
            return this.attribute.equals(other.attribute) && this.multiplicand == other.multiplicand;
        }
        return false;
    }

    public int hashCode()
    {
        return HashUtil.combineHashes(0x3742A232 ^ this.attribute.hashCode(), HashUtil.hash(multiplicand));
    }

    public boolean execute(double object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(object * multiplicand, realContext.getWrappedContext());
    }

    public boolean executeForNull(Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((NullHandlingProcedure)realContext.getWrappedProcedure()).executeForNull(realContext.getWrappedContext());
    }

    public boolean execute(int object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((double)object) * multiplicand, realContext.getWrappedContext());
    }

    public boolean execute(float object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((double)object) * multiplicand, realContext.getWrappedContext());
    }

    public boolean execute(long object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(((double)object) * multiplicand, realContext.getWrappedContext());
    }

    public boolean execute(BigDecimal object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute(object.multiply(BigDecimal.valueOf(multiplicand)), realContext.getWrappedContext());
    }
}
