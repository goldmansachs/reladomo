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
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbstractSingleAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.WrappedProcedureAndContext;
import com.gs.fw.common.mithra.attribute.calculator.procedure.NullHandlingProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.BigDecimalUtil;
import com.gs.fw.common.mithra.extractor.BigDecimalExtractor;
import java.math.BigDecimal;



public class ConstMultiplicationCalculatorBigDecimal extends AbstractSingleAttributeCalculator
{

    private BigDecimal multiplicand;
    private int scale;
    private int precision;

    public ConstMultiplicationCalculatorBigDecimal(NumericAttribute attribute, BigDecimal multiplicand)
    {
        super(attribute);
        this.multiplicand = multiplicand;
        this.scale = BigDecimalUtil.calculateProductScale(attribute.getScale(), multiplicand.scale());
        this.precision = BigDecimalUtil.calculateProductPrecision(attribute.getPrecision(), multiplicand.precision());
    }
    
    public int getScale()
    {
        return this.scale;
    }

    public int getPrecision()
    {
        return this.precision;
    }

    public int intValueOf(Object o)
    {
        return this.bigDecimalValueOf(o).intValue();
    }

    public float floatValueOf(Object o)
    {
        return this.bigDecimalValueOf(o).floatValue();
    }

    public long longValueOf(Object o)
    {
        return this.bigDecimalValueOf(o).longValue();
    }

    public double doubleValueOf(Object o)
    {
        return this.bigDecimalValueOf(o).doubleValue();
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return ((BigDecimalExtractor)this.attribute).bigDecimalValueOf(o).multiply(multiplicand);
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
        toStringContext.append(multiplicand.toString());
        toStringContext.append(")");
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj instanceof ConstMultiplicationCalculatorBigDecimal)
        {
            ConstMultiplicationCalculatorBigDecimal other = (ConstMultiplicationCalculatorBigDecimal) obj;
            return this.attribute.equals(other.attribute) && this.multiplicand.equals(other.multiplicand);
        }
        return false;
    }

    public int hashCode()
    {
        return HashUtil.combineHashes(0x3742A274 ^ this.attribute.hashCode(), HashUtil.hash(multiplicand));
    }

    public boolean execute(double object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute((BigDecimal.valueOf(object)).multiply(multiplicand), realContext.getWrappedContext());
    }

    public boolean executeForNull(Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((NullHandlingProcedure)realContext.getWrappedProcedure()).executeForNull(realContext.getWrappedContext());
    }

    public boolean execute(int object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute((BigDecimal.valueOf(object)).multiply(multiplicand), realContext.getWrappedContext());
    }

    public boolean execute(float object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute((BigDecimal.valueOf(object)).multiply(multiplicand), realContext.getWrappedContext());
    }

    public boolean execute(long object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute((BigDecimal.valueOf(object)).multiply(multiplicand), realContext.getWrappedContext());
    }

    public boolean execute(BigDecimal object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute(object.multiply(multiplicand), realContext.getWrappedContext());
    }
}
