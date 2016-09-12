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

package com.gs.fw.common.mithra.attribute.calculator;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.PrimitiveNumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.ToStringContext;

import java.util.Set;
import java.math.BigDecimal;



public abstract class AbstractAbsoluteValueCalculator extends AbstractSingleAttributeCalculator
{

    public AbstractAbsoluteValueCalculator(NumericAttribute attribute)
    {
        super(attribute);
    }

    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return "abs("+this.attribute.getFullyQualifiedLeftHandExpression(query)+")";
    }

    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("abs(");
        ((Attribute)this.attribute).zAppendToString(toStringContext);
        toStringContext.append(")");
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj.getClass().equals(this.getClass()))
        {
            return this.attribute.equals(((AbstractAbsoluteValueCalculator)obj).attribute);
        }
        return false;
    }

    public int hashCode()
    {
        return 0x12345678 ^ this.attribute.hashCode();
    }

    public boolean executeForNull(Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((NullHandlingProcedure)realContext.getWrappedProcedure()).executeForNull(realContext.getWrappedContext());
    }

    public boolean execute(double object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((DoubleProcedure)realContext.getWrappedProcedure()).execute(Math.abs(object), realContext.getWrappedContext());
    }

    public boolean execute(int object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((IntegerProcedure)realContext.getWrappedProcedure()).execute(Math.abs(object), realContext.getWrappedContext());
    }

    public boolean execute(float object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((FloatProcedure)realContext.getWrappedProcedure()).execute(Math.abs(object), realContext.getWrappedContext());
    }

    public boolean execute(long object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((LongProcedure)realContext.getWrappedProcedure()).execute(Math.abs(object), realContext.getWrappedContext());
    }

        public boolean execute(BigDecimal object, Object context)
    {
        WrappedProcedureAndContext realContext = (WrappedProcedureAndContext) context;
        return ((BigDecimalProcedure)realContext.getWrappedProcedure()).execute(object.abs(), realContext.getWrappedContext());
    }

    public void addDependentPortalsToSet(Set set)
    {
        this.attribute.zAddDependentPortalsToSet(set);
    }

    public int getScale()
    {
        return 0;
    }

    public int getPrecision()
    {
        return 0;
    }

}
