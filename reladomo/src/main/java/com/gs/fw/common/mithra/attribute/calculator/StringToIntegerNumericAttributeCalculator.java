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
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.math.BigDecimal;
import java.util.Set;

public class StringToIntegerNumericAttributeCalculator implements NumericAttributeCalculator
{
    private StringAttribute attribute;

    public StringToIntegerNumericAttributeCalculator(StringAttribute attribute)
    {
        this.attribute = attribute;
    }

    @Override
    public boolean isAttributeNull(Object o)
    {
        return attribute.isAttributeNull(o);
    }

    @Override
    public MithraObjectPortal getOwnerPortal()
    {
        return attribute.getOwnerPortal();
    }

    @Override
    public MithraObjectPortal getTopLevelPortal()
    {
        return attribute.getTopLevelPortal();
    }

    @Override
    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return query.getDatabaseType().getConversionFunctionStringToInteger(this.attribute.getFullyQualifiedLeftHandExpression(query));
    }

    @Override
    public void addDepenedentAttributesToSet(Set set)
    {
        this.attribute.zAddDepenedentAttributesToSet(set);
    }

    @Override
    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.attribute.getAsOfAttributes();
    }

    @Override
    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.attribute.generateMapperSql(query);
    }

    @Override
    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void forEach(FloatProcedure proc, Object o, Object context)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void forEach(LongProcedure proc, Object o, Object context)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void forEach(IntegerProcedure proc, Object o, Object context)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public double doubleValueOf(Object o)
    {
        return this.intValueOf(o);
    }

    @Override
    public float floatValueOf(Object o)
    {
        return this.intValueOf(o);
    }

    @Override
    public long longValueOf(Object o)
    {
        return this.intValueOf(o);
    }

    @Override
    public int intValueOf(Object o)
    {
        try
        {
            return Integer.parseInt(this.attribute.stringValueOf(o));
        }
        catch (NumberFormatException e)
        {
            return 0; // we'll ignore it
        }
    }

    @Override
    public BigDecimal bigDecimalValueOf(Object o)
    {
        return BigDecimal.valueOf(intValueOf(o));
    }

    @Override
    public void addDependentPortalsToSet(Set set)
    {
        attribute.zAddDependentPortalsToSet(set);
    }

    @Override
    public Operation createMappedOperation()
    {
        return this.attribute.zCreateMappedOperation();
    }

    @Override
    public String getTopOwnerClassName()
    {
        return attribute.zGetTopOwnerClassName();
    }

    @Override
    public int getScale()
    {
        return 0;
    }

    @Override
    public int getPrecision()
    {
        return 0;
    }

    @Override
    public void appendToString(ToStringContext toStringContext)
    {
        toStringContext.append("toInteger(");
        this.attribute.zAppendToString(toStringContext);
        toStringContext.append(")");
    }

    @Override
    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleIntegerAttribute(pos, true, tupleTempContext);
    }

    @Override
    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        this.attribute.setUpdateCountDetachedMode(isDetachedMode);
    }

    @Override
    public int getUpdateCount()
    {
        return this.attribute.getUpdateCount();
    }

    @Override
    public int getNonTxUpdateCount()
    {
        return this.attribute.getNonTxUpdateCount();
    }

    @Override
    public void incrementUpdateCount()
    {
        this.attribute.incrementUpdateCount();
    }

    @Override
    public void commitUpdateCount()
    {
        this.attribute.commitUpdateCount();
    }

    @Override
    public void rollbackUpdateCount()
    {
        this.attribute.rollbackUpdateCount();
    }

    @Override
    public Operation optimizedIntegerEq(int value, CalculatedIntegerAttribute intAttribute)
    {
        return this.attribute.eq(Integer.toString(value));
    }
}
