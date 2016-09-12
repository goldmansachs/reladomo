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

import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.util.Set;
import java.math.BigDecimal;


public abstract class AbstractSingleAttributeCalculator implements NumericAttributeCalculator, DoubleProcedure, IntegerProcedure, FloatProcedure, LongProcedure, BigDecimalProcedure
{
    protected NumericAttribute attribute;

    public AbstractSingleAttributeCalculator(NumericAttribute attribute)
    {
        this.attribute = attribute;
    }

    public int getScale()
    {
        return this.attribute.getScale();
    }

    public int getPrecision()
    {
       return this.attribute.getPrecision();
    }

    @Override
    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleIntegerAttribute(pos, true, tupleTempContext);
    }

    public abstract int intValueOf(Object o);

    public abstract double doubleValueOf(Object o);

    public abstract float floatValueOf(Object o);

    public abstract long longValueOf(Object o);

    public abstract BigDecimal bigDecimalValueOf(Object o);

    public boolean isAttributeNull(Object o)
    {
        return this.attribute.isAttributeNull(o);
    }

    public String getTopOwnerClassName()
    {
        return this.attribute.zGetTopOwnerClassName();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.attribute.getOwnerPortal();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return this.attribute.getTopLevelPortal();
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.attribute.zAddDepenedentAttributesToSet(set);
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.attribute.getAsOfAttributes();
    }

    @Override
    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        this.attribute.setUpdateCountDetachedMode(isDetachedMode);
    }

    public int getUpdateCount()
    {
        return attribute.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return attribute.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        attribute.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        attribute.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        attribute.rollbackUpdateCount();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.attribute.generateMapperSql(query);
    }

    public Operation createMappedOperation()
    {
        return this.attribute.zCreateMappedOperation();
    }

    public void addDependentPortalsToSet(Set set)
    {
        this.attribute.zAddDependentPortalsToSet(set);
    }

    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
        context = new WrappedProcedureAndContext(proc, context);
        this.attribute.forEach((DoubleProcedure)this, o, context);
    }

    public void forEach(FloatProcedure proc, Object o, Object context)
    {
        context = new WrappedProcedureAndContext(proc, context);
        this.attribute.forEach((FloatProcedure)this, o, context);
    }

    public void forEach(LongProcedure proc, Object o, Object context)
    {
        context = new WrappedProcedureAndContext(proc, context);
        this.attribute.forEach((LongProcedure)this, o, context);
    }

    public void forEach(IntegerProcedure proc, Object o, Object context)
    {
        context = new WrappedProcedureAndContext(proc, context);
        this.attribute.forEach((IntegerProcedure)this, o, context);
    }

    public void forEach(BigDecimalProcedure proc, Object o, Object context)
     {
         context = new WrappedProcedureAndContext(proc, context);
         this.attribute.forEach((BigDecimalProcedure)this, o, context);
     }

    @Override
    public Operation optimizedIntegerEq(int value, CalculatedIntegerAttribute intAttribute)
    {
        return intAttribute.defaultEq(value);
    }
}
