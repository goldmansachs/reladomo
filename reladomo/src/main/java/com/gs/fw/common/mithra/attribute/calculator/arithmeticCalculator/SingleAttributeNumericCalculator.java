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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.math.BigDecimal;
import java.util.Set;

public abstract class SingleAttributeNumericCalculator<T extends Attribute> implements NumericAttributeCalculator
{
    protected T attribute;

    public SingleAttributeNumericCalculator(T attribute)
    {
        this.attribute = attribute;
    }

    public boolean isAttributeNull(Object o)
    {
        return attribute.isAttributeNull(o);
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return attribute.getOwnerPortal();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return attribute.getTopLevelPortal();
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return attribute.getAsOfAttributes();
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        attribute.zAddDepenedentAttributesToSet(set);
    }

    public void addDependentPortalsToSet(Set set)
    {
        attribute.zAddDependentPortalsToSet(set);
    }

    public String getTopOwnerClassName()
    {
        return attribute.zGetTopOwnerClassName();
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleIntegerAttribute(pos, attribute.getMetaData().isNullable(), tupleTempContext);
    }

    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        attribute.setUpdateCountDetachedMode(isDetachedMode);
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

    @Override
    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.attribute.generateMapperSql(query);
    }

    @Override
    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.doubleValueOf(o), context);
        }
    }

    @Override
    public void forEach(FloatProcedure proc, Object o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.floatValueOf(o), context);
        }
    }

    @Override
    public void forEach(LongProcedure proc, Object o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.longValueOf(o), context);
        }
    }

    @Override
    public void forEach(IntegerProcedure proc, Object o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.intValueOf(o), context);
        }
    }

    @Override
    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.bigDecimalValueOf(o), context);
        }
    }

    @Override
    public Operation createMappedOperation()
    {
        return this.attribute.zCreateMappedOperation();
    }

    @Override
    public double doubleValueOf(Object o)
    {
        return intValueOf(o);
    }

    @Override
    public float floatValueOf(Object o)
    {
        return intValueOf(o);
    }

    @Override
    public long longValueOf(Object o)
    {
        return intValueOf(o);
    }

    @Override
    public BigDecimal bigDecimalValueOf(Object o)
    {
        return BigDecimal.valueOf(intValueOf(o));
    }
}
