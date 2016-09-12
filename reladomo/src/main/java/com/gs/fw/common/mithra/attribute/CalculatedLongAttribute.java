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

package com.gs.fw.common.mithra.attribute;

import com.gs.collections.api.set.primitive.LongSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.LongExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.longop.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Set;


public class CalculatedLongAttribute<T> extends LongAttribute<T>
{
    private NumericAttributeCalculator calculator;

    public CalculatedLongAttribute(NumericAttributeCalculator calculator)
    {
        this.calculator = calculator;
    }

    public String zGetTopOwnerClassName()
    {
        return this.calculator.getTopOwnerClassName();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return this.calculator.getTopLevelPortal();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.calculator.getOwnerPortal();
    }

    public Object readResolve()
    {
        return this;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.calculator.getAsOfAttributes();
    }

    protected void serializedNonNullValue(Object o, ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    protected void deserializedNonNullValue(Object o, ObjectInput in) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public NumericAttributeCalculator getCalculator()
    {
        return this.calculator;
    }

    public boolean isAttributeNull(Object o)
    {
        return this.calculator.isAttributeNull(o);
    }

    public void setLongValue(Object o, double newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        return this.calculator.getFullyQualifiedCalculatedExpression(query);
    }

    @Override
    public void zAppendToString(ToStringContext toStringContext)
    {
        this.calculator.appendToString(toStringContext);
    }

    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a calculated attribute");
    }

    public Class valueType()
    {
        return Double.class;
    }

    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    public void setValue(T o, Long newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(T o, Long newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void setUntil(Object o, long newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(Object o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(this.longValueOf(o));
    }

    public boolean valueEquals(T first, T second)
    {
        if (first == second)
        {
            return true;
        }
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.longValueOf(first) == this.longValueOf(second);
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Long> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.longValueOf(first) == ((LongExtractor) secondExtractor).longValueOf(second);
        }
        return true;
    }

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public Long valueOf(T anObject)
    {
        return isAttributeNull(anObject) ? null : Long.valueOf(this.longValueOf(anObject));
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    public Operation eq(long parameter)
    {
        return new LongEqOperation(this, parameter);
    }

    public Operation notEq(long other)
    {
        return new LongNotEqOperation(this, other);
    }

    @Override
    public Operation in(LongSet longSet)
    {
        Operation op;
        switch (longSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(longSet.longIterator().next());
                break;
            default:
                op = new LongInOperation(this, longSet);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(LongSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.longIterator().next());
                break;
            default:
                op = new LongNotInOperation(this, set);
                break;
        }

        return op;
    }

    public Operation greaterThanEquals(long parameter)
    {
        return new LongGreaterThanEqualsOperation(this, parameter);
    }

    public Operation greaterThan(long parameter)
    {
        return new LongGreaterThanOperation(this, parameter);
    }

    public Operation lessThan(long parameter)
    {
        return new LongLessThanOperation(this, parameter);
    }

    public Operation lessThanEquals(long parameter)
    {
        return new LongLessThanEqualsOperation(this, parameter);
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        throw new RuntimeException("should never get here");
    }

    public Operation eq(LongAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation joinEq(LongAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation filterEq(LongAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(LongAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public int getUpdateCount()
    {
        return this.calculator.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return this.calculator.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        this.calculator.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        this.calculator.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        this.calculator.rollbackUpdateCount();
    }

    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        return other instanceof CalculatedLongAttribute && ((CalculatedLongAttribute)other).calculator.equals(this.calculator);
    }

    public int hashCode()
    {
        return super.hashCode() ^ this.calculator.hashCode();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.calculator.generateMapperSql(query);
    }

    public Operation zCreateMappedOperation()
    {
        return this.calculator.createMappedOperation();
    }

    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
       this.calculator.forEach(proc, o, context);
    }

    public void forEach(FloatProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(LongProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public long longValueOf(Object o)
    {
        return this.calculator.longValueOf(o);
    }

    public void setLongValue(Object o, long newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void zAddDependentPortalsToSet(Set set)
    {
        this.calculator.addDependentPortalsToSet(set);
    }

    public void zAddDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }
}

