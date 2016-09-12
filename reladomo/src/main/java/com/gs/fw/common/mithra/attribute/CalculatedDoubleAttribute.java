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

import com.gs.collections.api.set.primitive.DoubleSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.doubleop.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Set;


public class CalculatedDoubleAttribute<T> extends DoubleAttribute<T> implements DoubleExtractor<T, Double>
{
    private NumericAttributeCalculator calculator;

    public CalculatedDoubleAttribute(NumericAttributeCalculator calculator)
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

    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");
    }

    public NumericAttributeCalculator getCalculator()
    {
        return this.calculator;
    }

    public boolean isAttributeNull(T o)
    {
        return this.calculator.isAttributeNull(o);
    }

    public double doubleValueOf(T o)
    {
        return this.calculator.doubleValueOf(o);
    }

    public void setDoubleValue(T o, double newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(T o)
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

    public void setValue(T o, Double newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(T o, Double newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    public void setUntil(T o, Double newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(this.doubleValueOf(o));
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
            return this.doubleValueOf(first) == this.doubleValueOf(second);
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Double> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.doubleValueOf(first) == ((DoubleExtractor) secondExtractor).doubleValueOf(second);
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

    public Double valueOf(T anObject)
    {
        return (this.isAttributeNull(anObject)) ? null : Double.valueOf(this.doubleValueOf(anObject));
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    public Operation eq(double parameter)
    {
        return new DoubleEqOperation(this, parameter);
    }

    public Operation notEq(double other)
    {
        return new DoubleNotEqOperation(this, other);
    }

    @Override
    public Operation in(DoubleSet doubleSet)
    {
        Operation op;
        switch (doubleSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(doubleSet.doubleIterator().next());
                break;
            default:
                op = new DoubleInOperation(this, doubleSet);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(DoubleSet doubleSet)
    {
        Operation op;
        switch (doubleSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(doubleSet.doubleIterator().next());
                break;
            default:
                op = new DoubleNotInOperation(this, doubleSet);
                break;
        }

        return op;
    }

    public Operation greaterThanEquals(double parameter)
    {
        return new DoubleGreaterThanEqualsOperation(this, parameter);
    }

    public Operation greaterThan(double parameter)
    {
        return new DoubleGreaterThanOperation(this, parameter);
    }

    public Operation lessThan(double parameter)
    {
        return new DoubleLessThanOperation(this, parameter);
    }

    public Operation lessThanEquals(double parameter)
    {
        return new DoubleLessThanEqualsOperation(this, parameter);
    }

    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        throw new RuntimeException("should never get here");
    }

    public Operation eq(DoubleAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation joinEq(DoubleAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation filterEq(DoubleAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(DoubleAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public int getUpdateCount()
    {
        return this.calculator.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return calculator.getNonTxUpdateCount();
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
        return other instanceof CalculatedDoubleAttribute &&  ((CalculatedDoubleAttribute)other).calculator.equals(this.calculator);
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

    public void zAddDependentPortalsToSet(Set set)
    {
        this.calculator.addDependentPortalsToSet(set);
    }

    public void zAddDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }
}
