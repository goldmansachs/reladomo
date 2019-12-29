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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.extractor.BigDecimalExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.NonPrimitiveEqOperation;
import com.gs.fw.common.mithra.finder.NonPrimitiveNotEqOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalGreaterThanOperation;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalLessThanOperation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;
import org.eclipse.collections.api.set.primitive.DoubleSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Set;


public class CalculatedBigDecimalAttribute<T> extends BigDecimalAttribute<T>  implements BigDecimalExtractor<T, BigDecimal>
{
    private NumericAttributeCalculator calculator;

    public CalculatedBigDecimalAttribute(NumericAttributeCalculator calculator)
    {
        this.calculator = calculator;
    }

    public int getScale()
    {
        return this.calculator.getScale();
    }

    public int getPrecision()
    {
        return this.calculator.getPrecision();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return this.calculator.getTopLevelPortal();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.calculator.getOwnerPortal();
    }

    public String zGetTopOwnerClassName()
    {
     return this.calculator.getTopOwnerClassName();
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

    protected void deserializedNonNullValue(Object o, ObjectInput in) throws IOException, ClassNotFoundException
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

    public BigDecimal bigDecimalValueOf(Object o)
    {
        return this.calculator.bigDecimalValueOf(o);
    }

    public void setBigDecimalValue(Object o, BigDecimal newValue)
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


    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    public void setValue(T o, BigDecimal newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(T o, BigDecimal newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    public void setUntil(Object o, Object newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(Object o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(this.bigDecimalValueOf(o));
    }

    public boolean valueEquals(Object first, Object second)
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
            return this.bigDecimalValueOf(first).equals(this.bigDecimalValueOf(second));
        }
        return true;
    }

    public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.bigDecimalValueOf(first).equals(((BigDecimalExtractor) secondExtractor).bigDecimalValueOf(second));
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

    public BigDecimal valueOf(T anObject)
    {
        return (this.isAttributeNull(anObject)) ? null : this.bigDecimalValueOf(anObject);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    public Operation eq(BigDecimal parameter)
    {
        return new NonPrimitiveEqOperation(this, parameter);
    }

    public Operation notEq(BigDecimal parameter)
    {
        return new NonPrimitiveNotEqOperation(this,parameter);
    }

    public Operation greaterThanEquals(BigDecimal parameter)
    {
        return new BigDecimalGreaterThanEqualsOperation(this, parameter);
    }

    public Operation greaterThan(BigDecimal parameter)
    {
        return new BigDecimalGreaterThanOperation(this, parameter);
    }

    public Operation lessThan(BigDecimal parameter)
    {
        return new BigDecimalLessThanOperation(this, parameter);
    }

    public Operation lessThanEquals(BigDecimal parameter)
    {
        return new BigDecimalLessThanEqualsOperation(this, parameter);
    }

    public Operation eq(double other)
    {
        return eq(createBigDecimalFromDouble(other));
    }

    public Operation notEq(double other)
    {
        return notEq(createBigDecimalFromDouble(other));
    }

    @Override
    public Operation in(DoubleSet doubleSet)
    {
       return this.in(createBigDecimalSetFromDoubleSet(doubleSet));
    }

    @Override
    public Operation notIn(DoubleSet doubleSet)
    {
        return this.notIn(createBigDecimalSetFromDoubleSet(doubleSet));
    }

    public Operation greaterThan(double target)
    {
        return greaterThan(createBigDecimalFromDouble(target));
    }

    public Operation greaterThanEquals(double target)
    {
        return greaterThanEquals(createBigDecimalFromDouble(target));
    }

    public Operation lessThan(double target)
    {
        return lessThan(createBigDecimalFromDouble(target));
    }

    public Operation lessThanEquals(double target)
    {
        return lessThanEquals(createBigDecimalFromDouble(target));
    }

    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        throw new RuntimeException("should never get here");
    }

    public Operation eq(BigDecimalAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation joinEq(BigDecimalAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation filterEq(BigDecimalAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(BigDecimalAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
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
        return other instanceof CalculatedBigDecimalAttribute &&  ((CalculatedBigDecimalAttribute)other).calculator.equals(this.calculator);
    }

    public int hashCode()
    {
        return super.hashCode() ^ this.calculator.hashCode();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.calculator.generateMapperSql(query);
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
