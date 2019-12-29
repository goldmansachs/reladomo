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
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.FloatExtractor;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.EqualityMapper;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.floatop.FloatEqOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatGreaterThanOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatInOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatLessThanOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatNotEqOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatNotInOperation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;
import org.eclipse.collections.api.set.primitive.FloatSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Set;


public class CalculatedFloatAttribute<T> extends FloatAttribute<T>
{
    private NumericAttributeCalculator calculator;

    public CalculatedFloatAttribute(NumericAttributeCalculator calculator)
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

    public boolean isAttributeNull(T o)
    {
        return this.calculator.isAttributeNull(o);
    }

    public float floatValueOf(T o)
    {
        return this.calculator.floatValueOf(o);
    }

    public void setFloatValue(T o, float newValue)
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
        return Float.class;
    }

    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation eq(float other)
    {
        return new FloatEqOperation(this, other);
    }

    public Operation notEq(float other)
    {
        return new FloatNotEqOperation(this, other);
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    public void setValue(T o, Float newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(T o, Float newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    public void setUntil(T o, Float newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(this.floatValueOf(o));
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
            return this.floatValueOf(first) == this.floatValueOf(second);
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Float> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.floatValueOf(first) == ((FloatExtractor) secondExtractor).floatValueOf(second);
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

    public Float valueOf(T anObject)
    {
        return this.isAttributeNull(anObject) ? null : Float.valueOf(this.floatValueOf(anObject));
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    public Operation eq(int parameter)
    {
        return new FloatEqOperation(this, parameter);
    }

    public Operation notEq(int other)
    {
         return new FloatNotEqOperation(this, other);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(com.gs.collections.api.set.primitive.FloatSet floatSet)
    {
        Operation op;
        switch (floatSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(floatSet.floatIterator().next());
                break;
            default:
                op = new FloatInOperation(this, floatSet);
                break;
        }

        return op;
    }

    @Override
    public Operation in(FloatSet floatSet)
    {
        Operation op;
        switch (floatSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(floatSet.floatIterator().next());
                break;
            default:
                op = new FloatInOperation(this, floatSet);
                break;
        }

        return op;
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(com.gs.collections.api.set.primitive.FloatSet floatSet)
    {
        Operation op;
        switch (floatSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(floatSet.floatIterator().next());
                break;
            default:
                op = new FloatNotInOperation(this, floatSet);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(FloatSet floatSet)
    {
        Operation op;
        switch (floatSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(floatSet.floatIterator().next());
                break;
            default:
                op = new FloatNotInOperation(this, floatSet);
                break;
        }

        return op;
    }

    // join operation:
    public Operation eq(FloatAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation joinEq(FloatAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation filterEq(FloatAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(FloatAttribute other)
    {
        return null;
    }

    public Operation greaterThan(float parameter)
    {
        return new FloatGreaterThanOperation(this, parameter);
    }

    public Operation greaterThanEquals(float parameter)
    {
        return new FloatGreaterThanEqualsOperation(this, parameter);
    }

    public Operation lessThan(float parameter)
    {
        return new FloatLessThanOperation(this, parameter);
    }

    public Operation lessThanEquals(float parameter)
    {
        return new FloatLessThanEqualsOperation(this, parameter);
    }

    public void forEach(FloatProcedure proc, Object o, Object context)
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

    // join operation:
    public Operation eq(IntegerAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(IntegerAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    // todo: rezaem: create an in operation that takes an extractor
// this is tricky, because it requires a new set that not only takes a hashcode strategy
// but has a new method: contains(object, hashStrategy)
/*
    public Operation in(List dataHolders, Extractor extractor)
    {
        return new InOperationWithExtractor(this, dataHolders, extractor);
    }
*/

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
        return other instanceof CalculatedFloatAttribute && ((CalculatedFloatAttribute)other).calculator.equals(this.calculator);
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

    public void forEach(DoubleProcedure proc, Object obj, Object context)
    {
        //this.calculator.forEach(proc, o, context);
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
