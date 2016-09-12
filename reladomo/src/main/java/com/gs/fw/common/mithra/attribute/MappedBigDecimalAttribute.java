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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.set.primitive.DoubleSet;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;

import java.math.BigDecimal;
import java.util.Set;


public class MappedBigDecimalAttribute<Owner> extends BigDecimalAttribute<Owner> implements MappedAttribute
{
    private BigDecimalAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedBigDecimalAttribute(BigDecimalAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedBigDecimalAttribute)
        {
            MappedBigDecimalAttribute ma = (MappedBigDecimalAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (BigDecimalAttribute) ma.getWrappedAttribute();
            this.parentSelector = new ChainedAttributeValueSelector(this.parentSelector, ma.getParentSelector());
        }
    }

    @Override
    public String getAttributeName()
    {
        if (super.getAttributeName() == null)
        {
            computeMappedAttributeName(this.wrappedAttribute, this.parentSelector);
        }
        return super.getAttributeName();
    }

    @Override
    public int getScale()
    {
        return this.wrappedAttribute.getScale();
    }

    @Override
    public int getPrecision()
    {
        return this.wrappedAttribute.getPrecision();
    }

    @Override
    public Attribute getSourceAttribute()
    {
        return this.wrappedAttribute.getSourceAttribute();
    }

    @Override
    public SourceAttributeType getSourceAttributeType()
    {
        return this.wrappedAttribute.getSourceAttributeType();
    }

    @Override
    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.wrappedAttribute.getAsOfAttributes();
    }

    public Function getParentSelector()
    {
        return parentSelector;
    }

    public MappedAttribute cloneForNewMapper(Mapper mapper, Function parentSelector)
    {
        throw new RuntimeException("not implemented");
    }

    public Attribute getWrappedAttribute()
    {
        return wrappedAttribute;
    }

    public Mapper getMapper()
    {
        return mapper;
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        if (parentSelector == null) return null;
        Object result = this.parentSelector.valueOf(o);
        if (result == null) return null;
        return wrappedAttribute.bigDecimalValueOf(result);
    }

    public void setBigDecimalValue(Object o, BigDecimal newValue)
    {
        if (parentSelector == null) return;
        Object result = this.parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setBigDecimalValue(result, newValue);
    }

    @Override
    public boolean isAttributeNull(Object o)
    {
        return MappedAttributeUtil.isAttributeNull(o, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void setValueNull(Object o)
    {
        MappedAttributeUtil.setValueNull(o, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public Operation eq(BigDecimal other)
    {
        return new MappedOperation(mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(BigDecimal other)
    {
        return new MappedOperation(mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation in(Set<BigDecimal> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(set));
    }

    @Override
    public Operation notIn(Set<BigDecimal> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(set));
    }

    @Override
    public Operation eq(BigDecimalAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(BigDecimalAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.joinEq(other));
    }

    @Override
    public Operation filterEq(BigDecimalAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation isNull()
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.isNull());
    }

    @Override
    public Operation isNotNull()
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.isNotNull());
    }

    @Override
    public Operation notEq(BigDecimalAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public Operation greaterThan(BigDecimal target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(BigDecimal target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(BigDecimal target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(BigDecimal target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
    }

    @Override
    public Operation eq(double other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(double other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation in(DoubleSet doubleSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(doubleSet));
    }

    @Override
    public Operation notIn(DoubleSet doubleSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(doubleSet));
    }

    @Override
    public Operation greaterThan(double target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(double target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(double target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(double target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
    }

    public Object readResolve()
    {
        return this;
    }

    @Override
    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        this.mapper.pushMappers(query);
        String result = this.wrappedAttribute.getFullyQualifiedLeftHandExpression(query);
        this.mapper.popMappers(query);
        return result;
    }

    @Override
    public void zAppendToString(ToStringContext toStringContext)
    {
        toStringContext.pushMapper(this.mapper);
        super.zAppendToString(toStringContext);
        toStringContext.popMapper();
    }

    @Override
    public String zGetTopOwnerClassName()
    {
       return this.mapper.getResultOwnerClassName();
    }


    public boolean equals(Object other)
    {
        if (other instanceof MappedBigDecimalAttribute)
        {
            MappedBigDecimalAttribute o = (MappedBigDecimalAttribute) other;
            return this.wrappedAttribute.equals(o.wrappedAttribute) && this.mapper.equals(o.mapper);
        }
        return false;
    }

    public int hashCode()
    {
        return this.wrappedAttribute.hashCode() ^ this.mapper.hashCode();
    }

    @Override
    public MithraObjectPortal getOwnerPortal()
    {
        return this.wrappedAttribute.getOwnerPortal();
    }

    @Override
    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a mapped attribute");
    }

    @Override
    public boolean isSourceAttribute()
    {
        return this.wrappedAttribute.isSourceAttribute();
    }

    @Override
    public MithraObjectPortal getTopLevelPortal()
    {
        return this.mapper.getResultPortal();
    }

    @Override
    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.mapper.generateSql(query);
        this.wrappedAttribute.generateMapperSql(query);
        this.mapper.popMappers(query);
    }

        // ByteAttribute operands

    @Override
    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // ShortAttribute operands

    @Override
    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // IntegerAttribute operands

    @Override
    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // LongAttribute operands

    @Override
    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // FloatAttribute operands

    @Override
    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // DoubleAttribute operands

    @Override
    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }


    // BigDecimalAttribute operands

    @Override
    public BigDecimalAttribute plus(BigDecimalAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute minus(BigDecimalAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute times(BigDecimalAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public BigDecimalAttribute dividedBy(BigDecimalAttribute attribute)
    {
        return (BigDecimalAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }


    @Override
    public void forEach(final LongProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    public void forEach(final DoubleProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void forEach(final FloatProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void forEach(final IntegerProcedure proc, Object o, final Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void forEach(final ObjectProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public TupleAttribute tupleWith(Attribute attr)
    {
        return MappedAttributeUtil.tupleWith(this, attr);
    }

    @Override
    public TupleAttribute tupleWith(Attribute... attrs)
    {
        return MappedAttributeUtil.tupleWith(this, attrs);
    }

    @Override
    public TupleAttribute tupleWith(TupleAttribute attr)
    {
        return MappedAttributeUtil.tupleWith(this, attr);
    }

    @Override
    public Operation zCreateMappedOperation()
    {
        Operation op = this.wrappedAttribute.zCreateMappedOperation();
        if (op == NoOperation.instance())
        {
            op = new All(this.mapper.getAnyRightAttribute());
        }
        return new MappedOperation(this.mapper, op);
    }

    @Override
    public boolean isNullable()
    {
        return this.wrappedAttribute.isNullable();
    }
}
