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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.finder.*;

import java.math.BigDecimal;


public class MappedDoubleAttribute<T> extends DoubleAttribute<T> implements MappedAttribute
{
    private DoubleAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedDoubleAttribute(DoubleAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedDoubleAttribute)
        {
            MappedDoubleAttribute ma = (MappedDoubleAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (DoubleAttribute) ma.getWrappedAttribute();
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
    public String zGetTopOwnerClassName()
    {
        return this.mapper.getResultOwnerClassName();
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

    @Override
    public boolean isSourceAttribute()
    {
        return this.wrappedAttribute.isSourceAttribute();
    }

    public Function getParentSelector()
    {
        return parentSelector;
    }

    public MappedAttribute cloneForNewMapper(Mapper mapper, Function parentSelector)
    {
        return new MappedDoubleAttribute((DoubleAttribute) this.getWrappedAttribute(), mapper, parentSelector);
    }

    public Attribute getWrappedAttribute()
    {
        return wrappedAttribute;
    }

    public Mapper getMapper()
    {
        return mapper;
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

    public double doubleValueOf(Object o)
    {
        if (parentSelector == null) return 0;
        Object result = parentSelector.valueOf(o);
        if (result == null) return 0;
        return this.wrappedAttribute.doubleValueOf(result);
    }

    public void setDoubleValue(Object o, double newValue)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setDoubleValue(result, newValue);
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

    public void setValueNull(Object o)
    {
        MappedAttributeUtil.setValueNull(o, this.parentSelector, this.wrappedAttribute);
    }

    public boolean isAttributeNull(Object o)
    {
        return MappedAttributeUtil.isAttributeNull(o, this.parentSelector, this.wrappedAttribute);
    }

    public Object readResolve()
    {
        return this;
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
    public Operation notEq(DoubleAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
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

    @Override
    public Operation eq(DoubleAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(DoubleAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(DoubleAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public DoubleAttribute absoluteValue()
    {
        return new MappedDoubleAttribute(this.wrappedAttribute.absoluteValue(), this.mapper, this.parentSelector);
    }

    @Override
    public DoubleAttribute dividedBy(double divisor)
    {
        return new MappedDoubleAttribute(this.wrappedAttribute.dividedBy(divisor), this.mapper, this.parentSelector);
    }

    @Override
    public DoubleAttribute plus(double addend)
    {
        return new MappedDoubleAttribute(this.wrappedAttribute.plus(addend), this.mapper, this.parentSelector);
    }

    @Override
    public DoubleAttribute times(double multiplicand)
    {
        return new MappedDoubleAttribute(this.wrappedAttribute.times(multiplicand), this.mapper, this.parentSelector);
    }

    @Override
    public BigDecimalAttribute dividedBy(BigDecimal divisor)
    {
        return new MappedBigDecimalAttribute(this.wrappedAttribute.dividedBy(divisor), this.mapper, this.parentSelector);
    }

    @Override
    public BigDecimalAttribute plus(BigDecimal addend)
    {
        return new MappedBigDecimalAttribute(this.wrappedAttribute.plus(addend), this.mapper, this.parentSelector);
    }

    @Override
    public BigDecimalAttribute times(BigDecimal multiplicand)
    {
        return new MappedBigDecimalAttribute(this.wrappedAttribute.times(multiplicand), this.mapper, this.parentSelector);
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
    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a mapped attribute");
    }

    public boolean equals(Object other)
    {
        if (other instanceof MappedDoubleAttribute)
        {
            MappedDoubleAttribute o = (MappedDoubleAttribute) other;
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
    public MithraObjectPortal getTopLevelPortal()
    {
        return this.mapper.getResultPortal();
    }

    @Override
    protected NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder)
    {
        return new MappedDoubleAttribute(this.wrappedAttribute, mapperRemainder, mapperRemainder.getParentSelectorRemainder(((DeepRelationshipAttribute) this.parentSelector)));
    }

    @Override
    public void forEach(final DoubleProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    public void forEach(final BigDecimalProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void forEach(final ObjectProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.mapper.generateSql(query);
        this.wrappedAttribute.generateMapperSql(query);
        this.mapper.popMappers(query);
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
    public boolean zFindDeepRelationshipInMemory(Operation op)
    {
        Mapper reverseMapper = this.getMapper().getReverseMapper();
        if (reverseMapper == null) return false;
        Operation newOp = new MappedOperation(reverseMapper, op);
        return newOp.getResultObjectPortal().zFindInMemory(newOp, null) != null;
    }

    // ByteAttribute operands

    @Override
    public DoubleAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // ShortAttribute operands

    @Override
    public DoubleAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // IntegerAttribute operands

    @Override
    public DoubleAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // LongAttribute operands

    @Override
    public DoubleAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // FloatAttribute operands

    @Override
    public DoubleAttribute plus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute minus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute times(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // DoubleAttribute operands

    @Override
    public DoubleAttribute plus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute minus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute times(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return (DoubleAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
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
    public boolean isNullable()
    {
        return this.wrappedAttribute.isNullable();
    }
}
