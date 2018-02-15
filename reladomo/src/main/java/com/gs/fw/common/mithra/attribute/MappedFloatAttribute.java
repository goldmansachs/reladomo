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

import com.gs.collections.api.set.primitive.FloatSet;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.*;


public class MappedFloatAttribute<T> extends FloatAttribute<T> implements MappedAttribute
{
    private FloatAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedFloatAttribute(FloatAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedFloatAttribute)
        {
            MappedFloatAttribute ma = (MappedFloatAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (FloatAttribute) ma.getWrappedAttribute();
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
        return new MappedFloatAttribute((FloatAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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

    public float floatValueOf(Object o)
    {
        if (parentSelector == null) return 0;
        Object result = parentSelector.valueOf(o);
        if (result == null) return 0;
        return this.wrappedAttribute.floatValueOf(result);
    }

    public void setFloatValue(Object o, float newValue)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setFloatValue(result, newValue);
    }

    @Override
    public Operation eq(float other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(float other)
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

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(FloatSet floatSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(floatSet));
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.FloatSet floatSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(floatSet));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(FloatSet floatSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(floatSet));
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.FloatSet floatSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(floatSet));
    }

    @Override
    public Operation notEq(FloatAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation greaterThan(float target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(float target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(float target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(float target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
    }

    // join operation:
    @Override
    public Operation eq(FloatAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(FloatAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(FloatAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public FloatAttribute absoluteValue()
    {
        return new MappedFloatAttribute(this.wrappedAttribute.absoluteValue(), this.mapper, this.parentSelector);
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
        if (other instanceof MappedFloatAttribute)
        {
            MappedFloatAttribute o = (MappedFloatAttribute) other;
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
    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.mapper.generateSql(query);
        this.wrappedAttribute.generateMapperSql(query);
        this.mapper.popMappers(query);
    }

    @Override
    protected NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder)
    {
        return new MappedFloatAttribute(this.wrappedAttribute, mapperRemainder, mapperRemainder.getParentSelectorRemainder(((DeepRelationshipAttribute) this.parentSelector)));
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

    public void forEach(final BigDecimalProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    @Override
    public void forEach(final ObjectProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    // ByteAttribute operands

    @Override
    public FloatAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // ShortAttribute operands

    @Override
    public FloatAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // IntegerAttribute operands

    @Override
    public FloatAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // LongAttribute operands

    @Override
    public FloatAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // FloatAttribute operands

    @Override
    public FloatAttribute plus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute minus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute times(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
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
    public FloatAttribute plus(BigDecimalAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute minus(BigDecimalAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute times(BigDecimalAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public FloatAttribute dividedBy(BigDecimalAttribute attribute)
    {
        return (FloatAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
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
