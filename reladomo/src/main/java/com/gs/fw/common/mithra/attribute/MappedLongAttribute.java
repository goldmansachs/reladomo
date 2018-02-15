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

import com.gs.collections.api.set.primitive.LongSet;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.*;


public class MappedLongAttribute<T> extends LongAttribute<T> implements MappedAttribute
{
    private LongAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedLongAttribute(LongAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedLongAttribute)
        {
            MappedLongAttribute ma = (MappedLongAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (LongAttribute) ma.getWrappedAttribute();
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
        return new MappedLongAttribute((LongAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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

    public long longValueOf(Object o)
    {
        if (parentSelector == null) return 0;
        Object result = parentSelector.valueOf(o);
        if (result == null) return 0;
        return this.wrappedAttribute.longValueOf(result);
    }

    public void setLongValue(Object o, long newValue)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setLongValue(result, newValue);
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
    public Operation eq(long other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(long other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(LongSet longSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(longSet));
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.LongSet longSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(longSet));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(LongSet longSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(longSet));
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.LongSet longSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(longSet));
    }

    @Override
    public Operation notEq(LongAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation greaterThan(long target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(long target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(long target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(long target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
    }

    // join operation:
    @Override
    public Operation eq(LongAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(LongAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(LongAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public LongAttribute absoluteValue()
    {
        return new MappedLongAttribute(this.wrappedAttribute.absoluteValue(), this.mapper, this.parentSelector);
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
        if (other instanceof MappedLongAttribute)
        {
            MappedLongAttribute o = (MappedLongAttribute) other;
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
        return new MappedLongAttribute(this.wrappedAttribute, mapperRemainder, mapperRemainder.getParentSelectorRemainder(((DeepRelationshipAttribute) this.parentSelector)));
    }

    @Override
    public void forEach(final LongProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    public void forEach(final BigDecimalProcedure proc, Object o, Object context)
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
    public void forEach(final ObjectProcedure proc, Object o, Object context)
    {
        MappedAttributeUtil.forEach(proc, o, context, this.parentSelector, this.wrappedAttribute);
    }

    // ByteAttribute operands

    @Override
    public LongAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // ShortAttribute operands

    @Override
    public LongAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // IntegerAttribute operands

    @Override
    public LongAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
    }

    // LongAttribute operands

    @Override
    public LongAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.plus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.minus(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.times(this, (NumericAttribute) attribute);
    }

    @Override
    public LongAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return (LongAttribute) MappedAttributeUtil.dividedBy(this, (NumericAttribute) attribute);
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
