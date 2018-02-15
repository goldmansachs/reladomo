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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DateProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.*;

import java.util.Date;
import java.util.Set;



public class MappedDateAttribute<Owner> extends DateAttribute<Owner> implements MappedAttribute
{
    private DateAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentExtractor;

    public MappedDateAttribute(DateAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentExtractor = parentSelector;
        while (this.wrappedAttribute instanceof MappedDateAttribute)
        {
            MappedDateAttribute ma = (MappedDateAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (DateAttribute) ma.getWrappedAttribute();
            this.parentExtractor = new ChainedAttributeValueSelector(this.parentExtractor, ma.getParentExtractor());
        }
    }

    @Override
    public String getAttributeName()
    {
        if (super.getAttributeName() == null)
        {
            computeMappedAttributeName(this.wrappedAttribute, this.parentExtractor);
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

    public Function getParentExtractor()
    {
        return parentExtractor;
    }

    public Attribute getWrappedAttribute()
    {
        return wrappedAttribute;
    }

    public Function getParentSelector()
    {
        return this.parentExtractor;
    }

    public MappedAttribute cloneForNewMapper(Mapper mapper, Function parentSelector)
    {
        return new MappedDateAttribute((DateAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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

    public Date dateValueOf(Object o)
    {
        if (parentExtractor == null) return null;
        Object result = parentExtractor.valueOf(o);
        if (result == null) return null;
        return this.wrappedAttribute.dateValueOf(result);
    }

    public void setDateValue(Object o, Date newValue)
    {
        if (parentExtractor == null) return;
        Object result = parentExtractor.valueOf(o);
        if (result == null) return;
        this.wrappedAttribute.setDateValue(result, newValue);
    }

    @Override
    public Operation eq(Date other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(Date other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation in(Set<Date> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(set));
    }

    @Override
    public Operation notIn(Set<Date> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(set));
    }

    // join operation:
    @Override
    public Operation eq(DateAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(DateAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(DateAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation notEq(DateAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public boolean isAttributeNull(Object o)
    {
        if (parentExtractor == null) return true;
        Object result = parentExtractor.valueOf(o);
        if (result == null) return true;
        return this.wrappedAttribute.isAttributeNull(result);
    }

    public Object readResolve()
    {
        return this;
    }

    @Override
    public void forEach(final DateProcedure proc, Object o, Object context)
    {
        if (parentExtractor == null) proc.execute(null, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)this.parentExtractor).forEach(parentProc, o, context);
        }
    }

    @Override
    public Operation greaterThan(Date target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(Date target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(Date target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(Date target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
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
        if (other instanceof MappedDateAttribute)
        {
            MappedDateAttribute o = (MappedDateAttribute) other;
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

    @Override
    public IntegerAttribute year()
    {
        return new MappedIntegerAttribute(this.wrappedAttribute.year(), this.mapper, this.parentExtractor);
    }

    @Override
    public IntegerAttribute month()
    {
        return new MappedIntegerAttribute(this.wrappedAttribute.month(), this.mapper, this.parentExtractor);
    }

    @Override
    public IntegerAttribute dayOfMonth()
    {
        return new MappedIntegerAttribute(this.wrappedAttribute.dayOfMonth(), this.mapper, this.parentExtractor);
    }
}
