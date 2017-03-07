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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.TimestampProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.Function;
import com.gs.fw.common.mithra.util.TimestampPool;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Set;



public class MappedTimestampAttribute<Owner> extends TimestampAttribute<Owner> implements MappedAttribute
{
    private TimestampAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedTimestampAttribute(TimestampAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedTimestampAttribute)
        {
            MappedTimestampAttribute ma = (MappedTimestampAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (TimestampAttribute) ma.getWrappedAttribute();
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
        return new MappedTimestampAttribute((TimestampAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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

    @Override
    public Operation eq(TimestampAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(TimestampAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(TimestampAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation notEq(TimestampAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation in(Set<Timestamp> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(set));
    }

    @Override
    public Operation notIn(Set<Timestamp> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(set));
    }

    @Override
    public Operation greaterThan(Timestamp target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(Timestamp target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(Timestamp target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(Timestamp target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
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

    public Timestamp timestampValueOf(Object o)
    {
        if (parentSelector == null) return null;
        Object result = parentSelector.valueOf(o);
        if (result == null) return null;
        return this.wrappedAttribute.timestampValueOf(result);
    }

    @Override
    public long timestampValueOfAsLong(Owner o)
    {
        if (parentSelector == null) return TimestampPool.OFF_HEAP_NULL;
        Object result = parentSelector.valueOf(o);
        if (result == null) return TimestampPool.OFF_HEAP_NULL;
        return this.wrappedAttribute.timestampValueOfAsLong(result);
    }

    public void setTimestampValue(Object o, Timestamp newValue)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setTimestampValue(result, newValue);
    }

    @Override
    public boolean isAttributeNull(Object o)
    {
        if (parentSelector == null) return true;
        Object result = parentSelector.valueOf(o);
        if (result == null) return true;
        return wrappedAttribute.isAttributeNull(result);
    }

    @Override
    public Operation eq(Timestamp other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(Timestamp other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
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
    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a mapped attribute");
    }

    public boolean equals(Object other)
    {
        if (other instanceof MappedTimestampAttribute)
        {
            MappedTimestampAttribute o = (MappedTimestampAttribute) other;
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
    public void forEach(final TimestampProcedure proc, Object o, Object context)
    {
        if (parentSelector == null) proc.execute(null, context);
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
            ((DeepRelationshipAttribute)this.parentSelector).forEach(parentProc, o, context);
        }
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
        return new MappedIntegerAttribute(this.wrappedAttribute.year(), this.mapper, this.parentSelector);
    }

    @Override
    public IntegerAttribute month()
    {
        return new MappedIntegerAttribute(this.wrappedAttribute.month(), this.mapper, this.parentSelector);
    }

    @Override
    public IntegerAttribute dayOfMonth()
    {
        return new MappedIntegerAttribute(this.wrappedAttribute.dayOfMonth(), this.mapper, this.parentSelector);
    }
}
