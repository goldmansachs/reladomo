
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
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.Function;

import java.util.Set;

public class MappedEnumAttribute<Owner, E extends Enum<E>> extends EnumAttribute<Owner, E> implements MappedAttribute
{

    private EnumAttribute<Owner, E> wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedEnumAttribute(EnumAttribute<Owner, E> wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        super(wrappedAttribute.getDelegate());
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedEnumAttribute)
        {
            MappedEnumAttribute<Owner, E> nestedMappedEnumAttribute = (MappedEnumAttribute<Owner, E>) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, nestedMappedEnumAttribute.getMapper());
            this.wrappedAttribute = nestedMappedEnumAttribute.getWrappedAttribute();
            this.parentSelector = new ChainedAttributeValueSelector(this.parentSelector, nestedMappedEnumAttribute.getParentSelector());
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

    public EnumAttribute<Owner, E> getWrappedAttribute()
    {
        return this.wrappedAttribute;
    }

    public Mapper getMapper()
    {
        return this.mapper;
    }

    public Function getParentSelector()
    {
        return this.parentSelector;
    }

    public E enumValueOf(Owner o)
    {
        if (this.parentSelector == null)
        {
            return null;
        }
        Owner result = (Owner) this.parentSelector.valueOf(o);
        if (result == null)
        {
            return null;
        }
        return this.wrappedAttribute.enumValueOf(result);
    }

    public void setEnumValue(Owner o, E newValue)
    {
        if (this.parentSelector == null)
        {
            return;
        }
        Owner result = (Owner) this.parentSelector.valueOf(o);
        if (result == null)
        {
            return;
        }
        this.wrappedAttribute.setEnumValue(result, newValue);
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
    public Class valueType()
    {
        return this.wrappedAttribute.valueType();
    }

    public MappedAttribute cloneForNewMapper(Mapper mapper, Function parentSelector)
    {
        return new MappedEnumAttribute<Owner, E>(this.wrappedAttribute, this.mapper, this.parentSelector);
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

    //TODO: ledav genericize this

    @Override
    public Operation eq(E value)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq((E) value));
    }

    @Override
    public Operation notEq(E value)
    {
         return new MappedOperation(this.mapper, this.wrappedAttribute.notEq((E)value));
    }

    @Override
    public Operation in(Set<E> enumSet)
    {
         return new MappedOperation(this.mapper, this.wrappedAttribute.in(enumSet));
    }

    @Override
    public Operation notIn(Set<E> enumSet)
    {
         return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(enumSet));
    }

    @Override
    public <Owner2> Operation eq(EnumAttribute<Owner2, E> other)
    {
         return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public <Owner2> Operation notEq(EnumAttribute<Owner2, E> other)
    {
         return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
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
    public Attribute getSourceAttribute()
    {
        return this.wrappedAttribute.getSourceAttribute();
    }

    @Override
    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.wrappedAttribute.getAsOfAttributes();
    }

    @Override
    public SourceAttributeType getSourceAttributeType()
    {
        return this.wrappedAttribute.getSourceAttributeType();
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
    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.mapper.generateSql(query);
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

    public boolean equals(Object other)
    {
        if (other instanceof MappedEnumAttribute)
        {
            MappedEnumAttribute o = (MappedEnumAttribute) other;
            return this.wrappedAttribute.equals(o.wrappedAttribute) && this.mapper.equals(o.mapper);
        }
        return false;
    }

    public int hashCode()
    {
        return this.wrappedAttribute.hashCode() ^ this.mapper.hashCode();
    }

    public Object readResolve()
    {
        return this;
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
