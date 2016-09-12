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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.StringProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.finder.*;

import java.util.Set;



public class MappedStringAttribute<Owner> extends StringAttribute<Owner> implements MappedAttribute
{
    private static final long serialVersionUID = 8276132122235153271L;
    private StringAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedStringAttribute(StringAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        //super(wrappedAttribute.getColumnName());
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedStringAttribute)
        {
            MappedStringAttribute ma = (MappedStringAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (StringAttribute) ma.getWrappedAttribute();
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

    public Function getParentSelector()
    {
        return parentSelector;
    }

    public MappedAttribute cloneForNewMapper(Mapper mapper, Function parentSelector)
    {
        return new MappedStringAttribute((StringAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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
    public StringAttribute toLowerCase()
    {
        return new MappedStringAttribute(this.wrappedAttribute.toLowerCase(), this.mapper, this.parentSelector);
    }

    @Override
    public StringAttribute<Owner> substring(int start, int end)
    {
        return new MappedStringAttribute(this.wrappedAttribute.substring(start, end), this.mapper, this.parentSelector);
    }

    public String stringValueOf(Object o)
    {
        if (parentSelector == null) return null;
        Object result = this.parentSelector.valueOf(o);
        if (result == null) return null;
        return wrappedAttribute.stringValueOf(result);
    }

    public void setStringValue(Object o, String newValue)
    {
        if (parentSelector == null) return;
        Object result = this.parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setStringValue(result, newValue);
    }

    @Override
    public boolean isAttributeNull(Object o)
    {
        if (parentSelector == null) return true;
        Object result = this.parentSelector.valueOf(o);
        if (result == null) return true;
        return wrappedAttribute.isAttributeNull(result);
    }

    @Override
    public Operation eq(String other)
    {
        return new MappedOperation(mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(String other)
    {
        return new MappedOperation(mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation in(Set<String> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(set));
    }

    @Override
    public Operation notIn(Set<String> set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(set));
    }

    // join operation:
    @Override
    public Operation eq(StringAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(StringAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(StringAttribute other)
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
    public Operation notEq(StringAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation greaterThan(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThanEquals(target));
    }

    @Override
    public Operation startsWith(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.startsWith(target));
    }

    @Override
    public Operation wildCardEq(String pattern)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.wildCardEq(pattern));
    }

    @Override
    public Operation wildCardIn(Set<String> patterns)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.wildCardIn(patterns));
    }

    @Override
    public Operation wildCardNotEq(String pattern)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.wildCardNotEq(pattern));
    }

    @Override
    public Operation endsWith(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.endsWith(target));
    }

    @Override
    public Operation contains(String searchString)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.contains(searchString));
    }

    @Override
    public Operation notStartsWith(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notStartsWith(target));
    }

    @Override
    public Operation notEndsWith(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEndsWith(target));
    }

    @Override
    public Operation notContains(String target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notContains(target));
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

    public boolean equals(Object other)
    {
        if (other instanceof MappedStringAttribute)
        {
            MappedStringAttribute o = (MappedStringAttribute) other;
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
        return this.wrappedAttribute.isSourceAttribute();  //To change body of implemented methods use File | Settings | File Templates.
    }
    
    @Override
    public void forEach(final StringProcedure proc, Object o, Object context)
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
}
