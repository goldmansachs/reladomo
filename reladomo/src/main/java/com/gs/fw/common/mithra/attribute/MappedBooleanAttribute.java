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

import com.gs.collections.api.set.primitive.BooleanSet;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BooleanProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.Function;


public class MappedBooleanAttribute<T> extends BooleanAttribute<T> implements MappedAttribute
{
    private BooleanAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedBooleanAttribute(BooleanAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedBooleanAttribute)
        {
            MappedBooleanAttribute ma = (MappedBooleanAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (BooleanAttribute) ma.getWrappedAttribute();
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
        return new MappedBooleanAttribute((BooleanAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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

    public boolean booleanValueOf(Object o)
    {
        if (parentSelector == null) return false;
        Object result = parentSelector.valueOf(o);
        if (result == null) return false;
        return this.wrappedAttribute.booleanValueOf(result);
    }

    public void setBooleanValue(Object o, boolean newValue)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setBooleanValue(result, newValue);
    }

    @Override
    public Operation eq(boolean other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(boolean other)
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
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(BooleanSet set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(set));
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.BooleanSet set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(set));
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(BooleanSet set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(set));
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.BooleanSet set)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(set));
    }

    // join operation:
    @Override
    public Operation eq(BooleanAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(BooleanAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(BooleanAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation notEq(BooleanAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public void zAppendToString(ToStringContext toStringContext)
    {
        toStringContext.pushMapper(this.mapper);
        super.zAppendToString(toStringContext);
        toStringContext.popMapper();
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
    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a mapped attribute");
    }

    public boolean equals(Object other)
    {
        if (other instanceof MappedBooleanAttribute)
        {
            MappedBooleanAttribute o = (MappedBooleanAttribute) other;
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
    public void forEach(final BooleanProcedure proc, Object o, Object context)
    {
        if (parentSelector == null) proc.execute(false, context);
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
