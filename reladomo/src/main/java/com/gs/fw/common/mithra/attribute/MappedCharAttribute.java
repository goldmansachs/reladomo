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
import com.gs.fw.common.mithra.attribute.calculator.procedure.CharacterProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.ObjectProcedure;
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.ChainedMapper;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import org.eclipse.collections.api.set.primitive.CharSet;


public class MappedCharAttribute<T> extends CharAttribute<T> implements MappedAttribute
{
    private CharAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedCharAttribute(CharAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedCharAttribute)
        {
            MappedCharAttribute ma = (MappedCharAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (CharAttribute) ma.getWrappedAttribute();
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

    public Attribute getWrappedAttribute()
    {
        return wrappedAttribute;
    }

    public Function getParentSelector()
    {
        return parentSelector;
    }

    public MappedAttribute cloneForNewMapper(Mapper mapper, Function parentSelector)
    {
        return new MappedCharAttribute((CharAttribute)this.getWrappedAttribute(), mapper, parentSelector);
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

    public char charValueOf(Object o)
    {
        if (parentSelector == null) return 0;
        Object result = parentSelector.valueOf(o);
        if (result == null) return 0;
        return this.wrappedAttribute.charValueOf(result);
    }

    public void setCharValue(Object o, char newValue)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setCharValue(result, newValue);
    }

    public void setValueNull(Object o)
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setValueNull(result);
    }

    @Override
    public Operation eq(char other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation notEq(char other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    public boolean isAttributeNull(Object o)
    {
        if (parentSelector == null) return true;
        Object result = parentSelector.valueOf(o);
        if (result == null) return true;
        return this.wrappedAttribute.isAttributeNull(result);
    }

    public Object readResolve()
    {
        return this;
    }

    @Override
    public Operation in(CharSet charSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.in(charSet));
    }

    @Override
    public Operation notIn(CharSet charSet)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notIn(charSet));
    }

    @Override
    public Operation eq(CharAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation joinEq(CharAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation filterEq(CharAttribute other)
    {
        return filterEqForMappedAttribute(other);
    }

    @Override
    public Operation notEq(CharAttribute other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.notEq(other));
    }

    @Override
    public Operation greaterThan(char target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThan(target));
    }

    @Override
    public Operation greaterThanEquals(char target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.greaterThanEquals(target));
    }

    @Override
    public Operation lessThan(char target)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.lessThan(target));
    }

    @Override
    public Operation lessThanEquals(char target)
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
        if (other instanceof MappedCharAttribute)
        {
            MappedCharAttribute o = (MappedCharAttribute) other;
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
    public void forEach(final CharacterProcedure proc, Object o, Object context)
    {
        if (parentSelector == null) proc.execute((char)0, context);
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
