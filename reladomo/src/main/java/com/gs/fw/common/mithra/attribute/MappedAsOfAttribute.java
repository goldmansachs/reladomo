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
import com.gs.fw.common.mithra.extractor.ChainedAttributeValueSelector;
import com.gs.fw.common.mithra.finder.ChainedMapper;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;



public class MappedAsOfAttribute<T> extends AsOfAttribute<T> implements MappedAttribute
{
    private AsOfAttribute wrappedAttribute;
    private Mapper mapper;
    private Function parentSelector;

    public MappedAsOfAttribute(AsOfAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        super(wrappedAttribute.getAttributeName(), wrappedAttribute.getBusClassNameWithDots(), wrappedAttribute.getBusClassName(),
                wrappedAttribute.isNullable(), false, null, null, false, false,
                wrappedAttribute.getFromAttribute(), wrappedAttribute.getToAttribute(),
                wrappedAttribute.getInfinityDate(), wrappedAttribute.isFutureExpiringRowsExist(),
                wrappedAttribute.isToIsInclusive(), wrappedAttribute.getDefaultDate(),
                wrappedAttribute.isProcessingDate());
        this.wrappedAttribute = wrappedAttribute;
        this.mapper = mapper;
        this.parentSelector = parentSelector;
        while (this.wrappedAttribute instanceof MappedAsOfAttribute)
        {
            MappedAsOfAttribute ma = (MappedAsOfAttribute) this.wrappedAttribute;
            this.mapper = new ChainedMapper(this.mapper, ma.getMapper());
            this.wrappedAttribute = (AsOfAttribute) ma.getWrappedAttribute();
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
        return new MappedAsOfAttribute((AsOfAttribute) this.getWrappedAttribute(), mapper, parentSelector);
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

    public Timestamp timestampValueOf(Object o)
    {
        if (parentSelector == null) return null;
        Object result = parentSelector.valueOf(o);
        if (result == null) return null;
        return this.wrappedAttribute.timestampValueOf(result);
    }

    @Override
    public Operation eq(Timestamp other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation eq(Date other)
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.eq(other));
    }

    @Override
    public Operation equalsEdgePoint()
    {
        return new MappedOperation(this.mapper, this.wrappedAttribute.equalsEdgePoint());
    }

    @Override
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

    public boolean equals(Object other)
    {
        if (other instanceof MappedAsOfAttribute)
        {
            MappedAsOfAttribute o = (MappedAsOfAttribute) other;
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
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, T reladomoObject) throws IOException
    {
        throw new RuntimeException("should not get here");
    }
}
