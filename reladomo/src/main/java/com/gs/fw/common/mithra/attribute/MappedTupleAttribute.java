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

import com.gs.fw.common.mithra.AggregateList;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.MappedOperation;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.TupleSet;

import java.util.List;


public class MappedTupleAttribute implements TupleAttribute
{
    private Mapper mapper;
    private TupleAttribute tupleAttribute;

    public MappedTupleAttribute(MappedAttribute first, MappedAttribute second)
    {
        this.mapper = first.getMapper();
        this.tupleAttribute = first.getWrappedAttribute().tupleWith(second.getWrappedAttribute());
    }

    public MappedTupleAttribute(MappedAttribute first, Attribute[] attrs)
    {
        this.mapper = first.getMapper();
        this.tupleAttribute = first.getWrappedAttribute().tupleWith(((MappedAttribute)attrs[0]).getWrappedAttribute());
        for(int i=1;i<attrs.length;i++)
        {
            this.tupleAttribute = this.tupleAttribute.tupleWith(((MappedAttribute)attrs[i]).getWrappedAttribute());
        }
    }

    public MappedTupleAttribute(MappedAttribute first, MappedTupleAttribute second)
    {
        this.mapper = first.getMapper();
        this.tupleAttribute = first.getWrappedAttribute().tupleWith(second.tupleAttribute);
    }

    public MappedTupleAttribute(MappedTupleAttribute mappedTupleAttribute, MappedAttribute second)
    {
        this.mapper = mappedTupleAttribute.mapper;
        this.tupleAttribute = mappedTupleAttribute.tupleAttribute.tupleWith(second.getWrappedAttribute());
    }

    public MappedTupleAttribute(MappedTupleAttribute mappedTupleAttribute, Attribute[] attrs)
    {
        this.mapper = mappedTupleAttribute.getMapper();
        this.tupleAttribute = mappedTupleAttribute.tupleAttribute.tupleWith(((MappedAttribute)attrs[0]).getWrappedAttribute());
        for(int i=1;i<attrs.length;i++)
        {
            this.tupleAttribute = this.tupleAttribute.tupleWith(((MappedAttribute)attrs[i]).getWrappedAttribute());
        }
    }

    public MappedTupleAttribute(MappedTupleAttribute first, MappedTupleAttribute second)
    {
        this.mapper = first.getMapper();
        this.tupleAttribute = first.tupleAttribute.tupleWith(second.tupleAttribute);
    }

    public TupleAttribute tupleWith(Attribute attr)
    {
        if (attr instanceof MappedAttribute)
        {
            MappedAttribute second = (MappedAttribute) attr;
            if (this.mapper.equals(second.getMapper()))
            {
                return new MappedTupleAttribute(this, second);
            }
        }
        throw new MithraBusinessException("Cannot form tuples across relationships. The tuple must be created from attributes of the same object.");

    }

    public TupleAttribute tupleWith(Attribute... attrs)
    {
        for(Attribute a: attrs)
        {
            if (!(a instanceof MappedAttribute) || !((MappedAttribute)a).getMapper().equals(this.getMapper()))
            {
                throw new MithraBusinessException("Cannot form tuples across relationships. The tuple must be created from attributes of the same object.");
            }
        }
        return new MappedTupleAttribute(this, attrs);
    }

    public TupleAttribute tupleWith(TupleAttribute attr)
    {
        if (attr instanceof MappedTupleAttribute)
        {
            MappedTupleAttribute second = (MappedTupleAttribute) attr;
            if (this.getMapper().equals(second.getMapper()))
            {
                return new MappedTupleAttribute(this, second);
            }
        }
        throw new MithraBusinessException("Cannot form tuples across relationships. The tuple must be created from attributes of the same object.");
    }

    public Operation in(TupleSet tupleSet)
    {
        return new MappedOperation(this.mapper, tupleAttribute.in(tupleSet));
    }

    public Operation in(List dataHolders, Extractor[] extractors)
    {
        return new MappedOperation(this.mapper, tupleAttribute.in(dataHolders, extractors));
    }

    public Operation inIgnoreNulls(List dataHolders, Extractor[] extractors)
    {
        return new MappedOperation(this.mapper, tupleAttribute.inIgnoreNulls(dataHolders, extractors));
    }

    public Operation in(AggregateList aggList, String... aggregateAttributeName)
    {
        return new MappedOperation(this.mapper, tupleAttribute.in(aggList, aggregateAttributeName));
    }

    public Mapper getMapper()
    {
        return mapper;
    }
}
