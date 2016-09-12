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

import java.util.*;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.*;


public class TupleAttributeImpl implements TupleAttribute
{
    private Attribute[] attributes;

    public TupleAttributeImpl(Attribute... attributes)
    {
        this.attributes = Arrays.copyOf(attributes, attributes.length);
    }

    public TupleAttributeImpl(Attribute first, Attribute second)
    {
        this.attributes = new Attribute[2];
        this.attributes[0] = first;
        this.attributes[1] = second;
    }

    protected TupleAttributeImpl(TupleAttributeImpl tuple, Attribute attr)
    {
        this.attributes = new Attribute[tuple.attributes.length+1];
        System.arraycopy(tuple.attributes, 0, this.attributes, 0, tuple.attributes.length);
        this.attributes[tuple.attributes.length] = attr;
    }

    protected TupleAttributeImpl(Attribute attr, TupleAttributeImpl tuple)
    {
        this.attributes = new Attribute[tuple.attributes.length+1];
        this.attributes[0] = attr;
        System.arraycopy(tuple.attributes, 0, this.attributes, 1, tuple.attributes.length);
    }

    protected TupleAttributeImpl(Attribute attr, Attribute... attributes)
    {
        this.attributes = new Attribute[attributes.length+1];
        this.attributes[0] = attr;
        System.arraycopy(attributes, 0, this.attributes, 1, attributes.length);
    }

    protected TupleAttributeImpl(TupleAttributeImpl first, TupleAttributeImpl second)
    {
        this.attributes = new Attribute[first.attributes.length+second.attributes.length];
        System.arraycopy(first.attributes, 0, this.attributes, 0, first.attributes.length);
        System.arraycopy(second.attributes, 0, this.attributes, first.attributes.length, second.attributes.length);
    }

    public TupleAttributeImpl(TupleAttributeImpl first, Attribute[] attributes)
    {
        this.attributes = new Attribute[first.attributes.length+attributes.length];
        System.arraycopy(first.attributes, 0, this.attributes, 0, first.attributes.length);
        System.arraycopy(attributes, 0, this.attributes, first.attributes.length, attributes.length);
    }

    public TupleAttribute tupleWith(Attribute attr)
    {
        return new TupleAttributeImpl(this, attr);
    }

    public TupleAttribute tupleWith(TupleAttribute attr)
    {
        return new TupleAttributeImpl(this, (TupleAttributeImpl) attr);
    }

    public TupleAttribute tupleWith(Attribute... attr)
    {
        return new TupleAttributeImpl(this, attr);
    }

    public Operation in(TupleSet tupleSet)
    {
        if (tupleSet.size() == 0)
        {
            return new None(this.attributes[0]);
        }
        return new MultiInOperation(this.attributes, (MithraTupleSet) tupleSet);
    }

    public Operation in(List dataHolders, Extractor[] extractors)
    {
        if (dataHolders.size() == 0)
        {
            return new None(this.attributes[0]);
        }
        for(int i=0;i<attributes.length;i++)
        {
            if (attributes[i].isSourceAttribute())
            {
                return createInWithSourceAttribute(i, dataHolders, extractors, false);
            }
        }
        return createMultiInWithConstantCheck(this.attributes, dataHolders, extractors, false);
    }

    private Operation createMultiInWithConstantCheck(Attribute[] attributes, List dataHolders, Extractor[] extractors, boolean ignoreNulls)
    {
        boolean[] hasSingleValue = new boolean[extractors.length];
        int constantCount = extractors.length;
        for(int i=0;i<hasSingleValue.length;i++)
        {
            hasSingleValue[i] = true;
        }
        Object first = dataHolders.get(0);
        for(int i=1;constantCount > 0 && i<dataHolders.size();i++)
        {
            for(int j=0;j<extractors.length;j++)
            {
                if (hasSingleValue[j] && !extractors[j].valueEquals(first, dataHolders.get(i)))
                {
                    hasSingleValue[j] = false;
                    constantCount--;
                }
            }
        }
        if (constantCount > 0)
        {
            Operation inOperation = NoOperation.instance();
            Operation constantOp = NoOperation.instance();
            int leftOver = extractors.length - constantCount;
            if (leftOver == 0)
            {
                for(int j=0;j<extractors.length;j++)
                {
                    constantOp = constantOp.and(attributes[j].nonPrimitiveEq(extractors[j].valueOf(first)));
                }
            }
            else if (leftOver == 1)
            {
                for(int j=0;j<extractors.length;j++)
                {
                    if (hasSingleValue[j])
                    {
                        constantOp = constantOp.and(attributes[j].nonPrimitiveEq(extractors[j].valueOf(first)));
                    }
                    else
                    {
                        inOperation = attributes[j].in(dataHolders, extractors[j]);
                    }
                }
            }
            else
            {
                Extractor[] extractorSubset = new Extractor[leftOver];
                Attribute[] attributeSubset = new Attribute[leftOver];
                int extractorCount = 0;
                for(int j=0;j<extractors.length;j++)
                {
                    if (hasSingleValue[j])
                    {
                        constantOp = constantOp.and(attributes[j].nonPrimitiveEq(extractors[j].valueOf(first)));
                    }
                    else
                    {
                        attributeSubset[extractorCount] = attributes[j];
                        extractorSubset[extractorCount++] = extractors[j];
                    }
                }
                inOperation = new MultiInOperation(attributeSubset, new MithraArrayTupleTupleSet(extractorSubset, dataHolders, ignoreNulls));
            }
            return constantOp.and(inOperation);
        }
        else
        {
            return new MultiInOperation(attributes, new MithraArrayTupleTupleSet(extractors, dataHolders, ignoreNulls));
        }
    }

    public Operation inIgnoreNulls(List dataHolders, Extractor[] extractors)
    {
        if (dataHolders.size() == 0)
        {
            return new None(this.attributes[0]);
        }
        for(int i=0;i<attributes.length;i++)
        {
            if (attributes[i].isSourceAttribute())
            {
                return createInWithSourceAttribute(i, dataHolders, extractors, true);
            }
        }
        return new MultiInOperation(this.attributes, new MithraArrayTupleTupleSet(extractors, dataHolders, true));
    }

    private Operation createInWithSourceAttribute(int sourceAttributeIndex, List dataHolders, Extractor[] extractors, boolean ignoreNulls)
    {
        Object first = dataHolders.get(0);
        Extractor dataHolderSourceAttribute = extractors[sourceAttributeIndex];
        for(int i=1;i<dataHolders.size();i++)
        {
            Object data = dataHolders.get(i);
            if (!dataHolderSourceAttribute.valueEquals(first, data))
            {
                return createMultiInWithConstantCheck(this.attributes, dataHolders, extractors, ignoreNulls);
            }
        }
        Operation op = this.attributes[sourceAttributeIndex].nonPrimitiveEq(dataHolderSourceAttribute.valueOf(first));

        Attribute[] withoutSource = new Attribute[this.attributes.length - 1];
        Extractor[] extractorsWithoutSource = new Extractor[this.attributes.length - 1];
        int count = 0;
        for(int i=0;i<this.attributes.length;i++)
        {
            if (i != sourceAttributeIndex)
            {
                withoutSource[count] = this.attributes[i];
                extractorsWithoutSource[count] = extractors[i];
                count++;
            }
        }
        return op.and(createMultiInWithConstantCheck(withoutSource, dataHolders, extractorsWithoutSource, ignoreNulls));
    }

    public Operation in(AggregateList aggList, String... aggregateAttributeName)
    {
        MithraArrayTupleTupleSet set = new MithraArrayTupleTupleSet();

        int tupleLength = aggregateAttributeName.length;
        if (aggList.size() == 0)
        {
            return new None(this.attributes[0]);
        }
        for(int i=0;i<aggList.size();i++)
        {
            Object[] tuple = new Object[tupleLength];
            for(int j=0;j<tupleLength;j++)
            {
                tuple[j] = aggList.get(i).getAttributeAsObject(aggregateAttributeName[j]);
            }
            set.add(tuple);
        }
        return this.in(set);
    }

    public Attribute[] getAttributes()
    {
        return attributes;
    }
}
