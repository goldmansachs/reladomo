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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.util.List;


// this class is an optimization used mostly by the SemiUniqueDatedIndex implementations for Bitemporal objects
// it's also useful for many instances where the object has two primary keys, for example an id and acmap
public class TwoExtractorHashStrategy extends ExtractorBasedHashStrategy
{
    private Extractor first;
    private Extractor second;
    private Extractor[] extractors;

    public TwoExtractorHashStrategy(Extractor first, Extractor second)
    {
        this.first = first;
        this.second = second;
        if (first instanceof Attribute && second instanceof Attribute)
        {
            extractors = new Attribute[] { (Attribute) first, (Attribute) second};
        }
        else
        {
            extractors = new Extractor[] { first, second };
        }
    }

    public int computeHashCode(Object o)
    {
        return HashUtil.combineHashes(first.valueHashCode(o), second.valueHashCode(o));
    }

    public boolean equals(Object first, Object second)
    {
        return this.first.valueEquals(first, second) && this.second.valueEquals(first, second);
    }

    @Override
    public boolean equals(Object first, Object second, List secondExtractors)
    {
        return this.first.valueEquals(first, second, (Extractor)secondExtractors.get(0)) &&
                this.second.valueEquals(first, second, (Extractor)secondExtractors.get(1));
    }

    @Override
    public boolean equals(Object first, Object second, Extractor[] secondExtractors)
    {
        return this.first.valueEquals(first, second, secondExtractors[0]) &&
                this.second.valueEquals(first, second, secondExtractors[1]);
    }

    @Override
    public int computeUpdatedHashCode(Object o, AttributeUpdateWrapper updateWrapper)
    {
        int h = 0;
        Attribute attribute = updateWrapper.getAttribute();
        if (this.first.equals(attribute))
        {
            h = updateWrapper.getNewValueHashCode();
        }
        else
        {
            h = this.first.valueHashCode(o);
        }
        if (this.second.equals(attribute))
        {
            h = HashUtil.combineHashes(h, updateWrapper.getNewValueHashCode());
        }
        else
        {
            h = HashUtil.combineHashes(h, this.second.valueHashCode(o));
        }
        return h;
    }

    @Override
    public boolean equalsIncludingUpdate(Object original, Object newObject, AttributeUpdateWrapper updateWrapper)
    {
        boolean result = true;
        if (this.first.equals(updateWrapper.getAttribute()))
        {
            result = this.first.valueEquals(original, updateWrapper, updateWrapper);
        }
        else
        {
            result = this.first.valueEquals(original, newObject);
        }
        if (result)
        {
            if (this.second.equals(updateWrapper.getAttribute()))
            {
                result = this.second.valueEquals(original, updateWrapper, updateWrapper);
            }
            else
            {
                result = this.second.valueEquals(original, newObject);
            }
        }
        return result;
    }

    @Override
    public Extractor getFirstExtractor()
    {
        return this.first;
    }

    @Override
    public Extractor[] getExtractors()
    {
        return extractors;
    }

    @Override
    public int computeHashCode(Object o, List extractors)
    {
        int h = ((Extractor)extractors.get(0)).valueHashCode(o);
        h = HashUtil.combineHashes(h,((Extractor)extractors.get(1)).valueHashCode(o));
        return h;

    }

    @Override
    public int computeHashCode(Object o, Extractor[] extractors)
    {
        int h = extractors[0].valueHashCode(o);
        h = HashUtil.combineHashes(h,extractors[1].valueHashCode(o));
        return h;
    }

    @Override
    public int computeCombinedHashCode(Object o, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, this.first.valueHashCode(o));
        h = HashUtil.combineHashes(h, this.second.valueHashCode(o));
        return h;
    }
}
