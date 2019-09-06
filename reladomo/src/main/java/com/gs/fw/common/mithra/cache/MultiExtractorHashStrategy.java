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



public class MultiExtractorHashStrategy extends ExtractorBasedHashStrategy
{
    private Extractor[] extractors;

    public MultiExtractorHashStrategy(Extractor[] extractors)
    {
        this.extractors = extractors;
    }

    public int computeHashCode(Object o)
    {
        int h = extractors[0].valueHashCode(o);
        h = HashUtil.combineHashes(h, extractors[1].valueHashCode(o));
        for(int i=2;i<extractors.length;i++)
        {
            h = HashUtil.combineHashes(h, extractors[i].valueHashCode(o));
        }
        return h;
    }

    public boolean equals(Object first, Object second)
    {
        boolean result = extractors[0].valueEquals(first, second);
        for(int i=1;i<extractors.length && result;i++)
        {
            result = extractors[i].valueEquals(first,  second);
        }
        return result;
    }

    @Override
    public boolean equals(Object first, Object second, List secondExtractors)
    {
        if (extractors[0].valueEquals(first, second, (Extractor)secondExtractors.get(0)))
        {
            boolean result = extractors[1].valueEquals(first, second, (Extractor)secondExtractors.get(1));
            for(int i=2;result && i < extractors.length;i++)
            {
                result = extractors[i].valueEquals(first, second, (Extractor)secondExtractors.get(i));
            }
            return result;
        }
        return false;
    }

    @Override
    public boolean equals(Object first, Object second, Extractor[] secondExtractors)
    {
        if (extractors[0].valueEquals(first, second, secondExtractors[0]))
        {
            boolean result = extractors[1].valueEquals(first, second, secondExtractors[1]);
            for(int i=2;result && i < extractors.length;i++)
            {
                result = extractors[i].valueEquals(first, second, secondExtractors[i]);
            }
            return result;
        }
        return false;
    }

    @Override
    public int computeUpdatedHashCode(Object o, AttributeUpdateWrapper updateWrapper)
    {
        int h = 0;
        Attribute attribute = updateWrapper.getAttribute();
        if (extractors[0].equals(attribute))
        {
            h = updateWrapper.getNewValueHashCode();
        }
        else h = extractors[0].valueHashCode(o);
        for(int i=1;i<extractors.length;i++)
        {
            if (extractors[i].equals(attribute))
            {
                h = HashUtil.combineHashes(h, updateWrapper.getNewValueHashCode());
            }
            else
            {
                h = HashUtil.combineHashes(h, extractors[i].valueHashCode(o));
            }
        }
        return h;
    }

    @Override
    public boolean equalsIncludingUpdate(Object original, Object newObject, AttributeUpdateWrapper updateWrapper)
    {
        boolean result = true;
        for(int i=0;i<extractors.length && result;i++)
        {
            if (extractors[i].equals(updateWrapper.getAttribute()))
            {
                result = extractors[i].valueEquals(original, updateWrapper, updateWrapper);
            }
            else
            {
                result = extractors[i].valueEquals(original, newObject);
            }
        }
        return result;
    }

    @Override
    public Extractor getFirstExtractor()
    {
        return extractors[0];
    }

    @Override
    public Extractor[] getExtractors()
    {
        return this.extractors;
    }

    @Override
    public int computeHashCode(Object o, List extractors)
    {
        int h = ((Extractor)extractors.get(0)).valueHashCode(o);
        h = HashUtil.combineHashes(h,((Extractor)extractors.get(1)).valueHashCode(o));
        for(int i=2;i<extractors.size();i++)
        {
            h = HashUtil.combineHashes(h,((Extractor)extractors.get(i)).valueHashCode(o));
        }
        return h;

    }

    @Override
    public int computeHashCode(Object o, Extractor[] extractors)
    {
        int h = (extractors[0]).valueHashCode(o);
        h = HashUtil.combineHashes(h,(extractors[1]).valueHashCode(o));
        for(int i=2;i<extractors.length;i++)
        {
            h = HashUtil.combineHashes(h,(extractors[i]).valueHashCode(o));
        }
        return h;
    }

    @Override
    public int computeCombinedHashCode(Object o, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, extractors[0].valueHashCode(o));
        h = HashUtil.combineHashes(h, extractors[1].valueHashCode(o));
        for(int i=2;i<extractors.length;i++)
        {
            h = HashUtil.combineHashes(h, extractors[i].valueHashCode(o));
        }
        return h;
    }
}
