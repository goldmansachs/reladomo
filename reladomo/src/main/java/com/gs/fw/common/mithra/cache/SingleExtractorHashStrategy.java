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

import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.util.List;



public class SingleExtractorHashStrategy extends ExtractorBasedHashStrategy
{

    private Extractor extractor;
    private Extractor[] extractors;

    public SingleExtractorHashStrategy(Extractor extractor)
    {
        this.extractor = extractor;
        if (extractor instanceof Attribute)
        {
            extractors = new Attribute[] { (Attribute) extractor };
        }
        else
        {
            extractors = new Extractor[] { extractor };
        }
    }

    public int computeHashCode(Object o)
    {
        return extractor.valueHashCode(o);
    }

    public boolean equals(Object first, Object second)
    {
        return extractor.valueEquals(first, second);
    }

    @Override
    public boolean equals(Object first, Object second, List secondExtractors)
    {
        return extractor.valueEquals(first, second, (Extractor)secondExtractors.get(0));
    }

    @Override
    public boolean equals(Object first, Object second, Extractor[] secondExtractors)
    {
        return extractor.valueEquals(first, second, secondExtractors[0]);
    }

    @Override
    public int computeUpdatedHashCode(Object o, AttributeUpdateWrapper updateWrapper)
    {
        return updateWrapper.getNewValueHashCode();
    }

    @Override
    public boolean equalsIncludingUpdate(Object original, Object newObject, AttributeUpdateWrapper updateWrapper)
    {
        if (this.extractor.equals(updateWrapper.getAttribute()))
        {
            return extractor.valueEquals(original, newObject, updateWrapper);
        }
        else
        {
            return extractor.valueEquals(original, newObject);
        }
    }

    @Override
    public Extractor getFirstExtractor()
    {
        return this.extractor;
    }

    @Override
    public Extractor[] getExtractors()
    {
        return extractors;
    }

    @Override
    public int computeHashCode(Object o, List extractors)
    {
        return ((Extractor)extractors.get(0)).valueHashCode(o);
    }

    @Override
    public int computeHashCode(Object o, Extractor[] extractors)
    {
        return extractors[0].valueHashCode(o);
    }

    @Override
    public int computeCombinedHashCode(Object o, int incomingHash)
    {
        return HashUtil.combineHashes(incomingHash, this.extractor.valueHashCode(o));
    }
}
