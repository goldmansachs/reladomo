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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.util.List;

public class MultiExtractorBasedOffHeapHashStrategy extends ExtractorBasedOffHeapHashStrategy
{
    private OffHeapExtractor[] offHeapExtractors;
    private Extractor[] onHeapExtractors;

    public MultiExtractorBasedOffHeapHashStrategy(OffHeapExtractor[] offHeapExtractors, Extractor[] onHeapExtractors)
    {
        this.offHeapExtractors = offHeapExtractors;
        this.onHeapExtractors = onHeapExtractors;
    }

    @Override
    public int computeHashCode(OffHeapDataStorage dataStorage, int dataOffset)
    {
        int hash = offHeapExtractors[0].computeHash(dataStorage, dataOffset);
        for (int i=1;i<offHeapExtractors.length;i++)
        {
            hash = HashUtil.combineHashes(hash, offHeapExtractors[i].computeHash(dataStorage, dataOffset));
        }
        return hash;
    }

    @Override
    public int computeHashCode(Object valueHolder, List extractors)
    {
        int hash = offHeapExtractors[0].computeHashFromOnHeapExtractor(valueHolder, (Extractor) extractors.get(0));
        for (int i=1;i<offHeapExtractors.length;i++)
        {
            hash = HashUtil.combineHashes(hash, offHeapExtractors[i].computeHashFromOnHeapExtractor(valueHolder, (Extractor) extractors.get(i)));
        }
        return hash;
    }

    @Override
    public int computeHashCode(Object valueHolder, Extractor[] extractors)
    {
        int hash = offHeapExtractors[0].computeHashFromOnHeapExtractor(valueHolder, extractors[0]);
        for (int i=1;i<offHeapExtractors.length;i++)
        {
            hash = HashUtil.combineHashes(hash, offHeapExtractors[i].computeHashFromOnHeapExtractor(valueHolder, extractors[i]));
        }
        return hash;
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, List extractors)
    {
        for(int i=0;i<offHeapExtractors.length;i++)
        {
            if (!offHeapExtractors[i].equals(dataStorage, dataOffset, valueHolder, (Extractor) extractors.get(i)))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int cur, Object valueHolder, Extractor[] extractors)
    {
        for(int i=0;i<offHeapExtractors.length;i++)
        {
            if (!offHeapExtractors[i].equals(dataStorage, cur, valueHolder, extractors[i]))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder)
    {
        for(int i=0;i<offHeapExtractors.length;i++)
        {
            if (!offHeapExtractors[i].equals(dataStorage, dataOffset, valueHolder, onHeapExtractors[i]))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int offsetOne, int offsetTwo)
    {
        for(int i=0;i<offHeapExtractors.length;i++)
        {
            if (!offHeapExtractors[i].valueEquals(dataStorage, offsetOne, offsetTwo) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int computeHashCode(Object object)
    {
        int hash = offHeapExtractors[0].computeHashFromOnHeapExtractor(object, onHeapExtractors[0]);
        for (int i=1;i<offHeapExtractors.length;i++)
        {
            hash = HashUtil.combineHashes(hash, offHeapExtractors[i].computeHashFromOnHeapExtractor(object, onHeapExtractors[i]));
        }
        return hash;
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        for(int i=0;i<onHeapExtractors.length;i++)
        {
            if (!onHeapExtractors[i].valueEquals(o1, o2) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int computeCombinedHashCode(OffHeapDataStorage dataStorage, int data, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, offHeapExtractors[0].computeHash(dataStorage, data));
        h = HashUtil.combineHashes(h, offHeapExtractors[1].computeHash(dataStorage, data));
        for(int i=2;i<offHeapExtractors.length;i++)
        {
            h = HashUtil.combineHashes(h, offHeapExtractors[i].computeHash(dataStorage, data));
        }
        return h;
    }

    @Override
    public int computeOnHeapCombinedHashCode(Object data, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, offHeapExtractors[0].computeHashFromOnHeapExtractor(data, onHeapExtractors[0]));
        h = HashUtil.combineHashes(h, offHeapExtractors[1].computeHashFromOnHeapExtractor(data, onHeapExtractors[1]));
        for(int i=2;i<offHeapExtractors.length;i++)
        {
            h = HashUtil.combineHashes(h, offHeapExtractors[i].computeHashFromOnHeapExtractor(data, onHeapExtractors[i]));
        }
        return h;
    }
}
