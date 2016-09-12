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

public class SingleExtractorBasedOffHeapHashStrategy extends ExtractorBasedOffHeapHashStrategy
{
    private OffHeapExtractor offHeapExtractor;
    private Extractor onHeapExtractor;

    public SingleExtractorBasedOffHeapHashStrategy(OffHeapExtractor offHeapExtractor, Extractor onHeapExtractor)
    {
        this.offHeapExtractor = offHeapExtractor;
        this.onHeapExtractor = onHeapExtractor;
    }

    @Override
    public int computeHashCode(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return offHeapExtractor.computeHash(dataStorage, dataOffset);
    }

    @Override
    public int computeHashCode(Object valueHolder, List extractors)
    {
        return offHeapExtractor.computeHashFromOnHeapExtractor(valueHolder, (Extractor) extractors.get(0));
    }

    @Override
    public int computeHashCode(Object valueHolder, Extractor[] extractors)
    {
        return offHeapExtractor.computeHashFromOnHeapExtractor(valueHolder, extractors[0]);
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, List extractors)
    {
        return offHeapExtractor.equals(dataStorage, dataOffset, valueHolder, (Extractor) extractors.get(0));
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int cur, Object valueHolder, Extractor[] extractors)
    {
        return offHeapExtractor.equals(dataStorage, cur, valueHolder, extractors[0]);
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder)
    {
        return offHeapExtractor.equals(dataStorage, dataOffset, valueHolder, onHeapExtractor);
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int offsetOne, int offsetTwo)
    {
        return offHeapExtractor.valueEquals(dataStorage, offsetOne, offsetTwo);
    }

    @Override
    public int computeHashCode(Object object)  // this is a on heap object
    {
        return offHeapExtractor.computeHashFromOnHeapExtractor(object, onHeapExtractor);
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return onHeapExtractor.valueEquals(o1, o2);
    }

    public int computeCombinedHashCode(OffHeapDataStorage dataStorage, int data, int incomingHash)
    {
        return HashUtil.combineHashes(incomingHash, computeHashCode(dataStorage, data));
    }

    public int computeOnHeapCombinedHashCode(Object data, int incomingHash)
    {
        return HashUtil.combineHashes(incomingHash, this.computeHashCode(data));
    }
}
