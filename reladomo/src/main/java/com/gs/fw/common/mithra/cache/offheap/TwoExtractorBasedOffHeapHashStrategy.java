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

public class TwoExtractorBasedOffHeapHashStrategy extends ExtractorBasedOffHeapHashStrategy
{
    private OffHeapExtractor firstOffHeapExtractor;
    private OffHeapExtractor secondOffHeapExtractor;
    private Extractor firstOnHeapExtractor;
    private Extractor secondOnHeapExtractor;

    public TwoExtractorBasedOffHeapHashStrategy(OffHeapExtractor[] offHeapExtractors, Extractor[] onHeapExtractors)
    {
        firstOffHeapExtractor = offHeapExtractors[0];
        secondOffHeapExtractor = offHeapExtractors[1];
        firstOnHeapExtractor = onHeapExtractors[0];
        secondOnHeapExtractor = onHeapExtractors[1];
    }

    @Override
    public int computeHashCode(OffHeapDataStorage dataStorage, int dataOffset)
    {
        return HashUtil.combineHashes(firstOffHeapExtractor.computeHash(dataStorage, dataOffset),
                secondOffHeapExtractor.computeHash(dataStorage, dataOffset));
    }

    @Override
    public int computeHashCode(Object valueHolder, List extractors)
    {
        return HashUtil.combineHashes(firstOffHeapExtractor.computeHashFromOnHeapExtractor(valueHolder, (Extractor) extractors.get(0)),
                secondOffHeapExtractor.computeHashFromOnHeapExtractor(valueHolder, (Extractor) extractors.get(1)));
    }

    @Override
    public int computeHashCode(Object valueHolder, Extractor[] extractors)
    {
        return HashUtil.combineHashes(firstOffHeapExtractor.computeHashFromOnHeapExtractor(valueHolder, extractors[0]),
                secondOffHeapExtractor.computeHashFromOnHeapExtractor(valueHolder, extractors[1]));
    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, List extractors)
    {
        return firstOffHeapExtractor.equals(dataStorage, dataOffset, valueHolder, (Extractor) extractors.get(0))
            && secondOffHeapExtractor.equals(dataStorage, dataOffset, valueHolder, (Extractor) extractors.get(1));

    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int cur, Object valueHolder, Extractor[] extractors)
    {
        return firstOffHeapExtractor.equals(dataStorage, cur, valueHolder, extractors[0]) &&
            secondOffHeapExtractor.equals(dataStorage, cur, valueHolder, extractors[1]);

    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder)
    {
        return firstOffHeapExtractor.equals(dataStorage, dataOffset, valueHolder, firstOnHeapExtractor) &&
            secondOffHeapExtractor.equals(dataStorage, dataOffset, valueHolder, secondOnHeapExtractor);

    }

    @Override
    public boolean equals(OffHeapDataStorage dataStorage, int offsetOne, int offsetTwo)
    {
        return firstOffHeapExtractor.valueEquals(dataStorage, offsetOne, offsetTwo) &&
            secondOffHeapExtractor.valueEquals(dataStorage, offsetOne, offsetTwo);
    }

    @Override
    public int computeHashCode(Object object)
    {
        return HashUtil.combineHashes(firstOffHeapExtractor.computeHashFromOnHeapExtractor(object, firstOnHeapExtractor),
                secondOffHeapExtractor.computeHashFromOnHeapExtractor(object, secondOnHeapExtractor));
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return firstOnHeapExtractor.valueEquals(o1, o2) && secondOnHeapExtractor.valueEquals(o1, o2);
    }

    public int computeCombinedHashCode(OffHeapDataStorage dataStorage, int data, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, firstOffHeapExtractor.computeHash(dataStorage, data));
        h = HashUtil.combineHashes(h, secondOffHeapExtractor.computeHash(dataStorage, data));
        return h;
    }

    public int computeOnHeapCombinedHashCode(Object data, int incomingHash)
    {
        int h = HashUtil.combineHashes(incomingHash, firstOffHeapExtractor.computeHashFromOnHeapExtractor(data, firstOnHeapExtractor));
        h = HashUtil.combineHashes(h, secondOffHeapExtractor.computeHashFromOnHeapExtractor(data, secondOnHeapExtractor));
        return h;
    }
}
