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


import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.CommonExtractorBasedHashingStrategy;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OffHeapableExtractor;
import com.gs.fw.common.mithra.extractor.TimestampExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.util.List;

public abstract class ExtractorBasedOffHeapHashStrategy implements CommonExtractorBasedHashingStrategy
{
    public abstract int computeHashCode(Object valueHolder, List extractors);

    public abstract boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder, List extractors);

    public abstract boolean equals(OffHeapDataStorage dataStorage, int cur, Object valueHolder, Extractor[] extractors);

    public abstract boolean equals(OffHeapDataStorage dataStorage, int dataOffset, Object valueHolder);

    public abstract int computeHashCode(OffHeapDataStorage dataStorage, int dataOffset);

    public abstract boolean equals(OffHeapDataStorage dataStorage, int offsetOne, int offsetTwo);

    public abstract int computeCombinedHashCode(OffHeapDataStorage dataStorage, int data, int incomingHash);

    public abstract int computeOnHeapCombinedHashCode(Object data, int incomingHash);

    public static OffHeapExtractor[] makeOffHeap(Extractor[] onHeapExtractors)
    {
        OffHeapExtractor[] result = new OffHeapExtractor[onHeapExtractors.length];
        for(int i=0;i<onHeapExtractors.length;i++)
        {
            result[i] = ((OffHeapableExtractor)onHeapExtractors[i]).zCreateOffHeapExtractor();
        }
        return result;
    }

    public static ExtractorBasedOffHeapHashStrategy create(Extractor[] onHeapExtractors)
    {
        switch(onHeapExtractors.length)
        {
            case 1:
                return new SingleExtractorBasedOffHeapHashStrategy(((OffHeapableExtractor)onHeapExtractors[0]).zCreateOffHeapExtractor(), onHeapExtractors[0]);
            case 2:
                if (onHeapExtractors[0] instanceof TimestampExtractor && onHeapExtractors[1] instanceof TimestampExtractor)
                {
                    return new TwoOffHeapTimestampHashStrategy(onHeapExtractors);
                }
                return new TwoExtractorBasedOffHeapHashStrategy(makeOffHeap(onHeapExtractors), onHeapExtractors);
            default:
                return new MultiExtractorBasedOffHeapHashStrategy(makeOffHeap(onHeapExtractors), onHeapExtractors);
        }
    }
}
