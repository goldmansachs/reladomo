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
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;

import java.util.List;



public abstract class ExtractorBasedHashStrategy implements CommonExtractorBasedHashingStrategy
{
    public static final ExtractorBasedHashStrategy IDENTITY_HASH_STRATEGY = ExtractorBasedHashStrategy.create(IdentityExtractor.getArrayInstance());

    public static ExtractorBasedHashStrategy create(Extractor[] extractors)
    {
        switch(extractors.length)
        {
            case 1:
                return new SingleExtractorHashStrategy(extractors[0]);
            case 2:
                return new TwoExtractorHashStrategy(extractors[0], extractors[1]);
            default:
                return new MultiExtractorHashStrategy(extractors);
        }
    }

    public abstract int computeHashCode(Object o, List extractors);

    public abstract int computeCombinedHashCode(Object o, int incomingHash);

    public abstract boolean equals(Object first, Object second, List secondExtractors);

    public abstract boolean equals(Object first, Object second, Extractor[] extractors);

    public abstract int computeUpdatedHashCode(Object o, AttributeUpdateWrapper updateWrapper);

    public abstract boolean equalsIncludingUpdate(Object original, Object newObject, AttributeUpdateWrapper updateWrapper);

    public abstract Extractor getFirstExtractor();

    public abstract Extractor[] getExtractors();
}
