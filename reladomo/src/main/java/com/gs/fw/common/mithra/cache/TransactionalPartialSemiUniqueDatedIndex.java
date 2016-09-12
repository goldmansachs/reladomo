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

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDataStorage;
import com.gs.fw.common.mithra.extractor.Extractor;



public class TransactionalPartialSemiUniqueDatedIndex extends TransactionalSemiUniqueDatedIndex
{

    public TransactionalPartialSemiUniqueDatedIndex(String indexName, Extractor[] extractors,
            AsOfAttribute[] asOfAttributes, long timeToLive, long relationshipTimeToLive)
    {
        super(indexName, extractors, asOfAttributes, timeToLive, relationshipTimeToLive);
    }

    @Override
    protected SemiUniqueDatedIndex createMainIndex(String indexName, Extractor[] extractors,
            AsOfAttribute[] asOfAttributes, Extractor[] pkExtractors, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage)
    {
        return new PartialSemiUniqueDatedIndex(indexName, extractors, asOfAttributes, pkExtractors, timeToLive, relationshipTimeToLive);
    }
}
