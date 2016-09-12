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

import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.MithraDataObject;



public class TransactionalPartialUniqueIndex extends TransactionalUniqueIndex
{

    public TransactionalPartialUniqueIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive)
    {
        super(indexName, extractors, timeToLive, relationshipTimeToLive);
    }

    @Override
    protected PrimaryKeyIndex createMainIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive)
    {
        return new PartialWeakUniqueIndex(indexName, extractors, timeToLive, relationshipTimeToLive);
    }
//
//    public Object markDirty(MithraDataObject object)
//    {
//        return this.removeUsingUnderlying(object);
//    }
}
