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

import java.io.ObjectStreamException;
import java.io.Serializable;



/**
 * IndexReference
 */
public final class IndexReference implements Serializable
{
    public final int cacheId;
    public final int indexReference;
    public static final int AS_OF_PROXY_INDEX_ID = 1000000;

    public IndexReference(Cache cache, int indexReference)
    {
        this.cacheId = cache.getId();
        this.indexReference = indexReference;
    }

    public boolean isForCache(Cache cache)
    {
        return this.cacheId == cache.getId();
    }

    public boolean isValid()
    {
        return this.indexReference > 0;
    }

    private Object writeReplace() throws ObjectStreamException
    {
        return null;
    }
}