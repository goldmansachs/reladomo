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

package com.gs.fw.common.mithra.querycache;

import com.gs.fw.common.mithra.cache.LruQueryIndex;
import com.gs.fw.common.mithra.cache.CacheClock;
import com.gs.fw.common.mithra.cache.NonLruQueryIndex;
import com.gs.fw.common.mithra.cache.QueryIndex;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.finder.sqcache.SubQueryCache;


public class QueryCache
{
    private QueryIndex cache;
    private SubQueryCache subQueryCache;

    public QueryCache(int relationshipCacheSize, int minQueriesToKeep)
    {
        this(relationshipCacheSize, minQueriesToKeep, 0, 0, false);
    }

    public QueryCache(int relationshipCacheSize, int minQueriesToKeep, long timeToLive, long relationshipTimeToLive, boolean fullCache)
    {
        if (fullCache || (relationshipCacheSize == 0 && minQueriesToKeep == 0 && timeToLive == 0 && relationshipTimeToLive == 0))
        {
            this.cache = new NonLruQueryIndex();
        }
        else
        {
            this.cache = new LruQueryIndex(minQueriesToKeep, relationshipCacheSize, timeToLive, relationshipTimeToLive);
            if (timeToLive > 0) CacheClock.register(timeToLive);
            if (relationshipTimeToLive > 0) CacheClock.register(relationshipTimeToLive);
        }
        if (!fullCache)
        {
            subQueryCache = new SubQueryCache();
        }
    }

    public void cacheQueryForRelationship(CachedQuery query)
    {
        this.cache.put(query, true);
        cacheSubquery(query);
    }

    public void cacheQuery(CachedQuery query)
    {
        this.cache.put(query, false);
        cacheSubquery(query);
    }

    private void cacheSubquery(CachedQuery query)
    {
        if (this.subQueryCache != null && !query.wasDefaulted())
        {
            this.subQueryCache.cacheQuery(query);
        }
    }

    public CachedQuery findByEquality(Operation op)
    {
        return this.cache.get(op, false);
    }

    public CachedQuery findByEquality(Operation op, boolean forRelationship)
    {
        return this.cache.get(op, forRelationship);
    }

    public void clearCache()
    {
        this.cache.clear();
        if (this.subQueryCache != null)
        {
            this.subQueryCache.clear();
        }
    }

    public int roughSize()
    {
        return this.cache.roughSize();
    }

    public void destroy()
    {
        this.cache.destroy();
        this.cache = null;
    }

    public void cacheQueryForSubQuery(CachedQuery query, boolean forRelationship)
    {
        if (query.prepareToCacheQuery(forRelationship, this))
        {
            this.cache.put(query, forRelationship);
        }
    }

    public CachedQuery findBySubQuery(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship)
    {
        if (this.subQueryCache != null)
        {
            return this.subQueryCache.resolveAndCacheSubQuery(op, analyzedOperation, orderby, this, forRelationship);
        }
        return null;
    }
}
