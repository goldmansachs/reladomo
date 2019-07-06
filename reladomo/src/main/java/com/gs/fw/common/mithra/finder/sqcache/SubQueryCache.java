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

package com.gs.fw.common.mithra.finder.sqcache;

import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.QueryCache;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;

public class SubQueryCache
{
    private static final int MAX_REFS = 8;
    private static final int BITS_MASK = MAX_REFS - 1;
    private static final int MAX_SUBQUERY_SIZE = 30000; // see TestPerformance
    //todo: use Unsafe to write these as volatile
    private WeakReference<CachedQuery>[] hashTable = new WeakReference[MAX_REFS];
    private WeakReference<CachedQuery> allOperation;

    public void cacheQuery(CachedQuery query)
    {
        if (query.getResult().size() > MAX_SUBQUERY_SIZE)
        {
            return;
        }
        if (query.getOperation() instanceof All)
        {
            allOperation = new WeakReference<CachedQuery>(query);
        }
        else if (query.getOperation().zIsShapeCachable() && !query.isSubQuery())
        {
            int index = query.getOperation().zShapeHash() & BITS_MASK;
            hashTable[index] = new WeakReference<CachedQuery>(query);
        }
    }

    public CachedQuery resolveAndCacheSubQuery(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderBy, QueryCache cache, boolean forRelationship)
    {
        Operation toFind = op;
        if (analyzedOperation != null && analyzedOperation.isAnalyzedOperationDifferent())
        {
            toFind = analyzedOperation.getAnalyzedOperation();
        }
        CachedQuery result = null;
        for(int i=0;i<MAX_REFS;i++)
        {
            CachedQuery query = unwrap(hashTable[i]);
            if (query != null)
            {
                ShapeMatchResult shapeMatchResult = toFind.zShapeMatch(query.getOperation());
                List resolved = shapeMatchResult.resolve(cache);
                result = createAndCacheCachedQuery(op, analyzedOperation, orderBy, cache, forRelationship, resolved, query);
            }
        }
        if (result != null) return result;

        CachedQuery all = unwrap(allOperation);
        if (all != null && all.getOperation().getResultObjectPortal() == toFind.getResultObjectPortal() && toFind.zCanFilterInMemory())
        {
            result = createAndCacheCachedQuery(op, analyzedOperation, orderBy, cache, forRelationship,
                    op.applyOperation(all.getResult()), all);
        }
        return result;
    }

    private CachedQuery createAndCacheCachedQuery(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderBy,
            QueryCache cache, boolean forRelationship, List resolved, CachedQuery baseQuery)
    {
        if (resolved != null)
        {
            if (orderBy != null && resolved.size() > 1) Collections.sort(resolved, orderBy);

            boolean wasDefaulted = analyzedOperation != null && analyzedOperation.isAnalyzedOperationDifferent();
            if (wasDefaulted)
            {
                CachedQuery query2 = new CachedQuery(analyzedOperation.getAnalyzedOperation(), orderBy);
                query2.setResult(resolved);
                if (!baseQuery.isExpired())
                {
                    cache.cacheQueryForSubQuery(query2, forRelationship);
                }
            }

            CachedQuery result = new CachedQuery(op, orderBy);
            result.setResult(resolved);
            if (wasDefaulted)
            {
                result.setWasDefaulted();
            }
            if (!baseQuery.isExpired())
            {
                cache.cacheQueryForSubQuery(result, forRelationship);
            }
            return result;
        }
        return null;
    }

    private CachedQuery unwrap(WeakReference<CachedQuery> opRef)
    {
        if (opRef != null)
        {
            CachedQuery cachedQuery = opRef.get();
            if (cachedQuery != null && !cachedQuery.isExpired())
            {
                return cachedQuery;
            }
        }
        return null;
    }

    public void clear()
    {
        this.allOperation = null;
        for(int i=0;i<MAX_REFS;i++)
        {
            this.hashTable[i] = null;
        }
    }
}
