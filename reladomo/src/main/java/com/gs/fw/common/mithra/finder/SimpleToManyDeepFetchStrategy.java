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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.QueryCache;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ListFactory;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class SimpleToManyDeepFetchStrategy extends SingleLinkDeepFetchStrategy
{

    public SimpleToManyDeepFetchStrategy(Mapper mapper, OrderBy orderBy)
    {
        super(mapper, orderBy);
    }

    public SimpleToManyDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper)
    {
        super(mapper, orderBy, alternateMapper);
    }

    public SimpleToManyDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper, int chainPosition)
    {
        super(mapper, orderBy, alternateMapper, chainPosition);
    }

    public List deepFetch(DeepFetchNode node, boolean bypassCache, boolean forceImplicitJoin)
    {
        List immediateParentList = getImmediateParentList(node);
        if (immediateParentList.size() == 0)
        {
            return cacheEmptyResult(node);
        }
        HashMap<Operation, List> opToListMap = populateOpToListMapWithEmptyList(immediateParentList);
        HashMap<Operation, CachedQuery> opToCachedQueryMap = populateOpToCachedQueryMapWithEmptyCachedQuery(opToListMap);
        MithraList complexList = (MithraList) this.mapOpToList(node);
        complexList.setForceImplicitJoin(forceImplicitJoin);
        if (!bypassCache)
        {
            List result = deepFetchToManyInMemory(opToListMap, opToCachedQueryMap, immediateParentList, complexList.getOperation(), node);
            if (result != null)
            {
                return result;
            }
        }
        return deepFetchToManyFromServer(opToListMap, opToCachedQueryMap, bypassCache, immediateParentList, complexList, node);
    }

    private List cacheResultsForToMany(HashMap<Operation, List> opToListMap, HashMap<Operation, CachedQuery> opToCachedQueryMap, List immediateParentList, List list, DeepFetchNode node, CachedQuery cachedQueryToAssociateWithResult)
    {
        int roughSize = (list.size() / immediateParentList.size()) + 1;
        int doNotCacheCount = associateResultsWithOps(list, opToListMap, roughSize, null);
        node.setResolvedList(list, chainPosition, cachedQueryToAssociateWithResult);
        return cacheResults(opToListMap, opToCachedQueryMap, doNotCacheCount);
    }

    protected List deepFetchToManyInMemory(HashMap<Operation, List> opToListMap, HashMap<Operation, CachedQuery> opToCachedQueryMap, List immediateParentList, Operation op, DeepFetchNode node)
    {
        MithraObjectPortal portal = op.getResultObjectPortal();
        if (portal.isCacheDisabled()) return null;

        QueryCache queryCache = portal.getQueryCache();
        CachedQuery cachedResult = queryCache.findByEquality(op);
        if (cachedResult != null)
        {
            return cacheResultsForToMany(opToListMap, opToCachedQueryMap, immediateParentList, cachedResult.getResult(), node, cachedResult);
        }

        // Must create CachedQuery here before executing queries to avoid race condition against concurrent updates.
        // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
        CachedQuery complexCachedQuery = new CachedQuery(op, this.orderBy, getImmediateParentCachedQuery(node), true);

        Iterator<Operation> it = opToListMap.keySet().iterator();
        FastList queries = null;
        FastList resolvedList = null;
        while(it.hasNext())
        {
            Operation oneOp = it.next();
            CachedQuery cachedQuery = queryCache.findByEquality(oneOp);
            if (cachedQuery == null) return null;
            if (queries == null) queries = new FastList(opToListMap.size());
            if (resolvedList == null) resolvedList = new FastList(opToListMap.size());
            resolvedList.addAll(cachedQuery.getResult());
            queries.add(cachedQuery);
        }
        if (this.orderBy != null)
        {
            resolvedList.sortThis(this.orderBy);
        }
        node.setResolvedList(resolvedList, this.chainPosition, complexCachedQuery);
        complexCachedQuery.setResult(resolvedList);
        cacheComplexQuery(complexCachedQuery, true);
        queries.add(complexCachedQuery);
        return queries;
    }

    @Override
    public DeepFetchResult deepFetchFirstLinkInMemory(DeepFetchNode node)
    {
        List immediateParentList = getImmediateParentList(node);
        if (immediateParentList.size() == 0)
        {
            node.setResolvedList(ListFactory.EMPTY_LIST, chainPosition, getImmediateParentCachedQuery(node));
            return DeepFetchResult.nothingToDo();
        }
        DeepFetchResult deepFetchResult = new DeepFetchResult(immediateParentList);
        HashMap<Operation, List> opToListMap = populateOpToListMapWithEmptyList(immediateParentList);
        MithraObjectPortal portal = this.getMapper().getFromPortal();
        if (portal.isCacheDisabled())
        {
            return deepFetchResult;
        }

        QueryCache queryCache = portal.getQueryCache();
        Iterator<Operation> it = opToListMap.keySet().iterator();
        FastList queries = null;
        FastList resolvedList = null;
        CachedQuery firstCachedQuery = null;
        while(it.hasNext())
        {
            Operation oneOp = it.next();
            CachedQuery cachedQuery = queryCache.findByEquality(oneOp);
            if (cachedQuery == null)
            {
                // Must create CachedQuery here before executing queries to avoid race condition against concurrent updates
                if (firstCachedQuery == null)
                {
                    // Must incorporate the parent cached query update counters so this query will go stale if the parent does
                    // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
                    cachedQuery = new CachedQuery(oneOp, orderBy, getImmediateParentCachedQuery(node), true);
                }
                else
                {
                    cachedQuery = new CachedQuery(oneOp, orderBy, firstCachedQuery);
                }

                List list = portal.zFindInMemoryWithoutAnalysis(oneOp, true);
                if (list != null)
                {
                    if (orderBy != null && list.size() > 1)
                    {
                        Collections.sort(list, orderBy);
                    }
                    if (firstCachedQuery == null) firstCachedQuery = cachedQuery;
                    cachedQuery.setResult(list);
                    cachedQuery.cacheQuery(true);
                }
                else
                {
                    return deepFetchResult;
                }
            }
            if (queries == null) queries = new FastList(opToListMap.size());
            if (resolvedList == null) resolvedList = new FastList(opToListMap.size());
            resolvedList.addAll(cachedQuery.getResult());
            queries.add(cachedQuery);
        }
        node.setResolvedList(resolvedList, this.chainPosition, firstCachedQuery);
        deepFetchResult.setResult(queries);
        deepFetchResult.setPercentComplete(100);
        return deepFetchResult;
    }

    @Override
    public List deepFetchAdhocUsingTempContext(DeepFetchNode node, TupleTempContext tempContext, Object parentPrototype, List immediateParentList)
    {
        MithraList complexList = this.createListForAdHocDeepFetch(tempContext, parentPrototype);
        return deepFetchWithComplexList(node, immediateParentList, complexList);
    }

    private List deepFetchWithComplexList(DeepFetchNode node, List immediateParentList, MithraList complexList)
    {
        if (immediateParentList == null)
        {
            immediateParentList = this.getImmediateParentList(node);
        }
        HashMap<Operation, List> opToListMap = populateOpToListMapWithEmptyList(immediateParentList);
        HashMap<Operation, CachedQuery> opToCachedQueryMap = populateOpToCachedQueryMapWithEmptyCachedQuery(opToListMap);

        // Must create all CachedQuery objects here before executing queries to avoid race condition against concurrent updates
        // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
        CachedQuery complexListCachedQuery = new CachedQuery(complexList.getOperation(), this.orderBy, getImmediateParentCachedQuery(node), true);
        CachedQuery alternateMapperCachedQuery = createCachedQueryForAlternateMapper(complexList.getOperation(), getImmediateParentCachedQuery(node));

        complexList.forceResolve();
        complexListCachedQuery.setResult(complexList);

        associateResultsWithAlternateMapper(complexList, alternateMapperCachedQuery);
        return cacheResultsForToMany(opToListMap, opToCachedQueryMap, immediateParentList, complexList, node, complexListCachedQuery);
    }

    protected List deepFetchToManyFromServer(HashMap<Operation, List> opToListMap, HashMap<Operation, CachedQuery> opToCachedQueryMap, boolean bypassCache,
                                             List immediateParentList, MithraList complexList, DeepFetchNode node)
    {
        // Must create all CachedQuery objects here before executing queries to avoid race condition against concurrent updates
        // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
        CachedQuery complexListCachedQuery = new CachedQuery(complexList.getOperation(), this.orderBy, getImmediateParentCachedQuery(node), true);
        CachedQuery alternateMapperCachedQuery = createCachedQueryForAlternateMapper(complexList.getOperation(), getImmediateParentCachedQuery(node));

        MithraList list = getResolvedListFromServer(bypassCache, immediateParentList, complexList, node);

        if (list != complexList)
        {
            associateSimplifiedResult(list, complexListCachedQuery);
        }
        associateResultsWithAlternateMapper(list, alternateMapperCachedQuery);
        return cacheResultsForToMany(opToListMap, opToCachedQueryMap, immediateParentList, list, node, complexListCachedQuery);
    }

    protected MithraList getResolvedListFromServer(boolean bypassCache, List immediateParentList, MithraList complexList, DeepFetchNode node)
    {
        MithraList list = complexList;
        Operation simplifiedJoinOp = node.getSimplifiedJoinOp(this.mapper, immediateParentList);
        if (simplifiedJoinOp != null)
        {
            list = findMany(simplifiedJoinOp);
        }
        list.setBypassCache(bypassCache);
        list.forceResolve();
        return list;
    }

    @Override
    public List finishAdhocDeepFetch(DeepFetchNode node, DeepFetchResult resultSoFar)
    {
        if (resultSoFar.getPercentComplete() != 100)
        {
            throw new RuntimeException("Should not get here");
        }
        return ListFactory.EMPTY_LIST;
    }

    @Override
    public List deepFetchAdhocUsingInClause(DeepFetchNode node, Attribute singleAttribute, List parentList)
    {
        Operation op = node.getSimplifiedJoinOp(this.getMapper(), parentList);
        if (op == null) return null;
        MithraList complexList = op.getResultObjectPortal().getFinder().findMany(op);
        return deepFetchWithComplexList(node, parentList, complexList);
    }
}
