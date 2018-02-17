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
        MithraList complexList = (MithraList) this.mapOpToList(node);
        complexList.setForceImplicitJoin(forceImplicitJoin);
        if (!bypassCache)
        {
            List result = deepFetchToManyInMemory(opToListMap, immediateParentList, complexList.getOperation(), node);
            if (result != null)
            {
                return result;
            }
        }
        return deepFetchToManyFromServer(opToListMap, bypassCache, immediateParentList, complexList, node);
    }

    private List cacheResultsForToMany(HashMap<Operation, List> opToListMap, List immediateParentList, List list, DeepFetchNode node)
    {
        int roughSize = (list.size() / immediateParentList.size()) + 1;
        int doNotCacheCount = associateResultsWithOps(list, opToListMap, roughSize, null);
        node.setResolvedList(list, chainPosition);
        return cacheResults(opToListMap, doNotCacheCount);
    }

    protected List deepFetchToManyInMemory(HashMap<Operation, List> opToListMap, List immediateParentList, Operation op, DeepFetchNode node)
    {
        MithraObjectPortal portal = op.getResultObjectPortal();
        if (portal.isCacheDisabled()) return null;

        QueryCache queryCache = portal.getQueryCache();
        CachedQuery cachedResult = queryCache.findByEquality(op);
        if (cachedResult != null)
        {
            return cacheResultsForToMany(opToListMap, immediateParentList, cachedResult.getResult(), node);
        }
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
        node.setResolvedList(resolvedList, this.chainPosition);
        CachedQuery complexCachedQuery = new CachedQuery(op, this.orderBy);
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
            node.setResolvedList(ListFactory.EMPTY_LIST, chainPosition);
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
                List list = portal.zFindInMemoryWithoutAnalysis(oneOp, true);
                if (list != null)
                {
                    if (orderBy != null && list.size() > 1)
                    {
                        Collections.sort(list, orderBy);
                    }
                    cachedQuery = new CachedQuery(oneOp, orderBy, firstCachedQuery);
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
        node.setResolvedList(resolvedList, this.chainPosition);
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
        complexList.forceResolve();
        associateResultsWithAlternateMapper(complexList.getOperation(), complexList);
        if (immediateParentList == null)
        {
            immediateParentList = this.getImmediateParentList(node);
        }
        HashMap<Operation, List> opToListMap = populateOpToListMapWithEmptyList(immediateParentList);
        return cacheResultsForToMany(opToListMap, immediateParentList, complexList, node);
    }

    protected List deepFetchToManyFromServer(HashMap<Operation, List> opToListMap, boolean bypassCache,
            List immediateParentList, MithraList complexList, DeepFetchNode node)
    {
        MithraList list = complexList;
        Operation simplifiedJoinOp = node.getSimplifiedJoinOp(this.mapper, immediateParentList);
        if (simplifiedJoinOp != null)
        {
            list = findMany(simplifiedJoinOp);
        }
        list.setBypassCache(bypassCache);
        list.forceResolve();

        if (list != complexList)
        {
            associateSimplifiedResult(complexList.getOperation(), list);
        }
        associateResultsWithAlternateMapper(complexList.getOperation(), list);
        return cacheResultsForToMany(opToListMap, immediateParentList, list, node);
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
