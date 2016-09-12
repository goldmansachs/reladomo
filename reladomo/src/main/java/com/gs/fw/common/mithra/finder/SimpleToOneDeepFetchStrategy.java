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

package com.gs.fw.common.mithra.finder;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.querycache.CachedQuery;

import java.util.*;

import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.ListFactory;


public class SimpleToOneDeepFetchStrategy extends SingleLinkDeepFetchStrategy
{

    private static final Extractor[] IDENTITY_EXTRACTORS = new Extractor[] { new IdentityExtractor()};

    public SimpleToOneDeepFetchStrategy(Mapper mapper, OrderBy orderBy)
    {
        super(mapper, orderBy);
    }

    public SimpleToOneDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper)
    {
        super(mapper, orderBy, alternateMapper);
    }

    public SimpleToOneDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper, int chainPosition)
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

        MithraList complexList = (MithraList) this.mapOpToList(node);
        if (!bypassCache)
        {
            List cachedQueryList = this.deepFetchToOneMostlyInMemory(immediateParentList, complexList.getOperation(), node);
            if (cachedQueryList != null) return cachedQueryList;
        }
        return deepFetchToOneFromServer(bypassCache, immediateParentList, complexList, node, forceImplicitJoin);
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

        MithraObjectPortal portal = this.getMapper().getFromPortal();
        if (portal.isCacheDisabled())
        {
            deepFetchResult.setPercentComplete(0);
            return deepFetchResult;
        }

        FullUniqueIndex fullResult = new FullUniqueIndex("identity", IDENTITY_EXTRACTORS);
        FastList notFound = new FastList();
        Map<Attribute, Object> tempOperationPool = new UnifiedMap();
        for (int i = 0; i < immediateParentList.size(); i++)
        {
            Object from = immediateParentList.get(i);
            Operation op = this.mapper.getOperationFromOriginal(from, tempOperationPool);
            op = this.addDefaultAsOfOp(op);
            CachedQuery oneResult = portal.zFindInMemory(op, null);
            if (oneResult == null)
            {
                notFound.add(from);
            }
            else
            {
                List result = oneResult.getResult();
                for(int j=0;j<result.size();j++)
                {
                    Object obj = result.get(j);
                    fullResult.putUsingUnderlying(obj, obj);
                }
            }
        }
        LocalInMemoryResult localInMemoryResult = new LocalInMemoryResult();
        localInMemoryResult.fullResult = fullResult;
        localInMemoryResult.notFound = notFound;
        deepFetchResult.setLocalResult(localInMemoryResult);
        int percentComplete = (immediateParentList.size() - notFound.size()) * 100 / immediateParentList.size();
        if (notFound.size() > 0 && percentComplete == 100)
        {
            percentComplete = 99;
        }
        deepFetchResult.setPercentComplete(percentComplete);
        if (notFound.isEmpty())
        {
            deepFetchResult.setResult(copyToList(fullResult));
            if (deepFetchResult.hasNothingToDo())
            {
                node.setResolvedList(ListFactory.EMPTY_LIST, chainPosition);
            }
            FastList parentList = localInMemoryResult.notFound;
            if (parentList.isEmpty())
            {
                node.setResolvedList(localInMemoryResult.fullResult.getAll(), chainPosition);
            }
        }
        return deepFetchResult;
    }

    @Override
    public boolean canFinishAdhocDeepFetchResult()
    {
        return true;
    }

    @Override
    public List finishAdhocDeepFetch(DeepFetchNode node, DeepFetchResult resultSoFar)
    {
        LocalInMemoryResult localResult = (LocalInMemoryResult) resultSoFar.getLocalResult();
        FastList parentList = localResult.notFound;
        Operation simplifiedJoinOp = node.getSimplifiedJoinOp(this.mapper, parentList);
        if (simplifiedJoinOp != null)
        {
            MithraList simplifiedList = findMany(simplifiedJoinOp);
            simplifiedList.setBypassCache(true);
            simplifiedList.forceResolve();
            node.setResolvedList(localResult.fullResult.getAll(), chainPosition);
            node.addToResolvedList(simplifiedList, chainPosition);
            return populateQueryCache(parentList, simplifiedList);
        }
        Extractor[] leftAttributesWithoutFilters = node.getRelatedFinder().zGetMapper().getLeftAttributesWithoutFilters();
        Set<Attribute> attrSet = UnifiedSet.newSet(leftAttributesWithoutFilters.length);
        for(Extractor e: leftAttributesWithoutFilters)
        {
            attrSet.add((Attribute) e);
        }
        node.createTempTableAndDeepFetchAdhoc(attrSet, node,
                parentList);
        node.addToResolvedList(localResult.fullResult.getAll(), chainPosition);
        return node.getCachedQueryList();
    }

    @Override
    public List deepFetchAdhocUsingTempContext(DeepFetchNode node, TupleTempContext tempContext, Object parentPrototype, List immediateParentList)
    {
        MithraList complexList = this.createListForAdHocDeepFetch(tempContext, parentPrototype);
        return deepFetchUsingComplexList(getImmediateParentList(node, immediateParentList), complexList, node);
    }

    protected List getImmediateParentList(DeepFetchNode node, List immediateParentList)
    {
        return immediateParentList == null ? getImmediateParentList(node) : immediateParentList;
    }

    @Override
    public List deepFetchAdhocUsingInClause(DeepFetchNode node, Attribute singleAttribute, List parentList)
    {
        Operation op = node.getSimplifiedJoinOp(this.getMapper(), parentList);
        if (op == null) return null;
        MithraList complexList = op.getResultObjectPortal().getFinder().findMany(op);
        return deepFetchUsingComplexList(getImmediateParentList(node, parentList), complexList, node);
    }

    private List copyToList(FullUniqueIndex fullResult)
    {
        final FastList list = new FastList(fullResult.size());
        fullResult.forAll(new DoUntilProcedure()
        {
            public boolean execute(Object object)
            {
                list.add(object);
                return false;
            }
        });
        return list;
    }

    protected void populateOpToListMapWithEmptyList(List immediateParentList, HashMap<Operation, List> opToListMap, UnifiedMap<Operation, Object> opToParentMap)
    {
        Map<Attribute, Object> tempOperationPool = new UnifiedMap();
        for (int i = 0; i < immediateParentList.size(); i++)
        {
            Object from = immediateParentList.get(i);
            Operation op = this.mapper.getOperationFromOriginal(from, tempOperationPool);
            op = addDefaultAsOfOp(op);
            if (opToListMap.put(op, ListFactory.EMPTY_LIST) == null)
            {
                opToParentMap.put(op, from);
            }
        }
    }

    protected List populateQueryCache(List immediateParentList, List resultList)
    {
        HashMap<Operation, List> opToListMap = new HashMap<Operation, List>();
        UnifiedMap<Operation, Object> opToParentMap = UnifiedMap.newMap();
        populateOpToListMapWithEmptyList(immediateParentList, opToListMap, opToParentMap);
        int doNotCacheCount = associateResultsWithOps(resultList, opToListMap, 1, opToParentMap);
        clearLeftOverObjectCache(opToParentMap);
        return cacheResults(opToListMap, doNotCacheCount);
    }

    private void clearLeftOverObjectCache(UnifiedMap<Operation, Object> opToParentMap)
    {
        if (opToParentMap.size() == 0)
        {
            return;
        }
        this.mapper.clearLeftOverFromObjectCache(opToParentMap.values(), null, null);
    }

    protected List deepFetchToOneFromServer(boolean bypassCache, List immediateParentList, MithraList complexList, DeepFetchNode node, boolean forceImplicitJoin)
    {
        if (bypassCache || complexList.getOperation().getResultObjectPortal().isCacheDisabled())
        {
            Operation simplifiedJoinOp = node.getSimplifiedJoinOp(this.mapper, immediateParentList);
            if (simplifiedJoinOp != null)
            {
                MithraList simplifiedList = findMany(simplifiedJoinOp);
                simplifiedList.setBypassCache(true);
                simplifiedList.forceResolve();
                node.setResolvedList(simplifiedList, chainPosition);
                associateSimplifiedResult(complexList.getOperation(), simplifiedList);
                associateResultsWithAlternateMapper(complexList.getOperation(), simplifiedList);
                return populateQueryCache(immediateParentList, simplifiedList);
            }
        }

        complexList.setBypassCache(bypassCache);
        complexList.setForceImplicitJoin(forceImplicitJoin);
        return deepFetchUsingComplexList(immediateParentList, complexList, node);
    }

    private List deepFetchUsingComplexList(List immediateParentList, MithraList complexList, DeepFetchNode node)
    {
        complexList.forceResolve();
        CachedQuery cachedQuery = new CachedQuery(complexList.getOperation(), this.orderBy);
        List result = Arrays.asList(complexList.toArray());
        cachedQuery.setResult(result);
        node.setResolvedList(result, chainPosition);
        cachedQuery.setOneQueryForMany(this.isResolvableInCache);
        boolean forRelationship = this.isResolvableInCache;
        cacheComplexQuery(cachedQuery, forRelationship);
        associateResultsWithAlternateMapper(complexList.getOperation(), complexList);

        List cachedQueryList = this.populateQueryCache(getImmediateParentList(node, immediateParentList), complexList);
        cachedQueryList.add(cachedQuery);
        return cachedQueryList;
    }

    protected List deepFetchToOneMostlyInMemory(List immediateParentList, Operation complexOperation, DeepFetchNode node)
    {
        MithraObjectPortal portal = complexOperation.getResultObjectPortal();
        if (portal.isCacheDisabled()) return null;

        FullUniqueIndex fullResult = new FullUniqueIndex("identity", IDENTITY_EXTRACTORS);
        FastList notFound = new FastList();
        Map<Attribute, Object> tempOperationPool = new UnifiedMap();
        for (int i = 0; i < immediateParentList.size(); i++)
        {
            Object from = immediateParentList.get(i);
            Operation op = this.mapper.getOperationFromOriginal(from, tempOperationPool);
            op = this.addDefaultAsOfOp(op);
            CachedQuery oneResult = portal.zFindInMemory(op, null);
            if (oneResult == null)
            {
                notFound.add(from);
            }
            else
            {
                List result = oneResult.getResult();
                for(int j=0;j<result.size();j++)
                {
                    Object obj = result.get(j);
                    fullResult.putUsingUnderlying(obj, obj);
                }
            }
        }
        List relatedList = null;
        boolean haveToGoToDatabase = notFound.size() > 0;
        List cachedQueryList = null;
        if (haveToGoToDatabase)
        {
            Operation simplifiedJoinOp = node.getSimplifiedJoinOp(this.mapper, notFound);
            if (simplifiedJoinOp != null)
            {
                MithraList partialList = findMany(simplifiedJoinOp);
                partialList.forceResolve();
                for(int j=0;j<partialList.size();j++)
                {
                    Object obj = partialList.get(j);
                    fullResult.putUsingUnderlying(obj, obj);
                }
                relatedList = copyToList(fullResult);
                cachedQueryList = populateQueryCache(immediateParentList, relatedList);
                haveToGoToDatabase = false;
            }
        }
        return finishDeepFetchInMemory(complexOperation, node, fullResult, relatedList, haveToGoToDatabase, cachedQueryList);
    }

    private List finishDeepFetchInMemory(Operation complexOperation, DeepFetchNode node, FullUniqueIndex fullResult,
            List relatedList, boolean haveToGoToDatabase, List cachedQueryList)
    {
        if (!haveToGoToDatabase)
        {
            if (relatedList == null) relatedList = copyToList(fullResult);
            node.setResolvedList(relatedList, chainPosition);
            if (cachedQueryList == null)
            {
                cachedQueryList = FastList.newList(2);
            }
            if (this.isResolvableInCache)
            {
                cachedQueryList.add(relatedList);
            }
            CachedQuery cachedQuery = new CachedQuery(complexOperation, this.orderBy);
            if (this.orderBy != null && relatedList.size() > 1) Collections.sort(relatedList, this.orderBy);
            cachedQuery.setResult(relatedList);
            cachedQuery.setOneQueryForMany(this.isResolvableInCache);
            cacheComplexQuery(cachedQuery, this.isResolvableInCache);
            cachedQueryList.add(cachedQuery);
            associateResultsWithAlternateMapper(complexOperation, relatedList);
            return cachedQueryList;
        }
        return null;
    }

    private static class LocalInMemoryResult
    {
        private FullUniqueIndex fullResult;
        private FastList notFound;
    }
}
