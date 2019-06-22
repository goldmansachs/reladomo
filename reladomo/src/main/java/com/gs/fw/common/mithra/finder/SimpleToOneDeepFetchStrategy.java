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
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.ListFactory;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.*;


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
            node.setResolvedList(ListFactory.EMPTY_LIST, chainPosition); // this adhoc deep fetch result will not be cached so a CachedQuery is not required
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
                node.setResolvedList(ListFactory.EMPTY_LIST, chainPosition); // this adhoc deep fetch result will not be cached so a CachedQuery is not required
            }
            FastList parentList = localInMemoryResult.notFound;
            if (parentList.isEmpty())
            {
                node.setResolvedList(localInMemoryResult.fullResult.getAll(), chainPosition); // this adhoc deep fetch result will not be cached so a CachedQuery is not required
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

        // Must create all CachedQuery objects here before executing queries to avoid race condition against concurrent updates
        final OpMapsBundle opMapsBundle = createOpMaps(parentList);

        MithraList simplifiedList = fetchSimplifiedJoinList(node, parentList, true);
        if (simplifiedList != null)
        {
            node.setResolvedList(localResult.fullResult.getAll(), chainPosition); // this adhoc deep fetch result will not be cached so a CachedQuery is not required
            node.addToResolvedList(simplifiedList, chainPosition);
            return populateQueryCache(simplifiedList, opMapsBundle);
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

    private OpMapsBundle createOpMaps(List immediateParentList)
    {
        final HashMap<Operation, List> opToListMap = new HashMap<Operation, List>();
        final UnifiedMap<Operation, Object> opToParentMap = UnifiedMap.newMap();
        populateOpToListMapWithEmptyList(immediateParentList, opToListMap, opToParentMap);

        final HashMap<Operation, CachedQuery> opToCachedQueryMap = populateOpToCachedQueryMapWithEmptyCachedQuery(opToListMap);

        return new OpMapsBundle(opToListMap, opToParentMap, opToCachedQueryMap);
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
        MithraList complexList = getSimplifiedJoinUnresolvedList(node, parentList);
        return deepFetchUsingComplexList(getImmediateParentList(node, parentList), complexList, node);
    }

    private Operation getSimplifiedJoinOp(DeepFetchNode node, List parentList)
    {
        return node.getSimplifiedJoinOp(this.getMapper(), parentList);
    }

    private boolean canSimplifyJoinOp(DeepFetchNode node, List parentList)
    {
        Operation op = getSimplifiedJoinOp(node, parentList);
        return op != null;
    }

    private MithraList getSimplifiedJoinUnresolvedList(DeepFetchNode node, List parentList)
    {
        Operation op = getSimplifiedJoinOp(node, parentList);
        if (op == null) return null;
        return findMany(op);
    }

    protected MithraList fetchSimplifiedJoinList(DeepFetchNode node, List parentList, boolean bypassCache)
    {
        final MithraList mithraList = getSimplifiedJoinUnresolvedList(node, parentList);
        if (mithraList != null)
        {
            mithraList.setBypassCache(bypassCache);
            mithraList.forceResolve();
        }
        return mithraList;
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

    protected List populateQueryCache(List resultList, OpMapsBundle opMapsBundle)
    {
        int doNotCacheCount = associateResultsWithOps(resultList, opMapsBundle.getOpToListMap(), 1, opMapsBundle.getOpToParentMap());
        clearLeftOverObjectCache(opMapsBundle.getOpToParentMap());
        return cacheResults(opMapsBundle.getOpToListMap(), opMapsBundle.getOpToCachedQueryMap(), doNotCacheCount);
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
            if (canSimplifyJoinOp(node, immediateParentList))
            {
                // Must create all CachedQuery objects here before executing queries to avoid race condition against concurrent updates
                // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
                OpMapsBundle opMapsBundle = createOpMaps(immediateParentList);
                CachedQuery complexListCachedQuery = new CachedQuery(complexList.getOperation(), this.orderBy, getImmediateParentCachedQuery(node), true);
                CachedQuery alternateMapperCachedQuery = createCachedQueryForAlternateMapper(complexList.getOperation(), getImmediateParentCachedQuery(node));

                MithraList simplifiedList = fetchSimplifiedJoinList(node, immediateParentList, true);
                if (simplifiedList != null)
                {
                    node.setResolvedList(simplifiedList, chainPosition, complexListCachedQuery);
                    associateSimplifiedResult(simplifiedList, complexListCachedQuery);
                    associateResultsWithAlternateMapper(simplifiedList, alternateMapperCachedQuery);
                    return populateQueryCache(simplifiedList, opMapsBundle);
                }
            }
        }

        complexList.setBypassCache(bypassCache);
        complexList.setForceImplicitJoin(forceImplicitJoin);
        return deepFetchUsingComplexList(immediateParentList, complexList, node);
    }

    private List deepFetchUsingComplexList(List immediateParentList, MithraList complexList, DeepFetchNode node)
    {
        // Must create all CachedQuery objects here before executing queries to avoid race condition against concurrent updates
        // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
        OpMapsBundle opMapsBundle = createOpMaps(getImmediateParentList(node, immediateParentList));
        CachedQuery cachedQuery = new CachedQuery(complexList.getOperation(), this.orderBy, getImmediateParentCachedQuery(node), true);
        CachedQuery alternateMapperCachedQuery = createCachedQueryForAlternateMapper(complexList.getOperation(), getImmediateParentCachedQuery(node));

        resolveComplexList(complexList);

        List result = Arrays.asList(complexList.toArray());
        cachedQuery.setResult(result);
        node.setResolvedList(result, chainPosition, cachedQuery);
        cachedQuery.setOneQueryForMany(this.isResolvableInCache);
        boolean forRelationship = this.isResolvableInCache;
        cacheComplexQuery(cachedQuery, forRelationship);
        associateResultsWithAlternateMapper(complexList, alternateMapperCachedQuery);

        List cachedQueryList = this.populateQueryCache(complexList, opMapsBundle);
        cachedQueryList.add(cachedQuery);
        return cachedQueryList;
    }

    protected void resolveComplexList(MithraList complexList)
    {
        complexList.forceResolve();
    }

    protected List deepFetchToOneMostlyInMemory(List immediateParentList, Operation complexOperation, DeepFetchNode node)
    {
        // Must create all CachedQuery objects here before executing queries to avoid race condition against concurrent updates
        // Merge the update counters of the parent CachedQuery so that if the parent list becomes stale then this cache entry will become stale too.
        OpMapsBundle opMapsBundle = createOpMaps(immediateParentList);
        CachedQuery complexOperationCachedQuery = new CachedQuery(complexOperation, this.orderBy, getImmediateParentCachedQuery(node), true);
        CachedQuery alternateMapperCachedQuery = createCachedQueryForAlternateMapper(complexOperation, getImmediateParentCachedQuery(node));

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
            MithraList partialList = fetchSimplifiedJoinList(node, notFound, false);
            if (partialList != null)
            {
                for(int j=0;j<partialList.size();j++)
                {
                    Object obj = partialList.get(j);
                    fullResult.putUsingUnderlying(obj, obj);
                }
                relatedList = copyToList(fullResult);
                cachedQueryList = populateQueryCache(relatedList, opMapsBundle);
                haveToGoToDatabase = false;
            }
        }
        return finishDeepFetchInMemory(node, fullResult, relatedList, haveToGoToDatabase, cachedQueryList,
                                       complexOperationCachedQuery, alternateMapperCachedQuery);
    }

    private List finishDeepFetchInMemory(DeepFetchNode node, FullUniqueIndex fullResult,
                                         List relatedList, boolean haveToGoToDatabase, List cachedQueryList,
                                         CachedQuery complexOperationCachedQuery, CachedQuery alternateMapperCachedQuery)
    {
        if (!haveToGoToDatabase)
        {
            if (relatedList == null) relatedList = copyToList(fullResult);
            node.setResolvedList(relatedList, chainPosition, complexOperationCachedQuery);
            if (cachedQueryList == null)
            {
                cachedQueryList = FastList.newList(2);
            }
            if (this.isResolvableInCache)
            {
                cachedQueryList.add(relatedList);
            }
            if (this.orderBy != null && relatedList.size() > 1) Collections.sort(relatedList, this.orderBy);
            complexOperationCachedQuery.setResult(relatedList);
            complexOperationCachedQuery.setOneQueryForMany(this.isResolvableInCache);
            cacheComplexQuery(complexOperationCachedQuery, this.isResolvableInCache);
            cachedQueryList.add(complexOperationCachedQuery);
            associateResultsWithAlternateMapper(relatedList, alternateMapperCachedQuery);
            return cachedQueryList;
        }
        return null;
    }

    private static class LocalInMemoryResult
    {
        private FullUniqueIndex fullResult;
        private FastList notFound;
    }

    private class OpMapsBundle
    {
        private final HashMap<Operation, List> opToListMap;
        private final UnifiedMap<Operation, Object> opToParentMap;
        private final HashMap<Operation, CachedQuery> opToCachedQueryMap;

        private OpMapsBundle(HashMap<Operation, List> opToListMap, UnifiedMap<Operation, Object> opToParentMap, HashMap<Operation, CachedQuery> opToCachedQueryMap)
        {
            this.opToListMap = opToListMap;
            this.opToParentMap = opToParentMap;
            this.opToCachedQueryMap = opToCachedQueryMap;
        }

        private HashMap<Operation, List> getOpToListMap()
        {
            return opToListMap;
        }

        private UnifiedMap<Operation, Object> getOpToParentMap()
        {
            return opToParentMap;
        }

        private HashMap<Operation, CachedQuery> getOpToCachedQueryMap()
        {
            return opToCachedQueryMap;
        }
    }
}
