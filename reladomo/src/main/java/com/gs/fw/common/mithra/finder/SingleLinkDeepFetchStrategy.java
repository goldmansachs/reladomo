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
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ListFactory;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public abstract class SingleLinkDeepFetchStrategy extends DeepFetchStrategy
{

    static private Logger logger = LoggerFactory.getLogger(SingleLinkDeepFetchStrategy.class.getName());

    protected static final List DONT_CACHE_LIST = new FastList(0);

    protected final Mapper mapper;
    protected final Mapper parentMapper;
    protected final OrderBy orderBy;
    protected final AsOfEqOperation[] defaultAsOfOperation;
    protected final Operation defaultAsOfOperationAsSingleOp;
    private final Mapper alternateMapper;
    protected final boolean isResolvableInCache;
    protected final int chainPosition;

    protected SingleLinkDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper)
    {
        this(mapper, orderBy, alternateMapper, 0);
    }

    protected SingleLinkDeepFetchStrategy(Mapper mapper, OrderBy orderBy, Mapper alternateMapper, int chainPosition)
    {
        this.mapper = mapper;
        this.alternateMapper = alternateMapper;
        this.parentMapper = mapper.getParentMapper();
        this.orderBy = orderBy;
        this.defaultAsOfOperation = mapper.getDefaultAsOfOperation(ListFactory.EMPTY_LIST);
        this.chainPosition = chainPosition;
        Operation op = null;
        if (defaultAsOfOperation != null)
        {
            op = defaultAsOfOperation[0];
            for(int i=1;i<defaultAsOfOperation.length;i++)
            {
                op = op.and(defaultAsOfOperation[i]);
            }
        }
        defaultAsOfOperationAsSingleOp = op;
        Map<Attribute, Object> tempOperationPool = new UnifiedMap();
        Operation cacheOp = this.mapper.getPrototypeOperation(tempOperationPool);
        cacheOp = addDefaultAsOfOp(cacheOp);
        this.isResolvableInCache = cacheOp.usesUniqueIndex() && cacheOp.zEstimateMaxReturnSize() <= 1;
    }

    public int getChainPosition()
    {
        return chainPosition;
    }

    protected SingleLinkDeepFetchStrategy(Mapper mapper, OrderBy orderBy)
    {
        this(mapper, orderBy, null);
    }

    protected Mapper getAlternateMapper()
    {
        return alternateMapper;
    }

    protected Mapper getMapper()
    {
        return this.mapper;
    }

    protected List mapOpToList(DeepFetchNode node)
    {
        Mapper mapper = this.mapper;
        if (alternateMapper != null) mapper = alternateMapper;
        Operation result = node.createMappedOperationForDeepFetch(mapper);
        result = addDefaultAsOfOp(result);
        return result.getResultObjectPortal().getFinder().findMany(result);
    }

    protected Operation addDefaultAsOfOp(Operation result)
    {
        if (defaultAsOfOperation != null)
        {
            Operation newOp = result.zInsertAsOfEqOperationOnLeft(defaultAsOfOperation);
            if (newOp == result)
            {
                result = result.and(defaultAsOfOperationAsSingleOp);
            }
            else
            {
                result = newOp;
            }
        }
        return result;
    }

    protected static Logger getLogger()
    {
        return logger;
    }

    protected MithraList findMany(Operation op)
    {
        return op.getResultObjectPortal().getFinder().findMany(op);
    }

    protected List cacheResults(HashMap<Operation, List> opToListMap, int doNotCacheCount, CachedQueryPair baseQuery)
    {
        int initialCapacity = opToListMap.size() - doNotCacheCount;
        if (initialCapacity <= 0 ) initialCapacity = 1;
        FastList cachedQueries = new FastList(initialCapacity);
        Set<Map.Entry<Operation, List>> entries = opToListMap.entrySet();
        CachedQuery first = null;
        for (Iterator<Map.Entry<Operation, List>> it = entries.iterator(); it.hasNext();)
        {
            Map.Entry<Operation, List> entry = it.next();
            Operation op = entry.getKey();
            List resultList = entry.getValue();
            if (resultList != DONT_CACHE_LIST)
            {
                CachedQuery cachedQuery = new CachedQuery(op, this.orderBy, first);
                if (this.orderBy != null && resultList.size() > 1) Collections.sort(resultList, this.orderBy);
                cachedQuery.setResult(resultList);
                if (baseQuery != null && baseQuery.isExpired())
                {
                    return cachedQueries;
                }
                cachedQuery.cacheQueryForRelationship();
                cachedQueries.add(cachedQuery);
                if (first == null) first = cachedQuery;
            }
        }
        return cachedQueries;
    }

    protected int associateResultsWithOps(List list, HashMap<Operation, List> opToListMap, int roughSize, UnifiedMap<Operation, Object> opToParentMap)
    {
        int doNotCacheCount = 0;
        Map<Attribute, Object> tempOperationPool = new UnifiedMap();
        for (int i = 0; i < list.size(); i++)
        {
            Object related = list.get(i);
            Operation op = this.mapper.getOperationFromResult(related, tempOperationPool);
            op = addDefaultAsOfOp(op);
            if (opToParentMap != null)
            {
                opToParentMap.remove(op);
            }
            List existing = opToListMap.get(op);
            if (existing == ListFactory.EMPTY_LIST)
            {
                if (this.isResolvableInCache)
                {
                    opToListMap.put(op, DONT_CACHE_LIST);
                    existing = DONT_CACHE_LIST;
                    doNotCacheCount++;
                }
                else
                {
                    existing = new FastList(roughSize);
                    opToListMap.put(op, existing);
                }
            }
            if (existing != null && existing != DONT_CACHE_LIST) // it is possible for this to happen when the database is changing under our feet.
            {
                existing.add(related);
            }
            else if (existing != DONT_CACHE_LIST)
            {
                this.getLogger().warn("Brought back more related objects than we should. " +
                        "Ignoring them for now. The database is changing as it's being queried. " +
                        "If you need better consistency, you must use a transaction.");
            }
        }
        return doNotCacheCount;
    }

    protected HashMap<Operation, List> populateOpToListMapWithEmptyList(List immediateParentList)
    {
        HashMap opToListMap = new HashMap();
        Map<Attribute, Object> tempOperationPool = new UnifiedMap();
        for (int i = 0; i < immediateParentList.size(); i++)
        {
            Object from = immediateParentList.get(i);
            Operation op = this.mapper.getOperationFromOriginal(from, tempOperationPool);
            op = addDefaultAsOfOp(op);
            opToListMap.put(op, ListFactory.EMPTY_LIST);
        }
        return opToListMap;
    }

    protected List getImmediateParentList(DeepFetchNode node)
    {
        List originalParentList = node.getImmediateParentList(this.chainPosition);
        List filteredParentList = this.mapper.filterLeftObjectList(originalParentList);
        if (filteredParentList == null) filteredParentList = originalParentList;
        return filteredParentList;
    }

    protected void associateSimplifiedResult(Operation op, MithraList list, CachedQueryPair baseQuery)
    {
        List resultList = new FastList(list);
        if (this.orderBy != null && resultList.size() > 1) Collections.sort(resultList, this.orderBy);
        associateSimplifiedResult(op, resultList, baseQuery);
    }

    protected void associateSimplifiedResult(Operation op, List resultList, CachedQueryPair baseQuery)
    {
        CachedQuery cachedQuery = new CachedQuery(op, this.orderBy);
        if (this.orderBy != null && resultList.size() > 1) Collections.sort(resultList, this.orderBy);
        cachedQuery.setResult(resultList);
        if (!baseQuery.isExpired())
        {
            cachedQuery.cacheQuery(false);
        }
    }

    protected void associateResultsWithAlternateMapper(Operation originalOp, MithraList list, CachedQueryPair baseQuery)
    {
        if (this.alternateMapper != null)
        {
            associateSimplifiedResult(this.mapper.createMappedOperationForDeepFetch(originalOp), list, baseQuery);
        }
    }

    protected void associateResultsWithAlternateMapper(Operation originalOp, List list, CachedQueryPair baseQuery)
    {
        if (this.alternateMapper != null)
        {
            associateSimplifiedResult(this.mapper.createMappedOperationForDeepFetch(originalOp), list, baseQuery);
        }
    }

    protected MithraList createListForAdHocDeepFetch(TupleTempContext tempContext, Object parentPrototype)
    {
        Map<Attribute, Attribute> attributeMap = tempContext.getPrototypeToTupleAttributeMap();
        Mapper mapper = this.mapper;
        if (alternateMapper != null) mapper = alternateMapper;
        mapper = mapper.createMapperForTempJoin(attributeMap, parentPrototype, this.chainPosition);
        Operation op;
        if (tempContext.hasSourceAttribute())
        {
            op = tempContext.getSourceOperation(tempContext.getPrototypeSourceAttribute().valueOf(parentPrototype), this.mapper.getFromPortal().getFinder().getSourceAttribute());
        }
        else
        {
            op = tempContext.all();
        }
        Operation result = mapper.createMappedOperationForDeepFetch(op);
        result = addDefaultAsOfOp(result);
        return result.getResultObjectPortal().getFinder().findMany(result);
    }

    protected List cacheEmptyResult(DeepFetchNode node)
    {
        MithraList complexList = (MithraList) this.mapOpToList(node);
        CachedQuery query = new CachedQuery(complexList.getOperation(), null);
        query.setResult(ListFactory.EMPTY_LIST);
        node.setResolvedList(ListFactory.EMPTY_LIST, chainPosition);
        query.cacheQuery(true);
        return FastList.newListWith(query);
    }

    protected void cacheComplexQuery(CachedQuery cachedQuery, boolean forRelationship)
    {
        AnalyzedOperation analyzedOperation = new AnalyzedOperation(cachedQuery.getOperation());
        if (analyzedOperation.isAnalyzedOperationDifferent())
        {
            cachedQuery.setWasDefaulted();
        }
        cachedQuery.cacheQuery(forRelationship);
    }

    protected CachedQuery getCachedQueryFromList(MithraList baseList)
    {
        DelegatingList delegatingList = (DelegatingList) baseList;
        return delegatingList.zGetDelegated().getCachedQuery(delegatingList);
    }

    protected static class CachedQueryPair
    {
        private CachedQuery one;
        private CachedQuery two;

        public CachedQueryPair(CachedQuery one)
        {
            this.one = one;
        }

        public void add(CachedQuery two)
        {
            this.two = two;
        }

        public boolean isExpired()
        {
            return this.one.isExpired() || (this.two != null && this.two.isExpired());
        }

        public CachedQuery getOne()
        {
            return this.one;
        }
    }
}
