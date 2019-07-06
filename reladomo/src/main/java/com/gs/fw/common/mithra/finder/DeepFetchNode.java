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

import com.gs.fw.common.mithra.DeepFetchTree;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ExecutorWithFinish;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.fw.common.mithra.util.MultiHashMap;
import com.gs.fw.common.mithra.util.PersisterId;
import com.gs.fw.common.mithra.util.ThreadConservingExecutor;
import com.gs.fw.finder.Navigation;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;


public class DeepFetchNode implements Serializable, DeepFetchTree
{
    private static final Comparator<? super Attribute> ADHOC_ATTRIBUTE_COMPARATOR = new Comparator<Attribute>()
    {
        @Override
        public int compare(Attribute o1, Attribute o2)
        {
            if (o1.equals(o2))
            {
                return 0;
            }
            int leftRank = o1.isAsOfAttribute() ? 100 : 0;
            int rightRank = o2.isAsOfAttribute() ? 100 : 0;
            if (leftRank == rightRank)
            {
                return o1.getAttributeName().compareTo(o2.getAttributeName());
            }
            else
            {
                return leftRank - rightRank;
            }
        }
    };

    private DeepFetchNode parent;
    private FastList<DeepFetchNode> children;
    private transient List cachedQueryList;
    private AbstractRelatedFinder relatedFinder;
    private Operation originalOperation;
    private boolean resolved = false;
    private boolean fullyResolved = false;
    private transient List resolvedList;
    private transient CachedQuery resolvedCachedQuery;
    private transient List[] chainedResults;
    private transient CachedQuery[] chainedCachedQueries;
    private static final int PERCENT_COMPLETE_TO_IGNORE = 80;
    private static final ExecutorWithFinish CURRENT_THREAD_EXECUTOR = new CurrentThreadExecutorService();

    public DeepFetchNode(DeepFetchNode parent, AbstractRelatedFinder relatedFinder)
    {
        this.parent = parent;
        this.relatedFinder = relatedFinder;
    }

    public DeepFetchNode copyForAdHoc()
    {
        return copyForAdHoc(null);
    }

    protected DeepFetchNode copyForAdHoc(DeepFetchNode parent)
    {
        DeepFetchNode result = new DeepFetchNode(parent, this.relatedFinder);
        result.resolved = this.resolved;
        result.fullyResolved = this.fullyResolved;
        if (cachedQueryList != null)
        {
            result.cachedQueryList = FastList.newList(this.cachedQueryList);
        }
        result.resolvedList = this.resolvedList;
        result.resolvedCachedQuery = this.resolvedCachedQuery;
        result.chainedResults = this.chainedResults;
        result.chainedCachedQueries = this.chainedCachedQueries;
        if (children != null)
        {
            result.children = FastList.newList(children.size());
            for(int i=0;i<this.children.size();i++)
            {
                result.children.add(this.children.get(i).copyForAdHoc(result));
            }
        }
        return result;
    }

    @Override
    public List<DeepFetchTree> getChildren()
    {
        if (children == null) return ListFactory.EMPTY_LIST;
        return (List<DeepFetchTree>) (List) this.children.toImmutable();
    }

    public void setResolvedList(List resolvedList, int chainPosition)
    {
        setResolvedList(resolvedList, chainPosition, null);
    }

    /**
     * Records the results of the deep fetch query for this node, e.g. for consumption by the child node.
     * @param resolvedList The resolved query results for this node
     * @param chainPosition The chain position, or zero if not applicable
     * @param resolvedCachedQuery An optional CachedQuery which may be used to determine if the query results may have expired.
     *                            Note: this query does not need to return the correct result set, it is merely indicative of cache expiry.
     *                            Set to null if not applicable.
     */
    public void setResolvedList(List resolvedList, int chainPosition, CachedQuery resolvedCachedQuery)
    {
        if (chainedResults == null || chainPosition == chainedResults.length - 1)
        {
            this.resolvedList = resolvedList;
            this.resolvedCachedQuery = resolvedCachedQuery;
        }
        else
        {
            this.chainedResults[chainPosition] = resolvedList;
            this.chainedCachedQueries[chainPosition] = resolvedCachedQuery;
        }
    }

    public boolean add(Navigation nav)
    {
        AbstractRelatedFinder arf = (AbstractRelatedFinder) nav;
        DeepRelationshipAttribute parentAttribute = arf.getParentDeepRelationshipAttribute();
        InternalList fullList = new InternalList(parentAttribute == null ? 1 : 6);
        fullList.add(arf);
        while(parentAttribute != null)
        {
            fullList.add(parentAttribute);
            parentAttribute = parentAttribute.getParentDeepRelationshipAttribute();
        }
        boolean added = this.addAll(fullList);
        if (added)
        {
            this.fullyResolved = false;
        }
        return added;
    }

    private boolean addAll(InternalList fullList)
    {
        int end = fullList.size() - 1;
        DeepFetchNode cur = this;
        boolean added = false;
        while(end >= 0)
        {
            AbstractRelatedFinder toAdd = (AbstractRelatedFinder) fullList.get(end);
            DeepFetchNode found = null;
            if (cur.children != null)
            {
                for(int i=0;i<cur.children.size();i++)
                {
                    DeepFetchNode o = cur.children.get(i);
                    if (o.equalsRelatedFinder(toAdd))
                    {
                        found = o;
                        break;
                    }
                }
            }
            if (found == null)
            {
                added = true;
                found = new DeepFetchNode(cur, toAdd);
                cur.addChild(found);
            }
            end--;
            cur = found;
        }
        return added;
    }

    private boolean equalsRelatedFinder(AbstractRelatedFinder toAdd)
    {
        return this.relatedFinder.equals(toAdd);
    }

    private void addChild(DeepFetchNode found)
    {
        if (this.children == null)
        {
            this.children = FastList.newList(4);
        }
        this.children.add(found);
        if (this.resolved && this.resolvedList != null && this.resolvedList.isEmpty())
        {
            found.setEmptyResult();
        }
    }

    public synchronized void deepFetch(CachedQuery resolved, boolean bypassCache, long queryTime, int numberOfParallelThreads, boolean forceImplicitJoin)
    {
        if (fullyResolved) return;
        if (numberOfParallelThreads < 1 || MithraManagerProvider.getMithraManager().isInTransaction())
        {
            numberOfParallelThreads = 1;
        }
        ExecutorWithFinish executor = numberOfParallelThreads == 1 ? CURRENT_THREAD_EXECUTOR :
                new ThreadConservingExecutor(numberOfParallelThreads);
        this.resolvedList = resolved.getResult();
        this.resolvedCachedQuery = resolved;
        this.originalOperation = resolved.getOperation();
        DeepFetchRunnable parent = new DeepFetchRunnable(null, bypassCache, executor, forceImplicitJoin);
        deepFetchChildren(bypassCache, executor, parent, forceImplicitJoin);
        executor.finish();
        this.resolved = true;
        this.fullyResolved = true;
    }

    private int deepFetchChildren(boolean bypassCache, Executor executor, DeepFetchRunnable parentRunnable, boolean forceImplicitJoin)
    {
        if (this.resolvedList == null || children == null) return 0;
        if (this.resolvedList.isEmpty())
        {
            markChildrenResolved();
            return 0;
        }
        //todo: choice for temp tables across children comes here.
        parentRunnable.setChildCount(children.size());
        for(int i=0;i<children.size();i++)
        {
            DeepFetchNode child = children.get(i);
            child.deepFetchStartingFromSelf(bypassCache, executor, parentRunnable, forceImplicitJoin);
        }
        return children.size();
    }

    private void markChildrenResolved()
    {
        if (children != null && !fullyResolved)
        {
            for(int i=0;i<children.size();i++)
            {
                DeepFetchNode child = children.get(i);
                child.setEmptyResultStartingFromSelf();
            }
        }
    }

    private void setEmptyResultStartingFromSelf()
    {
        setEmptyResult();
        markChildrenResolved();
        this.fullyResolved = true;
    }

    private void setEmptyResult()
    {
        if (!resolved)
        {
            this.resolvedList = ListFactory.EMPTY_LIST;
            this.resolvedCachedQuery = null;
            this.resolved = true;

            Operation rootOperation = this.getRootOperation();
            if (rootOperation != null)
            {
                CachedQuery query = new CachedQuery(this.relatedFinder.zGetMapper().createMappedOperationForDeepFetch(rootOperation), null, this.getParent().getResolvedCachedQuery());
                query.setResult(this.resolvedList);
                query.cacheQuery(true);
                this.resolvedCachedQuery = query;
            }
        }
    }

    public List getCachedQueryList()
    {
        return cachedQueryList;
    }

    private void addToCachedQueryList(List cachedQueryList)
    {
        //todo: this method should combine results instead of losing values from possibly other sources
        this.cachedQueryList = cachedQueryList;
        if (cachedQueryList == null)
        {
            this.resolvedList = ListFactory.EMPTY_LIST;
        }
    }

    private void deepFetchStartingFromSelf(boolean bypassCache, Executor executor, DeepFetchRunnable parentRunnable, boolean forceImplicitJoin)
    {
        DeepFetchRunnable runnable = new DeepFetchRunnable(parentRunnable, bypassCache, executor, forceImplicitJoin);
        executor.execute(runnable);
    }

    private boolean isRecursivelyPartiallyCached()
    {
        return !relatedFinder.zGetMapper().isFullyCachedIgnoringLeft() || this.isChildrenPartiallyCached()
                || MithraManagerProvider.getMithraManager().isInTransaction();
    }

    private boolean isChildrenPartiallyCached()
    {
        if (children == null || children.isEmpty()) return false;
        for(int i=0;i<children.size();i++)
        {
            if ((children.get(i)).isRecursivelyPartiallyCached()) return true;
        }
        return false;
    }

    public DeepFetchNode getParent()
    {
        return parent;
    }

    public List getResolvedList()
    {
        return resolvedList;
    }

    /**
     * Returns an optional CachedQuery, solely for the purpose of determining if the query results may be stale.
     * This query need not necessarily correspond to the same result set - it is merely indicative of cache expiry.
     * Returns null if not applicable.
     */
    public CachedQuery getResolvedCachedQuery()
    {
        return resolvedCachedQuery;
    }

    private Operation getRootOperation()
    {
        DeepFetchNode cur = this;
        while(cur.parent != null)
        {
            cur = cur.parent;
        }
        return cur.originalOperation;
    }

    public void clearResolved()
    {
        if (fullyResolved || resolved)
        {
            if (this.children != null)
            {
                for(int i=0;i<children.size();i++)
                {
                    DeepFetchNode child = children.get(i);
                    child.clearResolved();
                }
            }
            this.fullyResolved = false;
            this.resolved = false;
            this.resolvedList = null;
            this.resolvedCachedQuery = null;
            this.cachedQueryList = null;
        }
    }

    public void incrementalDeepFetch(CachedQuery resolved, boolean bypassCache, boolean forceImplicitJoin)
    {
        if (this.fullyResolved) return;
        this.resolvedList = resolved.getResult();
        this.resolvedCachedQuery = resolved;
        this.originalOperation = resolved.getOperation();
        incrementalDeepFetchChildren(bypassCache, forceImplicitJoin);
        this.fullyResolved = true;
    }

    private void incrementalDeepFetchChildren(boolean bypassCache, boolean forceImplicitJoin)
    {
        if (this.children != null)
        {
            //todo: choice for temp tables across children comes here.
            for(int i=0;i<children.size();i++)
            {
                DeepFetchNode child = children.get(i);
                child.incrementalDeepFetchStartingFromSelf(bypassCache, forceImplicitJoin);
            }
        }
    }

    public List getImmediateParentList(int chainPosition)
    {
        if (chainPosition == 0)
        {
            return this.parent.getResolvedList();
        }
        else
        {
            return chainedResults[chainPosition - 1];
        }
    }

    public CachedQuery getImmediateParentCachedQuery(int chainPosition)
    {
        if (chainPosition == 0)
        {
            return this.parent.getResolvedCachedQuery();
        }
        else
        {
            return chainedCachedQueries[chainPosition - 1];
        }
    }

    private void incrementalDeepFetchStartingFromSelf(boolean bypassCache, boolean forceImplicitJoin)
    {
        if (!this.resolved)
        {
            addToCachedQueryList(relatedFinder.zDeepFetch(this, bypassCache, forceImplicitJoin));
            this.resolved = true;
        }
        incrementalDeepFetchChildren(bypassCache, forceImplicitJoin);
    }

    public void incrementalDeepFetchAdhocList(List adHocList, boolean bypassCache)
    {
        //todo: implement this
        //throw new RuntimeException("not implemented");
    }

    public void deepFetchAdhocList(List adHocList, boolean bypassCache)
    {
        if (fullyResolved) return;
        this.resolvedList = adHocList;
        deepFetchChildrenForAdhoc(bypassCache);
        this.resolved = true;
        this.fullyResolved = true;
    }

    private void deepFetchChildrenForAdhoc(boolean bypassCache)
    {
        //analyze the children for commonality
        if (this.children != null)
        {
            //eliminate children that are resolvable in memory
            int originalSize = this.resolvedList.size();
            if (originalSize == 0) return;
            FastList<DeepFetchNode> childrenToProcess = FastList.newList(this.children.size());
            if (!bypassCache)
            {
                for(int i=0;i<children.size();i++)
                {
                    DeepFetchNode child = children.get(i);
                    DeepFetchResult result = child.deepFetchFirstLinkInMemory();
                    if (result.getPercentComplete() == 100) continue;
                    if (!child.canFinishAdhocDeepFetchResult() || (result.getPercentComplete() < PERCENT_COMPLETE_TO_IGNORE &&
                            ( result.getImmediateParentList() == null || result.getImmediateParentList().size() == originalSize)))
                    {
                        childrenToProcess.add(child);
                    }
                    else
                    {
                        // we get here if the original list has been filtered or most of it is completed in memory
                        child.finishAdHocDeepFetch(result);
                    }
                }
            }
            else
            {
                childrenToProcess.addAll(this.children);
            }

            if (!childrenToProcess.isEmpty())
            {
                deepFetchManyChildrenAdhoc(childrenToProcess);
            }
            for(int i=0;i<children.size();i++)
            {
                DeepFetchNode child = children.get(i);
                child.deepFetchChildrenForAdhoc(bypassCache);
            }
        }

    }

    private boolean canFinishAdhocDeepFetchResult()
    {
        return this.relatedFinder.zCanFinishAdhocDeepFetchResult();
    }

    public AbstractRelatedFinder getRelatedFinder()
    {
        return relatedFinder;
    }

    @Override
    public DeepRelationshipAttribute getRelationshipAttribute()
    {
        if (!(this.relatedFinder instanceof DeepRelationshipAttribute))
        {
            return null;
        }
        return (DeepRelationshipAttribute) this.relatedFinder;
    }

    private PersisterId getLeftPersisterId()
    {
        return relatedFinder.zGetMapper().getFromPortal().getPersisterId();
    }

    private void deepFetchManyChildrenAdhoc(List<DeepFetchNode> childrenToProcess)
    {
        List originalResolvedList = this.resolvedList;
        List<List> segregatedBySource = this.segregateBySource(originalResolvedList);
        try
        {

            for (int s = 0; s < segregatedBySource.size(); s++)
            {
                Map<DeepFetchKey, List<DeepFetchNode>> attributesToNodesMap = new HashMap<DeepFetchKey, List<DeepFetchNode>>(this.children.size());
                List resolvedBySourceList = segregatedBySource.get(s);
                this.resolvedList = resolvedBySourceList;
                UnifiedSet<Attribute> nonConstantSet = new UnifiedSet<Attribute>();
                UnifiedSet<Attribute> constantSet = new UnifiedSet<Attribute>();
                for (int i = 0; i < childrenToProcess.size(); i++)
                {
                    DeepFetchNode child = childrenToProcess.get(i);
                    Set<Attribute> leftAttributeSet = child.relatedFinder.zGetMapper().getAllLeftAttributes();
                    Extractor sourceAttribute = computeSourceAttribute(child.relatedFinder, leftAttributeSet);
                    removeConstants(leftAttributeSet, resolvedBySourceList, constantSet, nonConstantSet);
                    Extractor[] leftAttributesWithoutFilters = child.relatedFinder.zGetMapper().getLeftAttributesWithoutFilters();
                    UnifiedSet<Attribute> leftAttributesWithoutFiltersSet = UnifiedSet.newSet(leftAttributesWithoutFilters.length);
                    for(Extractor e: leftAttributesWithoutFilters)
                    {
                        leftAttributesWithoutFiltersSet.add((Attribute) e);
                    }
                    UnifiedSet<Attribute> localConstants = UnifiedSet.newSet(leftAttributesWithoutFiltersSet);
                    localConstants.removeAll(nonConstantSet);
                    leftAttributesWithoutFiltersSet.removeAll(constantSet);
                    if (!isNullConstant(localConstants, resolvedBySourceList))
                    {
                        if (leftAttributeSet.isEmpty() || child.isMappableForTempJoin(leftAttributeSet))
                        {
                            DeepFetchKey key = new DeepFetchKey(leftAttributeSet, leftAttributesWithoutFiltersSet, sourceAttribute, child.getLeftPersisterId());
                            List<DeepFetchNode> nodeList = attributesToNodesMap.get(key);
                            if (nodeList == null)
                            {
                                nodeList = new FastList<DeepFetchNode>(2);
                                attributesToNodesMap.put(key, nodeList);
                            }
                            nodeList.add(child);
                        }
                        else
                        {
                            child.deepFetchAdhocOneAtATime();
                        }
                    }
                    else
                    {
                        child.setEmptyResult();
                    }
                }
                for (Map.Entry<DeepFetchKey, List<DeepFetchNode>> entry : attributesToNodesMap.entrySet())
                {
                    List<DeepFetchNode> children = entry.getValue();
                    DeepFetchKey deepFetchKey = entry.getKey();
                    if (deepFetchKey.leftAttributesWithFilters.isEmpty())
                    {
                        //all the join values were constant
                        for (int i = 0; i < children.size(); i++)
                        {
                            children.get(i).setResolvedFromOne();
                        }
                    }
                    else
                    {
                        Attribute singleAttribute = deepFetchKey.leftAttributesWithFilters.iterator().next();
                        if (deepFetchKey.leftAttributesWithFilters.size() == 1 && !singleAttribute.isAsOfAttribute() && resolvedList.size() < DeepRelationshipUtility.MAX_SIMPLIFIED_IN)
                        {
                            FastList chainedChildren = FastList.newList(children.size());
                            for (int i = 0; i < children.size(); i++)
                            {
                                DeepFetchNode child = children.get(i);
                                boolean add = true;
                                if (child.relatedFinder.isSimple())
                                {
                                    add = !child.deepFetchSelfWithInClause(singleAttribute, resolvedBySourceList);
                                }
                                if (add)
                                {
                                    chainedChildren.add(child);
                                }
                            }
                            if (!chainedChildren.isEmpty())
                            {
                                deepFetchWithTempContext(resolvedBySourceList, chainedChildren, deepFetchKey);
                            }
                        }
                        else
                        {
                            deepFetchWithTempContext(resolvedBySourceList, children, deepFetchKey);
                        }
                    }
                }
            }
        }
        finally
        {
            this.resolvedList = originalResolvedList;
        }
    }

    private boolean isNullConstant(UnifiedSet<Attribute> constantSet, List resolvedBySourceList)
    {
        if (constantSet.isEmpty()) return false;
        Object one = resolvedBySourceList.get(0);
        for(Attribute a: constantSet)
        {
            if (a.isAttributeNull(one))
            {
                return true;
            }
        }
        return false;
    }

    private Extractor computeSourceAttribute(AbstractRelatedFinder relatedFinder, Set<Attribute> leftAttributeSet)
    {
        Mapper mapper = relatedFinder.zGetMapper();
        Attribute sourceAttribute = mapper.getAnyRightAttribute().getSourceAttribute();
        if (sourceAttribute != null)
        {
            //either we can rely on the mapping to the parent to have an equality to the source attribute, or
            //the source is in the mapper.
            MithraDatabaseIdentifierExtractor extractor = new MithraDatabaseIdentifierExtractor();
            MappedOperation mappedOperation = new MappedOperation(mapper, new All(mapper.getAnyLeftAttribute()));
            mappedOperation.registerOperation(extractor, true);
            if (extractor.hasEqualitySourceOperations())
            {
                sourceAttribute = leftAttributeSet.iterator().next().getSourceAttribute();
            }
            else
            {
                mapper.pushMappers(extractor);
                SourceOperation sourceOperation = extractor.getSourceOperation(extractor.getCurrentMapperList());
                if (sourceOperation instanceof OperationWithParameterExtractor)
                {
                    return ((OperationWithParameterExtractor) sourceOperation).getParameterExtractor();
                }
            }

        }
        return sourceAttribute;
    }

    private void deepFetchWithTempContext(List resolvedBySourceList, List<DeepFetchNode> children, DeepFetchKey deepFetchKey)
    {
        TupleTempContext tempContext = null;
        try
        {
            tempContext = createTempTableForDeepFetch(deepFetchKey.leftAttributesWithoutFilters, children.get(0).relatedFinder, resolvedBySourceList, deepFetchKey.sourceAttribute);
            if (tempContext != null)
            {
                for (int i = 0; i < children.size(); i++)
                {
                    children.get(i).deepFetchSelfWithTempContext(tempContext, null);
                }
            }
            else
            {
                for (int i = 0; i < children.size(); i++)
                {
                    children.get(i).setEmptyResult();
                }
            }
        }
        finally
        {
            if (tempContext != null) tempContext.destroy();
        }
    }

    private void deepFetchAdhocOneAtATime()
    {
        FullUniqueIndex resolvedIndex = new FullUniqueIndex("", IdentityExtractor.getArrayInstance());
        for(int i=0;i<parent.resolvedList.size();i++)
        {
            if (this.relatedFinder.isToOne())
            {
                resolvedIndex.put(this.relatedFinder.plainValueOf(parent.resolvedList.get(i)));
            }
            else
            {
                resolvedIndex.addAll((List)this.relatedFinder.plainValueOf(parent.resolvedList.get(i)));
            }
        }
        this.resolvedList = resolvedIndex.getAll();
        this.resolved = true;
    }

    private boolean isMappableForTempJoin(Set<Attribute> leftAttributeSet)
    {
        return this.relatedFinder.zGetMapper().isMappableForTempJoin(leftAttributeSet);
    }

    private void setResolvedFromOne()
    {
        if (this.relatedFinder.isToOne())
        {
            Object related = this.relatedFinder.plainValueOf(parent.resolvedList.get(0));
            if (related == null)
            {
                this.resolvedList = ListFactory.EMPTY_LIST;
            }
            else
            {
                this.resolvedList = ListFactory.create(related);
            }
        }
        else
        {
            this.resolvedList = (List) this.relatedFinder.plainValueOf(parent.resolvedList.get(0));
        }
        this.resolved = true;
    }

    private List<List> segregateBySource(List resolvedList)
    {
        Attribute sourceAttribute = this.relatedFinder.getSourceAttribute();
        if (sourceAttribute == null || resolvedList.size() == 1) return ListFactory.create(resolvedList);
        MultiHashMap map = null;
        Object first = resolvedList.get(0);
        for(int i=0;i < resolvedList.size(); i++)
        {
            Object current = resolvedList.get(i);
            if (map != null)
            {
                map.put(sourceAttribute.valueOf(current), current);
            }
            else if (!sourceAttribute.valueEquals(first, current))
            {
                map = new MultiHashMap();
                Object firstSource = sourceAttribute.valueOf(first);
                for(int j=0;j<i;j++)
                {
                    map.put(firstSource, resolvedList.get(j));
                }
                map.put(sourceAttribute.valueOf(current), current);
            }
        }

        if (map != null)
        {
            return map.valuesAsList();
        }
        else
        {
            return ListFactory.create(resolvedList);
        }
    }

    private void removeConstants(Set<Attribute> leftAttributeSet, List resolvedBySourceList, Set<Attribute> constantSet, Set<Attribute> nonConstantSet)
    {
        if (resolvedBySourceList.size() == 1)
        {
            leftAttributeSet.clear(); // everything is constant anyway
            return;
        }
        for(Iterator<Attribute> attributeIterator = leftAttributeSet.iterator(); attributeIterator.hasNext(); )
        {
            Attribute a = attributeIterator.next();
            if (constantSet.contains(a))
            {
                attributeIterator.remove();
            }
            else if (!nonConstantSet.contains(a))
            {
                boolean constant = true;
                Object first = resolvedBySourceList.get(0);
                for(int i=1;i<resolvedBySourceList.size();i++)
                {
                    if (!a.valueEquals(first, resolvedBySourceList.get(i)))
                    {
                        constant = false;
                        break;
                    }
                }
                if (constant)
                {
                    constantSet.add(a);
                    attributeIterator.remove();
                }
                else
                {
                    nonConstantSet.add(a);
                }
            }
        }
    }

    public void createTempTableAndDeepFetchAdhoc(Set<Attribute> allLeftAttributes, DeepFetchNode child, List parentList)
    {
        TupleTempContext tempContext = null;
        try
        {
            tempContext = createTempTableForDeepFetch(allLeftAttributes, child.relatedFinder, parentList, computeSourceAttribute(child.relatedFinder, allLeftAttributes));
            if (tempContext != null)
            {
                child.deepFetchSelfWithTempContext(tempContext, parentList);
            }
            else
            {
                child.setEmptyResult();
            }
        }
        finally
        {
            if (tempContext != null) tempContext.destroy();
        }
    }

    private void deepFetchSelfWithTempContext(TupleTempContext tempContext, List immediateParentList)
    {
        addToCachedQueryList(this.relatedFinder.zDeepFetchWithTempContext(this, tempContext, parent.resolvedList.get(0), immediateParentList));
    }

    private boolean deepFetchSelfWithInClause(Attribute singleAttribute, List parentList)
    {
        List list = this.relatedFinder.zDeepFetchWithInClause(this, singleAttribute, parentList);
        if (list == null)
        {
            return false;
        }
        addToCachedQueryList(list);
        return true;
    }

    private TupleTempContext createTempTableForDeepFetch(Set<Attribute> allLeftAttributes, AbstractRelatedFinder relatedFinder, List parentList, Extractor sourceAttribute)
    {
        Attribute[] arrayAttributes = new Attribute[allLeftAttributes.size()];
        allLeftAttributes.toArray(arrayAttributes);
        Arrays.sort(arrayAttributes, ADHOC_ATTRIBUTE_COMPARATOR);
        parentList = filterParentWithNullsOrFilters(arrayAttributes, relatedFinder.zGetMapper().filterLeftObjectList(parentList));
        TupleTempContext result = null;
        if (!parentList.isEmpty())
        {
            result = new TupleTempContext(arrayAttributes, sourceAttribute, null, true);
            result.enableRetryHook();
            result.insert(parentList, relatedFinder.zGetMapper().getFromPortal(), 100, false); //todo: set isParallel to true after implementing parallel deep fetch.
        }
        return result;
    }

    private List filterParentWithNullsOrFilters(Attribute[] arrayAttributes, List parentList)
    {
        List result = null;
        for(int i=0;i<parentList.size();i++)
        {
            Object o = parentList.get(i);
            boolean mustFilter = false;
            for (Attribute a : arrayAttributes)
            {
                mustFilter = a.isAttributeNull(o);
                if (mustFilter)
                {
                    if (result == null)
                    {
                        result = FastList.newList(parentList.size());
                        result.addAll(parentList.subList(0, i));
                    }
                    break;
                }
            }
            if (!mustFilter && result != null)
            {
                result.add(o);
            }
        }
        return result == null ? parentList : result;
    }

    private void finishAdHocDeepFetch(DeepFetchResult result)
    {
        addToCachedQueryList(this.relatedFinder.zFinishAdhocDeepFetch(this, result));
    }

    private DeepFetchResult deepFetchFirstLinkInMemory()
    {
        return this.relatedFinder.zDeepFetchFirstLinkInMemory(this);
    }

    public void allocatedChainedResults(int size)
    {
        this.chainedResults = new List[size];
        this.chainedCachedQueries = new CachedQuery[size];
    }

    // mapper is not necessarily this.relatedFinder.mapper. chained mapper and linked mapper's callbacks.
    public Operation createMappedOperationForDeepFetch(Mapper mapper)
    {
        Operation rootOperation = this.getRootOperation();
        if (rootOperation == null) return null;
        return mapper.createMappedOperationForDeepFetch(rootOperation);
    }

    // parent list may be partial (that is, a sublist of this.getImmediateParentList).
    // mapper is not necessarily this.relatedFinder.mapper. chained mapper
    public Operation getSimplifiedJoinOp(Mapper mapper, List parentList)
    {
        int maxSimplifiedIn = DeepRelationshipUtility.MAX_SIMPLIFIED_IN;
        boolean differentPersisterThanParent = differentPersisterIdThanParent(mapper);
        if (differentPersisterThanParent)
        {
            maxSimplifiedIn = Integer.MAX_VALUE;
        }
        return mapper.getSimplifiedJoinOp(parentList, maxSimplifiedIn, this, differentPersisterThanParent);
    }

    private boolean differentPersisterIdThanParent(Mapper mapper)
    {
        UnifiedSet<MithraObjectPortal> set = new UnifiedSet<MithraObjectPortal>();
        mapper.addDepenedentPortalsToSet(set);
        Iterator<MithraObjectPortal> portals = set.iterator();
        PersisterId id = portals.next().getPersisterId();
        while(portals.hasNext())
        {
            if (!id.equals(portals.next().getPersisterId())) return true;
        }
        return false;
    }

    public void addToResolvedList(List all, int chainPosition)
    {
        if (chainedResults == null || chainPosition == chainedResults.length - 1)
        {
            if (this.resolvedList == null || this.resolvedList.isEmpty())
            {
                this.resolvedList = all;
            }
            else
            {
                List temp = new MithraFastList(this.resolvedList.size() + all.size());
                temp.addAll(this.resolvedList);
                temp.addAll(all);
                this.resolvedList = temp;
            }
        }
        else
        {
            if (this.chainedResults[chainPosition] == null || this.chainedResults[chainPosition].isEmpty())
            {
                this.chainedResults[chainPosition] = all;
            }
            else
            {
                List temp = new MithraFastList(this.chainedResults[chainPosition].size() + all.size());
                temp.addAll(this.chainedResults[chainPosition]);
                temp.addAll(all);
                this.chainedResults[chainPosition] = temp;
            }
        }
    }

    private static class DeepFetchKey
    {
        private Set<Attribute> leftAttributesWithFilters;
        private Set<Attribute> leftAttributesWithoutFilters;
        private Extractor sourceAttribute;
        private PersisterId persisterId;
        private int hashCode = 0;

        private DeepFetchKey(Set<Attribute> leftAttributesWithFilters, Set<Attribute> leftAttributesWithoutFilters, Extractor sourceAttribute, PersisterId persisterId)
        {
            this.leftAttributesWithFilters = leftAttributesWithFilters;
            this.leftAttributesWithoutFilters = leftAttributesWithoutFilters;
            this.sourceAttribute = sourceAttribute;
            this.persisterId = persisterId;
        }

        private static boolean nullSafeEquals(Object value, Object other)
        {
            if (value == null)
            {
                if (other == null)
                {
                    return true;
                }
            }
            else if (other == value || value.equals(other))
            {
                return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            DeepFetchKey that = (DeepFetchKey) o;

            return leftAttributesWithFilters.equals(that.leftAttributesWithFilters) && persisterId.equals(that.persisterId) && nullSafeEquals(this.sourceAttribute, that.sourceAttribute);
        }

        @Override
        public int hashCode()
        {
            if (hashCode == 0) hashCode = computeHashCode();
            return hashCode;
        }

        public int computeHashCode()
        {
            int h = HashUtil.combineHashes(leftAttributesWithFilters.hashCode(), persisterId.hashCode());
            if (this.sourceAttribute != null)
            {
                h = HashUtil.combineHashes(h, this.sourceAttribute.hashCode());
            }
            return h;
        }
    }

    private static class CurrentThreadExecutorService implements ExecutorWithFinish
    {
        public void execute(Runnable command)
        {
            command.run();
        }

        public void finish()
        {
            //nothing to do
        }
    }

    private class DeepFetchRunnable implements Runnable
    {
        private DeepFetchRunnable parentRunnable;
        private boolean bypassCache;
        private Executor executor;
        private int childCount;
        private boolean forceImplicitJoin;

        private DeepFetchRunnable(DeepFetchRunnable parentRunnable, boolean bypassCache, Executor executor, boolean forceImplicitJoin)
        {
            this.parentRunnable = parentRunnable;
            this.bypassCache = bypassCache;
            this.executor = executor;
            this.forceImplicitJoin = forceImplicitJoin;
        }

        public void run()
        {
            if (!bypassCache)
            {
                if (relatedFinder.getMithraObjectPortal().isCacheDisabled())
                {
                    bypassCache = true;
                }
                DeepFetchNode parent = DeepFetchNode.this.parent;
                while (!bypassCache && parent != null)
                {
                    if (parent.relatedFinder.getMithraObjectPortal().isCacheDisabled())
                    {
                        bypassCache = true;
                    }
                    parent = parent.parent;
                }
            }
            if (bypassCache || isRecursivelyPartiallyCached())
            {
                List cachedQueryList = relatedFinder.zDeepFetch(DeepFetchNode.this, bypassCache, forceImplicitJoin);
                addToCachedQueryList(cachedQueryList);
            }
            DeepFetchNode.this.resolved = true;
            int childCount = deepFetchChildren(bypassCache, executor, this, forceImplicitJoin);
            if (childCount == 0)
            {
                decrementParent();
            }
        }

        private void decrementParent()
        {
            if (parentRunnable != null&& parentRunnable.decrementChildCount())
            {
                parentRunnable.runAfterChildren();
            }
        }

        private void runAfterChildren()
        {
            fullyResolved = true;
            decrementParent();
        }

        private synchronized boolean decrementChildCount()
        {
            childCount--;
            if (childCount == 0)
            {
                notifyAll();
                return true;
            }
            return false;
        }

        public synchronized void setChildCount(int childCount)
        {
            this.childCount = childCount;
        }
    }
}