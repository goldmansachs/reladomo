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

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.MappedAttribute;
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchRequiresExactSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.querycache.CompactUpdateCountOperation;
import com.gs.fw.common.mithra.util.*;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;

public class MappedOperation implements Operation
{
    private Operation op;
    private Mapper mapper;
    private transient Operation combinedOp;
    private static final Object DUMMY = new Object();

    public MappedOperation(Mapper mapper, Operation op)
    {
        if (op instanceof CompactUpdateCountOperation)
        {
            op = ((CompactUpdateCountOperation) op).forceGetCachableOperation();
        }
        this.op = op;
        this.mapper = mapper;
        if (mapper instanceof LinkedMapper)
        {
            LinkedMapper linkedMapper = (LinkedMapper) mapper;
            List mappers = linkedMapper.getMappers();
            this.mapper = (Mapper) mappers.get(0);
            for (int i = mappers.size() - 1; i > 0; i--)
            {
                Mapper m = (Mapper) mappers.get(i);
                this.op = new MappedOperation(m, this.op);
            }
        }
    }

    protected Operation getCombinedOp()
    {
        if (combinedOp == null)
        {
            Operation filterOp = mapper.getRightFilters();
            if (filterOp != null)
            {
                combinedOp = op.and(filterOp);
            }
            else
            {
                combinedOp = op;
            }
        }
        return combinedOp;
    }

    public List applyOperationToFullCache(EqualityOperation equalityOperation)
    {
        CachedQuery cachedQuery = op.getResultObjectPortal().zFindInMemory(getCombinedOp(), null);
        if (cachedQuery != null)
        {
            List joinedList = cachedQuery.getResult();
            return mapper.map(joinedList, equalityOperation);
        }
        return null;
    }

    public List applyOperationToFullCache()
    {
        if (this.zIsNone())
        {
            return new FastList(0);
        }
        //List joinedList = op.applyOperationToFullCache();
        // we choose to find here (instead of op.applyOperationToFullCache()) because it allows us
        // finer control over how sub-queries are resolved (for example, this sub-query may be
        // resolved from the query cache)
        // this can backfire when there are multiple sub-queries
        // todo: rezaem: in a transaction (with full cache), this results in going to the database on the partial query
        CachedQuery cachedQuery = op.getResultObjectPortal().zFindInMemory(getCombinedOp(), null);
        if (cachedQuery != null)
        {
            List joinedList = cachedQuery.getResult();
            return mapper.map(joinedList);
        }
        return null;
    }

    public List applyOperationToPartialCache()
    {
        if (this.zIsNone())
        {
            return new FastList(0);
        }
        if (mapper.isRightHandPartialCacheResolvable())
        {
            List joinedList = op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(getCombinedOp(), true);
            if (joinedList == null && mapper.getRightFilters() != null)
            {
                joinedList = op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, true);
            }
            if (joinedList != null)
            {
                if (joinedList.isEmpty())
                {
                    return new FastList(0);
                }
                return mapper.mapReturnNullIfIncompleteIndexHit(joinedList);
            }
        }
        return null;
    }

    public List applyOperation(List list)
    {
        Mapper reverseMapper = mapper.getReverseMapper();
        if (reverseMapper != null)
        {
            AsOfAttribute[] rightAsOfAttributes = ((PrivateReladomoClassMetaData)mapper.getFromPortal().getClassMetaData()).getCachedAsOfAttributes();
            AsOfAttribute[] leftAsOfAttributes = ((PrivateReladomoClassMetaData)mapper.getResultPortal().getClassMetaData()).getCachedAsOfAttributes();
            boolean forceOneByOne = false;
            Operation defaults = null;
            if (leftAsOfAttributes != null)
            {
                forceOneByOne = !mapper.hasLeftMappingsFor(leftAsOfAttributes);
            }
            if (rightAsOfAttributes != null)
            {
                if (!reverseMapper.hasLeftMappingsFor(rightAsOfAttributes))
                {
                    defaults = getDefaultOrExistingAsOfOps(rightAsOfAttributes, reverseMapper);
                    if (defaults == NoOperation.instance()) return null;
                }
            }
            if (forceOneByOne)
            {
                return applyOneByOne(list, reverseMapper, defaults);
            }
            if (mapper.getFromPortal().isPartiallyCached())
            {
                return applyForPartiallyCachedMapper(list, reverseMapper, defaults);
            }
            Operation toApply = this.op;
            if (defaults != null) toApply = defaults.and(this.op);
            List reverseMappedList = reverseMapper.map(list, toApply);
            if (reverseMappedList == null) return applyOneByOne(list, reverseMapper, defaults);
            if (reverseMappedList.size() == 0) return ListFactory.EMPTY_LIST;
            ConcurrentFullUniqueIndex rightIndex = mapper.mapMinusOneLevel(reverseMappedList);
            list = mapper.filterLeftObjectList(list);
            if (list != null)
            {
                return matchLeftToRight(list, rightIndex);
            }
        }
        return null;
    }

    private List applyForPartiallyCachedMapper(List list, Mapper reverseMapper, Operation defaults)
    {
        List result = applyOneByOne(list, reverseMapper, defaults);
        if (result == null)
        {
            List intersect = this.applyOperationToPartialCache();
            if (intersect != null)
            {
                result = intersectLists(intersect, list);
            }
        }
        return result;
    }

    private List matchLeftToRight(List list, ConcurrentFullUniqueIndex rightIndex)
    {
        Extractor[] extractors = this.mapper.getLeftAttributesWithoutFilters();
        if (list.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(list.size()))
        {
            return parallelMatchLeftToRight(list, rightIndex, extractors);
        }
        int size = list.size();
        MithraFastList result = new MithraFastList(size);
        addMatchingToList(list, rightIndex, extractors, result);
        return result;
    }

    private void addMatchingToList(List list, ConcurrentFullUniqueIndex rightIndex, Extractor[] extractors, MithraFastList result)
    {
        int size = list.size();
        for(int i=0;i< size;i++)
        {
            Object o = list.get(i);
            if (rightIndex.get(o, extractors) != null)
            {
                result.add(o);
            }
        }
    }

    private List parallelMatchLeftToRight(final List list, final ConcurrentFullUniqueIndex rightIndex, final Extractor[] extractors)
    {
        ThreadChunkSize threadChunkSize = new ThreadChunkSize(MithraCpuBoundThreadPool.getInstance().getThreads(), list.size(), 2);
        final int threads = threadChunkSize.getThreads();
        final MithraCompositeList result = new MithraCompositeList(threads);
        final ListBasedQueue queue = ListBasedQueue.createQueue(list, threadChunkSize.getChunkSize());
        final int expectedSize = list.size() / threads;
        MithraFastList first = new MithraFastList(0);
        result.synchronizedAddCompositedList(first);
        final MinExchange minExchange = new MinExchange(first, expectedSize);
        CooperativeCpuTaskFactory factory = new CooperativeCpuTaskFactory(MithraCpuBoundThreadPool.getInstance(), threads)
        {
            @Override
            protected CpuTask createCpuTask()
            {
                final MithraFastList localList = new MithraFastList(expectedSize);
                result.synchronizedAddCompositedList(localList);
                return new CpuTask()
                {
                    @Override
                    protected void execute()
                    {
                        MithraFastList result = localList;
                        List subList = queue.borrow(null);
                        while(subList != null)
                        {
                            addMatchingToList(subList, rightIndex, extractors, result);
                            subList = queue.borrow(subList);
                            result = (MithraFastList) minExchange.exchange(result);
                        }
                    }
                };
            }
        };
        factory.startAndWorkUntilFinished();
        return result;
    }

    private Operation getDefaultOrExistingAsOfOps(AsOfAttribute[] rightAsOfAttributes, Mapper reverseMapper)
    {
        AsOfAttribute[] probe = new AsOfAttribute[1];
        Operation defaults = NoOperation.instance();
        for(int i=0;i<rightAsOfAttributes.length;i++)
        {
            probe[0] = rightAsOfAttributes[i];
            if (!reverseMapper.hasLeftMappingsFor(probe))
            {
                Operation existing = this.op.zGetAsOfOp(probe[0]);
                if (existing == null)
                {
                    if (rightAsOfAttributes[i].getDefaultDate() == null) return null;
                    defaults = defaults.and(rightAsOfAttributes[i].eq(rightAsOfAttributes[i].getDefaultDate()));
                }
                else
                {
                    defaults = defaults.and(existing);
                }
            }
        }
        return defaults;
    }

    private List applyOneByOne(List joinedList, Mapper reverseMapper, Operation defaults)
    {
        Operation toApplyWith = this.op;
        Operation toApplyAfter = null;
        if (!AbstractMapper.isOperationEligibleForMapperCombo(toApplyWith))
        {
            toApplyWith = defaults;
            toApplyAfter = this.op;
        }
        else if (defaults != null)
        {
            toApplyWith = toApplyWith.and(defaults);
        }
        Set<Attribute> leftAttributes = mapper.getAllLeftAttributes();
        Extractor[] leftExtractors = new Extractor[leftAttributes.size()];
        leftAttributes.toArray(leftExtractors);
        FullUniqueIndex accepted = new FullUniqueIndex("", leftExtractors);
        FullUniqueIndex rejected = new FullUniqueIndex("", leftExtractors);
        MithraFastList result = new MithraFastList();
        for(int i=0;i<joinedList.size();i++)
        {
            Object joined = joinedList.get(i);
            if (accepted.contains(joined))
            {
                result.add(joined);
                continue;
            }
            if (rejected.contains(joined)) continue;
            List mapped = reverseMapper.mapOne(joined, toApplyWith);
            if (mapped == null) return null;
            if (toApplyAfter != null)
            {
                mapped = toApplyAfter.applyOperation(mapped);
                if (mapped == null) return null;
            }
            if (mapped.size() > 0)
            {
                accepted.put(joined);
                result.add(joined);
                if (accepted.size() == 1024)
                {
                    accepted.ensureCapacity(joinedList.size()/i*accepted.size());
                }
            }
            else
            {
                rejected.put(joined);
                if (rejected.size() == 1024)
                {
                    rejected.ensureCapacity(joinedList.size()/i*accepted.size());
                }
            }
        }
        return result;
    }

    public static List intersectLists(List list, List mappedList)
    {
        if (list.size() > mappedList.size())
        {
            List temp = list;
            list = mappedList;
            mappedList = temp;
        }
        IdentityHashMap map = new IdentityHashMap(list.size() * 2);
        for (int i = 0; i < list.size(); i++)
        {
            map.put(list.get(i), DUMMY);
        }
        MithraFastList result = new MithraFastList(list.size());
        for (int i = 0; i < mappedList.size(); i++)
        {
            Object key = mappedList.get(i);
            if (map.containsKey(key))
            {
                result.add(key);
            }
        }
        return result;
    }

    public boolean usesUniqueIndex()
    {
        return mapper.mapUsesUniqueIndex() && getCombinedOp().usesUniqueIndex();
    }

    public boolean usesImmutableUniqueIndex()
    {
        return mapper.mapUsesImmutableUniqueIndex() && getCombinedOp().usesImmutableUniqueIndex();
    }

    public boolean usesNonUniqueIndex()
    {
        return (mapper.mapUsesUniqueIndex() || mapper.mapUsesNonUniqueIndex()) &&
                (getCombinedOp().usesUniqueIndex() || getCombinedOp().usesNonUniqueIndex());
    }

    public int zEstimateReturnSize()
    {
        long estimate = (long) (this.getCombinedOp().zEstimateReturnSize() * mapper.estimateMappingFactor());
        return (int) Math.min(estimate, this.getResultObjectPortal().getCache().estimateQuerySize());
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        return mapper.estimateMaxReturnSize(this.getCombinedOp().zEstimateMaxReturnSize());
    }

    @Override
    public boolean zIsEstimatable()
    {
        return this.op.zIsEstimatable() && this.mapper.isEstimatable();
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        this.mapper.registerEqualitiesAndAtomicOperations(transitivePropagator);
        this.op.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
        this.mapper.popMappers(transitivePropagator);
    }

    public boolean zHazTriangleJoins()
    {
        return this.mapper.hasTriangleJoins();
    }

    public void zToString(ToStringContext toStringContext)
    {
        toStringContext.pushMapper(this.mapper);
        this.op.zToString(toStringContext);
        if (this.op instanceof All)
        {
            toStringContext.append(toStringContext.getCurrentAttributePrefix()).append("exists");
        }
        toStringContext.popMapper();
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    @Override
    public boolean zContainsMappedOperation()
    {
        return true;
    }

    @Override
    public boolean zHasParallelApply()
    {
        return true;
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        if (op instanceof MappedOperation)
        {
            MappedOperation mappedOperation = (MappedOperation) op;
            if (this.mapper.equals(mappedOperation.mapper))
            {
                return new MappedOperation(this.mapper, this.op.or(mappedOperation.op));
            }
        }
        if (op instanceof NotExistsOperation)
        {
            Operation combined = NotExistsOperation.zCombineNotExistsWithMapped((NotExistsOperation) op, this);
            if (combined != null) return combined;
        }
        return OrOperation.or(this, op);
    }

    protected Operation getInnerOperation()
    {
        return this.op;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        if (op instanceof MappedOperation)
        {
            MappedOperation mappedOperation = (MappedOperation) op;
            if (this.mapper.equals(mappedOperation.mapper))
            {
                return new MappedOperation(this.mapper, this.op.and(mappedOperation.op));
            }
        }
        return new AndOperation(this, op);
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return mapper.getResultPortal();
    }

    public String zGetResultClassName()
    {
        return mapper.getResultOwnerClassName();
    }

    public boolean zIsNone()
    {
        return op.zIsNone();
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        result.addAll(mapper.getAllLeftAttributes());
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        Mapper newMapper = this.mapper.createMapperForTempJoin(attributeMap, prototypeObject, 0);
        return new MappedOperation(newMapper, this.op);
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        Operation result = null;
        if (mapper.getLeftFilters() != null)
        {
            result = mapper.getLeftFilters().zGetAsOfOp(asOfAttribute);
        }
        return result;
    }

    public void generateSql(SqlQuery query)
    {
        mapper.generateSql(query);
        query.beginAnd();
        op.generateSql(query);
        query.endAnd();
        mapper.popMappers(query);
    }

    public int getClauseCount(SqlQuery query)
    {
        int count = this.op.getClauseCount(query);
        count += this.mapper.getClauseCount(query);
        return count;
    }

    public int hashCode()
    {
        return this.op.hashCode() ^ this.mapper.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof MappedOperation)
        {
            MappedOperation other = (MappedOperation) obj;
            return this.mapper.equals(other.mapper) && this.op.equals(other.op);
        }
        return false;
    }

    public void addDependentPortalsToSet(Set set)
    {
        mapper.addDepenedentPortalsToSet(set);
        op.addDependentPortalsToSet(set);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        mapper.addDepenedentAttributesToSet(set);
        op.addDepenedentAttributesToSet(set);
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return this.mapper.isJoinedWith(portal);
    }

    public Mapper getMapper()
    {
        return mapper;
    }

    public Operation getUnderlyingOperation()
    {
        return op;
    }

    public MappedOperation checkForSimpleCombination(MappedOperation other)
    {
        if (this.mapper.equals(other.mapper))
        {
            return new MappedOperation(this.mapper, this.op.and(other.op));
        }
        return null;
    }

    public boolean isSameMapFromTo(MappedOperation other)
    {
        return this.mapper.getFromPortal().equals(other.mapper.getFromPortal())
                && this.mapper.getResultPortal().equals(other.mapper.getResultPortal());
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return this.mapper.combineMappedOperations(this, op);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.op.zGetResultClassName()
                && !isSelfJoin())
        {
            return new MappedOperation(this.getMapper(), this.op.and(op));
        }
        return null;
    }

    private boolean isSelfJoin()
    {
        return this.getMapper().getFromPortal() == this.getMapper().getResultPortal();
    }

    public boolean underlyingOperationDependsOnAttribute(Attribute attribute)
    {
        HashSet existingAttributesWithOperations = new HashSet();
        this.getUnderlyingOperation().addDepenedentAttributesToSet(existingAttributesWithOperations);
        return existingAttributesWithOperations.contains(attribute);
    }

    public MappedOperation equalitySubstitute(Operation other)
    {
        if (other instanceof AtomicEqualityOperation)
        {
            return this.getMapper().equalitySubstituteWithAtomic(this, (AtomicOperation) other);
        }
        else if (other instanceof MultiEqualityOperation)
        {
            return this.getMapper().equalitySubstituteWithMultiEquality(this, (MultiEqualityOperation) other);
        }
        throw new RuntimeException("can only equality substitute with atomic or multi-equality");
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        return this.zCombinedAndWithAtomic(op);
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return this.zCombinedAndWithAtomic(op);
    }

    public Operation zCombinedAndWithAtomic(AtomicOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.op.zGetResultClassName()
                && !isSelfJoin())
        {
            return new MappedOperation(this.getMapper(), this.op.and(op));
        }
        return null;
    }

    /*
    returns the combined and operation. Many operation must be combined to resolve correctly
    (for example, anything that would result in a MultiEqualityMapper).
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing. Hands off!
    */
    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithMapped(this);
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return this.zCombinedAndWithAtomic(op);
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        this.mapper.registerAsOfAttributesAndOperations(checker);
        this.op.registerAsOfAttributesAndOperations(checker);
        this.mapper.popMappers(checker);
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            return this.and(MultiEqualityOperation.createEqOperation(asOfEqOperations));
        }
        this.mapper.pushMappers(stack);
        Operation newOperation = this.op.insertAsOfEqOperation(asOfEqOperations, insertPosition, stack);
        this.mapper.popMappers(stack);
        if (newOperation != null)
        {
            return new MappedOperation(this.getMapper(), newOperation);
        }
        Mapper newMapper = this.mapper.insertAsOfOperationInMiddle(asOfEqOperations, insertPosition, stack);
        if (newMapper != null)
        {
            return new MappedOperation(newMapper, this.op);
        }
        return null;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        if (insertPosition.equals(transitivePropagator.getCurrentMapperList()))
        {
            return new MappedOperation(mapper.insertOperationOnLeft(toInsert), this.op);
        }
        Operation op = transitivePropagator.constructAnd(insertPosition, this, toInsert);
        if (op != this)
        {
            return op;
        }
        this.mapper.pushMappers(transitivePropagator);
        Operation newOp = this.op.zInsertTransitiveOps(insertPosition, toInsert, transitivePropagator);
        this.mapper.popMappers(transitivePropagator);
        if (newOp != op)
        {
            return new MappedOperation(this.getMapper(), newOp);
        }
        Mapper newMapper = this.mapper.insertOperationInMiddle(insertPosition, toInsert, transitivePropagator);
        if (newMapper != null)
        {
            return new MappedOperation(newMapper, this.op);
        }
        return this;
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return new MappedOperation(mapper.insertAsOfOperationOnLeft(asOfEqOperations), this.op);
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        mapper.registerOperation(extractor, registerEquality);

        op.registerOperation(extractor, registerEquality);

        mapper.popMappers(extractor);
    }

    public boolean zHasAsOfOperation()
    {
        AsOfAttribute[] asOfAttributes = ((PrivateReladomoClassMetaData)this.getResultObjectPortal().getClassMetaData()).getCachedAsOfAttributes();
        return op.zHasAsOfOperation() && (!this.getResultObjectPortal().getClassMetaData().isDated() || this.mapper.hasLeftOrDefaultMappingsFor(asOfAttributes));
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        Mapper revMapper = mapper.getReverseMapper();
        if (revMapper.equals(this.mapper))
        {
            return new MappedOperation(mapper, new All(mapper.getAnyRightAttribute())).and(this.op);
        }
        return null;
    }

    public static Operation createExists(Attribute[] attributes, Attribute... joinedAttributes)
    {
        if (joinedAttributes[0] instanceof MappedAttribute)
        {
            return createExistsForMapped(attributes, joinedAttributes);
        }
        return createExistsForUnmappedOrMixed(attributes, joinedAttributes);
    }

    private static Operation createExistsForMapped(Attribute[] attributes, Attribute[] joinedAttributes)
    {
        MappedAttribute mappedAttribute = (MappedAttribute) joinedAttributes[0];
        Mapper rootMapper = mappedAttribute.getMapper();
        for(int i=1;i<joinedAttributes.length;i++)
        {
            if (!(joinedAttributes[i] instanceof MappedAttribute && ((MappedAttribute)joinedAttributes[i]).getMapper().equals(rootMapper)))
            {
                return createExistsForUnmappedOrMixed(attributes, joinedAttributes);
            }
        }
        MithraObjectPortal portal = joinedAttributes[0].getTopLevelPortal();
        Mapper mapper = mappedAttribute.getWrappedAttribute().constructEqualityMapper(attributes[0]);
        for(int i=1;i<joinedAttributes.length;i++)
        {
            if (joinedAttributes[i].getTopLevelPortal() != portal)
            {
                throw new MithraBusinessException("all joined attributes must come from the same finder "+joinedAttributes[0]+" "+joinedAttributes[i]);
            }
            mapper = mapper.and(((MappedAttribute)joinedAttributes[i]).getWrappedAttribute().constructEqualityMapper(attributes[i]));
        }
        return new MappedOperation(rootMapper, new MappedOperation(mapper, new All(attributes[0])));
    }

    private static Operation createExistsForUnmappedOrMixed(Attribute[] attributes, Attribute[] joinedAttributes)
    {
        Mapper mapper = joinedAttributes[0].constructEqualityMapper(attributes[0]);
        MithraObjectPortal portal = joinedAttributes[0].getTopLevelPortal();
        for(int i=1;i<joinedAttributes.length;i++)
        {
            if (joinedAttributes[i].getTopLevelPortal() != portal)
            {
                throw new MithraBusinessException("all joined attributes must come from the same finder "+joinedAttributes[0]+" "+joinedAttributes[i]);
            }
            mapper = mapper.and(joinedAttributes[i].constructEqualityMapper(attributes[i]));
        }
        return new MappedOperation(mapper, new All(attributes[0]));
    }

    public Boolean matches(Object o)
    {
        List result = this.applyOperation(Collections.singletonList(o));
        if (result == null) return null;
        return result.size() == 1;
    }

    public boolean zPrefersBulkMatching()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return ToStringContext.createAndToString(this);
    }

    @Override
    public boolean zCanFilterInMemory()
    {
        return false;
    }

    @Override
    public boolean zIsShapeCachable()
    {
        return false;
    }

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        return this.equals(existingOperation) ? ExactMatchSmr.INSTANCE : NoMatchRequiresExactSmr.INSTANCE;
    }

    @Override
    public int zShapeHash()
    {
        return this.hashCode();
    }
}
