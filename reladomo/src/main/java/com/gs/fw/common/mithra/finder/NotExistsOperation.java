
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.*;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;

public class NotExistsOperation implements Operation
{
    private Operation op;
    private Mapper mapper;
    private transient NotExistsOperation originalContainer;

    private static final long serialVersionUID = 1674543777147403981L;

    public NotExistsOperation(Mapper mapper, Operation op)
    {
        this.op = op;
        this.mapper = mapper;
        this.originalContainer = this;
        if (mapper instanceof LinkedMapper)
        {
            LinkedMapper linkedMapper = (LinkedMapper) mapper;
            List mappers = linkedMapper.getMappers();
            this.mapper = (Mapper) mappers.get(0);
            for (int i = mappers.size() - 1; i > 0; i--)
            {
                Mapper m = (Mapper) mappers.get(i);
                this.op = new NotExistsOperation(m, this.op);
            }
        }
    }

    protected NotExistsOperation(Mapper mapper, Operation op, NotExistsOperation originalContainer)
    {
        this(mapper, op);
        this.originalContainer = originalContainer;
    }

    public List applyOperationToFullCache()
    {
        if (this.mapper.getResultPortal().getCache().isDated()) return null;
        List everything = this.mapper.getResultPortal().getCache().getAll();
        return applyOperation(everything);
    }

    public List applyOperationToPartialCache()
    {
        return null;
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
                rejected.put(joined);
                if (rejected.size() == 1024)
                {
                    rejected.ensureCapacity(joinedList.size()/i*accepted.size());
                }
            }
            else
            {
                accepted.put(joined);
                result.add(joined);
                if (accepted.size() == 1024)
                {
                    accepted.ensureCapacity(joinedList.size()/i*accepted.size());
                }
            }
        }
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
            if (forceOneByOne || mapper.getFromPortal().isPartiallyCached())
            {
                return applyOneByOne(list, reverseMapper, defaults);
            }
            Operation toApply = this.op;
            if (defaults != null) toApply = defaults.and(this.op);
            List reverseMappedList = reverseMapper.map(list, toApply);
            if (reverseMappedList == null) return applyOneByOne(list, reverseMapper, defaults);
            ConcurrentFullUniqueIndex rightIndex = mapper.mapMinusOneLevel(reverseMappedList);
            List filteredList = mapper.filterLeftObjectList(list);
            if (filteredList != null)
            {
                return matchLeftToRight(filteredList, list, rightIndex);
            }
        }
        return null;
    }

    private List matchLeftToRight(List filteredList, List originalList, ConcurrentFullUniqueIndex rightIndex)
    {
        Extractor[] extractors = this.mapper.getLeftAttributesWithoutFilters();
        if (filteredList.size() == originalList.size() && filteredList.size() > 1 && MithraCpuBoundThreadPool.isParallelizable(filteredList.size()))
        {
            return parallelMatchLeftToRight(filteredList, rightIndex, extractors);
        }
        MithraFastList result = new MithraFastList(filteredList.size());
        if (filteredList.size() != originalList.size())
        {
            FullUniqueIndex filtered = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, filteredList.size());
            filtered.addAll(filteredList);
            for(int i=0;i<originalList.size();i++)
            {
                Object o = originalList.get(i);
                if (!filtered.contains(o))
                {
                    result.add(o);
                }
            }
        }
        return addMatchingToList(filteredList, rightIndex, extractors, result);
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

    private List addMatchingToList(List list, ConcurrentFullUniqueIndex rightIndex, Extractor[] extractors, MithraFastList result)
    {
        for(int i=0;i< list.size();i++)
        {
            Object o = list.get(i);
            if (rightIndex.get(o, extractors) == null)
            {
                result.add(o);
            }
        }
        return result;
    }

    public boolean usesUniqueIndex()
    {
        return false;
    }

    public boolean usesImmutableUniqueIndex()
    {
        return false;
    }

    public boolean usesNonUniqueIndex()
    {
        return false;
    }

    public int zEstimateReturnSize()
    {
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    @Override
    public boolean zIsEstimatable()
    {
        MithraObjectPortal portal = this.getResultObjectPortal();
        return portal.isFullyCached() && !portal.isForTempObject() && this.mapper.isEstimatable();
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        //better not to do anything, as the "not" is hard to handle is unlikely to help with query resolution
    }

    public boolean zHazTriangleJoins()
    {
        return this.mapper.hasTriangleJoins();
    }

    public void zToString(ToStringContext toStringContext)
    {
        toStringContext.pushMapper(mapper);
        toStringContext.append(toStringContext.getCurrentAttributePrefix());
        if (!(op instanceof All))
        {
            toStringContext.append("{").append(op.toString()).append("}");
        }
        toStringContext.append("not exists");
        toStringContext.popMapper();
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        if (op instanceof NotExistsOperation)
        {
            NotExistsOperation mappedOperation = (NotExistsOperation) op;
            if (this.mapper.equals(mappedOperation.mapper))
            {
                // not makes or into and
                return new NotExistsOperation(this.mapper, this.op.and(mappedOperation.op));
            }
        }
        if (op instanceof MappedOperation)
        {
            MappedOperation mappedOperation = (MappedOperation) op;
            Operation combined = zCombineNotExistsWithMapped(this, mappedOperation);
            if (combined != null) return combined;
        }
        return OrOperation.or(this, op);
    }

    public static Operation zCombineNotExistsWithMapped(NotExistsOperation notExists, MappedOperation mappedOperation)
    {
        if (notExists.op instanceof All && notExists.mapper.equals(mappedOperation.getMapper()) && mappedOperation.getInnerOperation() instanceof NegatableOperation)
        {
            NegatableOperation innerOperation = (NegatableOperation) mappedOperation.getInnerOperation();
            return new NotExistsOperation(notExists.mapper, innerOperation.zNegate());
        }
        return null;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        if (op instanceof NotExistsOperation)
        {
            NotExistsOperation mappedOperation = (NotExistsOperation) op;
            if (this.mapper.equals(mappedOperation.mapper))
            {
                // not makes and into or
                return new NotExistsOperation(this.mapper, this.op.or(mappedOperation.op));
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
        return false;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        result.addAll(mapper.getAllLeftAttributes());
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        Mapper newMapper = this.mapper.createMapperForTempJoin(attributeMap, prototypeObject, 0);
        return new NotExistsOperation(newMapper, this.op);
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public void generateSql(SqlQuery query)
    {
        pushContainer(query);
        query.setNotExistsForNextOperation();
        mapper.generateSql(query);
        boolean insertedAnd = query.beginAnd();
        op.generateSql(query);
        query.endAnd(insertedAnd);
        mapper.popMappers(query);
        query.popMapperContainer();
    }

    private void pushContainer(MapperStack query)
    {
        query.pushMapperContainer(this.originalContainer == null ? this : this.originalContainer); // can be null when deserialized
    }

    public int getClauseCount(SqlQuery query)
    {
        int count = this.op.getClauseCount(query);
        count += this.mapper.getClauseCount(query);
        return count;
    }

    public int hashCode()
    {
        return this.op.hashCode() ^ this.mapper.hashCode() ^ 0x34812695;
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof NotExistsOperation)
        {
            NotExistsOperation other = (NotExistsOperation) obj;
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

    protected Mapper getMapper()
    {
        return mapper;
    }

    public Operation zCombinedAndWithMapped(NotExistsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return null;
    }

    public Operation zCombinedAnd(Operation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return null;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        pushContainer(checker);
        this.mapper.registerAsOfAttributesAndOperations(checker);
        this.op.registerAsOfAttributesAndOperations(checker);
        this.mapper.popMappers(checker);
        checker.popMapperContainer();
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            return new AndOperation(MultiEqualityOperation.createEqOperation(asOfEqOperations), this);
        }
        pushContainer(stack);
        this.mapper.pushMappers(stack);
        Operation newOperation = this.op.insertAsOfEqOperation(asOfEqOperations, insertPosition, stack);
        this.mapper.popMappers(stack);
        if (newOperation != null)
        {
            stack.popMapperContainer();
            Operation result = new NotExistsOperation(this.getMapper(), newOperation, this);
            stack.substituteContainer(this, result);
            return result;
        }
        Mapper newMapper = this.mapper.insertAsOfOperationInMiddle(asOfEqOperations, insertPosition, stack);
        stack.popMapperContainer();
        if (newMapper != null)
        {
            Operation result = new NotExistsOperation(newMapper, this.op, this);
            stack.substituteContainer(this, result);
            return result;
        }
        return null;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return this;
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return new NotExistsOperation(mapper.insertAsOfOperationOnLeft(asOfEqOperations), this.op, this);
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        pushContainer(extractor);
        mapper.registerOperation(extractor, registerEquality);

        op.registerOperation(extractor, registerEquality);

        mapper.popMappers(extractor);
        extractor.popMapperContainer();
    }

    public boolean zHasAsOfOperation()
    {
        return false;
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        return null;
    }

    public Operation zFindEquality(TimestampAttribute attr)
    {
        return null;
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
    public boolean zContainsMappedOperation()
    {
        return true;
    }

    @Override
    public boolean zHasParallelApply()
    {
        return true;
    }
}
