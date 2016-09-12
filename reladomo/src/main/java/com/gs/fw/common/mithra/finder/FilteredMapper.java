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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.SmallSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class FilteredMapper extends AbstractMapper
{
    private final Mapper mapper;
    private final Operation leftFilters;
    private final Operation rightFilters;

    public FilteredMapper(Mapper mapper, Operation leftFilters, Operation rightFilters)
    {
        this.mapper = mapper;
        this.leftFilters = leftFilters;
        this.rightFilters = rightFilters;
    }

    private FilteredMapper(Mapper mapper, Operation leftFilters, Operation rightFilters, Mapper reverseMapper)
    {
        this(mapper, leftFilters, rightFilters);
        this.reverseMapper = reverseMapper;
    }

    public MithraObjectPortal getResultPortal()
    {
        return this.mapper.getResultPortal();
    }

    public MithraObjectPortal getFromPortal()
    {
        return this.mapper.getFromPortal();
    }

    public boolean isReversible()
    {
        return this.mapper.isReversible();
    }

    public boolean mapUsesUniqueIndex()
    {
        return this.mapper.mapUsesUniqueIndex();
    }

    public boolean mapUsesImmutableUniqueIndex()
    {
        return this.mapper.mapUsesImmutableUniqueIndex();
    }

    public boolean mapUsesNonUniqueIndex()
    {
        return this.mapper.mapUsesNonUniqueIndex();
    }

    public List map(List joinedList)
    {
        if (rightFilters != null) joinedList = rightFilters.applyOperation(joinedList);
        if (joinedList == null) return null;
        if (leftFilters != null)
        {
            return mapper.map(joinedList, leftFilters);
        }
        return mapper.map(joinedList);
    }

    public ConcurrentFullUniqueIndex mapMinusOneLevel(List joinedList)
    {
        if (rightFilters != null) joinedList = rightFilters.applyOperation(joinedList);
        if (joinedList == null) return null;
        return mapper.mapMinusOneLevel(joinedList);
    }

    public List map(List joinedList, Operation extraOperationOnResult)
    {
        if (rightFilters != null) joinedList = rightFilters.applyOperation(joinedList);
        if (joinedList == null) return null;
        List result;
        if (leftFilters != null)
        {
            result = mapper.map(joinedList, leftFilters.and(extraOperationOnResult));
        }
        else
        {
            result = mapper.map(joinedList, extraOperationOnResult);
        }
        return result;
    }

    public List mapOne(Object joined, Operation extraLeftOperation)
    {
        List joinedList = ListFactory.create(joined);
        if (rightFilters != null) joinedList = rightFilters.applyOperation(joinedList);
        if (joinedList == null) return null;
        if (joinedList.isEmpty()) return ListFactory.EMPTY_LIST;
        boolean mustApplyLeft = leftFilters != null;
        Operation op = null;
        if (mustApplyLeft)
        {
            if (AbstractMapper.isOperationEligibleForMapperCombo(leftFilters))
            {
                op = leftFilters;
                mustApplyLeft = false;
            }
        }
        if (extraLeftOperation != null)
        {
            if (op == null)
            {
                op = extraLeftOperation;
            }
            else
            {
                op = op.and(extraLeftOperation);
            }
        }
        List result = mapper.mapOne(joinedList.get(0), op);
        if (result == null) return null;
        if (mustApplyLeft) result = leftFilters.applyOperation(result);
        return result;
    }

    public Operation getSimplifiedJoinOp(List parentList, int maxInClause, DeepFetchNode node, boolean useTuple)
    {
        if (this.leftFilters != null)
        {
            parentList = this.leftFilters.applyOperation(parentList);
        }
        if (parentList != null)
        {
            Operation op = this.mapper.getSimplifiedJoinOp(parentList, maxInClause, node, useTuple);
            if (op == null) return null;
            if (rightFilters != null) op = op.and(rightFilters);
            return op;
        }
        return null;
    }

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool)
    {
        Operation op = mapper.getOperationFromResult(result, tempOperationPool);
        if (this.rightFilters != null)
        {
            op = op.and(rightFilters);
        }
        return op;
    }

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool)
    {
        Operation op = mapper.getOperationFromOriginal(original, tempOperationPool);
        if (this.leftFilters != null)
        {
            //todo: in case of a dangling join, we need to add a special operation that has to match the code generated in the getter for this relationship
        }
        if (this.rightFilters != null)
        {
            op = op.and(rightFilters);
        }
        return op;
    }

    @Override
    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        Operation op = mapper.getPrototypeOperation(tempOperationPool);
        if (this.leftFilters != null)
        {
            //todo: in case of a dangling join, we need to add a special operation that has to match the code generated in the getter for this relationship
        }
        if (this.rightFilters != null)
        {
            op = op.and(rightFilters);
        }
        return op;
    }

    public List<Mapper> getUnChainedMappers()
    {
        List<Mapper> list = this.mapper.getUnChainedMappers();
        if (list.size() == 1)
        {
            return ListFactory.<Mapper>create(this);
        }
        if (leftFilters != null)
        {
            list.set(0, new FilteredMapper(list.get(0), this.leftFilters, null));
        }
        if (rightFilters != null)
        {
            list.set(list.size() - 1, new FilteredMapper(list.get(list.size() - 1), null, this.rightFilters));
        }
        return list;
    }

    public boolean hasLeftOrDefaultMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        if (this.mapper.hasLeftOrDefaultMappingsFor(leftAsOfAttributes)) return true;
        if (leftFilters != null)
        {
            AsOfAttribute[] probe = new AsOfAttribute[1];
            int count = 0;
            SmallSet smallSet = new SmallSet(2);
            leftFilters.addDepenedentAttributesToSet(smallSet);
            for(int i=0;i<leftAsOfAttributes.length;i++)
            {
                probe[0] = leftAsOfAttributes[i];
                if (this.mapper.hasLeftOrDefaultMappingsFor(probe) || smallSet.contains(leftAsOfAttributes[i])) count++;
            }
            return count >= leftAsOfAttributes.length;
        }
        return false;
    }

    public boolean hasLeftMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        if (this.mapper.hasLeftMappingsFor(leftAsOfAttributes)) return true;
        if (leftFilters != null)
        {
            AsOfAttribute[] probe = new AsOfAttribute[1];
            int count = 0;
            for(int i=0;i<leftAsOfAttributes.length;i++)
            {
                probe[0] = leftAsOfAttributes[i];
                if (this.mapper.hasLeftMappingsFor(probe)) count++;
            }
            SmallSet smallSet = new SmallSet(2);
            leftFilters.addDepenedentAttributesToSet(smallSet);
            for(int i=0;i<leftAsOfAttributes.length;i++)
            {
                if (smallSet.contains(leftAsOfAttributes[i])) count++;
            }
            return count >= leftAsOfAttributes.length;
        }
        return false;
    }

    public Attribute getAnyRightAttribute()
    {
        return mapper.getAnyRightAttribute();
    }

    public Attribute getAnyLeftAttribute()
    {
        return mapper.getAnyLeftAttribute();
    }

    @Override
    public boolean isFullyCachedIgnoringLeft()
    {
        return this.mapper.isFullyCachedIgnoringLeft();
    }

    public String getResultOwnerClassName()
    {
        return this.mapper.getResultOwnerClassName();
    }

    public Set<Attribute> getAllLeftAttributes()
    {
        Set<Attribute> result =  this.mapper.getAllLeftAttributes();
        if (this.leftFilters != null)
        {
            this.leftFilters.zAddAllLeftAttributes(result);
        }
        return result;
    }

    public Extractor[] getLeftAttributesWithoutFilters()
    {
        return this.mapper.getLeftAttributesWithoutFilters();
    }

    @Override
    public List filterLeftObjectList(List objects)
    {
        if (this.leftFilters != null)
        {
            return this.leftFilters.applyOperation(objects);
        }
        else
        {
            return objects;
        }
    }

    @Override
    public Mapper createMapperForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject, int chainPosition)
    {
        Mapper newMapper = this.mapper.createMapperForTempJoin(attributeMap, prototypeObject, chainPosition);
        Operation newLeftFilters = null;
        if (this.leftFilters != null)
        {
            newLeftFilters = this.leftFilters.zSubstituteForTempJoin(attributeMap, prototypeObject);
        }
        return new FilteredMapper(newMapper, newLeftFilters, this.rightFilters);
    }

    public boolean isMappableForTempJoin(Set<Attribute> attributeMap)
    {
        return this.mapper.isMappableForTempJoin(attributeMap);
    }

    public double estimateMappingFactor()
    {
        return this.mapper.estimateMappingFactor();
    }

    @Override
    public int estimateMaxReturnSize(int multiplier)
    {
        return this.mapper.estimateMaxReturnSize(multiplier);
    }

    public void registerEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        if (this.leftFilters != null)
        {
            this.leftFilters.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
        }
        transitivePropagator.pushMapperContainer(this);
        this.mapper.registerEqualitiesAndAtomicOperations(transitivePropagator);
        if (this.rightFilters != null) this.rightFilters.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
    }

    public boolean hasTriangleJoins()
    {
        return this.mapper.hasTriangleJoins();
    }

    public boolean isRightHandPartialCacheResolvable()
    {
        return this.mapper.isRightHandPartialCacheResolvable();
    }

    public void appendSyntheticName(StringBuilder stringBuilder)
    {
        this.mapper.appendSyntheticName(stringBuilder);
        this.appendFilters(stringBuilder);
    }

    public boolean isSingleLevelJoin()
    {
        return this.mapper.isSingleLevelJoin();
    }

    @Override
    public boolean isEstimatable()
    {
        return this.mapper.isEstimatable();
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList)
    {
        if (rightFilters != null) joinedList = rightFilters.applyOperation(joinedList);
        if (joinedList == null) return null;
        List result;
        if (leftFilters != null)
        {
            result = mapper.mapReturnNullIfIncompleteIndexHit(joinedList, leftFilters);
        }
        else
        {
            result = mapper.mapReturnNullIfIncompleteIndexHit(joinedList);
        }
        return result;
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList, Operation extraOperationOnResult)
    {
        if (rightFilters != null) joinedList = rightFilters.applyOperation(joinedList);
        if (joinedList == null) return null;
        List result;
        if (leftFilters != null)
        {
            result = mapper.mapReturnNullIfIncompleteIndexHit(joinedList, leftFilters.and(extraOperationOnResult));
        }
        else
        {
            result = mapper.mapReturnNullIfIncompleteIndexHit(joinedList, extraOperationOnResult);
        }
        return result;
    }

    public void generateSql(SqlQuery query)
    {
        query.pushMapperContainer(this);
        if (!query.isMappedAlready(this.mapper))
        {
            if (leftFilters != null)
            {
                query.popMapperContainer();
                boolean insertedAnd = query.beginAnd();
                leftFilters.generateSql(query);
                query.endAnd(insertedAnd);
                query.pushMapperContainer(this);
            }
            mapper.generateSql(query);
            if (rightFilters != null)
            {
                boolean insertedAnd = query.beginAnd();
                rightFilters.generateSql(query);
                query.endAnd(insertedAnd);
            }
        }
        else
        {
            this.mapper.pushMappers(query);
        }
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        extractor.pushMapperContainer(this);
        if (!extractor.isMappedAlready(this.mapper))
        {
            if (leftFilters != null)
            {
                extractor.popMapperContainer();
                leftFilters.registerOperation(extractor, true);
                extractor.pushMapperContainer(this);
            }
            mapper.registerOperation(extractor, true);
            if (rightFilters != null)
            {
                rightFilters.registerOperation(extractor, true);
            }
        }
        else
        {
            this.mapper.pushMappers(extractor);
        }
    }

    public int getClauseCount(SqlQuery query)
    {
        int count = this.mapper.getClauseCount(query);
        if (this.leftFilters != null) count += this.leftFilters.getClauseCount(query);
        if (this.rightFilters != null) count += this.rightFilters.getClauseCount(query);

        return count;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        if (this.leftFilters != null) this.leftFilters.registerAsOfAttributesAndOperations(checker);
        checker.pushMapperContainer(this);
        this.mapper.registerAsOfAttributesAndOperations(checker);
        if (this.rightFilters != null) this.rightFilters.registerAsOfAttributesAndOperations(checker);
    }

    public Mapper getReverseMapper()
    {
        Mapper revMapper = this.reverseMapper;
        if (revMapper == null)
        {
            Mapper unfilteredRevMapper = this.mapper.getReverseMapper();
            if (unfilteredRevMapper != null)
            {
                revMapper = new FilteredMapper(unfilteredRevMapper, this.rightFilters, this.leftFilters, this);
            }
            this.reverseMapper = revMapper;
        }
        return revMapper;
    }

    public void addDepenedentPortalsToSet(Set set)
    {
        mapper.addDepenedentPortalsToSet(set);
        if (leftFilters != null) leftFilters.addDependentPortalsToSet(set);
        if (rightFilters != null) rightFilters.addDependentPortalsToSet(set);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        mapper.addDepenedentAttributesToSet(set);
        if (leftFilters != null) leftFilters.addDepenedentAttributesToSet(set);
        if (rightFilters != null) rightFilters.addDepenedentAttributesToSet(set);
    }

    public Attribute getDeepestEqualAttribute(Attribute attribute)
    {
        return this.mapper.getDeepestEqualAttribute(attribute);
    }

    public MappedOperation combineWithEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        EqualityMapper em = (EqualityMapper) otherMappedOperation.getMapper();
        if (em.hasMappedAttributes()) return null;
        return this.combineWithFilteredMapper(otherMappedOperation, mappedOperation);
    }

    public MappedOperation combineWithMultiEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        return this.combineWithFilteredMapper(otherMappedOperation, mappedOperation);
    }

    public MappedOperation equalitySubstituteWithAtomic(MappedOperation mappedOperation, AtomicOperation op)
    {
        Attribute replace = this.getDeepestEqualAttribute(op.getAttribute());
        if (replace != null && !mappedOperation.underlyingOperationDependsOnAttribute(replace))
        {
            Operation replacedOp = op.susbtituteOtherAttribute(replace);
            if (replacedOp != null)
            {
                return new MappedOperation(this,
                        mappedOperation.getUnderlyingOperation().and(replacedOp));
            }

        }
        return null;
    }

    public MappedOperation equalitySubstituteWithMultiEquality(MappedOperation mappedOperation, MultiEqualityOperation op)
    {
        //todo: rezaem: should we try harder to replace this?
        return null;
    }

    public Mapper getUnderlyingMapper()
    {
        return mapper;
    }

    public Operation getLeftFilters()
    {
        return leftFilters;
    }

    @Override
    public Operation getRightFilters()
    {
        return rightFilters;
    }

    @Override
    protected MappedOperation combineByType(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        return combineWithFilteredMapper(otherMappedOperation, mappedOperation);
    }

    public Mapper insertAsOfOperationInMiddle(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        stack.pushMapperContainer(this);
        this.mapper.pushMappers(stack);
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            this.mapper.popMappers(stack);
            stack.popMapperContainer();
            Operation newOp;
            if (this.rightFilters != null)
            {
                newOp = this.rightFilters;
                for(AtomicOperation asOfEqOperation: asOfEqOperations)
                {
                    newOp = this.rightFilters.and(asOfEqOperation);
                }
            }
            else
            {
                newOp = MultiEqualityOperation.createEqOperation(asOfEqOperations);
            }
            Mapper result = new FilteredMapper(this.mapper, this.leftFilters, newOp);
            stack.substituteContainer(this, result);
            result.setName(this.getRawName());
            return result;
        }
        this.mapper.popMappers(stack);
        Mapper newMapper = this.mapper.insertAsOfOperationInMiddle(asOfEqOperations, insertPosition, stack);
        stack.popMapperContainer();
        if (newMapper != null)
        {
            Mapper result = new FilteredMapper(newMapper, this.leftFilters, this.rightFilters);
            stack.substituteContainer(this, result);
            result.setName(this.getRawName());
            return result;
        }
        return null;
    }

    public Mapper insertOperationInMiddle(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        transitivePropagator.pushMapperContainer(this);
        this.mapper.pushMappers(transitivePropagator);
        if (insertPosition.equals(transitivePropagator.getCurrentMapperList()))
        {
            this.mapper.popMappers(transitivePropagator);
            transitivePropagator.popMapperContainer();
            Operation newOp = this.rightFilters == null ? NoOperation.instance() : this.rightFilters;
            newOp = newOp.and(this.constructAndOperation(toInsert));
            FilteredMapper filteredMapper = new FilteredMapper(this.mapper, this.leftFilters, newOp);
            filteredMapper.setName(this.getRawName());
            return filteredMapper;
        }
        this.mapper.popMappers(transitivePropagator);
        Mapper newMapper = this.mapper.insertOperationInMiddle(insertPosition, toInsert, transitivePropagator);
        transitivePropagator.popMapperContainer();
        if (newMapper != null)
        {
            FilteredMapper filteredMapper = new FilteredMapper(newMapper, this.leftFilters, this.rightFilters);
            filteredMapper.setName(this.getRawName());
            return filteredMapper;
        }
        return null;
    }

    @Override
    public Mapper insertAsOfOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        Operation newLeft = MultiEqualityOperation.createEqOperation(asOfEqOperations);
        if (this.leftFilters != null)
        {
            newLeft = newLeft.and(this.leftFilters);
        }
        FilteredMapper filteredMapper = new FilteredMapper(this.mapper, newLeft, this.rightFilters);
        filteredMapper.setName(this.getRawName());
        return filteredMapper;
    }

    public Mapper insertOperationOnLeft(InternalList toInsert)
    {
        Operation op = this.leftFilters == null ? NoOperation.instance() : this.leftFilters;
        for(int i=0;i<toInsert.size();i++)
        {
            op = op.and((Operation)toInsert.get(i));
        }
        FilteredMapper filteredMapper = new FilteredMapper(this.mapper, op, this.rightFilters);
        filteredMapper.setName(this.getRawName());
        return filteredMapper;
    }

    @Override
    protected void appendFilters(StringBuilder stringBuilder)
    {
        stringBuilder.append("(");
        boolean mustHaveBoth = this.getResultPortal().equals(this.getFromPortal());
        if (this.leftFilters != null)
        {
            if (mustHaveBoth) stringBuilder.append(this.getFromPortal().getBusinessClassName()).append(" filters: ");
            stringBuilder.append(this.leftFilters.toString());
        }
        else if (mustHaveBoth)
        {
            stringBuilder.append(this.getFromPortal().getBusinessClassName()).append(" filters: none");
        }

        if (this.rightFilters != null)
        {
            if (mustHaveBoth || leftFilters != null) stringBuilder.append(", ");
            if (mustHaveBoth) stringBuilder.append(this.getResultPortal().getBusinessClassName()).append(" filters: ");
            stringBuilder.append(this.rightFilters.toString());
        }
        else if (mustHaveBoth)
        {
            stringBuilder.append(", ");
            stringBuilder.append(this.getResultPortal().getBusinessClassName()).append(" filters: none");
        }
        stringBuilder.append(")");
    }

    public void pushMappers(MapperStack mapperStack)
    {
        mapperStack.pushMapperContainer(this);
        this.mapper.pushMappers(mapperStack);
    }

    public void popMappers(MapperStack mapperStack)
    {
        this.mapper.popMappers(mapperStack);
        mapperStack.popMapperContainer();
    }

    public int hashCode()
    {
        int result = this.mapper.hashCode();
        if (this.leftFilters != null)
        {
            result ^= this.leftFilters.hashCode();
        }
        else
        {
            result ^= HashUtil.NULL_HASH;
        }
        if (this.rightFilters != null)
        {
            result ^= this.rightFilters.hashCode();
        }
        else
        {
            result ^= HashUtil.NULL_HASH;
        }
        return result;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof FilteredMapper)) return false;

        final FilteredMapper filteredMapper = (FilteredMapper) o;

        if (leftFilters == null ? filteredMapper.leftFilters != null : !leftFilters.equals(filteredMapper.leftFilters)) return false;
        if (!mapper.equals(filteredMapper.mapper)) return false;
        return !(rightFilters == null ? filteredMapper.rightFilters != null : !rightFilters.equals(filteredMapper.rightFilters));
    }

    public AsOfEqOperation[] getDefaultAsOfOperation(List<AsOfAttribute> ignore)
    {
        if (this.rightFilters == null) return this.mapper.getDefaultAsOfOperation(ignore);
        AsOfAttribute[] asOfAttributes = getFromPortal().getFinder().getAsOfAttributes();
        if (asOfAttributes != null)
        {
            SmallSet smallSet = new SmallSet(2);
            smallSet.addAll(ignore);
            rightFilters.addDepenedentAttributesToSet(smallSet);
            return this.mapper.getDefaultAsOfOperation(smallSet);
        }
        return null;
    }

    @Override
    public void clearLeftOverFromObjectCache(Collection<Object> parentObjects, EqualityOperation extraEqOp, Operation extraOperation)
    {
        EqualityOperation finalEqOp = extraEqOp;
        Operation localExtraOperation = extraOperation;
        if (rightFilters != null)
        {
            EqualityOperation localEqOp = rightFilters.zExtractEqualityOperations();
            if (localEqOp != rightFilters)
            {
                localExtraOperation = localExtraOperation == null ? rightFilters : localExtraOperation.and(rightFilters);
            }
            if (localEqOp != null)
            {
                if (finalEqOp != null)
                {
                    finalEqOp = (EqualityOperation) finalEqOp.and(extraEqOp);
                }
                else
                {
                    finalEqOp = localEqOp;
                }
            }
        }
        this.mapper.clearLeftOverFromObjectCache(parentObjects, finalEqOp, localExtraOperation);
    }
}
