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
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.MithraFastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;

import java.util.*;



public class ChainedMapper extends AbstractMapper
{

    private Mapper firstMapper;
    private Mapper secondMapper;
    static private Logger logger = LoggerFactory.getLogger(ChainedMapper.class.getName());

    public ChainedMapper(Mapper firstMapper, Mapper secondMapper)
    {
        this.firstMapper = firstMapper;
        this.secondMapper = secondMapper;
    }

    public boolean mapUsesUniqueIndex()
    {
        return this.firstMapper.mapUsesUniqueIndex() && this.secondMapper.mapUsesUniqueIndex();
    }

    public boolean mapUsesImmutableUniqueIndex()
    {
        return this.firstMapper.mapUsesImmutableUniqueIndex() && this.secondMapper.mapUsesImmutableUniqueIndex();
    }

    public boolean mapUsesNonUniqueIndex()
    {
        boolean firstUsesIndex = this.firstMapper.mapUsesUniqueIndex() || this.firstMapper.mapUsesNonUniqueIndex();
        boolean secondUsesIndex = this.secondMapper.mapUsesUniqueIndex() || this.secondMapper.mapUsesNonUniqueIndex();

        return (firstUsesIndex && secondUsesIndex) && !(this.mapUsesUniqueIndex());
    }

    public List map(List joinedList)
    {
        List secondJoinedList = this.secondMapper.map(joinedList);
        if (secondJoinedList == null) return null;
        return this.firstMapper.map(secondJoinedList);
    }

    public ConcurrentFullUniqueIndex mapMinusOneLevel(List joinedList)
    {
        List secondJoinedList = this.secondMapper.map(joinedList);
        if (secondJoinedList == null) return null;
        return this.firstMapper.mapMinusOneLevel(secondJoinedList);
    }

    public List map(List joinedList, Operation extraOperationOnResult)
    {
        List result = this.secondMapper.map(joinedList);
        if (result != null)
        {
            result = this.firstMapper.map(result, extraOperationOnResult);
        }
        return result;
    }

    public List mapOne(Object joined, Operation extraLeftOperation)
    {
        List secondJoinedList = this.secondMapper.mapOne(joined, null);
        if (secondJoinedList == null) return null;
        MithraFastList result = new MithraFastList(secondJoinedList.size());
        for(int i=0;i<secondJoinedList.size();i++)
        {
            result.addAll(this.firstMapper.mapOne(secondJoinedList.get(i), extraLeftOperation));
        }
        return result;
    }

    public Operation getSimplifiedJoinOp(List parentList, int maxInClause, DeepFetchNode node, boolean useTuple)
    {
        return null;
    }

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        throw new RuntimeException("not implemented");
    }

    public List<Mapper> getUnChainedMappers()
    {
        FastList<Mapper> result = new FastList<Mapper>(4);
        result.add(this.firstMapper);
        result.addAll(this.secondMapper.getUnChainedMappers());
        return result;
    }

    public boolean hasLeftOrDefaultMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        return this.firstMapper.hasLeftOrDefaultMappingsFor(leftAsOfAttributes);
    }

    public boolean hasLeftMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        return this.firstMapper.hasLeftMappingsFor(leftAsOfAttributes);
    }

    public Attribute getAnyRightAttribute()
    {
        return secondMapper.getAnyRightAttribute();
    }

    public Attribute getAnyLeftAttribute()
    {
        return firstMapper.getAnyLeftAttribute();
    }

    public boolean isFullyCachedIgnoringLeft()
    {
        return this.firstMapper.isFullyCachedIgnoringLeft() && this.secondMapper.isFullyCached();
    }

    public String getResultOwnerClassName()
    {
        return this.firstMapper.getResultOwnerClassName();
    }

    public Set<Attribute> getAllLeftAttributes()
    {
        return this.firstMapper.getAllLeftAttributes();
    }

    public Extractor[] getLeftAttributesWithoutFilters()
    {
        return this.firstMapper.getLeftAttributesWithoutFilters();
    }

    public List filterLeftObjectList(List objects)
    {
        return this.firstMapper.filterLeftObjectList(objects);
    }

    public Mapper createMapperForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject, int chainPosition)
    {
        return new ChainedMapper(this.firstMapper.createMapperForTempJoin(attributeMap, prototypeObject, chainPosition), this.secondMapper);
    }

    public boolean isMappableForTempJoin(Set<Attribute> attributeMap)
    {
        return this.firstMapper.isMappableForTempJoin(attributeMap);
    }

    public double estimateMappingFactor()
    {
        return this.firstMapper.estimateMappingFactor() * this.secondMapper.estimateMappingFactor();
    }

    @Override
    public int estimateMaxReturnSize(int multiplier)
    {
        return this.firstMapper.estimateMaxReturnSize(this.secondMapper.estimateMaxReturnSize(multiplier));
    }

    public void registerEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        transitivePropagator.pushMapperContainer(this);
        this.firstMapper.registerEqualitiesAndAtomicOperations(transitivePropagator);
        this.secondMapper.registerEqualitiesAndAtomicOperations(transitivePropagator);
    }

    public boolean hasTriangleJoins()
    {
        return this.firstMapper.hasTriangleJoins() || this.secondMapper.hasTriangleJoins();
    }

    public boolean isRightHandPartialCacheResolvable()
    {
        return this.firstMapper.isRightHandPartialCacheResolvable() && this.secondMapper.isRightHandPartialCacheResolvable();
    }

    public void appendSyntheticName(StringBuilder stringBuilder)
    {
        stringBuilder.append("[ -> ");
        stringBuilder.append(this.getFromPortal().getBusinessClassName()).append(": ");
        this.firstMapper.appendSyntheticName(stringBuilder);
        this.secondMapper.appendSyntheticName(stringBuilder);
        stringBuilder.append(" ]");
    }

    public boolean isSingleLevelJoin()
    {
        return false;
    }

    @Override
    public boolean isEstimatable()
    {
        return this.firstMapper.isEstimatable() && this.secondMapper.isEstimatable();
    }

    public AsOfEqOperation[] getDefaultAsOfOperation(List<AsOfAttribute> ignore)
    {
        throw new RuntimeException("should not get here");
    }

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool)
    {
        throw new RuntimeException("not implemented");
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList)
    {
        List result = this.secondMapper.mapReturnNullIfIncompleteIndexHit(joinedList);
        if (result != null) result = this.firstMapper.mapReturnNullIfIncompleteIndexHit(result);
        return result;
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList, Operation extraOperationOnResult)
    {
        List result = this.secondMapper.mapReturnNullIfIncompleteIndexHit(joinedList);
        if (result != null)
        {
            result = this.firstMapper.mapReturnNullIfIncompleteIndexHit(result, extraOperationOnResult);
        }
        return result;
    }

    public MithraObjectPortal getResultPortal()
    {
        return firstMapper.getResultPortal();
    }

    public MithraObjectPortal getFromPortal()
    {
        return secondMapper.getFromPortal();
    }

    public void generateSql(SqlQuery query)
    {
        query.pushMapperContainer(this);
        this.firstMapper.generateSql(query);
        this.secondMapper.generateSql(query);
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        extractor.pushMapperContainer(this);
        this.firstMapper.registerOperation(extractor, true);
        this.secondMapper.registerOperation(extractor, true);
    }

    public int getClauseCount(SqlQuery query)
    {
        return this.firstMapper.getClauseCount(query) + this.secondMapper.getClauseCount(query);
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        checker.pushMapperContainer(this);
        this.firstMapper.registerAsOfAttributesAndOperations(checker);
        this.secondMapper.registerAsOfAttributesAndOperations(checker);
    }

    public boolean isReversible()
    {
        return this.firstMapper.isReversible() && this.secondMapper.isReversible();
    }

    public Mapper getReverseMapper()
    {
        Mapper revMapper = this.reverseMapper;
        if (revMapper == null)
        {
            Mapper secondReverseMapper = secondMapper.getReverseMapper();
            Mapper firstReverseMapper = firstMapper.getReverseMapper();
            if (secondReverseMapper == null || firstReverseMapper == null) return null;
            ChainedMapper revChainedMapper = new ChainedMapper(secondReverseMapper, firstReverseMapper);
            revChainedMapper.reverseMapper = this;
            revMapper = revChainedMapper;
            this.reverseMapper = revMapper;
        }
        return revMapper;
    }

    public void addDepenedentPortalsToSet(Set set)
    {
        firstMapper.addDepenedentPortalsToSet(set);
        secondMapper.addDepenedentPortalsToSet(set);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        firstMapper.addDepenedentAttributesToSet(set);
        secondMapper.addDepenedentAttributesToSet(set);
    }

    public Attribute getDeepestEqualAttribute(Attribute attribute)
    {
        Attribute result = null;
        Attribute first = this.firstMapper.getDeepestEqualAttribute(attribute);
        if (first != null)
        {
            result = this.secondMapper.getDeepestEqualAttribute(first);
        }
        return result;
    }

    public int hashCode()
    {
        return firstMapper.hashCode()*31 + secondMapper.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj instanceof ChainedMapper)
        {
            ChainedMapper other = (ChainedMapper) obj;
            return this.firstMapper.equals(other.firstMapper) && this.secondMapper.equals(other.secondMapper);
        }
        return false;
    }

    public MappedOperation combineWithMultiEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        return this.combineWithChainedMapper(otherMappedOperation, mappedOperation);
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

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        if (this.firstMapper.getFromPortal() == portal) return true;
        if (this.secondMapper.getFromPortal() == portal) return true;
        return false;
    }

    public MappedOperation equalitySubstituteWithMultiEquality(MappedOperation mappedOperation, MultiEqualityOperation op)
    {
        HashSet set = new HashSet();
        this.addDepenedentAttributesToSet(set);
        for(Iterator it = set.iterator();it.hasNext();)
        {
            Attribute a = (Attribute) it.next();
            if (op.operatesOnAttribute(a))
            {
                //todo: rezaem: multi equality substitution in chanied mapper
                this.getLogger().warn("this operation is at best not well optimized. At worst, totally broken.");
            }
        }
        return null;
    }

    public MappedOperation combineWithEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        EqualityMapper em = (EqualityMapper) otherMappedOperation.getMapper();
        if (em.hasMappedAttributes()) return null;
        return this.combineWithChainedMapper(otherMappedOperation, mappedOperation);
    }

    public MappedOperation combineWithFirstMapper(MappedOperation thisOperation, MappedOperation otherOperation)
    {
        MappedOperation mop = new MappedOperation(this.firstMapper, new All(this.firstMapper.getAnyRightAttribute()));
        MappedOperation combined = (MappedOperation) mop.zCombinedAnd(otherOperation);
        FilteredMapper filteredMapper = new FilteredMapper(combined.getMapper(), null, combined.getUnderlyingOperation());
        return new MappedOperation(new ChainedMapper(filteredMapper, this.secondMapper), thisOperation.getUnderlyingOperation());
    }

    public MappedOperation combineWithSecondMapper(MappedOperation thisOperation, MappedOperation otherOperation)
    {
        MappedOperation mop = new MappedOperation(this.secondMapper, thisOperation.getUnderlyingOperation());
        MappedOperation combined = (MappedOperation) mop.zCombinedAnd(otherOperation);
        return new MappedOperation(new ChainedMapper(firstMapper, combined.getMapper()), combined.getUnderlyingOperation());
    }

    protected MappedOperation combineWithChainedMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        ChainedMapper cm = (ChainedMapper) otherMappedOperation.getMapper();
        Mapper firstMapper = cm.firstMapper;
        Mapper secondMapper = cm.secondMapper;
        Mapper thisMapper = mappedOperation.getMapper();
        if (thisMapper.getFromPortal().equals(firstMapper.getFromPortal())
            && thisMapper.getResultPortal().equals(firstMapper.getResultPortal()))
        {
            return cm.combineWithFirstMapper(otherMappedOperation, mappedOperation);
        }
        else if (thisMapper.getFromPortal().equals(secondMapper.getFromPortal())
            && thisMapper.getResultPortal().equals(secondMapper.getResultPortal()))
        {
            return cm.combineWithSecondMapper(otherMappedOperation, mappedOperation);
        }
        return null;
    }

    public Mapper insertAsOfOperationInMiddle(AtomicOperation[] asOfEqOperation, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        Mapper result = null;
        stack.pushMapperContainer(this);
        Mapper mapper = firstMapper.insertAsOfOperationInMiddle(asOfEqOperation, insertPosition, stack);
        if (mapper != null)
        {
            stack.popMapperContainer();
            result = new ChainedMapper(mapper, secondMapper);
            result.setName(this.getRawName());
            stack.substituteContainer(this, result);
            return result;
        }
        firstMapper.pushMappers(stack);
        mapper = secondMapper.insertAsOfOperationInMiddle(asOfEqOperation, insertPosition, stack);
        if (mapper != null)
        {
            firstMapper.popMappers(stack);
            stack.popMapperContainer();
            result = new ChainedMapper(this.firstMapper, mapper);
            result.setName(this.getRawName());
            stack.substituteContainer(this, result);
            return result;
        }
        secondMapper.pushMappers(stack);
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            result = new FilteredMapper(this, null, MultiEqualityOperation.createEqOperation(asOfEqOperation));
            result.setName(this.getRawName());
        }
        secondMapper.popMappers(stack);
        firstMapper.popMappers(stack);
        stack.popMapperContainer();
        stack.substituteContainer(this, result);
        return result;
    }

    public Mapper insertOperationInMiddle(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        Mapper result = null;
        transitivePropagator.pushMapperContainer(this);
        Mapper mapper = firstMapper.insertOperationInMiddle(insertPosition, toInsert, transitivePropagator);
        if (mapper != null)
        {
            transitivePropagator.popMapperContainer();
            result = new ChainedMapper(mapper, secondMapper);
            result.setName(this.getRawName());
            return result;
        }
        firstMapper.pushMappers(transitivePropagator);
        mapper = secondMapper.insertOperationInMiddle(insertPosition, toInsert, transitivePropagator);
        if (mapper != null)
        {
            firstMapper.popMappers(transitivePropagator);
            transitivePropagator.popMapperContainer();
            result = new ChainedMapper(this.firstMapper, mapper);
            result.setName(this.getRawName());
            return result;
        }
        secondMapper.pushMappers(transitivePropagator);
        if (insertPosition.equals(transitivePropagator.getCurrentMapperList()))
        {
            result = new FilteredMapper(this, null, constructAndOperation(toInsert));
            result.setName(this.getRawName());
        }
        secondMapper.popMappers(transitivePropagator);
        firstMapper.popMappers(transitivePropagator);
        transitivePropagator.popMapperContainer();
        return result;
    }

    public void pushMappers(MapperStack mapperStack)
    {
        mapperStack.pushMapperContainer(this);
        this.firstMapper.pushMappers(mapperStack);
        this.secondMapper.pushMappers(mapperStack);
    }

    public void popMappers(MapperStack mapperStack)
    {
        this.secondMapper.popMappers(mapperStack);
        this.firstMapper.popMappers(mapperStack);
        mapperStack.popMapperContainer();
    }

    protected MappedOperation combineByType(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        return combineWithChainedMapper(otherMappedOperation, mappedOperation);
    }

    public static Logger getLogger()
    {
        return logger;
    }

    public Operation getRightFilters()
    {
        return secondMapper.getRightFilters();
    }

    @Override
    public Operation getLeftFilters()
    {
        return firstMapper.getLeftFilters();
    }

    @Override
    public void clearLeftOverFromObjectCache(Collection<Object> parentObjects, EqualityOperation extraEqOp, Operation extraOperation)
    {
        throw new RuntimeException("not implemented");
    }
}
