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

import com.gs.collections.api.block.function.Function;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;



public interface Mapper
extends Serializable
{

    public MithraObjectPortal getResultPortal();
    public MithraObjectPortal getFromPortal();
    public boolean isReversible();

    public boolean isJoinedWith(MithraObjectPortal portal);

    public boolean mapUsesUniqueIndex();
    public boolean mapUsesNonUniqueIndex();
    public boolean mapUsesImmutableUniqueIndex();
    public List map(List joinedList);
    public List map(List joinedList, Operation extraOperationOnResult);

    public ConcurrentFullUniqueIndex mapMinusOneLevel(List joinedList);

    public List mapReturnNullIfIncompleteIndexHit(List joinedList);

    public List mapReturnNullIfIncompleteIndexHit(List joinedList, Operation extraOperationOnResult);
    /**
     *
     * @param query
     */
    public void generateSql(SqlQuery query);
    //public void generateSql(SqlQuery query);

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality);

    public int getClauseCount(SqlQuery query);

    public Mapper getReverseMapper();

    public void addDepenedentPortalsToSet(Set set);
    public void addDepenedentAttributesToSet(Set set);

    public Attribute getDeepestEqualAttribute(Attribute attribute);

    public Mapper and(Mapper other);

    public Mapper andWithEqualityMapper(EqualityMapper other);

    public Mapper andWithMultiEqualityMapper(MultiEqualityMapper other);

    /**
     *
     * @param mappedOperation
     * @param otherMappedOperation
     * @return the combined (and) operation. This may modify the passed in operations. returns null if combination is not possible.
     */
    public MappedOperation combineMappedOperations(MappedOperation mappedOperation, MappedOperation otherMappedOperation);

    public MappedOperation combineWithFilteredMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation);
    public MappedOperation combineWithEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation);
    public MappedOperation combineWithMultiEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation);

    public MappedOperation equalitySubstituteWithAtomic(MappedOperation mappedOperation, AtomicOperation op);
    public MappedOperation equalitySubstituteWithMultiEquality(MappedOperation mappedOperation, MultiEqualityOperation op);

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker);

    public Mapper insertAsOfOperationInMiddle(AtomicOperation[] asOfEqOperation, MapperStackImpl insertPosition, AsOfEqualityChecker stack);

    public Mapper insertOperationInMiddle(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator);

    public Mapper insertAsOfOperationOnLeft(AtomicOperation[] asOfEqOperations);

    public Mapper insertOperationOnLeft(InternalList toInsert);

    public void pushMappers(MapperStack mapperStack);

    public void popMappers(MapperStack mapperStack);

    public void setAnonymous(boolean anonymous);
    public boolean isAnonymous();

    public void setToMany(boolean toMany);
    public boolean isToMany();

    public Mapper getCommonMapper(Mapper other);

    public Mapper getMapperRemainder(Mapper head);

    public Function getParentSelectorRemainder(DeepRelationshipAttribute parentSelector);

    public Function getTopParentSelector(DeepRelationshipAttribute parentSelector);

    public Operation createNotExistsOperation(Operation op);

    public Operation createRecursiveNotExistsOperation(Operation op);

    public Operation getRightFilters();

    public Operation getLeftFilters();

    public List mapOne(Object joined, Operation extraLeftOperation);

    public Operation getSimplifiedJoinOp(List parentList, int maxInClause, DeepFetchNode node, boolean useTuple);

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool);

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool);

    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool);

    public List<Mapper> getUnChainedMappers();

    public Operation createMappedOperationForDeepFetch(Operation op);

    public Mapper getParentMapper();

    public Mapper link(Mapper other);

    public AsOfEqOperation[] getDefaultAsOfOperation(List<AsOfAttribute> ignore);

    public boolean hasLeftOrDefaultMappingsFor(AsOfAttribute[] leftAsOfAttributes);

    public List getAllPossibleResultObjectsForFullCache();

    public boolean hasLeftMappingsFor(AsOfAttribute[] leftAsOfAttributes);

    public Attribute getAnyRightAttribute();

    public Attribute getAnyLeftAttribute();

    public boolean isFullyCachedIgnoringLeft();

    public boolean isFullyCached();

    public String getResultOwnerClassName();

    public Set<Attribute> getAllLeftAttributes();

    public Extractor[] getLeftAttributesWithoutFilters();

    public List filterLeftObjectList(List objects);

    public Mapper createMapperForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject, int chainPosition);

    public boolean isMappableForTempJoin(Set<Attribute> attributeSet);

    public double estimateMappingFactor();

    public int estimateMaxReturnSize(int multiplier);

    public void registerEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator);

    public boolean hasTriangleJoins();

    public boolean isRightHandPartialCacheResolvable();

    public void setName(String name);

    public void appendName(StringBuilder stringBuilder);

    public void appendSyntheticName(StringBuilder stringBuilder);

    public boolean isSingleLevelJoin();

    public String getRelationshipPath();

    public List<String> getRelationshipPathAsList();

    public boolean isEstimatable();

    public void clearLeftOverFromObjectCache(Collection<Object> parentObjects, EqualityOperation extraEqOp, Operation extraOperation);
}
