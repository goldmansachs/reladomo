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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.MappedAttribute;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.ListFactory;
import com.gs.fw.common.mithra.util.MithraFastList;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class MultiEqualityMapper extends AbstractMapper
{

    private InternalList equalityMappers;
    private Attribute[] leftAttributes;
    private transient IndexReference bestIndexRef;
    private int mappedIndex = -1;

    public MultiEqualityMapper(Attribute left, Attribute right)
    {
        initialize(2);
        this.addEqualityMapper(left.constructEqualityMapper(right));
    }

    public MultiEqualityMapper(MultiEqualityMapper source, EqualityMapper newMapper)
    {
        initialize(source.equalityMappers.size()+1);
        this.addEqualityMappers(source.equalityMappers);
        this.addEqualityMapper(newMapper);
    }

    private void initialize(int size)
    {
        this.equalityMappers = new InternalList(size);
        this.leftAttributes = new Attribute[size];
    }

    public MultiEqualityMapper(MultiEqualityMapper source1, MultiEqualityMapper source2)
    {
        initialize(source1.equalityMappers.size()+source2.equalityMappers.size());
        this.addEqualityMappers(source1.equalityMappers);
        for(int i=0;i<source2.equalityMappers.size();i++)
        {
            this.addEqualityMapper((EqualityMapper)source2.equalityMappers.get(i));
        }
    }

    public InternalList getEqualityMappers()
    {
        return this.equalityMappers;
    }

    public MultiEqualityMapper(EqualityMapper equalityMapper, EqualityMapper other)
    {
        initialize(2);
        this.addEqualityMapper(equalityMapper);
        this.addEqualityMapper(other);
    }

    public MultiEqualityMapper(InternalList equalityMappers)
    {
        this.equalityMappers = equalityMappers;
        sortMappers();
        this.leftAttributes = new Attribute[equalityMappers.size()];
        for(int i=0;i<equalityMappers.size();i++)
        {
            EqualityMapper mapper = (EqualityMapper) equalityMappers.get(i);
            this.leftAttributes[i] = mapper.getLeft();
        }
    }

    private void fixLeftAttributes()
    {
        int size = this.equalityMappers.size();
        if (this.leftAttributes.length != size)
        {
            Attribute[] fixed = new Attribute[size];
            for(int i=0;i<size;i++)
            {
                fixed[i] = this.leftAttributes[i];
            }
            this.leftAttributes = fixed;
        }
    }

    private void sortMappers()
    {
        for(int i=0;i<equalityMappers.size();i++)
        {
            EqualityMapper mapper = (EqualityMapper) equalityMappers.get(i);
            Attribute left = mapper.getLeft();
            if (left instanceof MappedAttribute)
            {
                if (mappedIndex < 0)
                {
                    mappedIndex = i;
                }
            }
            else
            {
                if (mappedIndex >=0)
                {
                    this.equalityMappers.swap(mappedIndex, i);
                    mappedIndex++;
                }
            }
        }
    }

    protected MultiEqualityMapper(InternalList equalityMappers, Mapper reverseMapper)
    {
        this(equalityMappers);
        this.reverseMapper = reverseMapper;
    }

    public void addAutoGeneratedAttributeMap(Attribute left, Attribute right)
    {
        EqualityMapper equalityMapper = left.constructEqualityMapper(right);
        equalityMapper.setAutoGenerated(true);
        this.addEqualityMapper(equalityMapper);
    }

    private void addEqualityMapper(EqualityMapper mapper)
    {
        if (!equalityMappers.contains(mapper))
        {
            Attribute left = mapper.getLeft();
            if (left instanceof MappedAttribute)
            {
                addToLeftAttributes(left);
                this.equalityMappers.add(mapper);
                if (mappedIndex < 0)
                {
                    mappedIndex = this.equalityMappers.size() - 1;
                }
            }
            else
            {
                if (mappedIndex >=0)
                {
                    insertIntoArray(this.leftAttributes, mappedIndex, left);
                    this.equalityMappers.add(mappedIndex, mapper);
                    mappedIndex++;
                }
                else
                {
                    addToLeftAttributes(left);
                    this.equalityMappers.add(mapper);
                }
            }
        }
    }

    private void addToLeftAttributes(Attribute left)
    {
        if (this.leftAttributes.length <= this.equalityMappers.size())
        {
            Attribute[] tmp = this.leftAttributes;
            this.leftAttributes = new Attribute[this.equalityMappers.size() + 1];
            System.arraycopy(tmp, 0, this.leftAttributes, 0, tmp.length);
        }
        this.leftAttributes[this.equalityMappers.size()] = left;
    }

    private void insertIntoArray(Attribute[] leftAttributes, int index, Attribute left)
    {
        System.arraycopy(leftAttributes, index, leftAttributes, index + 1, leftAttributes.length - index - 1);
        leftAttributes[index] = left;
    }

    private void addEqualityMappers(InternalList mappers)
    {
        for(int i=0;i<mappers.size();i++)
        {
            EqualityMapper equalityMapper = (EqualityMapper) mappers.get(i);
            addEqualityMapper(equalityMapper);
        }
    }


    protected EqualityMapper getFirstMapper()
    {
        return (EqualityMapper) equalityMappers.get(0);
    }

    public MithraObjectPortal getResultPortal()
    {
        return getFirstMapper().getResultPortal();
    }

    public MithraObjectPortal getFromPortal()
    {
        return this.getFirstMapper().getFromPortal();
    }

    public boolean isReversible()
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            if (!mapper.isReversible()) return false;
        }
        return true;
    }

    private int getBestIndexRef(Cache cache)
    {
        fixLeftAttributes();
        if (this.bestIndexRef == null || !this.bestIndexRef.isForCache(cache))
        {
            this.bestIndexRef = cache.getBestIndexReference(Arrays.asList(this.leftAttributes));
        }
        return this.bestIndexRef.indexReference;
    }

    private Cache getCache()
    {
        return this.getFirstMapper().getCache();
    }

    public boolean mapUsesUniqueIndex()
    {
        Cache cache = this.getCache();
        int indexRef = this.getBestIndexRef(cache);
        return indexRef > 0 && cache.isUnique(indexRef);
    }

    public boolean mapUsesImmutableUniqueIndex()
    {
        Cache cache = this.getCache();
        int indexRef = this.getBestIndexRef(cache);
        return indexRef > 0 && cache.isUniqueAndImmutable(indexRef);
    }

    public boolean mapUsesNonUniqueIndex()
    {
        Cache cache = this.getCache();
        int indexRef = this.getBestIndexRef(cache);
        return indexRef > 0 && indexRef != IndexReference.AS_OF_PROXY_INDEX_ID && !cache.isUnique(indexRef);
    }

    public List map(List joinedList)
    {
        return getResultViaMultiIn(joinedList, null);
    }

    public ConcurrentFullUniqueIndex mapMinusOneLevel(List joinedList)
    {
        Extractor[] extractors = new Extractor[this.equalityMappers.size()];
        for(int i=0;i<extractors.length;i++)
        {
            extractors[i] = ((EqualityMapper)this.equalityMappers.get(i)).getRight();
        }
        return ConcurrentFullUniqueIndex.parallelConstructIndexWithoutNulls(joinedList, extractors);
    }

    public List map(List joinedList, Operation extraOperationOnResult)
    {
        return mapReturnNullIfIncompleteIndexHit(joinedList, extraOperationOnResult);
    }

    public List mapOne(Object joined, Operation extraLeftOperation)
    {
        Operation op = getMultiEqualityOperation(joined);
        if (extraLeftOperation != null) op = op.and(extraLeftOperation);
        return op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, true);
    }

    public Operation getSimplifiedJoinOp(List parentList, int maxInClause, DeepFetchNode node, boolean useTuple)
    {
        Operation constants = NoOperation.instance();
        boolean[] needsInClause = new boolean[leftAttributes.length];
        Object firstParent = parentList.get(0);
        int inClauseCount = 0;
        TupleAttribute tupleAttribute = null;
        Attribute firstNonConstantAttribute = null;
        for(int leftIndex = 0; leftIndex < leftAttributes.length; leftIndex++)
        {
            Attribute right = ((EqualityMapper) this.equalityMappers.get(leftIndex)).getRight();
            boolean isConstant = true;
            Attribute left = leftAttributes[leftIndex];
            for(int i=1;i<parentList.size();i++)
            {
                Object parent = parentList.get(i);
                if (!left.valueEquals(firstParent, parent))
                {
                    isConstant = false;
                    break;
                }
            }
            if (isConstant)
            {
                constants = constants.and(right.nonPrimitiveEq(left.valueOf(firstParent)));
            }
            else
            {
                inClauseCount++;
                if (!useTuple && inClauseCount > 1)
                {
                    return null;
                }
                needsInClause[leftIndex] = true;
                if (firstNonConstantAttribute == null)
                {
                    firstNonConstantAttribute = right;
                }
                else if (tupleAttribute == null)
                {
                    tupleAttribute = firstNonConstantAttribute.tupleWith(right);
                }
                else
                {
                    tupleAttribute = tupleAttribute.tupleWith(right);
                }
            }
        }
        if (inClauseCount == 0) return constants;
        if (tupleAttribute != null)
        {
            Extractor[] extractors = new Extractor[inClauseCount];
            int count = 0;
            for(int i=0;i<equalityMappers.size();i++)
            {
                if (needsInClause[i])
                {
                    extractors[count] = leftAttributes[i];
                    count++;
                }
            }
            return constants.and(tupleAttribute.inIgnoreNulls(parentList, extractors));
        }
        else
        {
            Operation op = constants;
            for(int i=0;i<needsInClause.length;i++)
            {
                if (needsInClause[i])
                {
                    EqualityMapper mapper = (EqualityMapper) this.equalityMappers.get(i);
                    Operation localOp = mapper.getSimplifiedJoinOp(parentList, maxInClause, node, useTuple);
                    if (localOp == null) return null;
                    op = op.and(localOp);
                    break;
                }
            }
            return op;
        }
    }

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool)
    {
        AtomicOperation[] ops = new AtomicOperation[this.equalityMappers.size()];
        for(int i=0;i<ops.length;i++)
        {
            Operation op = ((EqualityMapper)this.equalityMappers.get(i)).getOperationFromResult(result, tempOperationPool);
            if(op.zIsNone())
            {
                return op;
            }
            ops[i] = (AtomicOperation) op;
        }
        return new MultiEqualityOperation(ops);
    }

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool)
    {
        AtomicOperation[] ops = new AtomicOperation[this.equalityMappers.size()];
        for(int i=0;i<ops.length;i++)
        {
            Operation op = ((EqualityMapper)this.equalityMappers.get(i)).getOperationFromOriginal(original, tempOperationPool);
            if(op.zIsNone())
            {
                return op;
            }
            ops[i] = (AtomicOperation) op;
        }
        return new MultiEqualityOperation(ops);
    }

    @Override
    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        AtomicOperation[] ops = new AtomicOperation[this.equalityMappers.size()];
        for(int i=0;i<ops.length;i++)
        {
            Operation op = ((EqualityMapper)this.equalityMappers.get(i)).getPrototypeOperation(tempOperationPool);
            ops[i] = (AtomicOperation) op;
        }
        return new MultiEqualityOperation(ops);
    }

    public List<Mapper> getUnChainedMappers()
    {
        return ListFactory.<Mapper>create(this);
    }

    public boolean hasLeftOrDefaultMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        int count = 0;
        fixLeftAttributes();
        for(int j=0;j<leftAsOfAttributes.length;j++)
        {
            boolean found = false;
            for(int i=0;!found && i<leftAttributes.length;i++)
            {
                if (leftAttributes[i].equals(leftAsOfAttributes[j])) found = true;
            }
            if (found || leftAsOfAttributes[j].getDefaultDate() != null) count++;
        }
        return count == leftAsOfAttributes.length;
    }

    public boolean hasLeftMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        int count = 0;
        for(int j=0;j<leftAsOfAttributes.length;j++)
        {
            boolean found = false;
            for(int i=0;!found && i<equalityMappers.size();i++)
            {
                EqualityMapper mapper = (EqualityMapper) equalityMappers.get(i);
                if (mapper.getLeft().equals(leftAsOfAttributes[j])) found = true;
            }
            if (found) count++;
        }
        return count == leftAsOfAttributes.length;
    }

    public Attribute getAnyRightAttribute()
    {
        return ((EqualityMapper)equalityMappers.get(0)).getAnyRightAttribute();
    }

    public Attribute getAnyLeftAttribute()
    {
        return ((EqualityMapper)equalityMappers.get(0)).getAnyLeftAttribute();
    }

    public String getResultOwnerClassName()
    {
        return getAnyLeftAttribute().zGetTopOwnerClassName();
    }

    public Set<Attribute> getAllLeftAttributes()
    {
        fixLeftAttributes();
        UnifiedSet result = new UnifiedSet(this.leftAttributes.length);
        for(int i=0;i<this.leftAttributes.length;i++) result.add(this.leftAttributes[i]);
        return result;
    }

    public Extractor[] getLeftAttributesWithoutFilters()
    {
        fixLeftAttributes();
        return this.leftAttributes;
    }

    public Mapper createMapperForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject, int chainPosition)
    {
        Operation op = NoOperation.instance();
        InternalList newMappers = new InternalList(this.equalityMappers.size());
        for(int i=0;i<equalityMappers.size();i++)
        {
            EqualityMapper mapper = (EqualityMapper) equalityMappers.get(i);
            Object newMapperOrOperation = mapper.createOperationOrMapperForTempJoin(attributeMap, prototypeObject);
            if (newMapperOrOperation instanceof Operation)
            {
                op = op.and((Operation)newMapperOrOperation);
            }
            else
            {
                newMappers.add(newMapperOrOperation);
            }
        }
        Mapper newMapper =  newMappers.size() == 1 ? (Mapper) newMappers.get(0) : new MultiEqualityMapper(newMappers);
        if (op != NoOperation.instance())
        {
            newMapper = new FilteredMapper(newMapper, null, op);
        }
        return newMapper;
    }

    public boolean isMappableForTempJoin(Set<Attribute> attributeMap)
    {
        for(int i=0;i<equalityMappers.size();i++)
        {
            EqualityMapper mapper = (EqualityMapper) equalityMappers.get(i);
            if (mapper.isMappableForTempJoin(attributeMap)) return true;
        }
        return false;
    }

    public double estimateMappingFactor()
    {
        int indexRef = this.getBestIndexRef(this.getCache());
        if (indexRef > 0)
        {
            return this.getCache().getAverageReturnSize(indexRef, 1);
        }
        else
        {
            double leftSize = this.getAnyLeftAttribute().getOwnerPortal().getCache().estimateQuerySize();
            double rightSize = this.getAnyRightAttribute().getOwnerPortal().getCache().estimateQuerySize();
            if (rightSize == 0) return 0;
            return leftSize / rightSize;
        }
    }

    @Override
    public int estimateMaxReturnSize(int multiplier)
    {
        int indexRef = this.getBestIndexRef(this.getCache());
        if (indexRef > 0)
        {
            return this.getCache().getMaxReturnSize(indexRef, multiplier);
        }
        else
        {
            double leftSize = this.getAnyLeftAttribute().getOwnerPortal().getCache().estimateQuerySize();
            double rightSize = this.getAnyRightAttribute().getOwnerPortal().getCache().estimateQuerySize();
            if (rightSize == 0) return 0;
            return (int) Math.min(leftSize  * multiplier/ rightSize, this.getCache().estimateQuerySize());
        }
    }

    private boolean hasRightAttribute(AsOfAttribute asOfAttribute)
    {
        for(int i=0;i<equalityMappers.size();i++)
        {
            EqualityMapper mapper = (EqualityMapper) equalityMappers.get(i);
            if (mapper.getRight().equals(asOfAttribute)) return true;
        }
        return false;
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList)
    {
        if (this.mapUsesUniqueIndex() || this.mapUsesNonUniqueIndex() || joinedList.size() < 2)
        {
            return getResultViaMultiIn(joinedList, null);
        }
        FastList result = null;
        for(int i=0;i<joinedList.size();i++)
        {
            Operation op = this.getMultiEqualityOperation(joinedList.get(i));
            List partialResult = op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, true);
            if (partialResult == null) return null;
            if (result == null) result = new MithraFastList(joinedList.size()*partialResult.size());
            result.addAll(partialResult);
        }
        return result;
    }

    //todo: rezaem: there is room here for optimizing MultiInOperation and equality
    public List mapReturnNullIfIncompleteIndexHit(List joinedList, Operation extraOperationOnResult)
    {
        if (joinedList.isEmpty()) return ListFactory.EMPTY_LIST;
        if (joinedList.size() == 1)
        {
            Operation op = getMultiEqualityOperation(joinedList.get(0)).and(extraOperationOnResult);
            return op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, true);
        }
        else
        {
            Operation prototype = getMultiEqualityOperation(joinedList.get(0));
            if (extraOperationOnResult != null)
            {
                Operation equalityOperation = extraOperationOnResult.zExtractEqualityOperations();
                if (equalityOperation != null)
                {
                    prototype = prototype.and(equalityOperation);
                }
            }
            if (prototype.usesUniqueIndex() || prototype.usesNonUniqueIndex())
            {
                List result = getResultViaMultiIn(joinedList, extraOperationOnResult);
                if (result == null)
                {
                    result = mapReturnNullIfIncompleteIndexHitNonUnique(joinedList, extraOperationOnResult);
                }
                return result;
            }
            else
            {
                return mapReturnNullIfIncompleteIndexHitNonUnique(joinedList, extraOperationOnResult);
            }
        }
    }

    private List getResultViaMultiIn(List joinedList, Operation extraOperationOnResult)
    {
        boolean isTiny = joinedList.size() == 1;
        if (isTiny)
        {
            Object target = joinedList.get(0);
            Operation op = getMultiEqualityOperation(target);
            if (extraOperationOnResult != null)
            {
                op = op.and(extraOperationOnResult);
            }
            return op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, isTiny);
        }
        else
        {
            MultiInOperation op = createMultiIn(joinedList);
            Operation leftOver = op.setExtraOperationAndReturnLeftOver(extraOperationOnResult);
            List list = op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, isTiny);
            if (list != null && leftOver != null)
            {
                list = leftOver.applyOperation(list);
            }
            return list;
        }
    }

    private List mapReturnNullIfIncompleteIndexHitNonUnique(List joinedList, Operation extraOperationOnResult)
    {
        FastList result = null;
        for(int i=0;i<joinedList.size();i++)
        {
            Operation op = this.getMultiEqualityOperation(joinedList.get(i)).and(extraOperationOnResult);
            List partialResult = op.getResultObjectPortal().zFindInMemoryWithoutAnalysis(op, true);
            if (partialResult == null) return null;
            if (result == null) result = new MithraFastList(joinedList.size()*partialResult.size());
            result.addAll(partialResult);
        }
        return result;
    }

    private MultiInOperation createMultiIn(List joinedList)
    {
        Attribute[] leftAttributes = new Attribute[this.equalityMappers.size()];
        Attribute[] rightAttributes = new Attribute[this.equalityMappers.size()];
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            leftAttributes[i] = mapper.getLeft();
            rightAttributes[i] = mapper.getRight();
        }
        return new MultiInOperation(leftAttributes, joinedList, rightAttributes);
    }

    private Operation getMultiEqualityOperation(Object target)
    {
        if (mappedIndex >=0) return getAndOperation(target);
        return getMultiEqualityOperationForSize(target, this.equalityMappers.size());
    }

    private Operation getMultiEqualityOperationForSize(Object target, int size)
    {
        AtomicOperation[] newOperations = new AtomicOperation[size];
        for(int i=0;i< size;i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            Attribute right = mapper.getRight();
            if (right.isAttributeNull(target))
            {
                return new None(mapper.getLeft());
            }
            newOperations[i] = (AtomicOperation) mapper.getLeft().nonPrimitiveEq(right.valueOf(target));
        }
        return new MultiEqualityOperation(newOperations);
    }

    private Operation getAndOperation(Object target)
    {
        Operation first = getMultiEqualityOperationForSize(target, this.mappedIndex);
        Operation second = NoOperation.instance();
        for(int i=this.mappedIndex;i< this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            Attribute right = mapper.getRight();
            if (right.isAttributeNull(target))
            {
                return new None(mapper.getLeft());
            }
            second = second.and(mapper.getLeft().nonPrimitiveEq(right.valueOf(target)));
        }
        return first.and(second);
    }

    public void generateSql(SqlQuery query)
    {
        boolean mappedAlready = query.isMappedAlready(this);
        String[] leftArray = null;
        if (!mappedAlready)
        {
            leftArray = new String[this.equalityMappers.size()];
            for(int i=0;i<this.equalityMappers.size();i++)
            {
                EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
                query.beginAnd();
                boolean appends = equalityMapper.addsToWhereClause();
                if (appends)
                {
                    leftArray[i] = equalityMapper.generateLeftHandSql(query, true);
                }
                query.endAnd();
            }
        }
        query.pushMapper(this);
        if (!mappedAlready)
        {
            for(int i=0;i<this.equalityMappers.size();i++)
            {
                EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
                boolean appends = equalityMapper.addsToWhereClause();
                if (appends)
                {
                    equalityMapper.generateRightHandSql(query, false, true, leftArray[i]);
                }
            }
        }
        query.addAsOfAttributeSql();
     }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        boolean mappedAlready = extractor.isMappedAlready(this);
        if (!mappedAlready)
        {
            for(int i=0;i<this.equalityMappers.size();i++)
            {
                EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
                equalityMapper.registerOperationForLeft(extractor, registerEquality, true);
            }
        }
        extractor.pushMapper(this);
        if (!mappedAlready)
        {
            for(int i=0;i<this.equalityMappers.size();i++)
            {
                EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
                equalityMapper.registerOperationForRight(extractor, registerEquality, true);
            }
        }
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            equalityMapper.registerLeftAsOfAttributesAndOperations(checker);
        }
        checker.pushMapper(this);
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            equalityMapper.registerRightAsOfAttributesAndOperations(checker);
        }
    }

    public void registerEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        transitivePropagator.pushMapper(this);
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            transitivePropagator.setEquality(equalityMapper.getLeft(), equalityMapper.getRight());
        }
    }

    public boolean hasTriangleJoins()
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            if (equalityMapper.hasTriangleJoins()) return true;
        }
        return false;
    }

    public boolean isRightHandPartialCacheResolvable()
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            if (!equalityMapper.isRightHandPartialCacheResolvable()) return false;
        }
        return true;
    }

    public void appendSyntheticName(StringBuilder stringBuilder)
    {
        stringBuilder.append("[ -> ");
        stringBuilder.append(this.getFromPortal().getBusinessClassName()).append(": ");
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            if (i > 0) stringBuilder.append(" & ");
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            equalityMapper.appendEquality(stringBuilder);
        }
        stringBuilder.append(" ]");
    }

    public boolean isSingleLevelJoin()
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            if (!((Mapper)this.equalityMappers.get(i)).isSingleLevelJoin()) return false;
        }
        return true;
    }

    @Override
    public boolean isEstimatable()
    {
        return this.getAnyLeftAttribute().getOwnerPortal().isFullyCached() && !this.getAnyLeftAttribute().getOwnerPortal().isForTempObject()
                && this.getAnyRightAttribute().getOwnerPortal().isFullyCached() && !this.getAnyRightAttribute().getOwnerPortal().isForTempObject();
    }

    public void pushMappers(MapperStack mapperStack)
    {
        mapperStack.pushMapper(this);
    }

    public void popMappers(MapperStack mapperStack)
    {
        mapperStack.popMapper();
    }

    public int getClauseCount(SqlQuery query)
    {
        return this.equalityMappers.size();
    }

    public Mapper getReverseMapper()
    {
        Mapper revMapper = this.reverseMapper;
        if (revMapper == null)
        {
            InternalList reverseEqualityMappers = new InternalList(this.equalityMappers.size());
            for(int i=0;i<this.equalityMappers.size();i++)
            {
                reverseEqualityMappers.add(((Mapper)this.equalityMappers.get(i)).getReverseMapper());
            }
            revMapper = new MultiEqualityMapper(reverseEqualityMappers, this);
            this.reverseMapper = revMapper;
        }
        return revMapper;
    }

    public void addDepenedentPortalsToSet(Set set)
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            ((Mapper)this.equalityMappers.get(i)).addDepenedentPortalsToSet(set);
        }
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            ((Mapper)this.equalityMappers.get(i)).addDepenedentAttributesToSet(set);
        }
    }

    public Attribute getDeepestEqualAttribute(Attribute attribute)
    {
        Attribute result = null;
        for(int i=0;i<this.equalityMappers.size() && result == null;i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            result = mapper.getDeepestEqualAttribute(attribute);
        }
        return result;
    }

    public Mapper and(Mapper other)
    {
        return other.andWithMultiEqualityMapper(this);
    }

    public Mapper andWithEqualityMapper(EqualityMapper other)
    {
        return new MultiEqualityMapper(this, other);
    }

    public Mapper andWithMultiEqualityMapper(MultiEqualityMapper other)
    {
        return new MultiEqualityMapper(this, other);
    }

    public MappedOperation combineWithEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        EqualityMapper em = (EqualityMapper) otherMappedOperation.getMapper();
        if (em.hasMappedAttributes()) return null;
        if (mappedOperation.isSameMapFromTo(otherMappedOperation))
        {
            Mapper newMapper = new MultiEqualityMapper(this, (EqualityMapper)otherMappedOperation.getMapper());
            return new MappedOperation(newMapper,
                    mappedOperation.getUnderlyingOperation().and(otherMappedOperation.getUnderlyingOperation()));
        }
        return null;
    }

    public MappedOperation combineWithMultiEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        if (mappedOperation.isSameMapFromTo(otherMappedOperation))
        {
            Mapper newMapper = new MultiEqualityMapper((MultiEqualityMapper) mappedOperation.getMapper(), (MultiEqualityMapper) otherMappedOperation.getMapper());
            return new MappedOperation(newMapper,
                    otherMappedOperation.getUnderlyingOperation().and(mappedOperation.getUnderlyingOperation()));
        }
        return null;
    }

    public MappedOperation equalitySubstituteWithAtomic(MappedOperation mappedOperation, AtomicOperation op)
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            if (op.getAttribute().equals(mapper.getLeft())  && !mappedOperation.underlyingOperationDependsOnAttribute(mapper.getRight()))
            {
                Operation replacedOp = op.susbtituteOtherAttribute(mapper.getRight());
                if (replacedOp != null)
                {
                    return new MappedOperation(this,
                            mappedOperation.getUnderlyingOperation().and(replacedOp));
                }
            }
        }
        return null;

    }

    public MappedOperation equalitySubstituteWithMultiEquality(MappedOperation mappedOperation, MultiEqualityOperation op)
    {
		Operation newOperation = mappedOperation.getUnderlyingOperation();
        HashSet existingAttributesWithOperations = new HashSet();
        newOperation.addDepenedentAttributesToSet(existingAttributesWithOperations);
		boolean found = false;
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            if (!existingAttributesWithOperations.contains(mapper.getRight()))
            {
                Operation susbstitutedEquality = op.getSusbstitutedEquality(mapper.getLeft(), mapper.getRight());
                if (susbstitutedEquality != null)
                {
                    newOperation = newOperation.and(susbstitutedEquality);
                    found = true;
                }
            }
        }
		if (found)
		{
			return new MappedOperation(this, newOperation);
		}
        return null;
    }

    protected MappedOperation combineByType(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        return otherMappedOperation.getMapper().combineWithMultiEqualityMapper(otherMappedOperation, mappedOperation);
    }

    public int hashCode()
    {
        int hashcode = 0;
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            // using the symmetric ^ operation to make sure order is irrelevant
            hashcode ^= this.equalityMappers.get(i).hashCode();
        }
        return hashcode;
    }

    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        boolean result = false;
        if (obj instanceof MultiEqualityMapper)
        {
            MultiEqualityMapper other = (MultiEqualityMapper) obj;
            if (other.equalityMappers.size() != this.equalityMappers.size()) return false;
            for(int i=0;i<other.equalityMappers.size();i++)
            {
                if (!this.equalityMappers.contains(other.equalityMappers.get(i))) return false;
            }
            return true;
        }
        return result;
    }

    public AsOfEqOperation[] getDefaultAsOfOperation(List<AsOfAttribute> ignore)
    {
        AsOfAttribute[] asOfAttributes = ((PrivateReladomoClassMetaData)this.getFromPortal().getClassMetaData()).getCachedAsOfAttributes();
        InternalList results = null;
        if (asOfAttributes != null)
        {
            for(int i=0;i<asOfAttributes.length;i++)
            {
                if (asOfAttributes[i].getDefaultDate() != null && !ignore.contains(asOfAttributes[i]) && !hasRightAttribute(asOfAttributes[i]))
                {
                    if (results == null) results = new InternalList(2);
                    results.add(asOfAttributes[i].eq(asOfAttributes[i].getDefaultDate()));
                }
            }
        }
        if (results == null) return null;
        AsOfEqOperation[] arrayResults = new AsOfEqOperation[results.size()];
        results.toArray(arrayResults);
        return arrayResults;
    }

    public Mapper insertAsOfOperationInMiddle(AtomicOperation[] asOfEqOperation, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        InternalList substituted = null;
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            Mapper mapper = equalityMapper.insertAsOfOperationInMiddleForLeft(asOfEqOperation, insertPosition, stack);
            if (mapper != null)
            {
                if (substituted == null)
                {
                    substituted = this.equalityMappers.copy();
                }
                substituted.set(i, mapper);
            }
        }
        if (substituted != null) return new MultiEqualityMapper(substituted);
        stack.pushMapper(this);
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            stack.popMapper();
            FilteredMapper mapper = new FilteredMapper(this, null, MultiEqualityOperation.createEqOperation(asOfEqOperation));
            mapper.setName(this.getRawName());
            return mapper;
        }
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper equalityMapper = ((EqualityMapper)this.equalityMappers.get(i));
            Mapper mapper = equalityMapper.insertAsOfOperationInMiddleForRight(asOfEqOperation, insertPosition, stack);
            if (mapper != null)
            {
                if (substituted == null)
                {
                    substituted = this.equalityMappers.copy();
                }
                substituted.set(i, mapper);
            }
        }
        stack.popMapper();
        if (substituted != null)
        {
            MultiEqualityMapper mapper = new MultiEqualityMapper(substituted);
            mapper.setName(this.getRawName());
            return mapper;
        }
        return null;
    }

    public Mapper insertOperationInMiddle(MapperStack insertPosition, InternalList toInsert, TransitivePropagator stack)
    {
        stack.pushMapper(this);
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            stack.popMapper();
            FilteredMapper mapper = new FilteredMapper(this, null, this.constructAndOperation(toInsert));
            mapper.setName(this.getRawName());
            return mapper;
        }
        stack.popMapper();
        return null;
    }

    @Override
    public void clearLeftOverFromObjectCache(Collection<Object> parentObjects, EqualityOperation extraEqOp, Operation extraOperation)
    {
        MithraFastList attrList = new MithraFastList(this.equalityMappers.size() + 2);
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            attrList.add(mapper.getRight());
        }
        if (extraEqOp != null)
        {
            extraEqOp.addEqAttributes(attrList);
        }
        Cache cache = ((Attribute)attrList.get(0)).getOwnerPortal().getCache();
        IndexReference bestIndexReference = cache.getBestIndexReference(attrList);
        if (bestIndexReference != null && bestIndexReference.isValid() && cache.isUnique(bestIndexReference.indexReference))
        {
            Attribute[] indexAttributes = cache.getIndexAttributes(bestIndexReference.indexReference);
            List<Extractor> extractors = new MithraFastList<Extractor>(indexAttributes.length);
            Operation localExtraOperation = extraOperation;
            if (indexAttributes.length != attrList.size())
            {
                localExtraOperation = localExtraOperation == null ? extraEqOp : extraEqOp.and(localExtraOperation);
            }
            int eqMapperCount = 0;
            for(Attribute a: indexAttributes)
            {
                EqualityMapper eqMapper = getEqualitMapperFor(a);
                if (eqMapper != null)
                {
                    extractors.add(eqMapper.getLeft());
                    eqMapperCount++;
                }
                else
                {
                    extractors.add(extraEqOp.getParameterExtractorFor(a));
                }
            }
            List<Extractor> leftOverExtractors = null;
            if (eqMapperCount != this.equalityMappers.size())
            {
                leftOverExtractors = new MithraFastList<Extractor>(this.equalityMappers.size() - eqMapperCount);
                for(int i=0;i<this.equalityMappers.size();i++)
                {
                    EqualityMapper eqMapper = (EqualityMapper) this.equalityMappers.get(i);
                    if (!containedInArray(indexAttributes, eqMapper.getRight()))
                    {
                        leftOverExtractors.add(eqMapper.getLeft());
                    }
                }
            }
            cache.markNonExistent(bestIndexReference.indexReference, parentObjects, extractors, leftOverExtractors, localExtraOperation);
        }
    }

    private boolean containedInArray(Attribute[] indexAttributes, Attribute right)
    {
        for(Attribute a: indexAttributes)
        {
            if (a.equals(right))
            {
                return true;
            }
        }
        return false;
    }

    private EqualityMapper getEqualitMapperFor(Attribute a)
    {
        for(int i=0;i<this.equalityMappers.size();i++)
        {
            EqualityMapper mapper = ((EqualityMapper)this.equalityMappers.get(i));
            if (mapper.getRight().equals(a))
            {
                return mapper;
            }
        }
        return null;
    }
}
