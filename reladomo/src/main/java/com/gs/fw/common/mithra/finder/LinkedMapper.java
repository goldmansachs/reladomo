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
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraList;
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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;



public class LinkedMapper implements Mapper, Externalizable
{

    private List<Mapper> mappers;
    private boolean isToMany = false;

    public LinkedMapper()
    {
        // for externalizable
        this.mappers = new FastList<Mapper>(4);
    }

    public LinkedMapper(Mapper first, Mapper second)
    {
        this.mappers = new FastList<Mapper>(4);
        this.addMapper(first);
        this.addMapper(second);
    }

    private LinkedMapper(List<Mapper> mappers)
    {
        this.mappers = mappers;
    }

    public void setAnonymous(boolean anonymous)
    {
        throw new RuntimeException("should never get here");
    }

    public boolean isAnonymous()
    {
        return false;
    }

    public void setToMany(boolean toMany)
    {
        isToMany = toMany;
    }

    public boolean isToMany()
    {
        return isToMany;
    }

    public String getRelationshipPath()
    {
        StringBuffer buf = new StringBuffer();
        for(Mapper mapper : this.mappers)
        {
            if (mapper instanceof AbstractMapper)
            {
                AbstractMapper aMapper = (AbstractMapper) mapper;
                buf.append(aMapper.getRawName() + ".");
            }
        }
        if (buf.length() > 1)
        {
            buf.deleteCharAt(buf.length()-1);
        }
        return buf.toString();
    }

    public List<String> getRelationshipPathAsList()
    {
        List<String> list = FastList.newList();
        for(Mapper mapper : this.mappers)
        {
            if (mapper instanceof AbstractMapper)
            {
                AbstractMapper aMapper = (AbstractMapper) mapper;
                list.add(aMapper.getRawName());
            }
        }
        return list;
    }

    @Override
    public boolean isEstimatable()
    {
        return false;
    }

    public Mapper getCommonMapper(Mapper other)
    {
        if (other instanceof LinkedMapper)
        {
            LinkedMapper otherLinkedMapper = (LinkedMapper) other;
            int i=0;
            int end = Math.min(this.mappers.size(), otherLinkedMapper.mappers.size());
            while (i < end && this.mappers.get(i).equals(otherLinkedMapper.mappers.get(i)))
            {
                i++;
            }
            if (i == this.mappers.size()) return this;
            if (i > 0)
            {
                return new LinkedMapper(this.mappers.subList(0, i));
            }
        }
        else
        {
            if (this.mappers.get(0).equals(other))
            {
                return other;
            }
        }
        return null;
    }

    public Mapper getMapperRemainder(Mapper head)
    {

        if (head instanceof LinkedMapper)
        {
            LinkedMapper headLinkedMapper = (LinkedMapper) head;

            //not sure about this.
            if(headLinkedMapper.mappers.equals(this.mappers))
            {
                return null;
            }

            if (headLinkedMapper.mappers.size() == this.mappers.size() - 1)
            {
                return getLastMapper();
            }
            return new LinkedMapper(this.mappers.subList(headLinkedMapper.mappers.size(), this.mappers.size()));
        }
        else
        {
            List<Mapper> mapperList = this.mappers.subList(1, this.mappers.size());
            if(mapperList.size() > 1)
               return new LinkedMapper(mapperList);
            else
               return mapperList.get(0);
        }
    }

    public Function getParentSelectorRemainder(DeepRelationshipAttribute parentSelector)
    {
        parentSelector = parentSelector.copy();
        DeepRelationshipAttribute rootSelector = parentSelector;
        for(int i = 0; i < mappers.size(); i++)
        {
            rootSelector = rootSelector.getParentDeepRelationshipAttribute();
        }
        rootSelector.setParentDeepRelationshipAttribute(null);

        return parentSelector;
    }

    public Function getTopParentSelector(DeepRelationshipAttribute parentSelector)
    {
        DeepRelationshipAttribute topParentSelector = parentSelector;
        for(int i = 0; i < mappers.size(); i++)
        {
            topParentSelector = topParentSelector.getParentDeepRelationshipAttribute();

        }
        return topParentSelector;
    }

    public Operation createNotExistsOperation(Operation op)
    {
        Mapper headMapper;
        if (this.mappers.size() > 2)
        {
            Mapper[] headLinkedMappers = new Mapper[this.mappers.size() - 1];
            for(int i=0;i<this.mappers.size() - 1; i++)
            {
                headLinkedMappers[i] = this.mappers.get(i);
            }
            headMapper = new LinkedMapper(Arrays.asList(headLinkedMappers));
        }
        else
        {
            headMapper = this.mappers.get(0);
        }

        return new MappedOperation(headMapper, new NotExistsOperation(getLastMapper(), op));
    }

    @Override
    public Operation createRecursiveNotExistsOperation(Operation op)
    {
        List<Mapper> mappers = this.getMappers();
        Mapper m = new ChainedMapper(mappers.get(mappers.size() - 2), mappers.get(mappers.size() - 1));
        for(int i=mappers.size() - 3; i >= 0 ; i --)
        {
            m = new ChainedMapper(mappers.get(i), m);
        }
        return new NotExistsOperation(m, op);
    }

    private void addMapper(Mapper mapper)
    {
        if (mapper instanceof LinkedMapper)
        {
            this.mappers.addAll(((LinkedMapper)mapper).mappers);
        }
        else
        {
            this.mappers.add(mapper);
        }
    }

    public List<Mapper> getMappers()
    {
        return mappers;
    }

    public MithraObjectPortal getResultPortal()
    {
        Mapper mapper = this.mappers.get(0);
        return mapper.getResultPortal();
    }

    public MithraObjectPortal getFromPortal()
    {
        Mapper mapper = this.mappers.get(mappers.size()-1);
        return mapper.getFromPortal();
    }

    public boolean isReversible()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            if (mapper.getFromPortal() == portal) return true;
        }
        return false;
    }

    public boolean mapUsesUniqueIndex()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean mapUsesImmutableUniqueIndex()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean mapUsesNonUniqueIndex()
    {
        throw new RuntimeException("not implemented");
    }

    public List map(List joinedList)
    {
        for(int i=this.mappers.size()-1;i>=0 && joinedList != null;i--)
        {
            Mapper mapper = this.mappers.get(i);
            joinedList = mapper.map(joinedList);
        }
        return joinedList;
    }

    public ConcurrentFullUniqueIndex mapMinusOneLevel(List joinedList)
    {
        for(int i=this.mappers.size()-1;i>=1 && joinedList != null;i--)
        {
            Mapper mapper = this.mappers.get(i);
            joinedList = mapper.map(joinedList);
        }
        if (joinedList == null) return null;
        return this.mappers.get(0).mapMinusOneLevel(joinedList);
    }

    public List mapOne(Object joined, Operation extraLeftOperation)
    {
        List joinedList = ListFactory.create(joined);
        for(int i=this.mappers.size()-1;i>0 && joinedList != null;i--)
        {
            Mapper mapper = this.mappers.get(i);
            joinedList = mapper.map(joinedList);
        }
        if (joinedList != null)
        {
            FastList result = new FastList(joinedList.size());
            Mapper firstMapper = this.mappers.get(0);
            for(int i=0;i<joinedList.size();i++)
            {
                result.addAll(firstMapper.mapOne(joinedList.get(i), extraLeftOperation));
            }
            return result;
        }
        return null;
    }

    public Operation getSimplifiedJoinOp(List parentList, int maxInClause, DeepFetchNode node, boolean useTuple)
    {
        Operation result = getLastMapper().getSimplifiedJoinOp(parentList, maxInClause, node, useTuple);
        int splitPoint = this.mappers.size() - 2;
        while(result == null && splitPoint >= 1)
        {
            Mapper parentMapper;
            if (splitPoint == 1)
            {
                parentMapper = this.mappers.get(0);
            }
            else
            {
                parentMapper = new LinkedMapper(this.mappers.subList(0, splitPoint));
            }

            Operation op = node.createMappedOperationForDeepFetch(parentMapper);
            if (op == null) return null;
            MithraList midParentList = op.getResultObjectPortal().getFinder().findMany(op);
            Mapper splitPointMapper = this.mappers.get(splitPoint);
            Operation simplifiedOp = splitPointMapper.getSimplifiedJoinOp(midParentList, maxInClause, node, useTuple);
            if (simplifiedOp != null)
            {
                for(int i=splitPoint+1;i<this.mappers.size();i++)
                {
                    Mapper postSplitMapper = this.mappers.get(i);
                    simplifiedOp = postSplitMapper.createMappedOperationForDeepFetch(simplifiedOp);
                }
                result = simplifiedOp;
            }
            splitPoint--;
        }
        return result;
    }

    public Mapper getLastMapper()
    {
        return this.mappers.get(this.mappers.size() - 1);
    }

    public Operation getOperationFromResult(Object result, Map<Attribute, Object> tempOperationPool)
    {
        return getLastMapper().getOperationFromResult(result, tempOperationPool);
    }

    public Operation getOperationFromOriginal(Object original, Map<Attribute, Object> tempOperationPool)
    {
        return getLastMapper().getOperationFromOriginal(original, tempOperationPool);
    }

    @Override
    public Operation getPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return getLastMapper().getPrototypeOperation(tempOperationPool);
    }

    public List<Mapper> getUnChainedMappers()
    {
        return getLastMapper().getUnChainedMappers();
    }

    public Operation createMappedOperationForDeepFetch(Operation op)
    {
        for(int i=0;i<mappers.size(); i++)
        {
            op = mappers.get(i).createMappedOperationForDeepFetch(op);
        }
        return op;
    }

    public Mapper getParentMapper()
    {
        if (this.mappers.size() == 2)
        {
            return this.mappers.get(0);
        }
        return new LinkedMapper(this.mappers.subList(0, this.mappers.size() - 1));
    }

    public Mapper link(Mapper other)
    {
        FastList<Mapper> newList = new FastList<Mapper>(this.mappers.size()+ 1);
        newList.addAll(this.mappers);
        newList.add(other);
        return new LinkedMapper(newList);
    }

    public boolean hasLeftOrDefaultMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        return this.mappers.get(0).hasLeftOrDefaultMappingsFor(leftAsOfAttributes);
    }

    public boolean hasLeftMappingsFor(AsOfAttribute[] leftAsOfAttributes)
    {
        return this.mappers.get(0).hasLeftMappingsFor(leftAsOfAttributes);
    }

    public Attribute getAnyRightAttribute()
    {
        Mapper mapper = this.mappers.get(mappers.size()-1);
        return mapper.getAnyRightAttribute();
    }

    public Attribute getAnyLeftAttribute()
    {
        return this.mappers.get(0).getAnyLeftAttribute(); 
    }

    public boolean isFullyCachedIgnoringLeft()
    {
        boolean result = this.mappers.get(0).isFullyCachedIgnoringLeft();
        for(int i=1;result && i<this.mappers.size();i++)
        {
            result = this.mappers.get(i).isFullyCached();
        }
        return result;
    }

    public boolean isFullyCached()
    {
        boolean result = true;
        for(int i=0;result && i<this.mappers.size();i++)
        {
            result = this.mappers.get(i).isFullyCached();
        }
        return result;
    }

    public String getResultOwnerClassName()
    {
        return this.mappers.get(0).getResultOwnerClassName();
    }

    public Set<Attribute> getAllLeftAttributes()
    {
        return this.getLastMapper().getAllLeftAttributes();
    }

    public Extractor[] getLeftAttributesWithoutFilters()
    {
        return this.getLastMapper().getLeftAttributesWithoutFilters();
    }

    public List filterLeftObjectList(List objects)
    {
        return this.getLastMapper().filterLeftObjectList(objects);
    }

    public Mapper createMapperForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject, int chainPosition)
    {
        return this.getLastMapper().createMapperForTempJoin(attributeMap, prototypeObject, chainPosition);
    }

    public boolean isMappableForTempJoin(Set<Attribute> attributeMap)
    {
        return this.getLastMapper().isMappableForTempJoin(attributeMap);
    }

    public double estimateMappingFactor()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int estimateMaxReturnSize(int multiplier)
    {
        throw new RuntimeException("not implemented");
    }

    public void registerEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean hasTriangleJoins()
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            if (mapper.hasTriangleJoins()) return true;
        }
        return false;
    }

    public boolean isRightHandPartialCacheResolvable()
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            if (!mapper.isRightHandPartialCacheResolvable()) return false;
        }
        return true;
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList)
    {
        throw new RuntimeException("not implemented");
    }

    public List map(List joinedList, Operation extraOperationOnResult)
    {
        throw new RuntimeException("not implemented");
    }

    public List mapReturnNullIfIncompleteIndexHit(List joinedList, Operation extraOperationOnResult)
    {
        throw new RuntimeException("not implemented");
    }

    public void generateSql(SqlQuery query)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            mapper.generateSql(query);
        }
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            mapper.registerOperation(extractor, registerEquality);
        }
    }

    public int getClauseCount(SqlQuery query)
    {
        throw new RuntimeException("not implemented");
    }

    public Mapper getReverseMapper()
    {
        List<Mapper> reverseMappers = new FastList<Mapper>(this.mappers.size());
        for(int i=this.mappers.size() - 1;i>=0;i--)
        {
            Mapper mapper = this.mappers.get(i);
            Mapper revMappper = mapper.getReverseMapper();
            if (revMappper == null) return null;
            reverseMappers.add(revMappper);
        }
        return new LinkedMapper(reverseMappers);
    }

    public void addDepenedentPortalsToSet(Set set)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            mapper.addDepenedentPortalsToSet(set);
        }
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        throw new RuntimeException("not implemented");
    }

    public Attribute getDeepestEqualAttribute(Attribute attribute)
    {
        throw new RuntimeException("not implemented");
    }

    public Mapper and(Mapper other)
    {
        throw new RuntimeException("not implemented");
    }

    public Mapper andWithEqualityMapper(EqualityMapper other)
    {
        throw new RuntimeException("not implemented");
    }

    public Mapper andWithMultiEqualityMapper(MultiEqualityMapper other)
    {
        throw new RuntimeException("not implemented");
    }

    public MappedOperation combineMappedOperations(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        throw new RuntimeException("not implemented");
    }

    public MappedOperation combineWithFilteredMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        throw new RuntimeException("not implemented");
    }

    public MappedOperation combineWithEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        throw new RuntimeException("not implemented");
    }

    public MappedOperation combineWithMultiEqualityMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        throw new RuntimeException("not implemented");
    }

    public MappedOperation equalitySubstituteWithAtomic(MappedOperation mappedOperation, AtomicOperation op)
    {
        throw new RuntimeException("not implemented");
    }

    public MappedOperation equalitySubstituteWithMultiEquality(MappedOperation mappedOperation, MultiEqualityOperation op)
    {
        throw new RuntimeException("not implemented");
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            mapper.registerAsOfAttributesAndOperations(checker);
        }
    }

    public Mapper insertAsOfOperationInMiddle(AtomicOperation[] asOfEqOperation, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            Mapper modifiedMapper = mapper.insertAsOfOperationInMiddle(asOfEqOperation, insertPosition, stack);
            if (modifiedMapper != null)
            {
                return popAndCreateNewLinkedMapper(stack, i, modifiedMapper);
            }
            mapper.pushMappers(stack);
        }
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            Mapper modifiedMapper = new FilteredMapper(this.mappers.get(this.mappers.size() - 1), null, MultiEqualityOperation.createEqOperation(asOfEqOperation));
            return popAndCreateNewLinkedMapper(stack, this.mappers.size() - 1, modifiedMapper);
        }
        popMappers(stack);
        return null;
    }

    private Mapper popAndCreateNewLinkedMapper(AsOfEqualityChecker stack, int i, Mapper modifiedMapper)
    {
        for(int j=i-1; j >=0; j--)
        {
            this.mappers.get(j).popMappers(stack);
        }
        List<Mapper> newMappers = FastList.newList(this.mappers);
        newMappers.set(i, modifiedMapper);
        return new LinkedMapper(newMappers);
    }

    public Mapper insertOperationInMiddle(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        throw new RuntimeException("not implemented");
    }

    public Mapper insertAsOfOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        throw new RuntimeException("not implemented");
    }

    public Mapper insertOperationOnLeft(InternalList toInsert)
    {
        throw new RuntimeException("not implemented");
    }

    public void pushMappers(MapperStack mapperStack)
    {
        for(int i=0;i<this.mappers.size();i++)
        {
            Mapper mapper = this.mappers.get(i);
            mapper.pushMappers(mapperStack);
        }
    }

    public void popMappers(MapperStack mapperStack)
    {
        for(int i=this.mappers.size()-1;i>=0;i--)
        {
            Mapper mapper = this.mappers.get(i);
            mapper.popMappers(mapperStack);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(this.mappers.size());
        for(int i=0;i<mappers.size();i++)
        {
            out.writeObject(mappers.get(i));
        }
        out.writeBoolean(this.isToMany);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int size = in.readInt();
        this.mappers = new FastList(size);
        for(int i=0;i<size;i++)
        {
            this.mappers.add((Mapper) in.readObject());
        }
        this.isToMany = in.readBoolean();
    }

    public Operation getRightFilters()
    {
        return null;
    }

    public Operation getLeftFilters()
    {
        return null;
    }

    public AsOfEqOperation[] getDefaultAsOfOperation(List<AsOfAttribute> ignore)
    {
        return this.getLastMapper().getDefaultAsOfOperation(ignore);
    }

    public void setName(String name)
    {
        //ignore
    }

    public void appendName(StringBuilder stringBuilder)
    {
        stringBuilder.append("linkedMapper");
    }

    public void appendSyntheticName(StringBuilder stringBuilder)
    {
        stringBuilder.append("linkedMapper");
    }

    public boolean isSingleLevelJoin()
    {
        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LinkedMapper that = (LinkedMapper) o;

        if (!mappers.equals(that.mappers)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return HashUtil.combineHashes(mappers.get(0).hashCode(), mappers.get(mappers.size() - 1).hashCode());
    }

    @Override
    public void clearLeftOverFromObjectCache(Collection<Object> parentObjects, EqualityOperation extraEqOp, Operation extraOperation)
    {
        this.getLastMapper().clearLeftOverFromObjectCache(parentObjects, extraEqOp, extraOperation);
    }
}
