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
import com.gs.fw.common.mithra.attribute.Attribute;

import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.ListFactory;

import java.util.List;
import java.util.Map;



public abstract class AbstractMapper implements Mapper
{
    private boolean isAnonymous = false;
    private boolean isToMany = true;
    protected Mapper reverseMapper;
    private String name;

    public boolean isToMany()
    {
        return this.isToMany;
    }

    public Mapper getCommonMapper(Mapper other)
    {
        if (other instanceof LinkedMapper)
        {
            return other.getCommonMapper(this);
        }
        if (this.equals(other))
        {
            return this;
        }
        return null;
    }

    public Mapper getMapperRemainder(Mapper head)
    {
        return null;
    }

    public Function getParentSelectorRemainder(DeepRelationshipAttribute parentSelector)
    {
        if (parentSelector.getParentDeepRelationshipAttribute() != null)
        {
            parentSelector = parentSelector.copy();
            parentSelector.setParentDeepRelationshipAttribute(null);
        }
        return parentSelector;
    }

    public Function getTopParentSelector(DeepRelationshipAttribute parentSelector)
    {
        return parentSelector.getParentDeepRelationshipAttribute();
    }

    public Operation createNotExistsOperation(Operation op)
    {
        return new NotExistsOperation(this, op);
    }

    @Override
    public Operation createRecursiveNotExistsOperation(Operation op)
    {
        return this.createNotExistsOperation(op);
    }

    public void setToMany(boolean toMany)
    {
        isToMany = toMany;
    }

    public boolean isAnonymous()
    {
        return isAnonymous;
    }

    public void setAnonymous(boolean anonymous)
    {
        isAnonymous = anonymous;
    }

    protected abstract MappedOperation combineByType(MappedOperation mappedOperation, MappedOperation otherMappedOperation);

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return this.getFromPortal() == portal;
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
        MappedOperation result = mappedOperation.checkForSimpleCombination(otherMappedOperation);
        if (result != null) return result;
        if (mappedOperation.getMapper().isAnonymous() && otherMappedOperation.getMapper().isAnonymous())
        {
            result = this.combineByType(mappedOperation, otherMappedOperation);
            if (result == null)
            {
                if (mappedOperation.getMapper().getResultPortal().equals(otherMappedOperation.getMapper().getFromPortal())
                        && otherMappedOperation.getUnderlyingOperation() instanceof All)
                {
                    result = new MappedOperation(new ChainedMapper(otherMappedOperation.getMapper(), mappedOperation.getMapper()), mappedOperation.getUnderlyingOperation());
                }
                else if (mappedOperation.getMapper().getFromPortal().equals(otherMappedOperation.getMapper().getResultPortal())
                        && mappedOperation.getUnderlyingOperation() instanceof All)
                {
                    result = new MappedOperation(new ChainedMapper(mappedOperation.getMapper(), otherMappedOperation.getMapper()), otherMappedOperation.getUnderlyingOperation());
                }
            }
            if (result != null)
            {
                result.getMapper().setAnonymous(true);
            }
        }
        return result;
    }

    public MappedOperation combineWithFilteredMapper(MappedOperation mappedOperation, MappedOperation otherMappedOperation)
    {
        if (mappedOperation.isSameMapFromTo(otherMappedOperation))
        {
            FilteredMapper fm = (FilteredMapper) otherMappedOperation.getMapper();
            MappedOperation mop = new MappedOperation(fm.getUnderlyingMapper(), otherMappedOperation.getUnderlyingOperation());
            MappedOperation combined = (MappedOperation) mop.zCombinedAnd(mappedOperation);
            FilteredMapper newFilteredMapper = new FilteredMapper(combined.getMapper(), fm.getLeftFilters(), fm.getRightFilters());
            return new MappedOperation(newFilteredMapper, combined.getUnderlyingOperation());
        }
        return null;
    }

    public Mapper insertAsOfOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        FilteredMapper mapper = new FilteredMapper(this, MultiEqualityOperation.createEqOperation(asOfEqOperations), null);
        mapper.setName(this.getRawName());
        return mapper;
    }

    public Mapper insertOperationOnLeft(InternalList toInsert)
    {
        Operation op = constructAndOperation(toInsert);
        FilteredMapper mapper = new FilteredMapper(this, op, null);
        mapper.setName(this.getRawName());
        return mapper;
    }

    protected Operation constructAndOperation(InternalList toInsert)
    {
        Operation op = (Operation) toInsert.get(0);
        for(int i=1;i<toInsert.size();i++)
        {
            op = op.and((Operation) toInsert.get(i));
        }
        return op;
    }

    public Operation getRightFilters()
    {
        return null;
    }

    public Operation getLeftFilters()
    {
        return null;
    }

    public Operation createMappedOperationForDeepFetch(Operation op)
    {
        Mapper revMapper = this.getReverseMapper();
        Operation result = null;
        if (!this.isToMany || revMapper.mapUsesUniqueIndex())
        {
            result = op.zFlipToOneMapper(revMapper);
        }
        if (result == null)
        {
            result = new MappedOperation(revMapper, op);
        }
        return result;
    }

    public Mapper getParentMapper()
    {
        return null;
    }

    public Mapper link(Mapper other)
    {
        return new LinkedMapper(this, other);
    }

    public boolean isFullyCachedIgnoringLeft()
    {
        return this.getFromPortal().getCache().isFullCache();
    }

    public boolean isFullyCached()
    {
        return this.isFullyCachedIgnoringLeft() && this.getResultPortal().getCache().isFullCache();
    }

    public List getAllPossibleResultObjectsForFullCache()
    {
        if (this.getResultPortal().getCache().isDated()) return null;
        return this.getResultPortal().getCache().getAll();
    }

    public List filterLeftObjectList(List objects)
    {
        return objects;
    }

    public Mapper createMapperForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject, int chainPosition)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public static boolean isOperationEligibleForMapperCombo(Operation op)
    {
        return (op instanceof AtomicEqualityOperation) || (op instanceof MultiEqualityOperation && !((MultiEqualityOperation)op).hasInClause());
    }

    public void appendName(StringBuilder stringBuilder)
    {
        if (name == null)
        {
            this.appendSyntheticName(stringBuilder);
        }
        else
        {
            stringBuilder.append(this.name);
            this.appendFilters(stringBuilder);
        }
    }

    public String getRelationshipPath()
    {
        return this.getRawName();
    }

    public List<String> getRelationshipPathAsList()
    {
        return ListFactory.create(this.getRawName());
    }

    protected void appendFilters(StringBuilder stringBuilder)
    {
        // nothing to do unless it's a FilteredMapper
    }

    protected String getRawName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
}
