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

package com.gs.fw.common.mithra.tempobject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchRequiresExactSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;


public class TupleExistsOperation implements Operation
{
    private Mapper mapper;

    public TupleExistsOperation(Mapper mapper)
    {
        this.mapper = mapper;
    }

    public void addDependentPortalsToSet(Set set)
    {
        mapper.addDepenedentPortalsToSet(set);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        mapper.addDepenedentAttributesToSet(set);
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        throw new RuntimeException("not implemented");
    }

    public List applyOperation(List list)
    {
        throw new RuntimeException("not implemented");
    }

    public List applyOperationToFullCache()
    {
        return null;
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    public void generateSql(SqlQuery query)
    {
        mapper.generateSql(query);
        mapper.popMappers(query);
    }

    public int getClauseCount(SqlQuery query)
    {
        return mapper.getClauseCount(query);
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
        throw new RuntimeException("not implemented");
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return this.mapper.isJoinedWith(portal);
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        throw new RuntimeException("not implemented");
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
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
        return portal.isFullyCached() && !portal.isForTempObject();
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        // nothing to do
    }

    public boolean zHazTriangleJoins()
    {
        return this.mapper.hasTriangleJoins();
    }

    public void zToString(ToStringContext toStringContext)
    {
        toStringContext.pushMapper(mapper);
        toStringContext.append(toStringContext.getCurrentAttributePrefix()).append("exists");
        toStringContext.popMapper();
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    public boolean usesUniqueIndex()
    {
        return false;
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return null;
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
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

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return null;
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zHasAsOfOperation()
    {
        return false;
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public Boolean matches(Object o)
    {
        throw new RuntimeException("not implemented");
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
        return false;
    }

    @Override
    public boolean zHasParallelApply()
    {
        return false;
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
