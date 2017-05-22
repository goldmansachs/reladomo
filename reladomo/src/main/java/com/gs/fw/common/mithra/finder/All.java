
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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;

public class All implements Operation, Serializable
{

    private Attribute attr;

    public All(Attribute attr)
    {
        this.attr = attr;
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
        Cache cache = attr.getOwnerPortal().getCache();
        return cache == null ? 0 : cache.estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        Cache cache = attr.getOwnerPortal().getCache();
        return cache == null ? 0 : cache.estimateQuerySize();
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
        return false;
    }

    public void zToString(ToStringContext toStringContext)
    {
        //nothing to do
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    public void addDependentPortalsToSet(Set set)
    {
        set.add(attr.getOwnerPortal());
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        // nothing to do
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return false;
    }

    public List applyOperationToFullCache()
    {
        return this.attr.getOwnerPortal().getCache().getAll();
    }

    public List applyOperationToPartialCache()
    {
        // we can never be sure we have everything
        return null;
    }

    public List applyOperation(List list)
    {
        return list;
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return this;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        return (Operation) op;
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        return op.zGetResultClassName() == this.zGetResultClassName() ? op : null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return attr.getOwnerPortal();
    }

    public String zGetResultClassName()
    {
        return attr.zGetTopOwnerClassName();
    }

    public boolean zIsNone()
    {
        return false;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        //nothing to do
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        return null;
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public void generateSql(SqlQuery query)
    {
        // nothing to do
    }

    public int getClauseCount(SqlQuery query)
    {
        return 0;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        checker.registerAsOfAttributes(this.getResultObjectPortal().getFinder().getAsOfAttributes());
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        Operation result = null;
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            result = MultiEqualityOperation.createEqOperation(asOfEqOperations);
        }
        return result;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return transitivePropagator.constructAnd(insertPosition, this, toInsert);
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return MultiEqualityOperation.createEqOperation(asOfEqOperations);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final All all = (All) o;

        return attr.getOwnerPortal().equals(all.attr.getOwnerPortal());
    }

    public int hashCode()
    {
        return attr.zGetTopOwnerClassName().hashCode();
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        //nothing to do
    }

    public boolean zHasAsOfOperation()
    {
        return !this.getResultObjectPortal().getClassMetaData().isDated();
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
        return Boolean.TRUE;
    }

    public boolean zPrefersBulkMatching()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return "all of "+this.getResultObjectPortal().getBusinessClassName();
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
}
