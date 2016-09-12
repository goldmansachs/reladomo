
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;

public class None implements Operation
{

    private Attribute attr;

    public None(Attribute attr)
    {
        this.attr = attr;
    }

    public boolean usesUniqueIndex()
    {
        return true;
    }

    public boolean usesImmutableUniqueIndex()
    {
        return true;
    }

    public boolean usesNonUniqueIndex()
    {
        return false;
    }

    public int zEstimateReturnSize()
    {
        return 0;
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        return 0;
    }

    @Override
    public boolean zIsEstimatable()
    {
        return true;
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
        toStringContext.append("NONE!");
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
        return new FastList(0);
    }

    public List applyOperationToPartialCache()
    {
        // we know the answer regardless of cache state
        return applyOperationToFullCache();
    }

    public List applyOperation(List list)
    {
        return new FastList(0);
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return (Operation) op;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        return this;
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        return op.zGetResultClassName() == this.zGetResultClassName() ? this : null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return this.zCombinedAnd(op);
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        // todo: rezaem: should this try and do something else?
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        return op.zGetResultClassName() == this.zGetResultClassName() ? this : null;
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
        return this.attr.getOwnerPortal();
    }

    public String zGetResultClassName()
    {
        return this.attr.zGetTopOwnerClassName();
    }

    public void generateSql(SqlQuery query)
    {
        query.appendWhereClause("1=2");
    }

    public int getClauseCount(SqlQuery query)
    {
        return 1;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        // nothing to do
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            return new AndOperation(MultiEqualityOperation.createEqOperation(asOfEqOperations), this);
        }
        return null;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return this;
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return this;
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        //nothing to do
    }

    public boolean zHasAsOfOperation()
    {
        return true;
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        return new None(mapper.getAnyLeftAttribute());
    }

    public Operation zFindEquality(TimestampAttribute attr)
    {
        return null;
    }

    public boolean zIsNone()
    {
        return true;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        //nothing to do
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        return new None(attributeMap.values().iterator().next());
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public Boolean matches(Object o)
    {
        return Boolean.FALSE;
    }

    public boolean zPrefersBulkMatching()
    {
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        None none = (None) o;

        return attr.getOwnerPortal().equals(none.attr.getOwnerPortal());
    }

    @Override
    public int hashCode()
    {
        return attr.zGetTopOwnerClassName().hashCode() ^ 0xFAE324A9;
    }

    @Override
    public String toString()
    {
        return "NONE!";
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
