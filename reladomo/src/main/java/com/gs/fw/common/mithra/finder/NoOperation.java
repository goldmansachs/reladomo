
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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;

public class NoOperation implements Operation
{
    private static NoOperation instance = new NoOperation();

    private NoOperation()
    {
    }

    public static NoOperation instance()
    {
        return instance;
    }

    public boolean usesUniqueIndex()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public boolean usesImmutableUniqueIndex()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public boolean usesNonUniqueIndex()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public int zEstimateReturnSize()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    @Override
    public boolean zIsEstimatable()
    {
        return false;
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
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public void addDependentPortalsToSet(Set set)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return false;
    }

    public List applyOperationToFullCache()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public List applyOperationToPartialCache()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public List applyOperation(List list)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return (Operation) op;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        return (Operation) op;
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return op;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public String zGetResultClassName()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public boolean zIsNone()
    {
        return false;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public void generateSql(SqlQuery query)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public int getClauseCount(SqlQuery query)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return transitivePropagator.constructAnd(insertPosition, this, toInsert);
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public boolean zHasAsOfOperation()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Operation zFindEquality(TimestampAttribute attr)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public void generateSqlForAggregation(SqlQuery sqlQuery, Operation listOperationForAggregation)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public Boolean matches(Object o)
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public boolean zPrefersBulkMatching()
    {
        throw new UnsupportedOperationException("this method call is not allowed for NoOperation");
    }

    public String toString()
    {
        return "NoOperation";
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
