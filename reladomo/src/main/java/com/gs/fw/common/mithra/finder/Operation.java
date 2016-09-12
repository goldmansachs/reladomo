
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
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.InternalList;

public interface Operation extends com.gs.fw.finder.Operation, Serializable
{

    public abstract boolean usesUniqueIndex();

    public abstract boolean usesNonUniqueIndex();

    public abstract boolean usesImmutableUniqueIndex();

    public void addDependentPortalsToSet(Set set);

    public void addDepenedentAttributesToSet(Set set);

    public boolean isJoinedWith(MithraObjectPortal portal);

    /**
     * applies the operation to the entire cache (possibly caches from different objects if relationships are traversed)
     */
    public abstract List applyOperationToFullCache();

    /**
     * applies the operation to the partial cache, expecting to find everything it needs; if the
     * operation is in doubt, it will return null.
     *
     * @return null if operation cannot be fulfilled against the partial cache
     */
    public abstract List applyOperationToPartialCache();

    /**
     * applies the operation to a pre-determined list (usually from another operation)
     * modifies the list directly. Returns null if operation cannot be applied.
     */
    public abstract List applyOperation(List list);


    public abstract Operation and(com.gs.fw.finder.Operation op);

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public abstract Operation zCombinedAnd(Operation op);

    public abstract Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op);

    public abstract Operation zCombinedAndWithMapped(MappedOperation op);

    public abstract Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op);

    public abstract Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op);

    public abstract Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op);

    public abstract Operation zCombinedAndWithAtomicLessThan(LessThanOperation op);

    public abstract Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op);

    public abstract Operation zCombinedAndWithIn(InOperation op);

    public abstract Operation or(com.gs.fw.finder.Operation op);

    public abstract MithraObjectPortal getResultObjectPortal();

    public void generateSql(SqlQuery query);

    public int getClauseCount(SqlQuery query);

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker);

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack);

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator);

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations);

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality);

    public boolean zHasAsOfOperation();

    public Operation zFlipToOneMapper(Mapper mapper);

    public Operation zFindEquality(TimestampAttribute attr);

    public String zGetResultClassName();

    public boolean zIsNone();

    public void zAddAllLeftAttributes(Set<Attribute> result);

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject);

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute);

    /** returns true if the object passes the operation's criteria. Can return null if we don't have enough information
     * determine the outcome (e.g. data not loaded into memory in a partial cache).
     * @param o object to match
     */
    public Boolean matches(Object o);

    public boolean zPrefersBulkMatching();

    public int zEstimateReturnSize();

    public int zEstimateMaxReturnSize();

    public boolean zIsEstimatable();

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator);

    public boolean zHazTriangleJoins();

    public void zToString(ToStringContext toStringContext);

    public EqualityOperation zExtractEqualityOperations();

    public boolean zContainsMappedOperation();

    public boolean zHasParallelApply();
}
