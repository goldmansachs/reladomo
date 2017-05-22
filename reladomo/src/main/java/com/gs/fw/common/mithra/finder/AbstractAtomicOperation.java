
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

import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.DoUntilProcedure;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.util.MithraCpuBoundThreadPool;
import com.gs.fw.common.mithra.util.MithraFastList;

public abstract class AbstractAtomicOperation implements AtomicOperation, SqlParameterSetter, Cloneable
{
    private Attribute attribute;
    private transient IndexReference indexRef;

    protected AbstractAtomicOperation()
    {
        // for externalizable
    }

    protected AbstractAtomicOperation(Attribute attribute)
    {
        this.attribute = attribute;
    }

    protected Cache getCache()
    {
        return this.getResultObjectPortal().getCache();
    }

    public Attribute getAttribute()
    {
        return attribute;
    }

    protected void setAttribute(Attribute attribute)
    {
        this.attribute = attribute;
    }

    public Operation susbtituteOtherAttribute(Attribute other)
    {
        if (other instanceof AsOfAttribute)
        {
            return null;
        }
        try
        {
            AbstractAtomicOperation op = (AbstractAtomicOperation) this.clone();
            op.attribute = other;
            op.indexRef = null;
            return op;
        }
        catch (CloneNotSupportedException e)
        {
            // impossible to get here.
        }
        return null;
    }

    protected abstract List getByIndex();

    public Boolean matches(Object o)
    {
        try
        {
            return this.matchesWithoutDeleteCheck(o);
        }
        catch (MithraDeletedException e)
        {
            // this is a rare exception, so we don't check for deleted up front
            return false;
        }
    }

    protected abstract Boolean matchesWithoutDeleteCheck(Object o);

    public boolean zPrefersBulkMatching()
    {
        return false;
    }

    public List applyOperationToFullCache()
    {
        if (this.isIndexed())
        {
            return getByIndex();
        }
        else
        {
            if (this.getResultObjectPortal().getClassMetaData().isDated())
            {
                return null;
            }
            ForAllMatchProcedure procedure = new ForAllMatchProcedure();
            this.getCache().forAll(procedure);
            return procedure.getResult();
        }
    }

    protected static MithraFastList allocateResultAndCopyHead(List list, int firstMismatchPosition)
    {
        int listSize = list.size();
        int listCapacity;
        if (firstMismatchPosition < 30)
        {
            listCapacity = Math.min(listSize - 1, 10);
        }
        else
        {
            listCapacity = listSize / (firstMismatchPosition -1);
        }
        MithraFastList result = new MithraFastList(listCapacity);
        for(int i=0; i < firstMismatchPosition; i++)
        {
            result.add(list.get(i));
        }
        return result;
    }

    public List applyOperation(List list)
    {
        int listSize = list.size();
        // alternatively, we could retrieve by index and intersect
        // todo: rezaem: this approach may be faster for simple attributes, but may be more expensive for String, Date, etc. ??
        if (MithraCpuBoundThreadPool.isParallelizable(list.size()))
        {
            return applyToLargeResultsInParallel(list);
        }
        MithraFastList result = null;
        for (int i = 0; i < listSize; i++)
        {
            Object item = list.get(i);
            boolean matches = this.matches(item);
            result = copyToResultAfterFirstMismatch(list, listSize, result, i, item, matches);
        }
        if (result == null) return list;
        return result;
    }

    private List applyToLargeResultsInParallel(List result)
    {
        return AndOperation.applyAtomicOperationsInParallel(result, new Operation[] { this });
    }

    protected static MithraFastList copyToResultAfterFirstMismatch(List list, int listSize, MithraFastList result, int currentPosition, Object item, boolean matches)
    {
        if (!matches)
        {
            if (result == null)
            {
                result = allocateResultAndCopyHead(list, currentPosition);
            }
        }
        else if (result != null)
        {
            result.addWithSizePrediction(item, currentPosition, listSize);
        }
        return result;
    }

    protected boolean isIndexed()
    {
        int indexRef = this.getIndexRef();
        return indexRef > 0 && indexRef != IndexReference.AS_OF_PROXY_INDEX_ID && this.getCache().isInitialized(indexRef);
    }

    public boolean usesUniqueIndex()
    {
        return this.isIndexed() && this.getCache().isUnique(this.indexRef.indexReference);
    }

    public boolean usesImmutableUniqueIndex()
    {
        return this.isIndexed() && this.getCache().isUniqueAndImmutable(this.indexRef.indexReference);
    }

    public boolean usesNonUniqueIndex()
    {
        if (this.isIndexed())
        {
            return !this.getCache().isUnique(this.indexRef.indexReference);
        }
        return false;
    }

    public int zEstimateReturnSize()
    {
        if (this.isIndexed())
        {
            return this.getCache().getAverageReturnSize(this.indexRef.indexReference, 1);
        }
        else return this.getCache().estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        if (this.isIndexed())
        {
            return this.getCache().getAverageReturnSize(this.indexRef.indexReference, 1);
        }
        else return this.getCache().estimateQuerySize();
    }

    @Override
    public boolean zIsEstimatable()
    {
        MithraObjectPortal portal = this.getResultObjectPortal();
        return portal.isFullyCached() && !portal.isForTempObject();
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        transitivePropagator.addAtomicOperation(this);
    }

    public boolean zHazTriangleJoins()
    {
        return false;
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        return new AndOperation(this, op);
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return OrOperation.or(this, op);
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return op.zCombinedAndWithAtomic(this);
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return this.getAttribute().getOwnerPortal();
    }

    public String zGetResultClassName()
    {
        return this.getAttribute().zGetTopOwnerClassName();
    }

    public boolean zIsNone()
    {
        return false;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        result.add(this.getAttribute());
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        Attribute newAttribute = attributeMap.get(this.attribute);
        if (newAttribute != null)
        {
            return susbtituteOtherAttribute(newAttribute);
        }
        return null;
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public int getClauseCount(SqlQuery query)
    {
        if (this.getAttribute().isSourceAttribute()) return 0;
        return 1;
    }

    public int getIndexRef()
    {
        Cache cache = this.getCache();
        if (this.indexRef == null || !this.indexRef.isForCache(cache))
        {
            this.indexRef = cache.getIndexRef(this.attribute);
        }
        return indexRef.indexReference;
    }

    public void addDependentPortalsToSet(Set set)
    {
        this.getAttribute().zAddDependentPortalsToSet(set);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.getAttribute().zAddDepenedentAttributesToSet(set);
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return false;
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

    protected class ForAllMatchProcedure implements DoUntilProcedure
    {
        private MithraFastList result = new MithraFastList();

        public boolean execute(Object o)
        {
            if (matches(o)) result.add(o);
            return false;
        }

        public MithraFastList getResult()
        {
            return result;
        }
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        checker.registerAsOfAttributes(this.getAttribute().getAsOfAttributes());
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            Operation result = this.and(asOfEqOperations[0]);
            for(int i=1;i<asOfEqOperations.length;i++)
            {
                result = result.and(asOfEqOperations[i]);
            }
            return result;
        }
        return null;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return transitivePropagator.constructAnd(insertPosition, this, toInsert);
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

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    public boolean equals(Object obj)
    {
        throw new RuntimeException("operations must override equals");
    }

    public int hashCode()
    {
        throw new RuntimeException("operations must override hashcode");
    }

    @Override
    public String toString()
    {
        return ToStringContext.createAndToString(this);
    }
}
