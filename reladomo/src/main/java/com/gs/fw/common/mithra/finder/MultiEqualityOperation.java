
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

import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.PositionBasedOperationParameterExtractor;
import com.gs.fw.common.mithra.finder.asofop.AsOfEdgePointOperation;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;
import com.gs.fw.common.mithra.finder.sqcache.SuperMatchSmr;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.*;
import com.gs.reladomo.metadata.PrivateReladomoClassMetaData;

public class MultiEqualityOperation implements Operation, EqualityOperation
{

    private AtomicOperation[] atomicOperations;
    private volatile boolean calculatedFalseHood = false;
    private transient IndexReference bestIndexRef;
    //if the same attribute must have 2 values, then we get:                                                                               
    private boolean isClearlyFalse = false;

    public MultiEqualityOperation(AtomicOperation op1, AtomicOperation op2)
    {
        this.atomicOperations = new AtomicOperation[2];
        this.atomicOperations[0] = op1;
        this.atomicOperations[1] = op2;
    }

    public MultiEqualityOperation(AtomicOperation op1, AtomicOperation[] op2)
    {
        this.atomicOperations = new AtomicOperation[1+op2.length];
        this.atomicOperations[0] = op1;
        for(int i=0;i<op2.length;i++)
        {
            this.atomicOperations[i+1] = op2[i];
        }
    }

    protected MultiEqualityOperation(AtomicOperation[] atomicOperations)
    {
        this.atomicOperations = atomicOperations;
    }

    protected MultiEqualityOperation(AtomicOperation[] toCopy, AtomicOperation toAdd)
    {
        atomicOperations = new AtomicOperation[toCopy.length + 1];
        System.arraycopy(toCopy, 0, atomicOperations, 0, toCopy.length);
        atomicOperations[toCopy.length] = toAdd;
    }

    protected MultiEqualityOperation(AtomicOperation[] toCopyOne, AtomicOperation[] toCopyTwo)
    {
        atomicOperations = new AtomicOperation[toCopyOne.length + toCopyTwo.length];
        System.arraycopy(toCopyOne, 0, atomicOperations, 0, toCopyOne.length);
        System.arraycopy(toCopyTwo, 0, atomicOperations, toCopyOne.length, toCopyTwo.length);
    }

    private static AtomicOperation[] removeDuplicates(AtomicOperation[] ops)
    {
        int duplicateCount = 0;
        // WARNING: this is a double nested loop (O(n^2)). It works faster for up to 10 operations than
        // a set implementation (O(n))
        for (int i = 0; i < ops.length; i++)
        {
            AtomicOperation aop = ops[i];
            if (aop instanceof InOperation) continue;
            if (aop instanceof AsOfEdgePointOperation) continue;
            Attribute attribute = aop.getAttribute();
            for (int j = 0; j < i; j++)
            {
                AtomicOperation first = ops[j];

                if (first != null && !(first instanceof InOperation) && !(first instanceof AsOfEdgePointOperation) && first.getAttribute().equals(attribute))
                {
                    if (first.equals(aop))
                    {
                        ops[i] = null;// mark to remove duplicate
                        duplicateCount++;
                    }
                    else
                    {
                        return null; // this is not just duplicate, but clearly false;
                    }
                }

            }
        }
        if (duplicateCount > 0)
        {
            AtomicOperation[] newOps = new AtomicOperation[ops.length - duplicateCount];
            int count = 0;
            for (int i = 0; i < ops.length; i++)
            {
                if (ops[i] != null)
                {
                    newOps[count] = ops[i];
                    count++;
                }
            }
            ops = newOps;
        }
        return ops;
    }

    private void calculateFalseHood()
    {
        if (!calculatedFalseHood)
        {
            synchronized (this)
            {
                AtomicOperation[] newOps = removeDuplicates(this.atomicOperations);
                if (newOps == null)
                {
                    this.isClearlyFalse = true;
                }
                else
                {
                    this.atomicOperations = newOps;
                }
            }
            calculatedFalseHood = true;
        }
    }

    public boolean operatesOnAttribute(Attribute attribute)
    {
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            if (this.atomicOperations[i].getAttribute().equals(attribute)) return true;
        }
        return false;
    }

    public void generateSql(SqlQuery query)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            AtomicOperation op = atomicOperations[i];
            query.beginAnd();
            op.generateSql(query);
            query.endAnd();
        }
    }

    public int getClauseCount(SqlQuery query)
    {
        int count = 0;
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation op = atomicOperations[i];
            count += op.getClauseCount(query);
        }
        return count;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation op = atomicOperations[i];
            op.registerAsOfAttributesAndOperations(checker);
        }
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            return new MultiEqualityOperation(this.atomicOperations, asOfEqOperations);
        }
        return null;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return transitivePropagator.constructAnd(insertPosition, this, toInsert);
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return new MultiEqualityOperation(this.atomicOperations, asOfEqOperations);
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            AtomicOperation op = atomicOperations[i];
            op.registerOperation(extractor, registerEquality);
        }
    }

    public boolean zHasAsOfOperation()
    {
        AsOfAttribute[] asOfAttributes = ((PrivateReladomoClassMetaData)this.getResultObjectPortal().getClassMetaData()).getCachedAsOfAttributes();
        if (asOfAttributes == null)
        {
            return true;
        }
        
        int count = 0;
        for (int i = 0; i < atomicOperations.length; i++)
        {
            if (atomicOperations[i].getAttribute().isAsOfAttribute())
            {
                count++;
            }
        }
        return asOfAttributes.length == count;
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        return null;
    }

    public Cache getCache()
    {
        return this.getResultObjectPortal().getCache();
    }

    private int getBestIndexRef(Cache cache)
    {
        if (this.bestIndexRef == null || !this.bestIndexRef.isForCache(cache))
        {
            this.calculateFalseHood();
            this.bestIndexRef = cache.getBestIndexReference(new AttributesAsList());
        }
        return this.bestIndexRef.indexReference;
    }

    public boolean usesUniqueIndex()
    {
        Cache c = this.getCache();
        int indexRef = this.getBestIndexRef(c);
        return indexRef > 0 && c.isUnique(indexRef) && !this.hasEdgePointOperation();
    }

    public boolean usesImmutableUniqueIndex()
    {
        Cache c = this.getCache();
        int indexRef = this.getBestIndexRef(c);
        return indexRef > 0 && c.isUniqueAndImmutable(indexRef) && !this.hasEdgePointOperation() && !hasInOperation(indexRef, c);
    }

    private boolean hasInOperation(int indexRef, Cache cache)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            AtomicOperation op = atomicOperations[i];
            if (op instanceof InOperation)
            {
                Attribute[] bestAttributes = cache.getIndexAttributes(indexRef);
                for(int j=0;j<bestAttributes.length;j++)
                {
                    if (bestAttributes[j].equals(op.getAttribute())) return true;
                }
            }
        }
        return false;
    }

    public boolean usesNonUniqueIndex()
    {
        Cache c = this.getCache();
        int indexRef = this.getBestIndexRef(c);
        return indexRef > 0 && indexRef != IndexReference.AS_OF_PROXY_INDEX_ID && !c.isUnique(indexRef);
    }

    public int zEstimateReturnSize()
    {
        Cache cache = this.getCache();
        int bestIndexRef = this.getBestIndexRef(cache);
        if (bestIndexRef > 0)
        {
            long items = 1;
            for (int i = 0; i < atomicOperations.length; i++)
            {
                Operation aeo = this.atomicOperations[i];
                if (aeo instanceof InOperation)
                {
                    items = items * (((InOperation)aeo).getSetSize());
                }
            }
            int intItems = (int) items;
            if (intItems < 0) intItems = Integer.MAX_VALUE;
            return (int) cache.getAverageReturnSize(bestIndexRef, intItems);
        }
        return cache.estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        Cache cache = this.getCache();
        int bestIndexRef = this.getBestIndexRef(cache);
        if (bestIndexRef > 0)
        {
            long items = 1;
            for (int i = 0; i < atomicOperations.length; i++)
            {
                Operation aeo = this.atomicOperations[i];
                if (aeo instanceof InOperation)
                {
                    items = items * (((InOperation)aeo).getSetSize());
                }
            }
            int intItems = (int) items;
            if (intItems < 0) intItems = Integer.MAX_VALUE;
            return (int) cache.getMaxReturnSize(bestIndexRef, intItems);
        }
        return cache.estimateQuerySize();
    }

    @Override
    public boolean zIsEstimatable()
    {
        MithraObjectPortal portal = this.getResultObjectPortal();
        return portal.isFullyCached() && !portal.isForTempObject();
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation op = atomicOperations[i];
            op.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
        }
    }

    public boolean zHazTriangleJoins()
    {
        return false;
    }

    public void zToString(ToStringContext toStringContext)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            toStringContext.beginAnd();
            Operation op = atomicOperations[i];
            op.zToString(toStringContext);
            toStringContext.endAnd();
        }
    }

    public void addDependentPortalsToSet(Set set)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation op = atomicOperations[i];
            op.addDependentPortalsToSet(set);
        }
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation op = atomicOperations[i];
            op.addDepenedentAttributesToSet(set);
        }
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return false;
    }

    protected int getOperationPosition(Attribute a)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            AtomicOperation op = atomicOperations[i];
            if (op.getAttribute().equals(a))
            {
                return i;
            }
        }
        return -1;
    }

    protected int getOperationPosition(Attribute a, int bestGuess)
    {
        if (atomicOperations[bestGuess].getAttribute().equals(a))
        {
            return bestGuess;
        }
        return getOperationPosition(a);
    }

    public List applyOperationToFullCache()
    {
        return this.applyOperation(false);
    }

    public List applyOperationToPartialCache()
    {
        if (this.usesUniqueIndex())
        {
            return this.applyOperation(true);
        }
        return null;
    }

    public List applyOperation(boolean returnNullIfNotFoundInIndex)
    {
        if (this.isClearlyFalse())
        {
            return new MithraFastList(0);
        }
        if (returnNullIfNotFoundInIndex && hasEdgePointOperation())
        {
            return null;
        }
        Cache cache = this.getCache();
        int bestIndexRef = this.getBestIndexRef(cache);
        if (bestIndexRef > 0)
        {
            Attribute[] bestAttributes = cache.getIndexAttributes(bestIndexRef);
            Extractor[] extractors = new Extractor[bestAttributes.length];
            int extractorPos = 0;

            int numberOfInOperations = 0;
            int[] inOperationPosition = null;
            boolean[] applied = new boolean[this.atomicOperations.length];
            for (int i = 0; i < bestAttributes.length; i++)
            {
                int opIndex = this.getOperationPosition(bestAttributes[i]);
                OperationWithParameterExtractor aeo = (OperationWithParameterExtractor) this.atomicOperations[opIndex];
                applied[opIndex] = true;
                if (aeo instanceof InOperation)
                {
                    if (numberOfInOperations == 0)
                    {
                        inOperationPosition = new int[bestAttributes.length];
                    }
                    inOperationPosition[numberOfInOperations] = i;
                    numberOfInOperations++;
                }
                extractors[extractorPos] = aeo.getParameterExtractor();
                extractorPos++;
            }
            List result;
            if (numberOfInOperations > 0)
            {
                result = getResultForInOperations(cache, bestIndexRef, extractors,
                        numberOfInOperations, inOperationPosition, returnNullIfNotFoundInIndex);
            }
            else
            {
                result = cache.get(bestIndexRef, this, extractors, true);
                if (returnNullIfNotFoundInIndex && result.size() == 0)
                {
                    result = null;
                }

            }
            if (result == null)
            {
                return null;
            }

            if (bestAttributes.length < this.atomicOperations.length && result.size() > 0)
            {
                result = filterResults(applied, result);
            }
            return result;
        }
        else
        {
            if (cache.isDated()) return null;
            return this.applyOperation(cache.getAll());
        }
    }

    public void filterResultsInPlace(boolean[] applied, List list)
    {
        int currentFilledIndex = 0;
        for (int i = 0; i < list.size(); i++)
        {
            Object item = list.get(i);
            if (matchesUnapplied(applied, item))
            {
                // keep it
                if (currentFilledIndex != i)
                {
                    list.set(currentFilledIndex, item);
                }
                currentFilledIndex++;
            }
        }
        this.resetTheEnd(list, currentFilledIndex);
    }

    private void resetTheEnd(List list, final int newCurrentFilledIndex)
    {
        int initialSize = list.size();
        for (int i = newCurrentFilledIndex; i < initialSize; i++)
        {
            list.remove(list.size() - 1);
        }
    }

    private List filterResults(boolean[] applied, List result)
    {
        if (result instanceof FastList)
        {
            filterResultsInPlace(applied, result);
            return result;
        }
        else if (result instanceof MithraCompositeList)
        {
            parallelFilter(applied, (MithraCompositeList)result);
            return result;
        }
        else
        {
            MithraFastList newResult = new MithraFastList(result.size());
            for(int i=0;i<result.size();i++)
            {
                Object o = result.get(i);
                if (matchesUnapplied(applied, o)) newResult.add(o);
            }
            result = newResult;
            return result;
        }
    }

    private void parallelFilter(final boolean[] applied, MithraCompositeList mithraCompositeList)
    {
        final FastList<FastList> lists = mithraCompositeList.getLists();
        CpuBoundTask[] tasks = new CpuBoundTask[lists.size()];
        for(int i=0;i<lists.size();i++)
        {
            final FastList toFilter = lists.get(i);
            tasks[i] = new CpuBoundTask()
            {
                @Override
                public void execute()
                {
                    filterResultsInPlace(applied, toFilter);
                }
            };
        }
        new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
    }

    private boolean matchesUnapplied(boolean[] applied, Object o)
    {
        boolean matched = true;
        for (int j = 0; j < this.atomicOperations.length && matched; j++)
        {
            AtomicOperation aeo = atomicOperations[j];
            if (!applied[j])
            {
                matched = aeo.matches(o);
            }
        }
        return matched;
    }

    private boolean hasEdgePointOperation()
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            if (atomicOperations[i] instanceof AsOfEdgePointOperation) return true;
        }
        return false;
    }

    private List getResultForInOperations(Cache cache, int bestIndexRef, Extractor[] extractors,
                                          int numberOfInOperations, int[] inOperationPosition, boolean returnNullIfNotFoundInIndex)
    {
        List finalResult = new MithraFastList();
        int[] max = new int[numberOfInOperations];
        for (int i = 0; i < numberOfInOperations; i++)
        {
            PositionBasedOperationParameterExtractor extractor = (PositionBasedOperationParameterExtractor) extractors[inOperationPosition[i]];
            max[i] = extractor.getSetSize();
        }
        NestedCounter counter = new NestedCounter(max);
        while (!counter.isDone())
        {
            for (int i = 0; i < numberOfInOperations; i++)
            {
                PositionBasedOperationParameterExtractor extractor = (PositionBasedOperationParameterExtractor) extractors[inOperationPosition[i]];
                extractor.setPosition(counter.getCounterAt(i));
            }
            List result = cache.get(bestIndexRef, this, extractors, true);
            if (returnNullIfNotFoundInIndex && result.size() == 0)
            {
                return null;
            }
            finalResult.addAll(result);
            counter.increment();
        }
        return finalResult;
    }

    private boolean isClearlyFalse()
    {
        this.calculateFalseHood();
        return this.isClearlyFalse;
    }

    public List applyOperation(List list)
    {
        if (this.isClearlyFalse())
        {
            return new MithraFastList(0);
        }
        MithraFastList result = null;
        if (this.atomicOperations.length > 0)
        {
            if (MithraCpuBoundThreadPool.isParallelizable(list.size()))
            {
                return applyToLargeResultsInParallel(list);
            }
            int listSize = list.size();
            for(int j=0;j< listSize;j++)
            {
                Object item = list.get(j);
                boolean matches = true;
                for (int i = 0; matches && i < atomicOperations.length; i++)
                {
                    matches = atomicOperations[i].matches(item);
                }
                result = AbstractAtomicOperation.copyToResultAfterFirstMismatch(list, listSize, result, j, item, matches);
            }
        }
        if (result == null) // everything matched
        {
            return list;
        }
        return result;
    }

    private List applyToLargeResultsInParallel(List list)
    {
        return AndOperation.applyAtomicOperationsInParallel(list, atomicOperations);
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return OrOperation.or(this, op);
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        Operation result = ((Operation) op).zCombinedAndWithMultiEquality(this);
        if (result == null)
        {
            result = new AndOperation(this, op);
        }
        return result;
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return this.atomicOperations[0].getResultObjectPortal();
    }

    public String zGetResultClassName()
    {
        return this.atomicOperations[0].zGetResultClassName();
    }

    public boolean zIsNone()
    {
        return this.isClearlyFalse();  
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        for(AtomicOperation o: this.atomicOperations) o.zAddAllLeftAttributes(result);
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        InternalList newOps = new InternalList(this.atomicOperations.length);
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation newOp = atomicOperations[i].zSubstituteForTempJoin(attributeMap, prototypeObject);
            if (newOp != null) newOps.add(newOp);
        }
        if (newOps.size() == 0) return null;
        if (newOps.size() == 1) return (Operation) newOps.get(0);
        AtomicOperation[] newOpArray = new AtomicOperation[newOps.size()];
        newOps.toArray(newOpArray);
        return new MultiEqualityOperation(newOpArray);
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        for (int i = 0; i < atomicOperations.length; i++)
        {
            Operation op = atomicOperations[i];
            Operation result = op.zGetAsOfOp(asOfAttribute);
            if (result != null) return result;
        }
        return null;
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithMultiEquality(this);
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return new MultiEqualityOperation(this.atomicOperations, op);
        }
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return op.zCombinedAndWithMultiEquality(this);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            AtomicOperation[] newOperations = new AtomicOperation[this.atomicOperations.length + op.atomicOperations.length];
            System.arraycopy(op.atomicOperations, 0, newOperations, 0, op.atomicOperations.length);
            System.arraycopy(this.atomicOperations, 0, newOperations, op.atomicOperations.length, this.atomicOperations.length);
            newOperations = removeDuplicates(newOperations);
            if (newOperations == null)
            {
                return new None(this.atomicOperations[0].getAttribute());
            }
            return new MultiEqualityOperation(newOperations);
        }
        return null;
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            for (int i = 0; i < this.atomicOperations.length; i++)
            {
                AtomicOperation ao = atomicOperations[i];
                if (ao instanceof AtomicEqualityOperation)
                {
                    AtomicEqualityOperation aeo = (AtomicEqualityOperation) ao;
                    Operation combined = op.zCombinedAndWithAtomicEquality(aeo);
                    if (combined != null)
                    {
                        if (combined.zIsNone())
                        {
                            return combined;
                        }
                        else
                        {
                            return this;
                        }
                    }
                }
            }
        }
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return new MultiEqualityOperation(this.atomicOperations, op);
        }
        return null;
    }

    public Operation getSusbstitutedEquality(Attribute original, Attribute newAttribute)
    {
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            AtomicOperation aeo = atomicOperations[i];
            if (aeo instanceof AtomicEqualityOperation && aeo.getAttribute().equals(original))
            {
                return aeo.susbtituteOtherAttribute(newAttribute);
            }
        }
        return null;
    }

    public int hashCode()
    {
        int hashcode = 0;
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            AtomicOperation aeo = atomicOperations[i];
            if (aeo instanceof AtomicEqualityOperation)
            {
                hashcode ^= HashUtil.combineHashes(aeo.getAttribute().hashCode(), ((AtomicEqualityOperation)aeo).getParameterHashCode());
            }
            else
            {
                hashcode ^= aeo.hashCode();
            }
        }
        return hashcode;
    }

    private boolean containsOperation(Operation op)
    {
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            if (atomicOperations[i].equals(op))
            {
                return true;
            }
        }
        return false;
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof MultiEqualityOperation)
        {
            MultiEqualityOperation other = (MultiEqualityOperation) obj;
            if (other.atomicOperations.length != this.atomicOperations.length)
            {
                return false;
            }
            for (int i = 0; i < other.atomicOperations.length; i++)
            {
                if (!this.containsOperation(other.atomicOperations[i]))
                {
                    return false;
                }
            }
            return true;
        }
        else if (obj instanceof RelationshipMultiEqualityOperation)
        {
            RelationshipMultiEqualityOperation other = (RelationshipMultiEqualityOperation) obj;
            return equalsExtractorBased(other);
        }
        return false;
    }

    public boolean equalsExtractorBased(RelationshipMultiEqualityOperation other)
    {
        List<Attribute> leftAttributes = other.getLeftAttributes();
        Object data = other.getData();
        List<Extractor> extractors = other.getExtractors();
        if (leftAttributes.size() != this.atomicOperations.length)
        {
            return false;
        }
        for(int i=0;i<leftAttributes.size();i++)
        {
            int position = this.getOperationPosition(leftAttributes.get(i), i);
            if (position < 0)
            {
                return false;
            }
            if (this.atomicOperations[position] instanceof AtomicEqualityOperation)
            {
                if (!((AtomicEqualityOperation)this.atomicOperations[position]).parameterValueEquals(data, extractors.get(i)))
                {
                    return false;
                }
            }
            else return false;
        }
        return true;
    }

    private class AttributesAsList extends AbstractList implements List
    {
        public Object get(int index)
        {
            return atomicOperations[index].getAttribute();
        }

        public int size()
        {
            return atomicOperations.length;
        }
    }

    public boolean hasInClause()
    {
        //todo: optimize this by memoization
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            if (atomicOperations[i] instanceof InOperation) return true;
        }
        return false;
    }

    public static Operation createEqOperation(AtomicOperation[] ops)
    {
        if (ops.length == 1) return ops[0];
        return new MultiEqualityOperation(ops);
    }

    public Boolean matches(Object o)
    {
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            AtomicOperation aeo = atomicOperations[i];
            if (!aeo.matches(o)) return false;
        }
        return true;
    }

    public boolean zPrefersBulkMatching()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return ToStringContext.createAndToString(this);
    }

    public int getEqualityOpCount()
    {
        return this.atomicOperations.length;
    }

    public void addEqAttributes(List attributeList)
    {
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            attributeList.add(atomicOperations[i].getAttribute());
        }
    }

    public Extractor getParameterExtractorFor(Attribute attribute)
    {
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            if (atomicOperations[i].getAttribute().equals(attribute))
            {
                return ((OperationWithParameterExtractor)atomicOperations[i]).getParameterExtractor();
            }
        }
        throw new RuntimeException("should not get here");
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        int inOpCount = 0;
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            if (atomicOperations[i] instanceof InOperation)
            {
                inOpCount++;
            }
        }
        if (inOpCount > 0)
        {
            int leftOver = this.atomicOperations.length - inOpCount;
            if (leftOver == 0)
            {
                return null;
            }
            if (leftOver == 1)
            {
                for (int i = 0; i < this.atomicOperations.length; i++)
                {
                    if (!(atomicOperations[i] instanceof InOperation))
                    {
                        return (EqualityOperation) atomicOperations[i];
                    }
                }
            }
            AtomicOperation[] newOps = new AtomicOperation[leftOver];
            int count = 0;
            for (int i = 0; i < this.atomicOperations.length; i++)
            {
                if (!(atomicOperations[i] instanceof InOperation))
                {
                    newOps[count] = atomicOperations[i];
                    count++;
                }
            }
            return new MultiEqualityOperation(newOps);
        }
        return this;
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
        return true;
    }

    @Override
    public boolean zIsShapeCachable()
    {
        return true;
    }

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        // the only case we care about is when existing is another
        // equality/multi-equality with fewer terms and matching attributes
        // all other cases are too complex.
        if (existingOperation instanceof AtomicOperation)
        {
            AtomicOperation atomicOp = (AtomicOperation) existingOperation;
            for(int i=0;i<this.atomicOperations.length; i++)
            {
                if (atomicOperations[i].getAttribute().equals(atomicOp.getAttribute()))
                {
                    ShapeMatchResult shapeMatchResult = atomicOperations[i].zShapeMatch(atomicOp);
                    if (shapeMatchResult.isExactMatch())
                    {
                        return new SuperMatchSmr(existingOperation, this, atomicOperations[i], this); //todo: can optimize filterOperation
                    }
                    else if (shapeMatchResult.isSuperMatch())
                    {
                        SuperMatchSmr superMatchSmr = (SuperMatchSmr) shapeMatchResult;
                        return new SuperMatchSmr(existingOperation, this, superMatchSmr.getLookUpOperation(), this);
                    }
                }
            }
        }
        else if (existingOperation instanceof MultiEqualityOperation)
        {
            MultiEqualityOperation multiEqualityOperation = (MultiEqualityOperation) existingOperation;
            if (multiEqualityOperation.hasInClause() || this.hasInClause())
            {
                return complexShapeMatch(multiEqualityOperation);
            }
            int equalityOpCount = multiEqualityOperation.getEqualityOpCount();
            if (equalityOpCount <= this.getEqualityOpCount())
            {
                Set set = (equalityOpCount > 8) ? new UnifiedSet(equalityOpCount) : new SmallSet(equalityOpCount);
                multiEqualityOperation.addDepenedentAttributesToSet(set);
                int matched = 0;
                for(int i=0;i<this.atomicOperations.length;i++)
                {
                    if (set.contains(this.atomicOperations[i].getAttribute()))
                    {
                        matched++;
                    }
                }
                if (matched == set.size())
                {
                    return equalityOpCount == this.getEqualityOpCount() ? ExactMatchSmr.INSTANCE :
                            MultiEqualityOperation.createSuperMatchSmr(existingOperation, this, multiEqualityOperation, this, set);
                }
            }
        }
        else if (existingOperation instanceof AndOperation)
        {
            return ((AndOperation)existingOperation).reverseShapeMatch(this, atomicOperations);
        }
        return NoMatchSmr.INSTANCE;
    }

    public static ShapeMatchResult createSuperMatchSmr(Operation existingOperation, Operation newOperation, MultiEqualityOperation superOperation, MultiEqualityOperation subOperation, Set set)
    {
        // we know these operations do NOT have in-clauses where sub overlaps with super
        subOperation.calculateFalseHood();
        superOperation.calculateFalseHood();
        int lookupCount = 0;
        AtomicOperation[] forLookup = new AtomicOperation[superOperation.getEqualityOpCount()];
        int filterCount = 0;
        AtomicOperation[] forFilter = new AtomicOperation[subOperation.getEqualityOpCount() - superOperation.getEqualityOpCount()];
        for(AtomicOperation eo: subOperation.atomicOperations)
        {
            if (set.contains(eo.getAttribute()))
            {
                forLookup[lookupCount++] = eo;
            }
            else
            {
                forFilter[filterCount++] = eo;
            }
        }
        if (filterCount == 1)
        {
            return new SuperMatchSmr(existingOperation, newOperation, new MultiEqualityOperation(forLookup), forFilter[0]);
        }
        else
        {
            return new SuperMatchSmr(existingOperation, newOperation, new MultiEqualityOperation(forLookup), new MultiEqualityOperation(forFilter));
        }
    }

    public static ShapeMatchResult createSuperMatchSmr(Operation existingOperation, RelationshipMultiEqualityOperation relationshipMultiEqualityOperation,
                                                       AtomicEqualityOperation superOperation, MultiEqualityOperation subOperation)
    {
        AtomicOperation[] atomicOperations = subOperation.atomicOperations;
        for(int i=0;i<atomicOperations.length; i++)
        {
            if (atomicOperations[i].getAttribute().equals(superOperation.getAttribute()) && !(atomicOperations[i] instanceof InOperation))
            {
                return new SuperMatchSmr(existingOperation, relationshipMultiEqualityOperation, atomicOperations[i], relationshipMultiEqualityOperation); //todo: can optimize filterOperation
            }
        }
        return NoMatchSmr.INSTANCE;
    }

    private ShapeMatchResult complexShapeMatch(MultiEqualityOperation existingOp)
    {
        UnifiedMap<Attribute, AtomicOperation> attrOpMap = new UnifiedMap(existingOp.getEqualityOpCount());
        for(AtomicOperation ao: existingOp.atomicOperations)
        {
            attrOpMap.put(ao.getAttribute(), ao);
        }
        int lookupCount = 0;
        AtomicOperation[] forLookup = new AtomicOperation[this.getEqualityOpCount()];
        for(AtomicOperation eo: this.atomicOperations)
        {
            AtomicOperation matching = attrOpMap.get(eo.getAttribute());
            if (matching != null)
            {
                ShapeMatchResult shapeMatchResult = eo.zShapeMatch(matching);
                if (shapeMatchResult.isExactMatch())
                {
                    forLookup[lookupCount++] = eo;
                }
                else if (shapeMatchResult.isSuperMatch())
                {
                    forLookup[lookupCount++] = (AtomicOperation) ((SuperMatchSmr)shapeMatchResult).getLookUpOperation();
                }
                else //NoMatch
                {
                    return shapeMatchResult;
                }
            }
            else
            {
                return NoMatchSmr.INSTANCE;
            }
        }
        return new SuperMatchSmr(existingOp, this, new MultiEqualityOperation(forLookup), this); // todo: can optimize filterOperation

    }

    @Override
    public int zShapeHash()
    {
        int hashcode = 0;
        for (int i = 0; i < this.atomicOperations.length; i++)
        {
            AtomicOperation aeo = atomicOperations[i];
            if (aeo instanceof AtomicEqualityOperation)
            {
                hashcode ^= aeo.getAttribute().hashCode();
            }
            else
            {
                hashcode ^= aeo.hashCode();
            }
        }
        return hashcode;
    }
}
