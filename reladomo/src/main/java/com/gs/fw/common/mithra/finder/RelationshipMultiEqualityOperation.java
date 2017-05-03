
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

import java.io.ObjectStreamException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.IndexReference;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.querycache.CompactUpdateCountOperation;
import com.gs.fw.common.mithra.util.*;

public class RelationshipMultiEqualityOperation implements Operation, EqualityOperation, CompactUpdateCountOperation
{
    private Object data;
    private RelationshipMultiExtractor multiExtractor;
    private volatile MultiEqualityOperation multiEqualityOperation;

    public RelationshipMultiEqualityOperation(RelationshipMultiExtractor multiExtractor, Object data)
    {
        this.multiExtractor = multiExtractor;
        this.data = data;
    }

    public void generateSql(SqlQuery query)
    {
        getOrCreateMultiEqualityOperation().generateSql(query);
    }

    public MultiEqualityOperation getOrCreateMultiEqualityOperation()
    {
        if (this.multiEqualityOperation == null)
        {
            AtomicOperation[] ops = createAtomicOps();
            this.multiEqualityOperation = new MultiEqualityOperation(ops);
        }

        return this.multiEqualityOperation;
    }

    private Operation createAtomicOperation(int index)
    {
        return multiExtractor.getLeftAttributes().get(index).nonPrimitiveEq(multiExtractor.getExtractorArray()[index].valueOf(data));
    }

    public int getClauseCount(SqlQuery query)
    {
        return getOrCreateMultiEqualityOperation().getClauseCount(query);
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        getOrCreateMultiEqualityOperation().registerAsOfAttributesAndOperations(checker);
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            AtomicOperation[] ops = createAtomicOps();
            return new MultiEqualityOperation(ops, asOfEqOperations);
        }
        return null;
    }

    private AtomicOperation[] createAtomicOps()
    {
        AtomicOperation[] ops = new AtomicOperation[multiExtractor.getLeftAttributes().size()];
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            ops[i] = (AtomicOperation) createAtomicOperation(i);
        }
        return ops;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        return transitivePropagator.constructAnd(insertPosition, getOrCreateMultiEqualityOperation(), toInsert);
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        return new MultiEqualityOperation(this.createAtomicOps(), asOfEqOperations);
    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        getOrCreateMultiEqualityOperation().registerOperation(extractor, registerEquality);
    }

    public boolean zHasAsOfOperation()
    {
        AsOfAttribute[] asOfAttributes = this.getResultObjectPortal().getFinder().getAsOfAttributes();
        if (asOfAttributes == null)
        {
            return true;
        }
        
        int count = 0;
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            if (multiExtractor.getLeftAttributes().get(i).isAsOfAttribute())
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

    public Operation zFindEquality(TimestampAttribute attr)
    {
        Operation result = null;
        for (int i = 0; i < multiExtractor.getLeftAttributes().size() && result == null; i++)
        {
            result =  createAtomicOperation(i).zFindEquality(attr);
        }
        return result;
    }

    public Cache getCache()
    {
        return this.getResultObjectPortal().getCache();
    }

    private int getBestIndexRef(Cache cache)
    {
        return this.multiExtractor.getBestIndexRef(cache).indexReference;
    }

    public boolean usesUniqueIndex()
    {
        Cache c = this.getCache();
        int indexRef = this.getBestIndexRef(c);
        return indexRef > 0 && c.isUnique(indexRef);
    }

    public boolean usesImmutableUniqueIndex()
    {
        Cache c = this.getCache();
        int indexRef = this.getBestIndexRef(c);
        return indexRef > 0 && c.isUniqueAndImmutable(indexRef);
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
            long size = cache.getAverageReturnSize(bestIndexRef, 1);
            return (int) Math.min(size, cache.size());
        }
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        Cache cache = this.getCache();
        int bestIndexRef = this.getBestIndexRef(cache);
        if (bestIndexRef > 0)
        {
            return cache.getMaxReturnSize(bestIndexRef, 1);
        }
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
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            Operation op = createAtomicOperation(i);
            op.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
        }
    }

    public boolean zHazTriangleJoins()
    {
        return false;
    }

    public void zToString(ToStringContext toStringContext)
    {
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            toStringContext.beginAnd();
            multiExtractor.getLeftAttributes().get(i).zAppendToString(toStringContext);
            toStringContext.append("=");
            toStringContext.append("\""+this.multiExtractor.getExtractorArray()[i].valueOf(data).toString()+"\"");
            toStringContext.endAnd();
        }
    }

    public void addDependentPortalsToSet(Set set)
    {
        this.multiExtractor.getLeftAttributes().get(0).zAddDependentPortalsToSet(set);
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            set.add(multiExtractor.getLeftAttributes().get(i));
        }
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        return false;
    }

    protected int getAttributePosition(Attribute a)
    {
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            if (multiExtractor.getLeftAttributes().get(i).equals(a))
            {
                return i;
            }
        }
        return -1;
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
        Cache cache = this.getCache();
        int bestIndexRef = this.getBestIndexRef(cache);
        if (bestIndexRef > 0)
        {
            Attribute[] bestAttributes = cache.getIndexAttributes(bestIndexRef);
            Extractor[] finalLeftExtractors = null;
            boolean[] applied = null;
            if (bestAttributes.length == this.multiExtractor.getLeftAttributes().size())
            {
                finalLeftExtractors = this.multiExtractor.getExtractorArray();
                for(int i=0;i<bestAttributes.length;i++)
                {
                    if (bestAttributes[i] != multiExtractor.getLeftAttributes().get(i))
                    {
                        finalLeftExtractors = null;
                        break;
                    }
                }
            }
            if (finalLeftExtractors == null)
            {
                finalLeftExtractors = new Extractor[bestAttributes.length];

                if (bestAttributes.length < this.multiExtractor.getLeftAttributes().size())
                {
                    applied = new boolean[this.multiExtractor.getLeftAttributes().size()];
                }
                for (int i = 0; i < bestAttributes.length; i++)
                {
                    int opIndex = this.getAttributePosition(bestAttributes[i]);
                    if (applied != null)
                    {
                        applied[opIndex] = true;
                    }
                    finalLeftExtractors[i] = multiExtractor.getExtractors().get(opIndex);
                }
            }
            List result = cache.get(bestIndexRef, data, finalLeftExtractors, true);
            if (returnNullIfNotFoundInIndex && result.size() == 0)
            {
                result = null;
            }
            if (result == null)
            {
                return null;
            }

            if (bestAttributes.length < this.multiExtractor.getLeftAttributes().size() && result.size() > 0)
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
        for (int i = 0; i < this.multiExtractor.getLeftAttributes().size() && matched; i++)
        {
            if (!applied[i])
            {
                matched = this.multiExtractor.getExtractorArray()[i].valueEquals(data, o, this.multiExtractor.getLeftAttributes().get(i));
            }
        }
        return matched;
    }

    public List applyOperation(List list)
    {
        MithraFastList result = null;
        int listSize = list.size();
        for(int j=0;j< listSize;j++)
        {
            Object item = list.get(j);
            boolean matches = true;
            for (int i = 0; matches && i < multiExtractor.getLeftAttributes().size(); i++)
            {
                matches = this.multiExtractor.getExtractorArray()[i].valueEquals(data, item, this.multiExtractor.getLeftAttributes().get(i));
            }
            result = AbstractAtomicOperation.copyToResultAfterFirstMismatch(list, listSize, result, j, item, matches);
        }
        if (result == null) // everything matched
        {
            return list;
        }
        return result;
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        return OrOperation.or(getOrCreateMultiEqualityOperation(), op);
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        Operation result = ((Operation) op).zCombinedAndWithMultiEquality(getOrCreateMultiEqualityOperation());
        if (result == null)
        {
            result = new AndOperation(getOrCreateMultiEqualityOperation(), op);
        }
        return result;
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return this.multiExtractor.getLeftAttributes().get(0).getOwnerPortal();
    }

    public String zGetResultClassName()
    {
        return this.multiExtractor.getLeftAttributes().get(0).zGetTopOwnerClassName();
    }

    public boolean zIsNone()
    {
        return false;  
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        result.addAll(this.multiExtractor.getLeftAttributes());
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        InternalList newOps = new InternalList(this.multiExtractor.getLeftAttributes().size());
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            Operation newOp = createAtomicOperation(i).zSubstituteForTempJoin(attributeMap, prototypeObject);
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
        for (int i = 0; i < multiExtractor.getLeftAttributes().size(); i++)
        {
            if (multiExtractor.getLeftAttributes().get(i).equals(asOfAttribute))
            {
                return createAtomicOperation(i);
            }
        }
        return null;
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithMultiEquality(getOrCreateMultiEqualityOperation());
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return getOrCreateMultiEqualityOperation().zCombinedAndWithAtomicEquality(op);
        }
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return op.zCombinedAndWithMultiEquality(getOrCreateMultiEqualityOperation());
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return getOrCreateMultiEqualityOperation().zCombinedAndWithMultiEquality(op);
        }
        return null;
    }

    public Operation zCombinedAndWithRangeOperation(RangeOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            for (int i = 0; i < this.multiExtractor.getLeftAttributes().size(); i++)
            {
                if (this.multiExtractor.getLeftAttributes().get(i).equals(op.getAttribute()))
                {
                    Operation combined = op.zCombinedAndWithAtomicEquality((AtomicEqualityOperation) createAtomicOperation(i));
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

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return this.zCombinedAndWithRangeOperation(op);
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return this.zCombinedAndWithRangeOperation(op);
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return this.zCombinedAndWithRangeOperation(op);
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return this.zCombinedAndWithRangeOperation(op);
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return getOrCreateMultiEqualityOperation().zCombinedAndWithIn(op);
        }
        return null;
    }

    public int hashCode()
    {
        int hashcode = 0;
        for (int i = 0; i < this.multiExtractor.getLeftAttributes().size(); i++)
        {
            hashcode ^= HashUtil.combineHashes(multiExtractor.getLeftAttributes().get(i).hashCode(), multiExtractor.getExtractorArray()[i].valueHashCode(data));
        }
        return hashcode;
    }

    public Object getData()
    {
        return data;
    }

    public List<Extractor> getLeftExtractors()
    {
        return multiExtractor.getExtractors();
    }

    public List<Attribute> getLeftAttributes()
    {
        return multiExtractor.getLeftAttributes();
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof RelationshipMultiEqualityOperation)
        {
            RelationshipMultiEqualityOperation other = (RelationshipMultiEqualityOperation) obj;
            return equalsOther(other);
        }
        else if (obj instanceof MultiEqualityOperation)
        {
            MultiEqualityOperation other = (MultiEqualityOperation) obj;
            return other.equalsExtractorBased(this);
        }
        
        return false;
    }

    private boolean equalsOther(RelationshipMultiEqualityOperation other)
    {
        if (other.multiExtractor.getLeftAttributes().size() != this.multiExtractor.getLeftAttributes().size())
        {
            return false;
        }
        if (this.multiExtractor.getLeftAttributes() == other.multiExtractor.getLeftAttributes())
        {
            for(int i=0;i<this.multiExtractor.getLeftAttributes().size();i++)
            {
                if (!this.multiExtractor.getExtractorArray()[i].valueEquals(data, other.data, other.multiExtractor.getExtractorArray()[i]))
                {
                    return false;
                }
            }
        }
        else
        {
            int matchedPosition;
            for(matchedPosition=0;matchedPosition<this.multiExtractor.getLeftAttributes().size();matchedPosition++)
            {
                if (!this.multiExtractor.getLeftAttributes().get(matchedPosition).equals(other.multiExtractor.getLeftAttributes().get(matchedPosition)))
                {
                    break;
                }
                if (!this.multiExtractor.getExtractorArray()[matchedPosition].valueEquals(data, other.data, other.multiExtractor.getExtractors().get(matchedPosition)))
                {
                    return false;
                }
            }
            for(;matchedPosition<this.multiExtractor.getLeftAttributes().size();matchedPosition++)
            {
                int pos = this.getAttributePosition(other.multiExtractor.getLeftAttributes().get(matchedPosition));
                if (pos < 0)
                {
                    return false;
                }
                if (!this.multiExtractor.getExtractorArray()[pos].valueEquals(data, other.data, other.multiExtractor.getExtractors().get(pos)))
                {
                    return false;
                }
            }
        }
        return true;
    }

    public Boolean matches(Object o)
    {
        for (int i = 0; i < this.multiExtractor.getLeftAttributes().size(); i++)
        {
            if (!multiExtractor.getExtractorArray()[i].valueEquals(data, o, multiExtractor.getLeftAttributes().get(i))) return false;
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
        return this.multiExtractor.getLeftAttributes().size();
    }

    public void addEqAttributes(List attributeList)
    {
        for (int i = 0; i < this.multiExtractor.getLeftAttributes().size(); i++)
        {
            attributeList.add(multiExtractor.getLeftAttributes().get(i));
        }
    }

    public Extractor getParameterExtractorFor(Attribute attribute)
    {
        for (int i = 0; i < this.multiExtractor.getLeftAttributes().size(); i++)
        {
            if (multiExtractor.getLeftAttributes().get(i).equals(attribute))
            {
                return ((OperationWithParameterExtractor)createAtomicOperation(i)).getParameterExtractor();
            }
        }
        throw new RuntimeException("should not get here");
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return getOrCreateMultiEqualityOperation();
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return getOrCreateMultiEqualityOperation();
    }

    @Override
    public UpdateCountHolder[] getUpdateCountHolders()
    {
        return this.multiExtractor.getUpdateCountHolders();
    }

    @Override
    public int[] getUpdateCountValues()
    {
        return this.multiExtractor.getUpdateCountValues();
    }

    @Override
    public Operation getCachableOperation()
    {
        return this.multiEqualityOperation;
    }

    @Override
    public boolean requiresAsOfEqualityCheck()
    {
        return false;
    }

    @Override
    public Operation forceGetCachableOperation()
    {
        return getOrCreateMultiEqualityOperation();
    }

    public List<Extractor> getExtractors()
    {
        return this.multiExtractor.getExtractors();
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
