
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
import com.gs.fw.common.mithra.cache.ConcurrentFullUniqueIndex;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.util.*;

public class OrOperation implements Operation
{

    private static int idCount;

    private Operation[] operations = null;
    private int id;

    public OrOperation(com.gs.fw.finder.Operation first, com.gs.fw.finder.Operation second)
    {
        int size1 = 1;
        int size2 = 1;
        if (first instanceof OrOperation)
        {
            size1 += ((OrOperation) first).getOperations().length;
        }
        if (second instanceof OrOperation)
        {
            size2 = ((OrOperation) second).getOperations().length;
        }
        this.operations = new Operation[size1 + size2];
        copyOperationsFrom(this.operations, first, 0);
        copyOperationsFrom(this.operations, second, size1);
        id = idCount++;
    }

    public static Operation or(com.gs.fw.finder.Operation first, com.gs.fw.finder.Operation second)
    {
        if (first == NoOperation.instance() || second instanceof All)
        {
            return (Operation) second;
        }
        if (second == NoOperation.instance() || first instanceof All)
        {
            return (Operation) first;
        }
        return new OrOperation(first, second);
    }

    private void copyOperationsFrom(com.gs.fw.finder.Operation[] ops, com.gs.fw.finder.Operation second, int size1)
    {
        if (second instanceof OrOperation)
        {
            OrOperation orOp = (OrOperation) second;
            System.arraycopy(orOp.getOperations(), 0, ops, size1, orOp.getOperations().length);
        }
        else
        {
            ops[size1] = second;
        }
    }

    public OrOperation(Operation[] operations)
    {
        this.operations = operations;
    }

    private OrOperation(int id, Operation[] operations)
    {
        this.id = id;
        this.operations = operations;
    }

    protected Operation[] getOperations()
    {
        return operations;
    }

    public boolean usesUniqueIndex()
    {
        for (int i = 0; i < operations.length; i++)
        {
            if (!operations[i].usesUniqueIndex())
            {
                return false;
            }
        }
        return true;
    }

    public boolean usesImmutableUniqueIndex()
    {
        return false;
    }

    public boolean usesNonUniqueIndex()
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            if (!op.usesNonUniqueIndex() && !op.usesUniqueIndex())
            {
                return false;
            }
        }
        return true;
    }

    public int zEstimateReturnSize()
    {
        int size = 0;
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            size += op.zEstimateReturnSize() * 0.9;
        }
        return (int) Math.min(this.getResultObjectPortal().getCache().estimateQuerySize(), size);
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        double size = 0;
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            size += op.zEstimateMaxReturnSize();
        }
        return (int) Math.min(this.getResultObjectPortal().getCache().estimateQuerySize(), size);
    }

    @Override
    public boolean zIsEstimatable()
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            if (!op.zIsEstimatable()) return false;
        }
        return true;
    }

    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        for (int i = 0; i < operations.length; i++)
        {
            transitivePropagator.pushMapperContainer(new DummyContainer(i));
            Operation op = operations[i];
            op.zRegisterEqualitiesAndAtomicOperations(transitivePropagator);
            transitivePropagator.popMapperContainer();
        }
    }

    public boolean zHazTriangleJoins()
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            if (op.zHazTriangleJoins())
            {
                return true;
            }
        }
        return false;
    }

    public void zToString(ToStringContext toStringContext)
    {
        toStringContext.beginBracket();
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            toStringContext.beginOr();
            toStringContext.beginBracket();
            op.zToString(toStringContext);
            toStringContext.endBracket();
            toStringContext.endOr();
        }
        toStringContext.endBracket();
    }

    public EqualityOperation zExtractEqualityOperations()
    {
        return null;
    }

    @Override
    public boolean zContainsMappedOperation()
    {
        for (Operation operation : operations)
        {
            if (operation.zContainsMappedOperation())
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean zHasParallelApply()
    {
        return true;
    }

    public List applyOperationToFullCache()
    {
        FullUniqueIndex result = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY);
        for (int i = 0; i < operations.length; i++)
        {
            result.addAll(operations[i].applyOperationToFullCache());
        }
        return result.getAll();
    }

    public List applyOperationToPartialCache()
    {
        Operation operation = operations[0];
        List partialResult = operation.getResultObjectPortal().zFindInMemoryWithoutAnalysis(operation, true);
        if (partialResult == null)
        {
            return null;
        }
        FullUniqueIndex result = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY);
        for (int i = 1; i < operations.length; i++)
        {
            partialResult = operation.getResultObjectPortal().zFindInMemoryWithoutAnalysis(operations[i], true);
            if (partialResult == null)
            {
                return null;
            }
            result.addAll(partialResult);
        }
        return result.getAll();
    }

    public List applyOperation(List list)
    {
        if (MithraCpuBoundThreadPool.isParallelizable(list.size()))
        {
            return applyToLargeResultsInParallel(list);
        }
        else
        {
            return applyOperationSerially(list);
        }
    }

    private List applyToLargeResultsInParallel(List list)
    {
        InternalList complexList = new InternalList(operations.length);
        InternalList atomicList = new InternalList(operations.length);
        separateComplexAndAtomic(complexList, atomicList);
        final Operation[] complex = new Operation[complexList.size()];
        complexList.toArray(complex);
        List leftOverList = list;
        List result = null;
        if (atomicList.size() > 0)
        {
            final Operation[] atomic = new Operation[atomicList.size()];
            atomicList.toArray(atomic);
            MithraCpuBoundThreadPool pool = MithraCpuBoundThreadPool.getInstance();
            List<List> lists = pool.split(list);
            int chunks = lists.size();
            CpuBoundTask[] tasks = new CpuBoundTask[chunks];
            final List[] localResults = new List[chunks];
            final List[] leftOverLists = complex.length > 0 ? new List[chunks] : null;
            for (int i = 0; i < chunks; i++)
            {
                final List sublist = lists.get(i);
                final int chunk = i;
                tasks[i] = new CpuBoundTask()
                {
                    @Override
                    public void execute()
                    {
                        FullUniqueIndex leftOverIndex = null;
                        if (complex.length > 0)
                        {
                            leftOverIndex = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, sublist.size());
                            leftOverIndex.addAll(sublist);
                        }
                        List newResultList = applyAtomic(sublist, atomic);
                        localResults[chunk] = newResultList;
                        if (complex.length > 0)
                        {
                            for (int j = 0; j < newResultList.size(); j++)
                            {
                                leftOverIndex.remove(newResultList.get(j));
                            }
                            leftOverLists[chunk] = leftOverIndex.getAll();
                        }
                    }
                };
            }
            new FixedCountTaskFactory(tasks).startAndWorkUntilFinished();
            result = new MithraCompositeList(localResults.length);
            for (List localResult : localResults)
            {
                result.addAll(localResult);
            }
            if (complex.length > 0)
            {
                leftOverList = new MithraCompositeList(leftOverLists.length);
                for(List leftOver: leftOverLists)
                {
                    leftOverList.addAll(leftOver);
                }
            }
        }
        if (complex.length > 0)
        {
            List fromComplex = applyOperationsSerially(leftOverList, complex);
            if (result == null)
            {
                result = fromComplex;
            }
            else
            {
                result.addAll(fromComplex);
            }
        }
        return result;
    }

    private List applyAtomic(List result, Operation[] atomic)
    {
        if (atomic.length > 0)
        {
            if (result instanceof FastList)
            {
                result = applyAtomicInPlace((FastList) result, atomic);
            }
            else result = applyAtomicNotInPlace(result, atomic);
        }
        return result;
    }

    private List applyAtomicInPlace(FastList result, Operation[] atomic)
    {
        int currentFilledIndex = 0;
        for (int i = 0; i < result.size(); i++)
        {
            Object o = result.get(i);
            if (matches(o, atomic))
            {
                // keep it
                if (currentFilledIndex != i)
                {
                    result.set(currentFilledIndex, o);
                }
                currentFilledIndex++;
            }
        }
        this.resetTheEnd(result, currentFilledIndex);
        return result;
    }

    private void resetTheEnd(List list, final int newCurrentFilledIndex)
    {
        int initialSize = list.size();
        for (int i = newCurrentFilledIndex; i < initialSize; i++)
        {
            list.remove(list.size() - 1);
        }
    }

    private List applyAtomicNotInPlace(List result, Operation[] atomic)
    {
        MithraFastList newResults = new MithraFastList(result.size());
        for (int i = 0; i < result.size(); i++)
        {
            Object o = result.get(i);
            if (matches(o, atomic))
            {
                newResults.add(o);
            }
        }
        return newResults;
    }

    private void separateComplexAndAtomic(InternalList complex, InternalList atomic)
    {
        for (int i = 0; i < operations.length; i++)
        {
            if (operations[i].zPrefersBulkMatching())
            {
                complex.add(operations[i]);
            }
            else
            {
                atomic.add(operations[i]);
            }
        }
    }

    private List applyOperationSerially(List list)
    {
        return applyOperationsSerially(list, operations);
    }

    private List applyOperationsSerially(List list, Operation[] ops)
    {
        if (ops.length == 1)
        {
            return ops[0].applyOperation(list);
        }
        MithraCompositeList resultList = new MithraCompositeList(this.operations.length);
        ConcurrentFullUniqueIndex leftOverIndex = new ConcurrentFullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY, list.size());
        leftOverIndex.addAll(list);
        List leftOverList = list;
        for (int i = 0; i < operations.length; i++)
        {
            List newResultList = operations[i].applyOperation(leftOverList);
            if (newResultList == null)
            {
                return null;
            }
            resultList.addAll(newResultList);
            if (i < operations.length - 1)
            {
                leftOverIndex.removeAllWithIdentity(newResultList);
                leftOverList = leftOverIndex.getAll();
            }
        }
        return resultList;
    }

    public Operation or(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        if (op instanceof All)
        {
            return (Operation) op;
        }
        int size = this.operations.length + 1;
        if (op instanceof OrOperation)
        {
            size += ((OrOperation) op).getOperations().length - 1;
        }
        Operation[] ops = new Operation[size];
        System.arraycopy(this.operations, 0, ops, 0, this.operations.length);
        copyOperationsFrom(ops, op, this.operations.length);
        return new OrOperation(ops);
    }

    public Operation and(com.gs.fw.finder.Operation op)
    {
        if (op == NoOperation.instance())
        {
            return this;
        }
        return new AndOperation(this, op);
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return operations[0].getResultObjectPortal();
    }

    public String zGetResultClassName()
    {
        return operations[0].zGetResultClassName();
    }

    public boolean zIsNone()
    {
        for (int i = 0; i < operations.length; i++)
        {
            if (!operations[i].zIsNone())
            {
                return false;
            }
        }
        return true;
    }

    public void zAddAllLeftAttributes(Set<Attribute> result)
    {
        for (int i = 0; i < operations.length; i++)
        {
            operations[i].zAddAllLeftAttributes(result);
        }
    }

    public Operation zSubstituteForTempJoin(Map<Attribute, Attribute> attributeMap, Object prototypeObject)
    {
        InternalList newOps = new InternalList(operations.length);
        for (int i = 0; i < operations.length; i++)
        {
            Operation newOp = operations[i].zSubstituteForTempJoin(attributeMap, prototypeObject);
            if (newOp != null) newOps.add(newOp);
        }
        if (newOps.size() == 0) return null;
        if (newOps.size() == 1) return (Operation) newOps.get(0);
        Operation[] newOpArray = new Operation[newOps.size()];
        newOps.toArray(newOpArray);
        return new OrOperation(newOpArray);
    }

    public Operation zGetAsOfOp(AsOfAttribute asOfAttribute)
    {
        return null;
    }

    public void generateSql(SqlQuery query)
    {
        query.beginBracket();
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            query.pushMapperContainer(new DummyContainer(i));
            boolean insertedOr = query.beginOr();
            query.beginBracket();
            op.generateSql(query);
            query.endBracket();
            query.endOr(insertedOr);
            query.popMapperContainer();
        }
        query.endBracket();
    }

    public int getClauseCount(SqlQuery query)
    {
        int count = 0;
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            count += op.getClauseCount(query);
        }
        return count;
    }

    public void registerAsOfAttributesAndOperations(AsOfEqualityChecker checker)
    {
        // todo: rezaem: this is broken: operations on as of attributes cannot be meaninfully or'ed together (yet?)
        for (int i = 0; i < operations.length; i++)
        {
            checker.pushMapperContainer(new DummyContainer(i));
            Operation op = operations[i];
            op.registerAsOfAttributesAndOperations(checker);
            checker.popMapperContainer();
        }
    }

    public Operation insertAsOfEqOperation(AtomicOperation[] asOfEqOperations, MapperStackImpl insertPosition, AsOfEqualityChecker stack)
    {
        Operation result = null;
        if (insertPosition.equals(stack.getCurrentMapperList()))
        {
            Operation asMultiEq = null;
            Operation[] newOperations = new Operation[this.operations.length];
            for (int i = 0; i < operations.length; i++)
            {
                newOperations[i] = operations[i].zInsertAsOfEqOperationOnLeft(asOfEqOperations);
                if (newOperations[i] == operations[i])
                {
                    if (asMultiEq == null)
                    {
                        asMultiEq = MultiEqualityOperation.createEqOperation(asOfEqOperations);
                    }
                    newOperations[i] = operations[i].and(asMultiEq);
                }
            }
            result = new OrOperation(this.id, newOperations);
        }
        else
        {
            boolean haveNew = false;
            Operation[] newOperations = new Operation[this.operations.length];
            for (int i = 0; i < operations.length; i++)
            {
                stack.pushMapperContainer(new DummyContainer(i));
                Operation op = operations[i];
                Operation newOp = op.insertAsOfEqOperation(asOfEqOperations, insertPosition, stack);
                if (newOp != null)
                {
                    newOperations[i] = newOp;
                    haveNew = true;
                }
                else
                {
                    newOperations[i] = op;
                }
                stack.popMapperContainer();
            }
            if (haveNew) result = new OrOperation(this.id, newOperations);
        }
        if (result != null)
        {
            stack.substituteContainer(this, result);
        }
        return result;
    }

    public Operation zInsertTransitiveOps(MapperStack insertPosition, InternalList toInsert, TransitivePropagator transitivePropagator)
    {
        Operation op = transitivePropagator.constructAnd(insertPosition, this, toInsert);
        if (op != this)
        {
            return op;
        }
        boolean haveNew = false;
        Operation[] newOperations = new Operation[this.operations.length];
        for (int i = 0; i < operations.length; i++)
        {
            transitivePropagator.pushMapperContainer(new DummyContainer(i));
            op = operations[i];
            Operation newOp = op.zInsertTransitiveOps(insertPosition, toInsert, transitivePropagator);
            if (newOp != null)
            {
                newOperations[i] = newOp;
                haveNew = true;
            }
            else
            {
                newOperations[i] = op;
            }
            transitivePropagator.popMapperContainer();
        }
        if (haveNew) return new OrOperation(this.id, newOperations);
        return this;
    }

    public Operation zInsertAsOfEqOperationOnLeft(AtomicOperation[] asOfEqOperations)
    {
        boolean returnNew = false;
        Operation[] newOperations = new Operation[this.operations.length];
        for (int i = 0; i < operations.length; i++)
        {
            newOperations[i] = operations[i].zInsertAsOfEqOperationOnLeft(asOfEqOperations);
            if (newOperations[i] != operations[i]) returnNew = true;
        }
        if (returnNew) new OrOperation(this.id, newOperations);
        return this;

    }

    public void registerOperation(MithraDatabaseIdentifierExtractor extractor, boolean registerEquality)
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            extractor.pushMapperContainer(new DummyContainer(i));
            op.registerOperation(extractor, true);
            extractor.popMapperContainer();
        }
    }

    public boolean zHasAsOfOperation()
    {
        for (int i = 0; i < operations.length; i++)
        {
            if (!operations[i].zHasAsOfOperation())
            {
                return false;
            }
        }
        return true;
    }

    public Operation zFlipToOneMapper(Mapper mapper)
    {
        return null;
    }

    public Operation zFindEquality(TimestampAttribute attr)
    {
        Operation result = null;
        for (int i = 0; i < operations.length; i++)
        {
            Operation newResult = operations[i].zFindEquality(attr);
            if (result == null)
            {
                result = newResult;
            }
            else
            {
                if (!result.equals(newResult)) return null;
            }
        }
        return result;

    }


    /**
     * the xor operation is used to ensure the order of operands does not impact the hashcode
     *
     * @return the hashcode
     */
    public int hashCode()
    {
        int hashcode = this.operations[0].hashCode();
        for (int i = 1; i < operations.length; i++)
        {
            hashcode ^= operations[i].hashCode();
        }
        return hashcode;
    }

    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj instanceof OrOperation)
        {
            OrOperation other = (OrOperation) obj;
            if (this.operations.length != other.operations.length)
            {
                return false;
            }
            if (this.hashCode() != other.hashCode())
            {
                return false;
            }
            for (int i = 0; i < this.operations.length; i++)
            {
                if (!this.operations[i].equals(other.operations[i]))
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void addDependentPortalsToSet(Set set)
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            op.addDependentPortalsToSet(set);
        }
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            op.addDepenedentAttributesToSet(set);
        }
    }

    public boolean isJoinedWith(MithraObjectPortal portal)
    {
        for (int i = 0; i < operations.length; i++)
        {
            Operation op = operations[i];
            if (op.isJoinedWith(portal))
            {
                return true;
            }
        }
        return false;
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        if (op.equals(this))
        {
            return this;
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
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

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return null;
    }

    public Boolean matches(Object o)
    {
        Operation[] ops = operations;
        return matches(o, ops);
    }

    private Boolean matches(Object o, Operation[] ops)
    {
        for (int i = 0; i < ops.length; i++)
        {
            Boolean matched = ops[i].matches(o);
            if (matched == Boolean.TRUE) return true;
        }
        return false;
    }

    public boolean zPrefersBulkMatching()
    {
        for (int i = 0; i < this.operations.length; i++)
        {
            if (this.operations[i].zPrefersBulkMatching()) return true;
        }
        return false;
    }

    protected class DummyContainer
    {
        private int i;

        public DummyContainer(int i)
        {
            this.i = i;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o instanceof DummyContainer)
            {
                final DummyContainer that = (DummyContainer) o;
                return (i == that.i && OrOperation.this.id == that.getOrOperation().id);
            }
            return false;
        }

        private OrOperation getOrOperation()
        {
            return OrOperation.this;
        }

        public int hashCode()
        {
            return i;
        }
    }

    @Override
    public String toString()
    {
        return ToStringContext.createAndToString(this);
    }
}