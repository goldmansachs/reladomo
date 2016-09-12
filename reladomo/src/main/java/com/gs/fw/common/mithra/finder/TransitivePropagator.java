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

import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.InternalList;

import java.util.Iterator;
import java.util.Map;



public class TransitivePropagator implements MapperStack
{
    private static final ObjectWithMapperStack[] EMPTY_ARRAY = new ObjectWithMapperStack[0];

    private UnifiedMap<ObjectWithMapperStack<Attribute>, UnifiedSet<ObjectWithMapperStack<Attribute>>> equalityMap;
    private MapperStackImpl mapperStackImpl = new MapperStackImpl();
    private UnifiedMap<ObjectWithMapperStack<Attribute>, AtomicOperation> existingOperationsWithMapperStack;

    private Operation op;

    public TransitivePropagator(Operation op)
    {
        this.op = op;
        this.processOperation();
    }

    protected void processOperation()
    {
        registerEqualitiesAndAtomicOperations();

        propagateEqualities();
    }

    private void propagateEqualities()
    {
        if (equalityMap != null && existingOperationsWithMapperStack != null)
        {
            processEqualities();

            UnifiedMap<MapperStack, InternalList> operationsWithMapperStackToInsert = createOperationsToInsert();

            int expectedReturnSize = Integer.MAX_VALUE;

            Operation bestOp = this.op;
            if (this.op.usesNonUniqueIndex())
            {
                expectedReturnSize = this.op.zEstimateReturnSize();
            }

            for(Iterator<Map.Entry<MapperStack, InternalList>> it = operationsWithMapperStackToInsert.entrySet().iterator(); it.hasNext();)
            {
                Map.Entry<MapperStack, InternalList> entry = it.next();
                MapperStack stack = entry.getKey();
                InternalList toInsert = entry.getValue();
                Operation newOp = op.zInsertTransitiveOps(stack, toInsert, this);
                if (newOp != null)
                {
                    if (newOp.usesUniqueIndex())
                    {
                        bestOp = newOp;
                        break;
                    }
                    if (newOp.zEstimateReturnSize() < expectedReturnSize)
                    {
                        bestOp = newOp;
                        expectedReturnSize = newOp.zEstimateReturnSize();
                    }
                }
            }
            this.op = bestOp;
        }
    }

    public Operation constructAnd(MapperStack insertPosition, Operation other, InternalList toInsert)
    {
        if (insertPosition.equals(this.getCurrentMapperList()))
        {
            Operation op = other;
            for(int i=0;i<toInsert.size();i++)
            {
                op = op.and((Operation)toInsert.get(i));
            }
            return op;
        }
        return other;
    }

    private UnifiedMap<MapperStack, InternalList> createOperationsToInsert()
    {
        UnifiedMap<MapperStack, InternalList> operationsWithMapperStackToInsert = new UnifiedMap<MapperStack, InternalList>();

        for(Iterator<ObjectWithMapperStack<Attribute>> it = equalityMap.keySet().iterator(); it.hasNext();)
        {
            ObjectWithMapperStack<Attribute> attributeWithStack = it.next();
            if (existingOperationsWithMapperStack.get(attributeWithStack) == null)
            {
                for(Iterator<ObjectWithMapperStack<Attribute>> candidateIt = equalityMap.get(attributeWithStack).iterator(); candidateIt.hasNext();)
                {
                    AtomicOperation otherOp = existingOperationsWithMapperStack.get(candidateIt.next());
                    if (otherOp != null)
                    {
                        Operation toInsert = otherOp.susbtituteOtherAttribute(attributeWithStack.getObject());
                        if (toInsert != null)
                        {
                            InternalList insertList = operationsWithMapperStackToInsert.get(attributeWithStack.getMapperStack());
                            if (insertList == null)
                            {
                                insertList = new InternalList(2);
                                operationsWithMapperStackToInsert.put(attributeWithStack.getMapperStack(), insertList);
                            }
                            insertList.add(toInsert);
                            //todo: instead of breaking here, we could pick the best operation out of several possible
                            break;
                        }
                    }
                }
            }
        }
        return operationsWithMapperStackToInsert;
    }

    private void registerEqualitiesAndAtomicOperations()
    {
        op.zRegisterEqualitiesAndAtomicOperations(this);
    }

    public Operation getOperation()
    {
        return op;
    }

    public void pushMapper(Mapper mapper)
    {
        mapperStackImpl.pushMapper(mapper);
    }

    public Mapper popMapper()
    {
        return mapperStackImpl.popMapper();
    }

    public void pushMapperContainer(Object mapper)
    {
        mapperStackImpl.pushMapperContainer(mapper);
    }

    public void popMapperContainer()
    {
        mapperStackImpl.popMapperContainer();
    }

    public ObjectWithMapperStack constructWithMapperStack(Object o)
    {
        return mapperStackImpl.constructWithMapperStack(o);
    }

    public ObjectWithMapperStack constructWithMapperStackWithoutLastMapper(Object o)
    {
        return mapperStackImpl.constructWithMapperStackWithoutLastMapper(o);
    }

    public MapperStackImpl getCurrentMapperList()
    {
        return mapperStackImpl.getCurrentMapperList();
    }

    public void setEquality(Attribute left, Attribute right)
    {
        ObjectWithMapperStack<Attribute> leftWithMapperStack = this.constructWithMapperStackWithoutLastMapper(left);
        ObjectWithMapperStack<Attribute> rightWithMapperStack = this.constructWithMapperStack(right);
        if (!hasOrOperations())
        {
            addEquality(leftWithMapperStack, rightWithMapperStack);
        }
        addEquality(rightWithMapperStack, leftWithMapperStack);
    }

    private boolean hasOrOperations()
    {
        InternalList containers = this.mapperStackImpl.getMapperContainerStack();
        for(int i=0;i<containers.size();i++)
        {
            if (containers.get(i) instanceof OrOperation.DummyContainer) return true;
        }
        return false;
    }

    private void addEquality(ObjectWithMapperStack<Attribute> left, ObjectWithMapperStack<Attribute> right)
    {
        UnifiedSet<ObjectWithMapperStack<Attribute>> existing = this.getEqualityMap().get(left);
        if (existing == null)
        {
            existing = new UnifiedSet<ObjectWithMapperStack<Attribute>>(2);
            this.getEqualityMap().put(left, existing);
        }
        existing.add(right);
    }

    public void addAtomicOperation(AtomicOperation atomicOp)
    {
        ObjectWithMapperStack<Attribute> atomicOpWithStack = this.constructWithMapperStack(atomicOp.getAttribute());
        if (existingOperationsWithMapperStack == null)
        {
            existingOperationsWithMapperStack = new UnifiedMap<ObjectWithMapperStack<Attribute>, AtomicOperation>();
        }
        //todo: we might want to take the most efficient atomic operation here instead of just one of them
        existingOperationsWithMapperStack.put(atomicOpWithStack, atomicOp);
    }

    private void processEqualities()
    {
        final UnifiedMap<ObjectWithMapperStack<Attribute>, UnifiedSet<ObjectWithMapperStack<Attribute>>> equalityMap = this.getEqualityMap();
        final boolean[] anyChanged = new boolean[1];
        do
        {
            anyChanged[0] = false;
            equalityMap.forEachKeyValue(new Procedure2<ObjectWithMapperStack<Attribute>, UnifiedSet<ObjectWithMapperStack<Attribute>>>()
            {
                public void value(ObjectWithMapperStack<Attribute> attribute, UnifiedSet<ObjectWithMapperStack<Attribute>> equalities)
                {
                    InternalList equalityList = new InternalList(equalities);
                    for (int i = 0; i < equalityList.size(); i++)
                    {
                        UnifiedSet<ObjectWithMapperStack<Attribute>> setToAdd = equalityMap.get(equalityList.get(i));
                        for (Iterator<ObjectWithMapperStack<Attribute>> setIterator = setToAdd.iterator(); setIterator.hasNext(); )
                        {
                            ObjectWithMapperStack<Attribute> possibleNewEquality = setIterator.next();
                            if (!possibleNewEquality.equals(attribute) && equalities.add(possibleNewEquality))
                            {
                                anyChanged[0] = true;
                                equalityList.add(possibleNewEquality); // growing the collection that we're looping over.
                                equalities.add(possibleNewEquality);
                            }
                        }
                    }
                }
            });
        } while(anyChanged[0]);
    }

    private UnifiedMap<ObjectWithMapperStack<Attribute>, UnifiedSet<ObjectWithMapperStack<Attribute>>> getEqualityMap()
    {
        if (equalityMap == null) equalityMap = new UnifiedMap<ObjectWithMapperStack<Attribute>, UnifiedSet<ObjectWithMapperStack<Attribute>>>(2);
        return equalityMap;
    }
}