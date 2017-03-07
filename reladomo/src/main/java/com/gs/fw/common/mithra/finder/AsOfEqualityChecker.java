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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.UnifiedMap;
import com.gs.fw.common.mithra.attribute.TemporalAttribute;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.CollectionUtil;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.finder.asofop.AsOfEdgePointOperation;
import com.gs.fw.common.mithra.finder.asofop.AsOfEqOperation;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.util.Pair;

import java.util.*;



public class AsOfEqualityChecker implements MapperStack
{
    private static final ObjectWithMapperStack[] EMPTY_ARRAY = new ObjectWithMapperStack[0];
    private static final AsOfAttributeWithStackComparator AS_OF_ATTRIBUTE_WITH_STACK_COMPARATOR = new AsOfAttributeWithStackComparator();

    private UnifiedMap<ObjectWithMapperStack<? extends TemporalAttribute>, ObjectWithMapperStack<AsOfOperation>> asOfOperationMap;
    private UnifiedMap<ObjectWithMapperStack<? extends TemporalAttribute>, InternalList> equalityAsOfOperationMap;
    private FastList<Pair<MultiInOperation, TupleTempContext>> multiInTempContexts;
    private boolean processedAsOfEqualities = false;
    private boolean hasAsOfAttributes;
    private ObjectWithMapperStack<TemporalAttribute>[] allAsOfAttributesWithMapperStack;
    private Set<ObjectWithMapperStack<TemporalAttribute>> registeredAsOfAttributesWithMapperStack;
    private MapperStackImpl mapperStackImpl = new MapperStackImpl();
    private UnifiedMap<MapperStack, InternalList> asOfAttributeWithMapperStackToInsert;
    private InternalList[] allToInsert;

    private ObjectWithMapperStack<TemporalAttribute> temp;
    private UnifiedMap<MapperStackImpl, MapperStackImpl> msiPool;

    private Operation op;
    private OrderBy orderBy;
    private static final Comparator DEEP_FIRST = new DeepFirstComparator();

    public AsOfEqualityChecker(Operation op, OrderBy orderBy)
    {
        this.op = op;
        this.orderBy = orderBy;
        this.processOperation();
    }

    protected void processOperation()
    {
        registerAsOfAttributes();

        if (hasAsOfAttributes)
        {
            populateAllAsOfAttributes();
            lookForMissingDefaults();
            insertEqualitiesAndDefaults();
        }
    }

    private void insertEqualitiesAndDefaults()
    {
        if (asOfAttributeWithMapperStackToInsert != null)
        {
            boolean insertedDefault = false;
            allToInsert = new InternalList[asOfAttributeWithMapperStackToInsert.size()];
            asOfAttributeWithMapperStackToInsert.values().toArray(allToInsert);
            Arrays.sort(allToInsert, DEEP_FIRST);
            AtomicOperation[] arrayOfOne = new AtomicOperation[1];
            for(InternalList toInsertList: allToInsert)
            {
                AtomicOperation[] arrayToInsert = arrayOfOne;
                if (toInsertList.size() == 1)
                {
                    arrayToInsert[0] = ((ObjectWithMapperStackAndAsOfOperation) toInsertList.get(0)).asOfOperation;
                }
                else
                {
                    arrayToInsert = new AtomicOperation[toInsertList.size()];
                    for(int i=0;i<toInsertList.size();i++)
                    {
                        arrayToInsert[i] = ((ObjectWithMapperStackAndAsOfOperation) toInsertList.get(i)).asOfOperation;
                    }
                }
                ObjectWithMapperStackAndAsOfOperation toInsert = (ObjectWithMapperStackAndAsOfOperation) toInsertList.get(0);
                MapperStackImpl insertPosition = toInsert.asOfAttributeWithMapperStack.getMapperStack();
                Operation result = this.op.insertAsOfEqOperation(arrayToInsert, insertPosition, this);
                if (result == null && insertPosition.isAtContainerBoundry())
                {
                    MapperStackImpl copy = (MapperStackImpl) insertPosition.clone();
                    while(copy.isAtContainerBoundry()) copy.popMapperContainer();
                    result = this.op.insertAsOfEqOperation(arrayToInsert, copy, this);
                }
                if (result == null)
                {
                    if (orderBy == null)
                    {
                        throw new RuntimeException("failed to insert as of operation");
                    }
                }
                else
                {
                    this.op = result;
                    insertedDefault = true;
                }
            }
            if (insertedDefault)
            {
                this.registeredAsOfAttributesWithMapperStack.clear();
                this.asOfOperationMap.clear();
                if (this.equalityAsOfOperationMap != null) this.equalityAsOfOperationMap.clear();
                this.mapperStackImpl = new MapperStackImpl();
                this.processedAsOfEqualities = false;
                this.registerAsOfAttributes();
                populateAllAsOfAttributes();
            }
        }
    }

    public void substituteContainer(Object originalContainer, Object newContainer)
    {
        // this is too slow and most operations don't need it.
        // todo: figure out how to speed this up.
//        for(int i=currentInsertPos+1;i<this.allToInsert.length;i++)
//        {
//            InternalList toInsertList = allToInsert[i];
//            for(int j=0;j<toInsertList.size();j++)
//            {
//                ObjectWithMapperStackAndAsOfOperation toInsert = (ObjectWithMapperStackAndAsOfOperation) toInsertList.get(j);
//                toInsert.asOfAttributeWithMapperStack.getMapperStack().substituteContainer(originalContainer, newContainer);
//            }
//        }
    }

    private void lookForMissingDefaults()
    {
        if (allAsOfAttributesWithMapperStack.length > 1) CollectionUtil.psort(allAsOfAttributesWithMapperStack, AS_OF_ATTRIBUTE_WITH_STACK_COMPARATOR);
        for(int i=0;i<this.allAsOfAttributesWithMapperStack.length;i++)
        {
            ObjectWithMapperStack<TemporalAttribute> asOfAttributeWithMapperStack = allAsOfAttributesWithMapperStack[i];
            if (this.getAsOfOperation(asOfAttributeWithMapperStack) == null)
            {
                TemporalAttribute temporalAttribute = asOfAttributeWithMapperStack.getObject();
                if (temporalAttribute.isAsOfAttribute())
                {
                    AsOfAttribute asOfAttribute = (AsOfAttribute) temporalAttribute;
                    if (asOfAttribute.getDefaultDate() != null)
                    {
                        AsOfOperation asOfOperation = (AsOfOperation)asOfAttribute.eq(asOfAttribute.getDefaultDate());
                        addToInsert(asOfAttributeWithMapperStack, (AtomicOperation) asOfOperation);
                        this.setAsOfOperation(asOfAttributeWithMapperStack, asOfOperation);
                        this.processedAsOfEqualities = false;
                    }
                }
            }
        }
    }

    private void addToInsert(ObjectWithMapperStack asOfAttributeWithMapperStack, AtomicOperation asOfOperation)
    {
        if (asOfOperation == null) return;
        InternalList value;
        if (asOfAttributeWithMapperStackToInsert == null)
        {
            asOfAttributeWithMapperStackToInsert = new UnifiedMap<MapperStack, InternalList>(4);
            value = new InternalList(2);
            asOfAttributeWithMapperStackToInsert.put(asOfAttributeWithMapperStack.getMapperStack(), value);
        }
        else
        {
            value = asOfAttributeWithMapperStackToInsert.get(asOfAttributeWithMapperStack.getMapperStack());
            if (value == null)
            {
                value = new InternalList(2);
                asOfAttributeWithMapperStackToInsert.put(asOfAttributeWithMapperStack.getMapperStack(), value);
            }
        }
        value.add(new ObjectWithMapperStackAndAsOfOperation(asOfAttributeWithMapperStack, asOfOperation));
    }

    private void populateAllAsOfAttributes()
    {
        this.allAsOfAttributesWithMapperStack = new ObjectWithMapperStack[registeredAsOfAttributesWithMapperStack.size()];
        int count = 0;
        for(Iterator it = registeredAsOfAttributesWithMapperStack.iterator(); it.hasNext(); count++)
        {
            this.allAsOfAttributesWithMapperStack[count] = (ObjectWithMapperStack) it.next();
        }
    }

    private void registerAsOfAttributes()
    {
        op.registerAsOfAttributesAndOperations(this);
        if (this.orderBy != null)
        {
            orderBy.registerAsOfAttributes(this);
        }
//        this.registerAsOfAttributes(op.getResultObjectPortal().getFinder().getAsOfAttributes());

        this.hasAsOfAttributes = this.registeredAsOfAttributesWithMapperStack != null;
    }

    public ObjectWithMapperStack[] getAllAsOfAttributesWithMapperStack()
    {
        if (!this.hasAsOfAttributes)
        {
            return EMPTY_ARRAY;
        }
        return allAsOfAttributesWithMapperStack;
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
        return mapperStackImpl.constructWithMapperStack(o, getMsiPool());
    }

    public ObjectWithMapperStack constructWithMapperStackWithoutLastMapper(Object o)
    {
        return mapperStackImpl.constructWithMapperStackWithoutLastMapper(o, getMsiPool());
    }

    public MapperStackImpl getCurrentMapperList()
    {
        return mapperStackImpl.getCurrentMapperList();
    }

    public void setEqualityAsOfOperation(TemporalAttribute left, TemporalAttribute right)
    {
        ObjectWithMapperStack<TemporalAttribute> leftWithMapperStack = this.constructWithMapperStackWithoutLastMapper(left);
        ObjectWithMapperStack<TemporalAttribute> rightWithMapperStack = this.constructWithMapperStack(right);
        addEqualityAsOfOperation(leftWithMapperStack, rightWithMapperStack);
        addEqualityAsOfOperation(rightWithMapperStack, leftWithMapperStack);
    }

    private void addEqualityAsOfOperation(ObjectWithMapperStack<TemporalAttribute> left, ObjectWithMapperStack<TemporalAttribute> right)
    {
        InternalList existing = this.getEqualityAsOfOperationMap().get(left);
        if (existing == null)
        {
            existing = new InternalList(4);
            this.getEqualityAsOfOperationMap().put(left, existing);
        }
        existing.add(right);
    }

    public void setAsOfOperation(ObjectWithMapperStack<? extends TemporalAttribute> attributeWithMapperStack, AsOfOperation op)
    {
        MapperStackImpl atBoundary = this.mapperStackImpl.ifAtContainerBoundaryCloneAndPop();
        this.setAsOfOperation(attributeWithMapperStack, new ObjectWithMapperStack<AsOfOperation>(atBoundary, op, getMsiPool()));
    }

    private boolean setAsOfOperation(ObjectWithMapperStack<? extends TemporalAttribute> attributeWithMapperStack, ObjectWithMapperStack<AsOfOperation> op)
    {
        ObjectWithMapperStack asOfEqOp = this.getAsOfOperationMap().get(attributeWithMapperStack);
        if (asOfEqOp == null)
        {
            return setAsOfOperationUnconditionally(attributeWithMapperStack, op);
        }
        else
        {

            AsOfOperation newAsOfOperation = op.getObject();
            AsOfOperation existingAsOfOperation = (AsOfOperation) asOfEqOp.getObject();
            if (existingAsOfOperation.zGetAsOfOperationPriority() < newAsOfOperation.zGetAsOfOperationPriority())
            {
                return setAsOfOperationUnconditionally(attributeWithMapperStack, op);
            }
            else if (newAsOfOperation instanceof AsOfEqOperation && existingAsOfOperation instanceof AsOfEqOperation)
            {
                AsOfEqOperation first = (AsOfEqOperation) newAsOfOperation;
                AsOfEqOperation second = (AsOfEqOperation) existingAsOfOperation;
                if (!first.getParameter().equals(second.getParameter())) throw new MithraBusinessException("can't have multiple asOf operations");
            }

        }

        return false;
    }

    private boolean setAsOfOperationUnconditionally(ObjectWithMapperStack<? extends TemporalAttribute> attributeWithMapperStack, ObjectWithMapperStack<AsOfOperation> op)
    {
        MapperStackImpl mapperStack = attributeWithMapperStack.getMapperStack();
        if (mapperStack.isAtContainerBoundry())
        {
            MapperStackImpl copy = mapperStack.ifAtContainerBoundaryCloneAndPop();
            this.getAsOfOperationMap().put(new ObjectWithMapperStack<TemporalAttribute>(copy, attributeWithMapperStack.getObject(), getMsiPool()), op);
        }
        else
        {
            this.getAsOfOperationMap().put(attributeWithMapperStack, op);
        }
        return true;
    }

    public void setAsOfEqOperation(AsOfEqOperation op)
    {
        this.setAsOfOperation(new ObjectWithMapperStack<TemporalAttribute>(this.mapperStackImpl, (TemporalAttribute) op.getAttribute(), getMsiPool()), op);
    }

    public void setAsOfEdgePointOperation(AsOfEdgePointOperation op)
    {
        this.setAsOfOperation(new ObjectWithMapperStack<TemporalAttribute>(this.mapperStackImpl, op.getAsOfAttribute(), getMsiPool()), op);
    }

    public UnifiedMap<MapperStackImpl, MapperStackImpl> getMsiPool()
    {
        if (msiPool == null) msiPool = UnifiedMap.newMap();
        return msiPool;
    }

    public ObjectWithMapperStack getAsOfOperation(ObjectWithMapperStack<? extends TemporalAttribute> asOfAttributeWithMapperStack)
    {
        if (this.equalityAsOfOperationMap != null && !processedAsOfEqualities)
        {
            processAsOfAttributeEqualities();
        }
        ObjectWithMapperStack<AsOfOperation> aop = this.getAsOfOperationMap().get(asOfAttributeWithMapperStack);
        if (aop == null && asOfAttributeWithMapperStack.getMapperStack().isAtContainerBoundry())
        {
            MapperStackImpl copy =  asOfAttributeWithMapperStack.getMapperStack().ifAtContainerBoundaryCloneAndPop();
            aop = this.getAsOfOperationMap().get(new ObjectWithMapperStack<TemporalAttribute>(copy, asOfAttributeWithMapperStack.getObject(), getMsiPool()));
        }
        return aop;
    }

    private void processAsOfAttributeEqualities()
    {
        InternalList attributesToProcess = new InternalList(this.getEqualityAsOfOperationMap().keySet());
        int numProcessedThisLoop;
        do
        {
            numProcessedThisLoop = 0;
            for (int i=0;i<attributesToProcess.size();)
            {
                ObjectWithMapperStack<TemporalAttribute> asOfAttribute = (ObjectWithMapperStack<TemporalAttribute>) attributesToProcess.get(i);
                if (processAsOfEquality(asOfAttribute))
                {
                    attributesToProcess.remove(i);
                    numProcessedThisLoop++;
                }
                else i++;
            }
        }
        while (numProcessedThisLoop > 0);
        this.processedAsOfEqualities = true;
    }

    private boolean processAsOfEquality(ObjectWithMapperStack<TemporalAttribute> attribute)
    {
        InternalList equalities = this.equalityAsOfOperationMap.get(attribute);
        if (equalities != null)
        {
            ObjectWithMapperStack<AsOfOperation> currentAsOfOperation = this.getAsOfOperationMap().get(attribute);
            if (currentAsOfOperation != null)
            {
                for(int i=0;i<equalities.size();i++)
                {
                    ObjectWithMapperStack<TemporalAttribute> rightAttribute = (ObjectWithMapperStack<TemporalAttribute>)equalities.get(i);
                    if (this.setAsOfOperation(rightAttribute, currentAsOfOperation))
                    {
                        addToInsert(rightAttribute,
                                currentAsOfOperation.getObject().createAsOfOperationCopy(rightAttribute.getObject(), this.op));
                    }
                }
                return true;
            }
        }
        return false;
    }

    private UnifiedMap<ObjectWithMapperStack<? extends TemporalAttribute>, ObjectWithMapperStack<AsOfOperation>> getAsOfOperationMap()
    {
        if (asOfOperationMap == null) asOfOperationMap = UnifiedMap.newMap(3);
        return asOfOperationMap;
    }

    private UnifiedMap<ObjectWithMapperStack<? extends TemporalAttribute>, InternalList> getEqualityAsOfOperationMap()
    {
        if (equalityAsOfOperationMap == null) equalityAsOfOperationMap = UnifiedMap.newMap(3);
        return equalityAsOfOperationMap;
    }

    public boolean hasAsOfAttributes()
    {
        return this.hasAsOfAttributes;
    }

    private ObjectWithMapperStack<TemporalAttribute> getOrCreateTemp()
    {
        if (temp == null) temp = new ObjectWithMapperStack<TemporalAttribute>(null, null);
        temp.resetWithoutClone(null, null);
        return temp;
    }

    public void registerAsOfAttributes(AsOfAttribute[] asOfAttributes)
    {
        if (asOfAttributes != null)
        {
            initRegisteredAsOfAttributesWithMapperStack();
            //todo: pop to container boundry
            ObjectWithMapperStack<TemporalAttribute> tempObject = getOrCreateTemp();
            MapperStackImpl atBoundary = this.mapperStackImpl.ifAtContainerBoundaryCloneAndPop();
            for(int i=0;i<asOfAttributes.length;i++)
            {
                tempObject.resetWithoutClone(atBoundary, asOfAttributes[i]);
                if (!this.registeredAsOfAttributesWithMapperStack.contains(tempObject))
                {
                    this.registeredAsOfAttributesWithMapperStack.add(tempObject.copyWithInternalClone(getMsiPool()));
                }
            }
        }
    }

    public void registerAsOfAttributesWithoutLastMapper(AsOfAttribute[] asOfAttributes)
    {
        if (asOfAttributes != null)
        {
            initRegisteredAsOfAttributesWithMapperStack();
            for(int i=0;i<asOfAttributes.length;i++)
            {
                this.registeredAsOfAttributesWithMapperStack.add(this.constructWithMapperStackWithoutLastMapper(asOfAttributes[i]));
            }
        }
    }

    private void initRegisteredAsOfAttributesWithMapperStack()
    {
        if (this.registeredAsOfAttributesWithMapperStack == null)
        {
            this.registeredAsOfAttributesWithMapperStack = new UnifiedSet<ObjectWithMapperStack<TemporalAttribute>>(8);
        }
    }

    public TupleTempContext getOrCreateMultiInTempContext(MultiInOperation op)
    {
        if (this.multiInTempContexts == null)
        {
            this.multiInTempContexts = FastList.newList(2);
            return createAndAddTempContext(op);
        }
        for(int i=0;i<multiInTempContexts.size();i++)
        {
            Pair<MultiInOperation, TupleTempContext> tupleTempContextPair = multiInTempContexts.get(i);
            if (tupleTempContextPair.getOne() == op)
            {
                return tupleTempContextPair.getTwo();
            }
        }
        return createAndAddTempContext(op);
    }

    private TupleTempContext createAndAddTempContext(MultiInOperation op)
    {
        TupleTempContext result = op.createTempContext();
        this.multiInTempContexts.add(new Pair<MultiInOperation, TupleTempContext>(op, result));
        return result;
    }

    public void registerTimestampTemporalAttribute(ObjectWithMapperStack<TemporalAttribute> attribute)
    {
        initRegisteredAsOfAttributesWithMapperStack();
        this.registeredAsOfAttributesWithMapperStack.add(attribute);
    }

    private static final class ObjectWithMapperStackAndAsOfOperation
    {
        private ObjectWithMapperStack asOfAttributeWithMapperStack;
        private AtomicOperation asOfOperation;

        private ObjectWithMapperStackAndAsOfOperation(ObjectWithMapperStack asOfAttributeWithMapperStack, AtomicOperation asOfOperation)
        {
            this.asOfAttributeWithMapperStack = asOfAttributeWithMapperStack;
            this.asOfOperation = asOfOperation;
        }
    }

    private static final class DeepFirstComparator implements Comparator<InternalList>
    {
        public int compare(InternalList leftList, InternalList rightList)
        {
            ObjectWithMapperStackAndAsOfOperation left = (ObjectWithMapperStackAndAsOfOperation) leftList.get(0);
            ObjectWithMapperStackAndAsOfOperation right = (ObjectWithMapperStackAndAsOfOperation) rightList.get(0);
            int result = right.asOfAttributeWithMapperStack.getMapperStack().getMapperStack().size() -
                    left.asOfAttributeWithMapperStack.getMapperStack().getMapperStack().size();
            if (result == 0)
            {
                result = right.asOfAttributeWithMapperStack.getMapperStack().getMapperContainerStack().size() -
                        left.asOfAttributeWithMapperStack.getMapperStack().getMapperContainerStack().size();
            }
            return result;
        }
    }

    private static class AsOfAttributeWithStackComparator implements Comparator<ObjectWithMapperStack<TemporalAttribute>>
    {
        public int compare(ObjectWithMapperStack<TemporalAttribute> o1, ObjectWithMapperStack<TemporalAttribute> o2)
        {
            int result = o1.getMapperStack().compareTo(o2.getMapperStack());
            if (result == 0)
            {
                int leftWeight = 0;
                int rightWeight = 0;
                if (o1.getObject().isAsOfAttribute())
                {
                    leftWeight = ((AsOfAttribute)o1.getObject()).isProcessingDate() ? 100 : 50;
                }
                if (o2.getObject().isAsOfAttribute())
                {
                    rightWeight = ((AsOfAttribute)o2.getObject()).isProcessingDate() ? 100 : 50;
                }
                result = leftWeight - rightWeight;
            }
            return result;
        }
    }
}
