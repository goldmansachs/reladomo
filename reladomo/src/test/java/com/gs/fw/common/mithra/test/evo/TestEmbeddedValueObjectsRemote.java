
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

package com.gs.fw.common.mithra.test.evo;

import com.gs.fw.common.mithra.test.RemoteMithraServerTestCase;
import com.gs.fw.common.mithra.test.TestMaxFromTable;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.inherited.TestTxInherited;

import java.util.HashSet;
import java.util.Set;

public class TestEmbeddedValueObjectsRemote extends RemoteMithraServerTestCase
{
    
    private TestEmbeddedValueObjects testEmbeddedValueObjects;

    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();
        super.addTestClassesFromOther(new TestEmbeddedValueObjects(), result);
        this.addTestClassesToBeDecoupled(result);
        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    //TODO: remove this
    private void addTestClassesToBeDecoupled(Set<Class> result)
    {
        addTestClassesFromOther(new TestMaxFromTable(), result);
        addTestClassesFromOther(new TestTxInherited(), result);
        result.add(Order.class);
        result.add(OrderItem.class);
        result.add(OrderStatus.class);
        result.add(OrderItemStatus.class);
        result.add(TinyBalance.class);
        result.add(NonAuditedBalance.class);
        result.add(TestAgeBalanceSheetRunRate.class);
        result.add(TestPositionPrice.class);
        result.add(TestBinaryArray.class);
        result.add(AccountTransactionException.class);
        result.add(SpecialAccountTransactionException.class);
        result.add(MithraTestSequence.class);
        result.add(NotDatedWithNullablePK.class);
        result.add(DatedWithNullablePK.class);
        result.add(AuditOnlyBalance.class);
        result.add(OptimisticOrder.class);
        result.add(OptimisticOrderWithTimestamp.class);
        result.add(SalesLineItem.class);
        result.add(Sale.class);
        result.add(Seller.class);
        result.add(ProductSpecification.class);
        result.add(SalesLineItem.class);
        result.add(SpecialAccount.class);
        result.add(FullyCachedTinyBalance.class);
        result.add(WallCrossImpl.class);
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.testEmbeddedValueObjects = new TestEmbeddedValueObjects();
    }

    public void slaveVmSetUp()
    {
        super.slaveVmSetUp();
    }

    public void testType1FindOne()
    {
        this.testEmbeddedValueObjects.testType1FindOne();
    }

    public void testType2FindOne()
    {
        this.testEmbeddedValueObjects.testType2FindOne();
    }

    public void testType3FindOne()
    {
        this.testEmbeddedValueObjects.testType3FindOne();
    }

    public void testType1DatedFindOne()
    {
        this.testEmbeddedValueObjects.testType1DatedFindOne();
    }

    public void testType2DatedFindOne()
    {
        this.testEmbeddedValueObjects.testType2DatedFindOne();
    }

//    public void testType3DatedFindOne()
//    {
//        this.testEmbeddedValueObjects.testType3DatedFindOne();
//    }

    public void testType1FindAll()
    {
        this.testEmbeddedValueObjects.testType1FindAll();
    }

    public void testType2FindAll()
    {
        this.testEmbeddedValueObjects.testType2FindAll();
    }

    public void testType3FindAll()
    {
        this.testEmbeddedValueObjects.testType3FindAll();
    }

    public void testType1DatedFindAll()
    {
        this.testEmbeddedValueObjects.testType1DatedFindAll();
    }

    public void testType2DatedFindAll()
    {
        this.testEmbeddedValueObjects.testType2DatedFindAll();
    }

//    public void testType3DatedFindAll()
//    {
//        this.testEmbeddedValueObjects.testType3DatedFindAll();
//    }

    public void testType1Insert()
    {
        this.testEmbeddedValueObjects.testType1Insert();
    }

    public void testType2Insert()
    {
        this.testEmbeddedValueObjects.testType2Insert();
    }

    public void testType3Insert()
    {
        this.testEmbeddedValueObjects.testType3Insert();
    }

    public void testType1DatedInsert()
    {
        this.testEmbeddedValueObjects.testType1DatedInsert();
    }

    public void testType2DatedInsert()
    {
        this.testEmbeddedValueObjects.testType2DatedInsert();
    }

//    public void testType3DatedInsert()
//    {
//        this.testEmbeddedValueObjects.testType3DatedInsert();
//    }

    public void testType1BatchInsert()
    {
        this.testEmbeddedValueObjects.testType1BatchInsert();
    }

    public void testType2BatchInsert()
    {
        this.testEmbeddedValueObjects.testType2BatchInsert();
    }

    public void testType3BatchInsert()
    {
        this.testEmbeddedValueObjects.testType3BatchInsert();
    }

    public void testType1DatedBatchInsert()
    {
        this.testEmbeddedValueObjects.testType1DatedBatchInsert();
    }

    public void testType2DatedBatchInsert()
    {
        this.testEmbeddedValueObjects.testType2DatedBatchInsert();
    }

//    public void testType3DatedBatchInsert()
//    {
//        this.testEmbeddedValueObjects.testType3DatedBatchInsert();
//    }

    public void testType1Update()
    {
        this.testEmbeddedValueObjects.testType1Update();
    }

    public void testType2Update()
    {
        this.testEmbeddedValueObjects.testType2Update();
    }

    public void testType3Update()
    {
        this.testEmbeddedValueObjects.testType3Update();
    }

    public void testType1DatedUpdate()
    {
        this.testEmbeddedValueObjects.testType1DatedUpdate();
    }

    public void testType2DatedUpdate()
    {
        this.testEmbeddedValueObjects.testType2DatedUpdate();
    }

//    public void testType3DatedUpdate()
//    {
//        this.testEmbeddedValueObjects.testType3DatedUpdate();
//    }

    public void testType1DatedUpdateUntil()
    {
        this.testEmbeddedValueObjects.testType1DatedUpdateUntil();
    }

    public void testType2DatedUpdateUntil()
    {
        this.testEmbeddedValueObjects.testType2DatedUpdateUntil();
    }

//    public void testType3DatedUpdateUntil()
//    {
//        this.testEmbeddedValueObjects.testType3DatedUpdateUntil();
//    }

    public void testType1DatedIncrement()
    {
        this.testEmbeddedValueObjects.testType1DatedIncrement();
    }

    public void testType2DatedIncrement()
    {
        this.testEmbeddedValueObjects.testType2DatedIncrement();
    }

//    public void testType3DatedIncrement()
//    {
//        this.testEmbeddedValueObjects.testType3DatedIncrement();
//    }

    public void testType1DatedIncrementUntil()
    {
        this.testEmbeddedValueObjects.testType1DatedIncrementUntil();
    }

    public void testType2DatedIncrementUntil()
    {
        this.testEmbeddedValueObjects.testType2DatedIncrementUntil();
    }

//    public void testType3DatedIncrementUntil()
//    {
//        this.testEmbeddedValueObjects.testEvoType3DatedIncrementUntil();
//    }

    public void testType1BatchUpdate()
    {
        this.testEmbeddedValueObjects.testType1BatchUpdate();
    }

    public void testType2BatchUpdate()
    {
        this.testEmbeddedValueObjects.testType2BatchUpdate();
    }

    public void testType3BatchUpdate()
    {
        this.testEmbeddedValueObjects.testType3BatchUpdate();
    }

    public void testType1DatedBatchUpdate()
    {
        this.testEmbeddedValueObjects.testType1DatedBatchUpdate();
    }

    public void testType2DatedBatchUpdate()
    {
        this.testEmbeddedValueObjects.testType2DatedBatchUpdate();
    }

//    public void testType3DatedBatchUpdate()
//    {
//        this.testEmbeddedValueObjects.testType3DatedBatchUpdate();
//    }

    public void testType1MultiUpdate()
    {
        this.testEmbeddedValueObjects.testType1MultiUpdate();
    }

    public void testType2MultiUpdate()
    {
        this.testEmbeddedValueObjects.testType2MultiUpdate();
    }

    public void testType3MultiUpdate()
    {
        this.testEmbeddedValueObjects.testType3MultiUpdate();
    }

    public void testType1DatedMultiUpdate()
    {
        this.testEmbeddedValueObjects.testType1DatedMultiUpdate();
    }

    public void testType2DatedMultiUpdate()
    {
        this.testEmbeddedValueObjects.testType2DatedMultiUpdate();
    }

//    public void testType3DatedMultiUpdate()
//    {
//        this.testEmbeddedValueObjects.testType3DatedMultiUpdate();
//    }

    public void testType1Delete()
    {
        this.testEmbeddedValueObjects.testType1Delete();
    }

    public void testType2Delete()
    {
        this.testEmbeddedValueObjects.testType2Delete();
    }

    public void testType3Delete()
    {
        this.testEmbeddedValueObjects.testType3Delete();
    }

    public void testType1BatchDelete()
    {
        this.testEmbeddedValueObjects.testType1BatchDelete();
    }

    public void testType2BatchDelete()
    {
        this.testEmbeddedValueObjects.testType2BatchDelete();
    }

    public void testType3BatchDelete()
    {
        this.testEmbeddedValueObjects.testType3BatchDelete();
    }

    public void testType1OperationBasedDelete()
    {
        this.testEmbeddedValueObjects.testType1OperationBasedDelete();
    }

    public void testType2OperationBasedDelete()
    {
        this.testEmbeddedValueObjects.testType2OperationBasedDelete();
    }

    public void testType3OperationBasedDelete()
    {
        this.testEmbeddedValueObjects.testType3OperationBasedDelete();
    }

    public void testType1ReadOnlyPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType1ReadOnlyPolymorphicEvo();
    }

    public void testType1TxnPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType1TxnPolymorphicEvo();
    }

    public void testType1DatedReadOnlyPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType1DatedReadOnlyPolymorphicEvo();
    }

    public void testType1DatedTxnPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType1DatedTxnPolymorphicEvo();
    }

    public void testType2ReadOnlyPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType2ReadOnlyPolymorphicEvo();
    }

    public void testType2TxnPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType2TxnPolymorphicEvo();
    }

    public void testType2DatedReadOnlyPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType2DatedReadOnlyPolymorphicEvo();
    }

    public void testType2DatedTxnPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType2DatedTxnPolymorphicEvo();
    }

    public void testType3ReadOnlyPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType3ReadOnlyPolymorphicEvo();
    }

    public void testType3TxnPolymorphicEvo()
    {
        this.testEmbeddedValueObjects.testType3TxnPolymorphicEvo();
    }

//    public void testType3DatedReadOnlyPolymorphicEvo()
//    {
//        this.testEmbeddedValueObjects.testType3DatedReadOnlyPolymorphicEvo();
//    }
//
//    public void testType3DatedTxnPolymorphicEvo()
//    {
//        this.testEmbeddedValueObjects.testType3DatedTxnPolymorphicEvo();
//    }
}
