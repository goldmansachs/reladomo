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

package com.gs.fw.common.mithra.test.pure;

import com.gs.fw.common.mithra.test.RemoteMithraServerTestCase;
import com.gs.fw.common.mithra.test.TestMaxFromTable;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.inherited.TestTxInherited;

import java.util.HashSet;
import java.util.Set;

public class TestPureObjectsRemote extends RemoteMithraServerTestCase
{

    private TestPureObjects testPureObjects;

    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();
        super.addTestClassesFromOther(new TestPureObjects(), result);
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
        result.add(WallCrossImpl.class);
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.testPureObjects = new TestPureObjects();
    }

    public void workerVmSetUp()
    {
        super.workerVmSetUp();
    }

    public void testType2FindOne()
    {
        this.testPureObjects.testType2FindOne();
    }

    public void testType2DatedFindOne()
    {
        //this.testPureObjects.testType2DatedFindOne();
    }

    public void testType2FindAll()
    {
        this.testPureObjects.testType2FindAll();
    }

    public void testType2DatedFindAll()
    {
        //this.testPureObjects.testType2DatedFindAll();
    }

    public void testType2Insert()
    {
        this.testPureObjects.testType2Insert();
    }

    public void testType2DatedInsert()
    {
        //this.testPureObjects.testType2DatedInsert();
    }
    public void testType2BatchInsert()
    {
        this.testPureObjects.testType2BatchInsert();
    }

    public void testType2DatedBatchInsert()
    {
        //this.testPureObjects.testType2DatedBatchInsert();
    }

    public void testType2Update()
    {
        this.testPureObjects.testType2Update();
    }

    public void testType2DatedUpdate()
    {
        //this.testPureObjects.testType2DatedUpdate();
    }

    public void testType2DatedUpdateUntil()
    {
        //this.testPureObjects.testType2DatedUpdateUntil();
    }

    public void testType2DatedIncrement()
    {
        //this.testPureObjects.testType2DatedIncrement();
    }

    public void testType2DatedIncrementUntil()
    {
        //this.testPureObjects.testType2DatedIncrementUntil();
    }

    public void testType2BatchUpdate()
    {
        this.testPureObjects.testType2BatchUpdate();
    }

    public void testType2DatedBatchUpdate()
    {
       // this.testPureObjects.testType2DatedBatchUpdate();
    }

    public void testType2MultiUpdate()
    {
        this.testPureObjects.testType2MultiUpdate();
    }

    public void testType2DatedMultiUpdate()
    {
        //this.testPureObjects.testType2DatedMultiUpdate();
    }

    public void testType2Delete()
    {
        this.testPureObjects.testType2Delete();
    }

    public void testType2BatchDelete()
    {
        this.testPureObjects.testType2BatchDelete();
    }

    public void testType2OperationBasedDelete()
    {
        this.testPureObjects.testType2OperationBasedDelete();
    }
}
