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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.Operation;

import javax.transaction.Synchronization;
import javax.transaction.Status;



public class DeleteAllInBatchesTransactionalCommand implements TransactionalCommand, Synchronization
{

    protected AbstractTransactionalOperationBasedList abstractTransactionalOperationBasedList;
    protected DelegatingList delegatingList;
    private final int[] batchSize;

    public DeleteAllInBatchesTransactionalCommand(DelegatingList delegatingList, AbstractTransactionalOperationBasedList abstractTransactionalOperationBasedList,
            int[] batchSize)
    {
        this.abstractTransactionalOperationBasedList = abstractTransactionalOperationBasedList;
        this.batchSize = batchSize;
        this.delegatingList = delegatingList;
    }

    public Object executeTransaction(MithraTransaction tx) throws Throwable
    {
        tx.registerSynchronization(this);
        Operation op = delegatingList.getOperation();
        prepareCache(op);
        int deletedRows = tx.deleteBatchUsingOperation(op, batchSize[0]);
        return Integer.valueOf(deletedRows);
    }

    protected void prepareCache(Operation op)
    {
        MithraObjectPortal resultObjectPortal = op.getResultObjectPortal();
        resultObjectPortal.prepareForMassDelete(op, abstractTransactionalOperationBasedList.isForceImplicitJoin());
    }

    public void afterCompletion(int status)
    {
        if (status != Status.STATUS_COMMITTED)
        {
            batchSize[0] /= 2;
        }
    }

    public void beforeCompletion()
    {
        // nothing to do
    }

}
