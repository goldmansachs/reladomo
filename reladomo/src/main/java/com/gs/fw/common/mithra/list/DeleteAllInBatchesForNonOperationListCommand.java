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

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalCommand;

import javax.transaction.Synchronization;
import javax.transaction.Status;
import java.util.List;



public class DeleteAllInBatchesForNonOperationListCommand implements TransactionalCommand, Synchronization
{

    private List toBeDeleted;
    private final int[] batchSize;
    private int currentStart;
    private int end;

    public DeleteAllInBatchesForNonOperationListCommand(List toBeDeleted, int[] batchSize)
    {
        this.toBeDeleted = toBeDeleted;
        this.batchSize = batchSize;
    }

    public Object executeTransaction(MithraTransaction tx) throws Throwable
    {
        tx.registerSynchronization(this);
        end = currentStart + batchSize[0];
        if (end > toBeDeleted.size()) end = toBeDeleted.size();
        List subList = toBeDeleted.subList(currentStart, end);
        for (int i = 0; i < subList.size(); i++)
        {
            ((MithraTransactionalObject) subList.get(i)).zPrepareForDelete();
        }
        for(int i=0;i < subList.size();i++)
        {
            ((MithraTransactionalObject)subList.get(i)).delete();
        }
        return subList.size();
    }

    public void afterCompletion(int status)
    {
        if (status == Status.STATUS_COMMITTED || status == Status.STATUS_COMMITTING)
        {
            currentStart = end;
        }
        else if (batchSize[0] > 100)
        {
            batchSize[0] /= 2;
        }
    }

    public void beforeCompletion()
    {
        // nothing to do
    }
}
