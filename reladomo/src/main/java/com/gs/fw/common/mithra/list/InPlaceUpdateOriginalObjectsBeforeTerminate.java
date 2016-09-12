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

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.List;



public class InPlaceUpdateOriginalObjectsBeforeTerminate implements TransactionalCommand
{

    private List toUpdateOrInsertList;
    private List toDeleteList;
    private MithraList list;

    public InPlaceUpdateOriginalObjectsBeforeTerminate(List toUpdateOrInsertList, List toDeleteList, MithraList list)
    {
        this.toUpdateOrInsertList = toUpdateOrInsertList;
        this.toDeleteList = toDeleteList;
        this.list = list;
    }

    public Object executeTransaction(MithraTransaction tx) throws Throwable
    {
        if (list != null && (this.toDeleteList != null || this.toUpdateOrInsertList != null))
        {
            Operation op = list.getOperation();
            if (op != null)
            {
                if (!op.getResultObjectPortal().getTxParticipationMode(tx).isOptimisticLocking())
                {
                    list.forceResolve();
                }
            }
            else
            {
                list.forceRefresh();
            }
        }
        // delete
        if (this.toDeleteList != null)
        {
            for(int i=0;i<toDeleteList.size();i++)
            {
                MithraTransactionalObject obj = (MithraTransactionalObject) toDeleteList.get(i);
                obj.zCascadeUpdateInPlaceBeforeTerminate();
            }
        }
        if (toUpdateOrInsertList != null)
        {
            for(int i=0;i<toUpdateOrInsertList.size();i++)
            {
                MithraTransactionalObject obj = (MithraTransactionalObject) toUpdateOrInsertList.get(i);
                obj.zCascadeUpdateInPlaceBeforeTerminate();
            }
        }
        return null;
    }
}
