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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;


public class PurgeAllTransactionalCommand implements TransactionalCommand
{

    private AbstractTransactionalOperationBasedList abstractTransactionalOperationBasedList;
    private DelegatingList delegatingList;

    public PurgeAllTransactionalCommand(DelegatingList delegatingList, AbstractTransactionalOperationBasedList abstractTransactionalOperationBasedList)
    {
        this.abstractTransactionalOperationBasedList = abstractTransactionalOperationBasedList;
        this.delegatingList = delegatingList;
    }

    public Object executeTransaction(MithraTransaction tx) throws Throwable
    {
        Operation op = delegatingList.getOperation();
        MithraObjectPortal resultObjectPortal = op.getResultObjectPortal();
        resultObjectPortal.prepareForMassPurge(op, this.abstractTransactionalOperationBasedList.isForceImplicitJoin());
        tx.purgeUsingOperation(op);
        return null;
    }
}
