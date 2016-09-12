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

package com.gs.fw.common.mithra.behavior.detached;

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalState;
import com.gs.fw.common.mithra.behavior.AbstractTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.behavior.inmemory.InMemoryTxEnrollBehavior;



public class DetachedTxEnrollBehavior extends InMemoryTxEnrollBehavior
{

    public DetachedTxEnrollBehavior()
    {
        super(true);
    }

    @Override
    public TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        if (mto.zEnrollInTransactionForDelete(prevState, new TransactionalState(threadTx, PersistenceState.DETACHED)))
        {
            return AbstractTransactionalBehavior.getDetachedSameTxBehavior();
        }
        return null;
    }

    public TransactionalBehavior enrollInTransactionForRead(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        if (mto.zEnrollInTransactionForRead(prevState, threadTx, PersistenceState.DETACHED))
        {
            return AbstractTransactionalBehavior.getDetachedSameTxBehavior();
        }
        return null;
    }

    public TransactionalBehavior enrollInTransactionForWrite(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        if (mto.zEnrollInTransactionForWrite(prevState, new TransactionalState(threadTx, PersistenceState.DETACHED)))
        {
            return AbstractTransactionalBehavior.getDetachedSameTxBehavior();
        }
        return null;
    }

    public MithraTransactionalObject updateOriginalOrInsert(MithraTransactionalObject obj)
    {
        throw new RuntimeException("should never get here");
    }
}
