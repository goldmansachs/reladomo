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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.transaction.MithraLocalTransaction;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;




public class TransactionalState
{

    private int persistenceState;
    private MithraDataObject txData; // when this is null, we're in shared-read mode
    private volatile Object currentTxOrArray; // can be null, a single MithraTransaction or an array of MithraTransaction
    private static final AtomicReferenceFieldUpdater currentTxOrArrayUpdater = AtomicReferenceFieldUpdater.newUpdater(TransactionalState.class, Object.class, "currentTxOrArray");

    public TransactionalState(MithraTransaction currentTx, int persistenceState)
    {
        this.currentTxOrArray = currentTx;
        this.persistenceState = persistenceState;
    }

    public TransactionalState(TransactionalState oldState, MithraTransaction threadTx, int persistenceState)
    {
        this.persistenceState = persistenceState;
        Object oldTxOrArray = oldState.currentTxOrArray;
        if (oldTxOrArray == null)
        {
            this.currentTxOrArray = threadTx;
        }
        else if (oldTxOrArray instanceof MithraTransaction)
        {
            MithraTransaction[] array = new MithraTransaction[2];
            array[0] = (MithraTransaction) oldTxOrArray;
            array[1] = threadTx;
            this.currentTxOrArray = array;
        }
        else
        {
            MithraTransaction[] oldArray = (MithraTransaction[]) oldTxOrArray;
            MithraTransaction[] array = new MithraTransaction[oldArray.length + 1];
            System.arraycopy(oldArray, 0, array, 0, oldArray.length);
            array[array.length - 1] = threadTx;
            this.currentTxOrArray = array;
        }
    }

    public int getPersistenceState()
    {
        return persistenceState;
    }

    public MithraDataObject getTxData()
    {
        return txData;
    }

    public MithraTransaction getCurrentTx()
    {
        //todo: concon
        throw new RuntimeException("concon");
    }

    public void setPersistenceState(int persistenceState)
    {
        if (this.txData == null) throw new RuntimeException("can only mutate writable state");
        this.persistenceState = persistenceState;
    }

    public void setTxData(MithraDataObject txData)
    {
        this.txData = txData;
    }

    public boolean isDeleted()
    {
        return this.persistenceState == PersistenceState.DELETED;
    }

    public boolean isInserted(int oldPersistenceState)
    {
        return this.persistenceState == PersistenceState.PERSISTED && oldPersistenceState == PersistenceState.IN_MEMORY;
    }

    public boolean isPersisted()
    {
        return this.persistenceState == PersistenceState.PERSISTED;
    }

    public boolean hasNoTransactions()
    {
        return this.currentTxOrArray == null;
    }

    public void waitForTransactions(MithraTransaction threadTx)
    {
        Object curTxOrArray = currentTxOrArray;
        if (curTxOrArray == null) return;
        if (curTxOrArray instanceof MithraTransaction)
        {
            ((MithraTransaction)curTxOrArray).waitForTransactionToFinish(threadTx);
        }
        else
        {
            MithraTransaction[] array = (MithraTransaction[]) curTxOrArray;
            for(MithraTransaction tx: array)
            {
                tx.waitForTransactionToFinish(threadTx);
            }
        }
    }

    public boolean isEnrolledForWrite(MithraTransaction threadTx)
    {
        return this.txData != null && threadTx.equals(currentTxOrArray);
    }

    public boolean hasNoTransactionsExcept(MithraTransaction threadTx)
    {
        return threadTx.equals(currentTxOrArray);
    }

    public boolean isParticipatingInReadOrWrite(MithraTransaction threadTx)
    {
        Object curTxOrArray = currentTxOrArray;
        if (threadTx.equals(curTxOrArray)) return true;
        if (curTxOrArray instanceof MithraTransaction[])
        {
            MithraTransaction[] array = (MithraTransaction[]) curTxOrArray;
            for(MithraTransaction tx: array)
            {
                if (threadTx.equals(tx)) return true;
            }
        }
        return false;
    }

    public void clearCurrentTx()
    {
        this.currentTxOrArray = null;
    }

    public void removeThreadTx(MithraLocalTransaction threadTx)
    {
        do
        {
            Object curTxOrArray = currentTxOrArray;
            if (threadTx.equals(curTxOrArray))
            {
                this.currentTxOrArray = null;
                return;
            }
            MithraTransaction[] array = (MithraTransaction[]) curTxOrArray;
            if (array.length == 2)
            {
                MithraTransaction replace = array[0].equals(threadTx) ? array[1] : array[0];
                if (currentTxOrArrayUpdater.compareAndSet(this, curTxOrArray, replace))
                {
                    return;
                }
            }
            else
            {
                if (removeFromLargeArray(threadTx, curTxOrArray, array)) return;
            }
        }
        while(true);
    }

    private boolean removeFromLargeArray(MithraLocalTransaction threadTx, Object curTxOrArray, MithraTransaction[] array)
    {
        MithraTransaction[] replace = new MithraTransaction[array.length - 1];
        int pos = 0;
        for(MithraTransaction tx: array)
        {
            if (!threadTx.equals(tx)) replace[pos++] = tx;
        }
        return currentTxOrArrayUpdater.compareAndSet(this, curTxOrArray, replace);
    }

    public void waitForTransactions()
    {
        Object curTxOrArray = currentTxOrArray;
        if (curTxOrArray == null) return;
        if (curTxOrArray instanceof MithraTransaction)
        {
            ((MithraTransaction)curTxOrArray).waitForTransactionToFinish();
        }
        else
        {
            MithraTransaction[] array = (MithraTransaction[]) curTxOrArray;
            for(MithraTransaction tx: array)
            {
                tx.waitForTransactionToFinish();
            }
        }
    }

    public MithraTransaction getExculsiveWriteTransaction()
    {
        if (txData != null) return (MithraTransaction) currentTxOrArray;
        return null;
    }

    public boolean isOnlyReader(MithraTransaction threadTx)
    {
        Object curTxOrArray = currentTxOrArray;
        return txData == null && curTxOrArray != null && curTxOrArray.equals(threadTx);
    }

    public void waitForWriteTransaction(MithraTransaction tx)
    {
        Object curTxOrArray = currentTxOrArray;
        if (this.txData != null && curTxOrArray != null)
        {
            ((MithraTransaction)curTxOrArray).waitForTransactionToFinish(tx);
        }
    }

    public boolean isEnrolledForWriteByOther(MithraTransaction threadTx)
    {
        return this.txData != null && currentTxOrArray != null && !currentTxOrArray.equals(threadTx);
    }

    public boolean isSharedReaderByOthers(MithraTransaction threadTx)
    {
        return this.txData == null && !this.isParticipatingInReadOrWrite(threadTx);
    }

    public void addToAllExcept(MithraLocalTransaction threadTx)
    {
        Object curTxOrArray = currentTxOrArray;
        if (curTxOrArray == null) return;
        if (curTxOrArray instanceof MithraTransaction)
        {
            if (curTxOrArray != threadTx) ((MithraTransaction)curTxOrArray).addSharedTransactionalState(this);
        }
        else
        {
            MithraTransaction[] array = (MithraTransaction[]) curTxOrArray;
            for(MithraTransaction tx: array)
            {
                if (tx != threadTx) tx.addSharedTransactionalState(this);
            }
        }
    }
}
