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

import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;import com.gs.fw.common.mithra.transaction.MithraLocalTransaction;

import java.sql.Timestamp;import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;



public class DatedTransactionalState
{

    private int persistenceState;
    private MithraDataObject txData;
    private byte dataVersion = -10;
    private boolean isCurrent;
    private TemporalContainer container;
    private Timestamp businessDate;
    private volatile Object currentTxOrArray; // can be null, a single MithraTransaction or an array of MithraTransaction
    private static final AtomicReferenceFieldUpdater currentTxOrArrayUpdater = AtomicReferenceFieldUpdater.newUpdater(DatedTransactionalState.class, Object.class, "currentTxOrArray");

    public DatedTransactionalState(MithraTransaction currentTx, int persistenceState,
        TemporalContainer container, MithraDataObject txData, Timestamp businessDate, boolean isCurrent)
    {
        this.persistenceState = persistenceState;
        this.txData = txData;
        if (txData != null)
        {
            this.dataVersion = txData.zGetDataVersion();
        }
        this.currentTxOrArray = currentTx;
        this.container = container;
        this.businessDate = businessDate;
        this.isCurrent = isCurrent;
    }

    public DatedTransactionalState(DatedTransactionalState oldState, MithraTransaction threadTx, int persistenceState)
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

    public void setContainer(TemporalContainer container)
    {
        this.container = container;
    }

    public MithraDataObject getTxData()
    {
        if (persistenceState == DatedPersistenceState.PERSISTED && isCurrent &&
                this.container != null && (this.txData == null || (this.txData.zGetDataVersion() != this.dataVersion)))
        {
            this.txData = this.container.getActiveDataFor(this.businessDate);
            if (this.txData == null)
            {
                if (this.container.isInactivatedOrSplit(this.businessDate))
                {
                    String msg = "cannot access deleted/terminated object. Check for call to terminate multiple times or check for bad chaining";

                    MithraDataObject anyData = container.getAnyData();
                    if (anyData != null)
                    {
                        msg += " on primary key: "+ anyData.zGetPrintablePrimaryKey();
                    }
                    throw new MithraDeletedException(msg);
                }
            }
            else
            {
                this.dataVersion = this.txData.zGetDataVersion();
            }
        }
        return txData;
    }

    public MithraDataObject getTxDataWithNoCheck()
    {
        return this.txData;
    }

    public void setPersistenceState(int persistenceState)
    {
        this.persistenceState = persistenceState;
    }

    public void setTxData(MithraDataObject txData)
    {
        if (!isExclusive())
        {
            throw new RuntimeException("Should not get here. Incorrect concurrency state.");
        }
        this.txData = txData;
        if (txData != null)
        {
            this.dataVersion = txData.zGetDataVersion();
        }
        else
        {
            this.dataVersion = -10;
        }
    }

    public boolean isDeleted()
    {
        if (this.persistenceState == DatedPersistenceState.DELETED)
        {
            return true;
        }
        if (this.container != null && (this.txData == null || (this.txData.zGetDataVersion() != this.dataVersion)))
        {
            this.txData = this.container.getActiveDataFor(this.businessDate);
            if (this.txData == null)
            {
                if (this.container.isInactivatedOrSplit(this.businessDate)) return true;
            }
        }
        return false;
    }

    public boolean isInserted(int oldPersistenceState)
    {
        return this.persistenceState == DatedPersistenceState.PERSISTED && oldPersistenceState == DatedPersistenceState.IN_MEMORY;
    }

    public boolean isPersisted()
    {
        return this.persistenceState == DatedPersistenceState.PERSISTED;
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
        return isExclusive() && threadTx.equals(currentTxOrArray);
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

    private boolean isExclusive()
    {
        return txData != null || container != null;
    }

    public MithraTransaction getExculsiveWriteTransaction()
    {
        if (isExclusive()) return (MithraTransaction) currentTxOrArray;
        return null;
    }

    public boolean isOnlyReader(MithraTransaction threadTx)
    {
        Object curTxOrArray = currentTxOrArray;
        return !isExclusive() && curTxOrArray != null && curTxOrArray.equals(threadTx);
    }

    public void waitForWriteTransaction(MithraTransaction tx)
    {
        Object curTxOrArray = currentTxOrArray;
        if (this.isExclusive() && curTxOrArray != null)
        {
            ((MithraTransaction)curTxOrArray).waitForTransactionToFinish(tx);
        }
    }

    public boolean isEnrolledForWriteByOther(MithraTransaction threadTx)
    {
        return isExclusive() && currentTxOrArray != null && !currentTxOrArray.equals(threadTx);
    }

    public boolean isSharedReaderByOthers(MithraTransaction threadTx)
    {
        return !isExclusive() && !this.isParticipatingInReadOrWrite(threadTx);
    }

    public void addToAllExcept(MithraLocalTransaction threadTx)
    {
        Object curTxOrArray = currentTxOrArray;
        if (curTxOrArray == null) return;
        if (curTxOrArray instanceof MithraTransaction)
        {
            if (curTxOrArray != threadTx) ((MithraTransaction)curTxOrArray).addSharedDatedTransactionalState(this);
        }
        else
        {
            MithraTransaction[] array = (MithraTransaction[]) curTxOrArray;
            for(MithraTransaction tx: array)
            {
                if (tx != threadTx) tx.addSharedDatedTransactionalState(this);
            }
        }
    }

    public void clearCurrentTx()
    {
        this.currentTxOrArray = null;
    }

    public boolean isTxDataDeleted()
    {
        if (txData == null) return false;
        if (persistenceState == DatedPersistenceState.PERSISTED && isCurrent &&
                this.container != null && this.txData.zGetDataVersion() != this.dataVersion)
        {
            MithraDataObject newData = this.container.getActiveDataFor(this.businessDate);
            if (newData == null)
            {
                return this.container.isInactivatedOrSplit(this.businessDate);
            }
            else
            {
                this.txData = newData;
                this.dataVersion = this.txData.zGetDataVersion();
            }
        }
        return false;

    }
}
