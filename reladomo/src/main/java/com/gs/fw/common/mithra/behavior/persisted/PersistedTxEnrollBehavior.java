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

package com.gs.fw.common.mithra.behavior.persisted;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.AbstractTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.util.Time;

import java.util.List;
import java.util.Date;
import java.sql.Timestamp;
import java.math.BigDecimal;



public class PersistedTxEnrollBehavior extends PersistedBehavior
{

    public PersistedTxEnrollBehavior()
    {
        super(NON_TRANSACTIONAL, null);
    }

    public MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject)
    {
        throw new RuntimeException("should never get here");
    }

    public TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        if (mto.zEnrollInTransactionForDelete(prevState, new TransactionalState(threadTx, PersistenceState.PERSISTED)))
        {
            return AbstractTransactionalBehavior.getPersistedSameTxBehavior();
        }
        return null;
    }

    public TransactionalBehavior enrollInTransactionForRead(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        mto.zRefreshWithLockForRead(this);
        return AbstractTransactionalBehavior.getPersistedSameTxBehavior();
    }

    public TransactionalBehavior enrollInTransactionForWrite(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        if (mto.zGetPortal().getTxParticipationMode(threadTx).isOptimisticLocking())
        {
            if (mto.zEnrollInTransactionForWrite(prevState, new TransactionalState(threadTx, PersistenceState.PERSISTED)))
            {
                return AbstractTransactionalBehavior.getPersistedSameTxBehavior();
            }
        }
        else
        {
            mto.zRefreshWithLockForWrite(this);
            return AbstractTransactionalBehavior.getPersistedSameTxBehavior();
        }
        return null;
    }

    public void setData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx.isCautious())
        {
            mithraObject.zSetTxData(newData);
        }
        else
        {
            mithraObject.zSetNonTxData(newData);
        }
    }

    public void delete(MithraTransactionalObject obj)
    {
        throw new RuntimeException("should never get here");
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject mithraTransactionalObject, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("Should never get here");
    }

    public void remoteUpdate(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("should never get here");
    }
}
