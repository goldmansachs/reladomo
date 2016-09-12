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
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistedState;
import com.gs.fw.common.mithra.util.Time;

import java.util.List;
import java.util.Date;
import java.sql.Timestamp;
import java.math.BigDecimal;



public class PersistedSameTxBehavior extends PersistedBehavior
{

    public PersistedSameTxBehavior()
    {
        super(TRANSACTIONAL, null);
    }

    public MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject)
    {
        return mithraObject.zGetTxDataForWrite();
    }

    public TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mithraObject, MithraTransaction tx, TransactionalState prevState)
    {
        // already enrolled
        return this;
    }

    public TransactionalBehavior enrollInTransactionForRead(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        return this;
    }

    public TransactionalBehavior enrollInTransactionForWrite(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        return this;
    }

    public void setData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        mithraObject.zSetTxData(newData);
    }

    public void delete(MithraTransactionalObject obj)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        tx.delete(obj);
        obj.zGetCache().remove(obj);
        obj.zSetTxPersistenceState(PersistedState.DELETED);
        obj.zGetPortal().incrementClassUpdateCount();
    }

    public void deleteForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        MithraDataObject data = obj.zGetTxDataForRead();
        MithraObjectPortal portal = data.zGetMithraObjectPortal(hierarchyDepth);
        portal.getMithraObjectPersister().delete(data);
        obj.zSetDeleted();
        obj.zGetCache().remove(obj);
        if (hierarchyDepth == 0)
        {
            obj.zSetTxPersistenceState(PersistedState.DELETED);
        }
        portal.incrementClassUpdateCount();
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        if (isReadonly)
        {
             throw new MithraBusinessException("cannot change a readonly attribute which is already persisted in database");
        }

        applyUpdate(obj, updateWrapper, triggerHook);
    }

    public MithraDataObject update(MithraTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        IntegerUpdateWrapper updateWrapper = new IntegerUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        ByteArrayUpdateWrapper updateWrapper = new ByteArrayUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        BooleanUpdateWrapper updateWrapper = new BooleanUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        BigDecimalUpdateWrapper updateWrapper = new BigDecimalUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        ByteUpdateWrapper updateWrapper = new ByteUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        CharUpdateWrapper updateWrapper = new CharUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        DateUpdateWrapper updateWrapper = new DateUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        TimeUpdateWrapper updateWrapper = new TimeUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        DoubleUpdateWrapper updateWrapper = new DoubleUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        FloatUpdateWrapper updateWrapper = new FloatUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        LongUpdateWrapper updateWrapper = new LongUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        ShortUpdateWrapper updateWrapper = new ShortUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        StringUpdateWrapper updateWrapper = new StringUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        TimestampUpdateWrapper updateWrapper = new TimestampUpdateWrapper(attr, data, newValue);
        applyUpdate(obj, updateWrapper, triggerHook);
        return data;
    }

    private MithraDataObject throwReadOnlyException()
    {
        throw new MithraBusinessException("cannot change a readonly or primary key attribute which is already persisted in database");
    }

    private void applyUpdate(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean triggerHook)
    {
        MithraManagerProvider.getMithraManager().getCurrentTransaction().update(obj, updateWrapper);
        obj.zGetCache().reindexForTransaction(obj, updateWrapper);
        updateWrapper.incrementUpdateCount();
        if (triggerHook) obj.triggerUpdateHook(updateWrapper);
    }

    public void remoteUpdate(MithraTransactionalObject obj, List updateWrappers)
    {
        this.remoteUpdateForBatch(obj, updateWrappers);
        ((AttributeUpdateWrapper)updateWrappers.get(0)).getAttribute().getOwnerPortal().getMithraObjectPersister().update(obj, updateWrappers);
        obj.zSetUpdated(updateWrappers);
    }

    public void remoteUpdateForBatch(MithraTransactionalObject obj, List updateWrappers)
    {
        MithraDataObject currentDataForWrite = this.getCurrentDataForWrite(obj);
        for(int i=0;i<updateWrappers.size();i++)
        {
            AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
            updateWrapper.setDataToUpdate(currentDataForWrite);
        }
        for(int i=0;i<updateWrappers.size();i++)
        {
            AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
            obj.zGetCache().reindexForTransaction(obj, updateWrapper);
            updateWrapper.incrementUpdateCount();
        }
    }
}
