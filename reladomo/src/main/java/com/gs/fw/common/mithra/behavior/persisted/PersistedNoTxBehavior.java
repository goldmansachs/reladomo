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



public class PersistedNoTxBehavior extends PersistedBehavior
{

    public PersistedNoTxBehavior()
    {
        super(NON_TRANSACTIONAL, null);
    }

    public MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject)
    {
        return mithraObject.zGetNonTxData();
    }

    public TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mithraObject, MithraTransaction tx, TransactionalState prevState)
    {
        return this; // no tx to enroll in
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
        mithraObject.zSetNonTxData(newData);
    }

    public void possiblyEnrollAndSetData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        mithraObject.zSetNonTxData(newData);
    }

    public void delete(MithraTransactionalObject obj)
    {
        try
        {
            obj.zGetPortal().getMithraObjectPersister().delete(obj.zGetNonTxData());
            obj.zGetCache().remove(obj);
            obj.zSetNonTxPersistenceState(PersistedState.DELETED);
            obj.zGetPortal().incrementClassUpdateCount();
        }
        finally
        {
            obj.zClearTempTransaction();
        }
    }

    public void deleteForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        try
        {
            MithraDataObject data = obj.zGetNonTxData();
            MithraObjectPortal portal = data.zGetMithraObjectPortal(hierarchyDepth);
            portal.getMithraObjectPersister().delete(data);
            obj.zSetDeleted();
            obj.zGetCache().remove(obj);
            if (hierarchyDepth == 0)
            {
                obj.zSetNonTxPersistenceState(PersistedState.DELETED);
            }
            portal.incrementClassUpdateCount();
        }
        finally
        {
            obj.zClearTempTransaction();
        }
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        if (isReadonly)
        {
             throw new MithraBusinessException("cannot change a readonly attribute which is already persisted in database");
        }
        if (obj.zGetPortal().getFinder().getVersionAttribute() != null)
        {
            throw new MithraBusinessException("Optimistically locked objects can only be updated in a transactions");
        }
        updateWrapper.getAttribute().getOwnerPortal().getMithraObjectPersister().update(obj, updateWrapper);
        obj.zGetCache().reindex(obj, updateWrapper);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    public MithraDataObject update(MithraTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new IntegerUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new ByteArrayUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new BooleanUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new BigDecimalUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new ByteUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new CharUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new DateUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new TimeUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new DoubleUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new FloatUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new LongUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new ShortUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new StringUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException();
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        applyUpdate(obj, new TimestampUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    private void applyUpdate(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean triggerHook)
    {
        if (obj.zGetPortal().getFinder().getVersionAttribute() != null)
        {
            throw new MithraBusinessException("Optimistically locked objects can only be updated in a transactions");
        }
        updateWrapper.getMithraObjectPortal().getMithraObjectPersister().update(obj, updateWrapper);
        obj.zGetCache().reindex(obj, updateWrapper);
        updateWrapper.incrementUpdateCount();
        if (triggerHook) obj.triggerUpdateHook(updateWrapper);
    }

    private MithraDataObject throwReadOnlyException()
    {
        throw new MithraBusinessException("cannot change a readonly or primary key attribute which is already persisted in database");
    }

    public void remoteUpdate(MithraTransactionalObject obj, List updateWrappers)
    {
        if (obj.zGetPortal().getFinder().getVersionAttribute() != null)
        {
            throw new MithraBusinessException("Optimistically locked objects can only be updated in a transactions");
        }
        try
        {
            MithraDataObject currentDataForWrite = this.getCurrentDataForWrite(obj);
            for(int i=0;i<updateWrappers.size();i++)
            {
                AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
                updateWrapper.setDataToUpdate(currentDataForWrite);
            }
            ((AttributeUpdateWrapper)updateWrappers.get(0)).getAttribute().getOwnerPortal().getMithraObjectPersister().update(obj, updateWrappers);
            for(int i=0;i<updateWrappers.size();i++)
            {
                AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
                obj.zGetCache().reindex(obj, updateWrapper);
                updateWrapper.incrementUpdateCount();
            }
            obj.zSetNonTxData(currentDataForWrite);
        }
        finally
        {
            obj.zClearTempTransaction();
        }
    }

    public void clearTempTransaction(MithraTransactionalObject obj)
    {
        obj.zClearTempTransaction();
    }
}
