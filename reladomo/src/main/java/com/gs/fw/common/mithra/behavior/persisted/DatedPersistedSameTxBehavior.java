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

import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.util.Time;



public class DatedPersistedSameTxBehavior extends DatedPersistedBehavior
{
    public DatedPersistedSameTxBehavior()
    {
        super(DATA_USE_TX, DATA_USE_TX, false);
    }

    public boolean isDeleted(MithraDatedTransactionalObject mithraObject)
    {
        return mithraObject.zIsTxDataDeleted();
    }

    public DatedTransactionalBehavior enrollInTransaction(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        // already enrolled
        return this;
    }

    public DatedTransactionalBehavior enrollInTransactionForWrite(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        return this;
    }

    public DatedTransactionalBehavior enrollInTransactionForDelete(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        return this;
    }

    public void resetDetachedData(MithraDatedTransactionalObject mithraObject, MithraDataObject newData)
    {
        throw new RuntimeException("not implemented");
//        mithraObject.zSetTxData(newData);
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().terminate(obj, container);
    }

    public void purge(MithraDatedTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().purge(obj, container);
    }

    public void update(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        if (isReadonly)
        {
            throw new MithraBusinessException("cannot update readonly attribute "+updateWrapper.getAttribute().getClass().getName());
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        issueUpdate(obj, container, updateWrapper, triggerHook);
    }

    private MithraDataObject throwReadOnlyException(Attribute attr)
    {
        throw new MithraBusinessException("cannot update readonly attribute "+attr.getClass().getName());
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException((Attribute) attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new BooleanUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    private void issueUpdate(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper, boolean triggerHook)
    {
        obj.zGetTemporalDirector().update(obj, container, updateWrapper);
        updateWrapper.incrementUpdateCount();
        if (triggerHook) obj.triggerUpdateHook(updateWrapper);
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new ByteArrayUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new ByteUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new CharUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new DateUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new TimeUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new DoubleUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new BigDecimalUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new FloatUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new IntegerUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new LongUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new ShortUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new StringUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        issueUpdate(obj, container, new TimestampUpdateWrapper(attr, data, newValue), triggerHook);
        return data;
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly)
    {
        if (isReadonly)
        {
            throw new MithraBusinessException("cannot update readonly attribute "+updateWrapper.getAttribute().getClass().getName());
        }
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().inPlaceUpdate(obj, container, updateWrapper);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        if (isReadonly)
        {
            throw new MithraBusinessException("cannot update readonly or primary key attribute "+attr.getClass().getName());
        }
        DoubleIncrementUpdateWrapper updateWrapper = new DoubleIncrementUpdateWrapper(attr,this.getCurrentDataForWrite(obj),increment);
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().increment(obj, container, updateWrapper);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean readOnly, BigDecimalAttribute attr)
    {
        checkReadOnly(readOnly,attr);
        BigDecimalIncrementUpdateWrapper updateWrapper = new BigDecimalIncrementUpdateWrapper(attr,this.getCurrentDataForWrite(obj),increment);

        obj.zGetTemporalDirector().increment(obj, getTemporalContainer(obj), updateWrapper);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    private void checkReadOnly(boolean readOnly, Attribute attr)
    {
        if (readOnly)
        {
            throwReadOnlyException(attr);
        }

    }
    private TemporalContainer getTemporalContainer(MithraDatedTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        return cache.getOrCreateContainer(obj.zGetTxDataForRead());
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until)
    {
        if (isReadonly)
        {
            throw new MithraBusinessException("cannot update readonly or primary key attribute "+attr.getClass().getName());
        }
        DoubleIncrementUpdateWrapper updateWrapper = new DoubleIncrementUpdateWrapper(attr,this.getCurrentDataForWrite(obj),increment);
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().incrementUntil(obj, container, updateWrapper, until);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until)
    {
        if (isReadonly)
        {
            throw new MithraBusinessException("cannot update readonly or primary key attribute "+attr.getClass().getName());
        }
        BigDecimalIncrementUpdateWrapper updateWrapper = new BigDecimalIncrementUpdateWrapper(attr,this.getCurrentDataForWrite(obj),increment);
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().incrementUntil(obj, container, updateWrapper, until);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    public void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, Timestamp until, boolean triggerHook)
    {
        if (isReadonly)
        {
            throw new MithraBusinessException("cannot update readonly attribute "+updateWrapper.getAttribute().getClass().getName());
        }
        issueUpdateUntil(obj, until, updateWrapper);
    }

    private void issueUpdateUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil, AttributeUpdateWrapper updateWrapper)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());
        obj.zGetTemporalDirector().updateUntil(obj, container, updateWrapper, exclusiveUntil);
        updateWrapper.incrementUpdateCount();
        obj.triggerUpdateHook(updateWrapper);
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        BooleanUpdateWrapper updateWrapper = new BooleanUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        ByteArrayUpdateWrapper updateWrapper = new ByteArrayUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        ByteUpdateWrapper updateWrapper = new ByteUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        CharUpdateWrapper updateWrapper = new CharUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        DateUpdateWrapper updateWrapper = new DateUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        TimeUpdateWrapper updateWrapper = new TimeUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        DoubleUpdateWrapper updateWrapper = new DoubleUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        BigDecimalUpdateWrapper updateWrapper = new BigDecimalUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        FloatUpdateWrapper updateWrapper = new FloatUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        IntegerUpdateWrapper updateWrapper = new IntegerUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        LongUpdateWrapper updateWrapper = new LongUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        ShortUpdateWrapper updateWrapper = new ShortUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        StringUpdateWrapper updateWrapper = new StringUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        if (readOnly)
        {
            return throwReadOnlyException(attr);
        }
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        TimestampUpdateWrapper updateWrapper = new TimestampUpdateWrapper(attr, data, newValue);
        issueUpdateUntil(obj, exclusiveUntil, updateWrapper);
        return data;
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().inactivateForArchiving(obj, container, processingDateTo, businessDateTo);
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().terminateUntil(obj, container, until);
    }
}
