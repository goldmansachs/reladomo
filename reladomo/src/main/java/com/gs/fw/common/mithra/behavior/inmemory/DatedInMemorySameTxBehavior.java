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

package com.gs.fw.common.mithra.behavior.inmemory;

import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransaction;import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.util.Time;



public class DatedInMemorySameTxBehavior extends DatedInMemoryBehavior
{
    protected DatedInMemorySameTxBehavior(byte persistenceState, boolean isDetached, boolean maySetPrimaryKey)
    {
        super(persistenceState, DATA_USE_TX, DATA_USE_TX, isDetached, maySetPrimaryKey);
    }

    public DatedInMemorySameTxBehavior()
    {
        super(DATA_USE_TX, DATA_USE_TX);
    }

    public DatedTransactionalBehavior enrollInTransaction(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should never get here");
    }

    public DatedTransactionalBehavior enrollInTransactionForWrite(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should never get here");
    }

    public DatedTransactionalBehavior enrollInTransactionForDelete(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should never get here");
    }

    public void resetDetachedData(MithraDatedTransactionalObject mithraObject, MithraDataObject newData)
    {
        mithraObject.zSetTxData(newData);
    }

    public void insert(MithraDatedTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().insert(obj, container);
        this.getCurrentDataForWrite(obj).clearRelationships();
    }

    public void insertForRecovery(MithraDatedTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().insertForRecovery(obj, container);
        this.getCurrentDataForWrite(obj).clearRelationships();
    }

    public void insertWithIncrement(MithraDatedTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().insertWithIncrement(obj, container);
        this.getCurrentDataForWrite(obj).clearRelationships();
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().insertWithIncrementUntil(obj, container, exclusiveUntil);
        this.getCurrentDataForWrite(obj).clearRelationships();
    }

    public void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        Cache cache = obj.zGetCache();
        TemporalContainer container = cache.getOrCreateContainer(obj.zGetTxDataForRead());

        obj.zGetTemporalDirector().insertUntil(obj, container, exclusiveUntil);
    }

    public void update(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        updateWrapper.updateData();
    }

    public MithraDataObject update(MithraDatedTransactionalObject MithraDatedTransactionalObject, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(MithraDatedTransactionalObject);
        attr.setIntValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setByteArrayValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setBooleanValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setByteValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setCharValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setDateValue(data, newValue);
        return data;
    }

    @Override
    public MithraDataObject update(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setTimeValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setDoubleValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setBigDecimalValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setFloatValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setLongValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setShortValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setStringValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setTimestampValue(data, newValue);
        return data;
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly)
    {
        updateWrapper.updateData();
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        attr.increment(this.getCurrentDataForWrite(obj), increment);
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr)
    {
        attr.increment(this.getCurrentDataForWrite(obj), increment);
    }

}
