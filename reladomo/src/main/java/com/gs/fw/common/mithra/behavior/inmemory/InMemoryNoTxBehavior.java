
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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.util.Time;

import java.util.Date;
import java.sql.Timestamp;
import java.math.BigDecimal;


public class InMemoryNoTxBehavior extends InMemoryBehavior
{

    public InMemoryNoTxBehavior()
    {
        super(NON_TRANSACTIONAL, null);
    }

    protected InMemoryNoTxBehavior(boolean detached)
    {
        super(detached, NON_TRANSACTIONAL, null);
    }

    public MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject)
    {
        return getOrAllocateNonTxDataForRead(mithraObject);
    }

    public TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mithraObject, MithraTransaction tx, TransactionalState prevState)
    {
        throw new RuntimeException("Should never get here");
    }

    public TransactionalBehavior enrollInTransactionForRead(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        throw new RuntimeException("Should never get here");
    }

    public TransactionalBehavior enrollInTransactionForWrite(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        throw new RuntimeException("Should never get here");
    }

    public void setData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        mithraObject.zSetNonTxData(newData);
    }

    public void insert(MithraTransactionalObject obj)
    {
        Cache cache = obj.zGetCache();
        obj.zSetNonTxPersistenceState(PersistenceState.PERSISTED);
        Object index = cache.preparePut(obj);
        try
        {
            MithraDataObject data = obj.zGetNonTxData();
            obj.zGetPortal().getMithraObjectPersister().insert(data);
            obj.zSetInserted();
            data.clearRelationships();
            cache.put(obj); // this is necessary because commit expects the object to have been added already
            obj.zGetPortal().incrementClassUpdateCount();
        }
        catch(MithraDatabaseException e)
        {
            obj.zSetNonTxPersistenceState(PersistenceState.IN_MEMORY);
            cache.rollback(null);
            throw e;
        }
        finally
        {
            cache.commitPreparedForIndex(index); // this just removes the prepared state
        }
    }

    public void insertForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        Cache cache = obj.zGetCache();
        obj.zSetNonTxPersistenceState(PersistenceState.PERSISTED);
        Object index = cache.preparePut(obj);
        try
        {
            MithraDataObject data = obj.zGetNonTxData();
            data.zGetMithraObjectPortal(hierarchyDepth).getMithraObjectPersister().insert(data);
            data.clearRelationships();
            cache.put(obj); // this is necessary because commit expects the object to have been added already
            obj.zGetPortal().incrementClassUpdateCount();
        }
        catch(MithraDatabaseException e)
        {
            obj.zSetNonTxPersistenceState(PersistenceState.IN_MEMORY);
            cache.rollback(null);
            throw e;
        }
        finally
        {
            cache.commitPreparedForIndex(index); // this just removes the prepared state
        }
    }

    public void bulkInsert(MithraTransactionalObject obj)
    {
        throw new RuntimeException("Should never get here");
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        updateWrapper.updateData();
    }

    public MithraDataObject update(MithraTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setIntValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setByteArrayValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setBooleanValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setBigDecimalValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setByteValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setCharValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setDateValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setTimeValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setDoubleValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setFloatValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setLongValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setShortValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setStringValue(data, newValue);
        return data;
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        MithraDataObject data = this.getCurrentDataForWrite(obj);
        attr.setTimestampValue(data, newValue);
        return data;
    }
}
