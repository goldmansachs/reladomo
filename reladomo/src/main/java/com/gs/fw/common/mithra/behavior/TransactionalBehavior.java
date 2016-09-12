
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

package com.gs.fw.common.mithra.behavior;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.sql.Timestamp;


public abstract class TransactionalBehavior
{

    protected static final short THROW_EXCEPTION = 100;
    protected static final short NON_TRANSACTIONAL = 0; // zero and 1 are special to the VM. makes them fast
    protected static final short TRANSACTIONAL = 1;

    private final short dataReadType;
    private final boolean isPersisted;
    private final boolean isDeleted;
    private final boolean isInMemory;
    private final boolean isDetached;
    private final String readDataExceptionMessage;

    protected TransactionalBehavior(boolean persisted, boolean deleted, boolean inMemory, boolean detached,
            short dataReadType, String readDataExceptionMessage)
    {
        isPersisted = persisted;
        isDeleted = deleted;
        isInMemory = inMemory;
        isDetached = detached;
        this.dataReadType = dataReadType;
        this.readDataExceptionMessage = readDataExceptionMessage;
    }

    public final boolean isDeleted()
    {
        return isDeleted;
    }

    public final boolean isDetached()
    {
        return isDetached;
    }

    public final boolean isInMemory()
    {
        return isInMemory;
    }

    public final boolean isPersisted()
    {
        return isPersisted;
    }

    public final MithraDataObject getCurrentDataForRead(MithraTransactionalObject mithraObject)
    {
        if (dataReadType == NON_TRANSACTIONAL)
        {
            MithraDataObject data = mithraObject.zGetNonTxData();
            if (data == null)
            {
                data = allocateNonTxData(mithraObject);
            }
            return data;
        }
        if (dataReadType == TRANSACTIONAL)
        {
            return mithraObject.zGetTxDataForRead();
        }
        throwReadNotAllowed();
        return null;
    }

    public final MithraDataObject getCurrentDataForReadEvenIfDeleted(MithraTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetNonTxData();
        if (dataReadType == NON_TRANSACTIONAL)
        {
            if (data == null)
            {
                data = allocateNonTxData(mithraObject);
            }
        }
        else if (dataReadType == TRANSACTIONAL)
        {
            data = mithraObject.zGetTxDataForRead();
        }
        if (data == null)
        {
            data = mithraObject.zGetTxDataForRead();
            if (data == null)
            {
                data = allocateNonTxData(mithraObject);
            }
        }
        return data;
    }

    private void throwReadNotAllowed()
    {
        throw new MithraDeletedException(readDataExceptionMessage);
    }

    private MithraDataObject allocateNonTxData(MithraTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zAllocateData();
        mithraObject.zSetNonTxData(data);
        return data;
    }

    public abstract Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats);

    public MithraTransactionalObject copyThenInsert(MithraTransactionalObject obj)
    {
        MithraTransactionalObject newOriginal = obj.getNonPersistentCopy();
        newOriginal.insert();
        obj.zSetTxPersistenceState(PersistenceState.DETACHED);
        return newOriginal;
    }

    public boolean isDirectReferenceAllowed()
    {
        return this.isPersisted() && this.dataReadType == NON_TRANSACTIONAL;
    }

    public abstract MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject);

    public abstract MithraDataObject getDataForPrimaryKey(MithraTransactionalObject mithraObject);

    public abstract TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mithraObject, MithraTransaction tx, TransactionalState prevState);

    public abstract TransactionalBehavior enrollInTransactionForWrite(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState);

    public abstract TransactionalBehavior enrollInTransactionForRead(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState);

    public abstract void setData(MithraTransactionalObject mithraObject, MithraDataObject newData);

    public abstract boolean maySetPrimaryKey();

    public abstract void insert(MithraTransactionalObject obj);

    public abstract void insertForRemote(MithraTransactionalObject obj, int hierarchyDepth);

    /**
     * <p>Insert the object as part of a bulk insert operation.</p>
     * <p>Usually a bulk insert operation will be using the native database load methods e.g. BCP on Sybase.</p>
     * @param obj The object to add to the bulk insert operation.
     */
    public abstract void bulkInsert(MithraTransactionalObject obj);

    public abstract void delete(MithraTransactionalObject obj);

    public abstract void deleteForRemote(MithraTransactionalObject obj, int hierarchyDepth);

    public abstract void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook);

    public abstract void remoteUpdate(MithraTransactionalObject obj, List updateWrappers);

    public abstract void remoteUpdateForBatch(MithraTransactionalObject obj, List updateWrappers);

    public abstract MithraTransactionalObject updateOriginalOrInsert(MithraTransactionalObject obj);

    public abstract void persistChildDelete(MithraTransactionalObject obj);

    public abstract MithraTransactionalObject getDetachedCopy(MithraTransactionalObject obj);

    public abstract boolean isModifiedSinceDetachment(MithraTransactionalObject obj);

    public abstract MithraDataObject update(MithraTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook);

    public void clearTempTransaction(MithraTransactionalObject obj)
    {
        // nothing to do
    }

    public byte getPersistenceState()
    {
        if (isPersisted) return PersistenceState.PERSISTED;
        if (isDeleted && isDetached) return PersistenceState.DETACHED_DELETED;
        if (isDeleted) return PersistenceState.DELETED;
        if (isDetached) return PersistenceState.DETACHED;
        return PersistenceState.IN_MEMORY;
    }

}
