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
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public abstract class DatedTransactionalBehavior
{
    protected static final byte DATA_CURRENT_WITH_CHECK = 0;
    protected static final byte DATA_GET_OR_ALLOCATE = 1;
    protected static final byte DATA_USE_TX = 2;
    protected static final byte DATA_USE_NON_TX = 3;
    protected static final byte DATA_DELETED = 4;
    protected static final byte DATA_NOT_SUPPORTED = 5;
    protected static final byte DATA_UNEXPECTED = 6;

    private byte persistenceState;
    private byte readDataMode;
    private byte writeDataMode;
    private boolean isPersisted;
    private boolean isDeleted;
    private boolean isInMemory;
    private boolean isDetached;
    private boolean isDirectReferenceAllowed;
    private boolean maySetPrimaryKey;

    protected DatedTransactionalBehavior(byte persistenceState, byte readDataMode, byte writeDataMode,
            boolean isPersisted, boolean isInMemory,
            boolean isDeleted, boolean isDetached, boolean isDirectReferenceAllowed, boolean maySetPrimaryKey)
    {
        this.persistenceState = persistenceState;
        this.readDataMode = readDataMode;
        this.writeDataMode = writeDataMode;
        this.isPersisted = isPersisted;
        this.isInMemory = isInMemory;
        this.isDeleted = isDeleted;
        this.isDetached = isDetached;
        this.isDirectReferenceAllowed = isDirectReferenceAllowed;
        this.maySetPrimaryKey = maySetPrimaryKey;
    }

    protected String getErrorMessageForOldObject(MithraDatedTransactionalObject mithraObject, String message)
    {
        MithraDataObject data = mithraObject.zGetCurrentData();
        if (data == null)
        {
            data = mithraObject.zGetTxDataForRead();
        }
        if (data != null)
        {
            message += " For old object " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data);
        }
        return message;
    }

    protected MithraDataObject getOrAllocateNonTxData(MithraDatedTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetCurrentData();
        if (data == null)
        {
            data = mithraObject.zAllocateData();
            mithraObject.zSetCurrentData(data);
        }
        return data;
    }

    public final MithraDataObject getCurrentDataForRead(MithraDatedTransactionalObject mithraObject)
    {
        if (readDataMode == DATA_CURRENT_WITH_CHECK)
        {
            return mithraObject.zGetCurrentDataWithCheck();
        }
        if (readDataMode == DATA_GET_OR_ALLOCATE)
        {
            return getOrAllocateNonTxData(mithraObject);
        }
        return unusualCurrentDataForRead(mithraObject);
    }

    private MithraDataObject unusualCurrentDataForRead(MithraDatedTransactionalObject mithraObject)
    {
        if (readDataMode == DATA_USE_TX)
        {
            return mithraObject.zGetTxDataForRead();
        }
        if (readDataMode == DATA_USE_NON_TX)
        {
            return mithraObject.zGetNonTxData();
        }
        if (readDataMode == DATA_DELETED)
        {
            throw new MithraDeletedException(getErrorMessageForOldObject(mithraObject, "Cannot access deleted object!"));
        }
        throw new RuntimeException("should not get here");
    }

    public final MithraDataObject getCurrentDataForWrite(MithraDatedTransactionalObject mithraObject)
    {
        switch(writeDataMode)
        {
            case DATA_GET_OR_ALLOCATE:
                return getOrAllocateNonTxData(mithraObject);
            case DATA_USE_TX:
                return mithraObject.zGetTxDataForWrite();
        }
        return unusualCurrentDataForWrite(mithraObject);
    }

    private MithraDataObject unusualCurrentDataForWrite(MithraDatedTransactionalObject mithraObject)
    {
        if (writeDataMode == DATA_DELETED)
        {
            throw new MithraDeletedException(getErrorMessageForOldObject(mithraObject, "Cannot change deleted object!"));
        }
        if (writeDataMode == DATA_NOT_SUPPORTED)
        {
            throw new RuntimeException("Dated object modification is not supported outside a transaction.");
        }
        throw new RuntimeException("should not get here");
    }

    public final boolean isDeleted()
    {
        return isDeleted;
    }

    public final boolean isPersisted()
    {
        return isPersisted;
    }

    public final boolean isInMemory()
    {
        return isInMemory;
    }

    public final boolean isDetached()
    {
        return isDetached;
    }

    public final boolean isDirectReferenceAllowed()
    {
        return isDirectReferenceAllowed;
    }

    public final boolean maySetPrimaryKey()
    {
        return maySetPrimaryKey;
    }

    public final byte getPersistenceState()
    {
        return persistenceState;
    }

    public abstract boolean isDeleted(MithraDatedTransactionalObject mithraObject);

    public abstract DatedTransactionalBehavior enrollInTransaction(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState);

    public abstract DatedTransactionalBehavior enrollInTransactionForWrite(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState);

    public abstract DatedTransactionalBehavior enrollInTransactionForDelete(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState);

    public abstract void resetDetachedData(MithraDatedTransactionalObject mithraObject, MithraDataObject newData);

    public abstract void insert(MithraDatedTransactionalObject obj);

    public abstract Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats);

    public abstract MithraDatedTransactionalObject copyThenInsert(MithraDatedTransactionalObject obj);

    public abstract void insertWithIncrement(MithraDatedTransactionalObject obj);

    public abstract void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil);

    public abstract void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil);

    public abstract void terminate(MithraDatedTransactionalObject obj);

    public abstract void purge(MithraDatedTransactionalObject obj);

    public abstract void insertForRecovery(MithraDatedTransactionalObject obj);

    public abstract void update(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook);

    public abstract void inPlaceUpdate(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper,  boolean isReadonly);

    public abstract void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr);

    public abstract void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean readOnly, BigDecimalAttribute attr);

    public abstract void incrementUntil(MithraDatedTransactionalObject obj,
            double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until);

    public abstract void incrementUntil(MithraDatedTransactionalObject obj,
            BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until);

    public abstract void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper,
            boolean isReadonly, Timestamp until, boolean triggerHook);

    public abstract void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until);

    public abstract void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo);

    public abstract MithraDatedTransactionalObject getDetachedCopy(MithraDatedTransactionalObject obj, Timestamp[] asOfAttributes);

    public abstract MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj);

    public abstract MithraDatedTransactionalObject updateOriginalOrInsertUntil(MithraDatedTransactionalObject obj, Timestamp until);

    public abstract boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject update(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook);

    public abstract MithraDataObject getCurrentDataForReadEvenIfDeleted(MithraDatedTransactionalObject obj);

    public abstract void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj);

}
