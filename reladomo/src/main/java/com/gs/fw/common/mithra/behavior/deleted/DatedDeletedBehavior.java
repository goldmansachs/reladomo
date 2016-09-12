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

package com.gs.fw.common.mithra.behavior.deleted;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;



public class DatedDeletedBehavior extends DatedTransactionalBehavior
{
    public DatedDeletedBehavior()
    {
        super((byte) DatedPersistenceState.DELETED, DATA_DELETED, DATA_DELETED, false, false, true, false, false, false);
    }

    protected DatedDeletedBehavior(boolean isDeleted)
    {
        super((byte) DatedPersistenceState.DELETED, DATA_DELETED, DATA_DELETED, false, false, isDeleted, false, false, false);
    }

    public boolean isDeleted(MithraDatedTransactionalObject mithraObject)
    {
        return true;
    }

    public DatedTransactionalBehavior enrollInTransaction(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(mithraObject, "Cannot enroll deleted object in transaction!"));
    }

    public DatedTransactionalBehavior enrollInTransactionForWrite(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(mithraObject, "Cannot enroll deleted object in transaction!"));
    }

    public DatedTransactionalBehavior enrollInTransactionForDelete(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(mithraObject, "Cannot enroll deleted object in transaction!"));
    }

    public void resetDetachedData(MithraDatedTransactionalObject mithraObject, MithraDataObject newData)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(mithraObject, "Cannot set data on deleted object!"));
    }

    public void insert(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot insert a deleted object!"));
    }

    public void insertWithIncrement(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot insert a deleted object!"));
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot insert a deleted object!"));
    }

    public void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot insert a deleted object!"));
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot delete a deleted object!"));
    }

    public void purge(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot purge a deleted object!"));
    }

    public void insertForRecovery(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot insert for recovery a deleted object!"));
    }

    public void update(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot in place update a deleted object!"));
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean readOnly, BigDecimalAttribute attr)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, Timestamp until, boolean triggerHook)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot update a deleted object!"));
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot terminate a deleted object!"));
    }

    public MithraDatedTransactionalObject getDetachedCopy(MithraDatedTransactionalObject obj, Timestamp[] asOfAttributes)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot detach a deleted object!"));
    }

    public MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot updateOriginalOrInsert a deleted object!"));
    }

    public void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot cascadeUpdateInPlaceBeforeTerminate a deleted object!"));
    }

    public MithraDatedTransactionalObject updateOriginalOrInsertUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot updateOriginalOrInsertUntil a deleted object!"));
    }

    public boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot interrogate a deleted object!"));
    }

    public MithraDatedTransactionalObject copyThenInsert(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException(getErrorMessageForOldObject(obj, "Cannot insert a deleted object!"));
    }

    public MithraDataObject getCurrentDataForReadEvenIfDeleted(MithraDatedTransactionalObject obj)
    {
        return obj.zGetCurrentOrTransactionalData();
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        throw new MithraBusinessException("Only detached objects can be updated.");
    }
}
