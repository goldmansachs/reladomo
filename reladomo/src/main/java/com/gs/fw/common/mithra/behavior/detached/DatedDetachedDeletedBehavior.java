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

package com.gs.fw.common.mithra.behavior.detached;

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



public abstract class DatedDetachedDeletedBehavior extends DatedTransactionalBehavior
{
    protected DatedDetachedDeletedBehavior()
    {
        super((byte) DatedPersistenceState.DETACHED_DELETED, DATA_DELETED, DATA_DELETED, false, false, true, true, false, false);
    }

    public boolean isDeleted(MithraDatedTransactionalObject mithraObject)
    {
        return true;
    }

    public void resetDetachedData(MithraDatedTransactionalObject mithraObject, MithraDataObject newData)
    {
        throw new RuntimeException("Should not get here");
    }

    public void insert(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException("Cannot insert a deleted object!");
    }

    public void insertForRecovery(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException("Cannot insert a deleted object!");
    }

    public void update(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly)
    {
        throw new MithraDeletedException("Cannot in place update a deleted object!");
    }

    public MithraDatedTransactionalObject getDetachedCopy(MithraDatedTransactionalObject obj, Timestamp[] asOfAttributes)
    {
        throw new MithraBusinessException("Can't detach a deleted object!");
    }

    public MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zFindOriginal();
        MithraDataObject currentDataForRead = obj.zGetTxDataForRead();
        if (original != null)
        {
            original.zCascadeUpdateInPlaceBeforeTerminate(currentDataForRead);
            original.cascadeTerminate();
        }
        return original;
    }

    public void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zFindOriginal();
        MithraDataObject currentDataForRead = obj.zGetTxDataForRead();
        if (original != null)
        {
            original.zCascadeUpdateInPlaceBeforeTerminate(currentDataForRead);
        }
    }

    public MithraDatedTransactionalObject updateOriginalOrInsertUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (original != null)
        {
            original.cascadeTerminate();
        }
        return original;
    }

    public boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj)
    {
        return true;
    }

    public DatedTransactionalBehavior enrollInTransaction(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should not get here");
    }

    public DatedTransactionalBehavior enrollInTransactionForWrite(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should not get here");
    }

    public DatedTransactionalBehavior enrollInTransactionForDelete(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should not get here");
    }

    public void insertWithIncrement(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("cannot insert a terminated object");
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("cannot insert a terminated object");
    }

    public void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("cannot insert a terminated object");
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("cannot terminate a terminated object");
    }

    public void purge(MithraDatedTransactionalObject obj)
    {
         throw new MithraBusinessException("cannot purge a terminated object");
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        throw new MithraBusinessException("cannot increment a terminated object");
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr)
    {
        throw new MithraBusinessException("cannot increment a terminated object");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until)
    {
        throw new MithraBusinessException("cannot incrementUntil a terminated object");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until)
    {
        throw new MithraBusinessException("cannot incrementUntil a terminated object");
    }


    public void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, Timestamp until, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("cannot updateUntil a terminated object");
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new MithraBusinessException("cannot inactivateForArchiving a terminated object");
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new MithraBusinessException("cannot terminateUntil a terminated object");
    }

    public MithraDatedTransactionalObject copyThenInsert(MithraDatedTransactionalObject obj)
    {
        throw new MithraDeletedException("Cannot insert a deleted object!");
    }

    public MithraDataObject getCurrentDataForReadEvenIfDeleted(MithraDatedTransactionalObject obj)
    {
        return obj.zGetCurrentOrTransactionalData();
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        throw new RuntimeException("should not get here");
//        MithraObjectUtils.zAddToNavigationStats(parentFinder, true, navigationStats);
//        obj.zAddNavigatedRelationshipsStatsForDelete(parentFinder, navigationStats);
//        return navigationStats;
    }
}
