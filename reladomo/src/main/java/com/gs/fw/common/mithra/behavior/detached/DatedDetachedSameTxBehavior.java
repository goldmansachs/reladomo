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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.behavior.inmemory.DatedInMemorySameTxBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;


public class DatedDetachedSameTxBehavior extends DatedInMemorySameTxBehavior
{
    public DatedDetachedSameTxBehavior()
    {
        super((byte) DatedPersistenceState.DETACHED, true, false);
    }

    public void insert(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("a detached object may not be inserted!");
    }

    public void insertWithIncrement(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("a detached object may not be inserted!");
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("a detached object may not be inserted!");
    }

    public void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("a detached object may not be inserted!");
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        throw new MithraBusinessException("detached increment not implemented");
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr)
    {
        throw new MithraBusinessException("detached increment not implemented");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until)
    {
        throw new MithraBusinessException("detached incrementUntil not implemented");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until)
    {
        throw new MithraBusinessException("detached incrementUntil not implemented");
    }

    public void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, Timestamp until, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("detached updateUntil not implemented");
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new MithraBusinessException("detached inactivateForArchiving not implemented");
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new MithraBusinessException("detached terminateUntil not implemented");
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        obj.zSetTxDetachedDeleted();
    }

    public void delete(MithraDatedTransactionalObject obj)
    {
    }

    @Override
    public MithraDatedTransactionalObject copyThenInsert(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (original == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        MithraDataObject currentDataForRead = this.getCurrentDataForRead(obj);
        original.zCopyAttributesFrom(currentDataForRead);
        return original;
    }

    public MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (original == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        MithraDataObject currentDataForRead = this.getCurrentDataForRead(obj);
        original.zCopyAttributesFrom(currentDataForRead);
        original.zPersistDetachedRelationships(currentDataForRead);
        return original;
    }

    public void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject mithraTransactionalObject = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (mithraTransactionalObject == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        MithraDataObject currentDataForRead = this.getCurrentDataForRead(obj);
        mithraTransactionalObject.zCascadeUpdateInPlaceBeforeTerminate(currentDataForRead);
    }

    public MithraDatedTransactionalObject updateOriginalOrInsertUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (original == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        MithraDataObject currentDataForRead = this.getCurrentDataForRead(obj);
        original.zCopyAttributesUntilFrom(currentDataForRead, until);
        original.zPersistDetachedRelationshipsUntil(currentDataForRead, until);
        return original;
    }

    public boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject mithraTransactionalObject = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (mithraTransactionalObject == null) return true;
        return mithraTransactionalObject.zIsDataChanged(this.getCurrentDataForRead(obj));
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        return obj.zAddNavigatedRelationshipsStatsForUpdate(parentFinder, navigationStats);
    }
}
