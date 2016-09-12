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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.math.BigDecimal;



public abstract class DatedInMemoryBehavior extends DatedTransactionalBehavior
{
    protected DatedInMemoryBehavior(byte persistenceState, byte readDataMode, byte writeDataMode, boolean isDetached, boolean maySetPrimaryKey) //byte persistenceState, boolean isPersisted, boolean isInMemory, boolean isDeleted, , boolean isDirectReferenceAllowed
    {
        super(persistenceState, readDataMode, writeDataMode, false, true, false, isDetached, false, maySetPrimaryKey);
    }

    protected DatedInMemoryBehavior(byte readDataMode, byte writeDataMode)
    {
        super((byte) DatedPersistenceState.IN_MEMORY, readDataMode, writeDataMode, false, true, false, false, false, true);
    }

    public boolean isDeleted(MithraDatedTransactionalObject mithraObject)
    {
        return false;
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Cannot delete an object that is not in the database!");
    }

    public void purge(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Cannot purge an object that is not in the database!");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until)
    {
        throw new MithraBusinessException("Can't incrementUntil on an in-memory object");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until)
    {
        throw new MithraBusinessException("Can't incrementUntil on an in-memory object");
    }

    public void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, Timestamp until, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new MithraBusinessException("Can't updateUntil on an in-memory object");
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new MithraBusinessException("Can't inactivateForArchiving on an in-memory object");
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new MithraBusinessException("Can't terminateUntil on an in-memory object");
    }

    public MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) obj.zCascadeCopyThenInsert();
        obj.zSetTxPersistenceState(PersistenceState.DETACHED);
        return original;
    }

    public void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj)
    {
        //nothing to do
    }

    public MithraDatedTransactionalObject copyThenInsert(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject copy = obj.getNonPersistentCopy();
        copy.insert();
        return copy;
    }

    public MithraDatedTransactionalObject updateOriginalOrInsertUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        MithraDataObject currentDataForRead = this.getCurrentDataForWrite(obj);
        this.insertUntil(obj, until);
        obj.zInsertRelationshipsUntil(currentDataForRead, until);
        currentDataForRead.clearRelationships();
        return obj;
    }


    public MithraDatedTransactionalObject getDetachedCopy(MithraDatedTransactionalObject obj, Timestamp[] asOfAttributes)
    {
        throw new MithraBusinessException("only persistent objects can be detached");
    }

    public boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj)
    {
       return true;
    }

    public MithraDataObject getCurrentDataForReadEvenIfDeleted(MithraDatedTransactionalObject obj)
    {
        return this.getCurrentDataForRead(obj);
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        // Do nothing
        return navigationStats;
    }
}
