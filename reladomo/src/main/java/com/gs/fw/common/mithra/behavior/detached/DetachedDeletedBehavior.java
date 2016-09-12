
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
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;

import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;

import java.util.List;
import java.util.Date;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.util.Map;


public abstract class DetachedDeletedBehavior extends TransactionalBehavior
{

    public DetachedDeletedBehavior()
    {
        super(false, true, false, true, THROW_EXCEPTION, "Cannot access deleted object!");
    }

    public MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject)
    {
        throw new MithraDeletedException("Cannot change deleted object!");
    }

    public void setData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        throw new MithraDeletedException("Cannot set data on deleted object!");
    }

    public boolean maySetPrimaryKey()
    {
        return false;
    }

    public void insert(MithraTransactionalObject obj)
    {
        throw new MithraDeletedException("Cannot insert a deleted object!");
    }

    public void insertForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        throw new MithraDeletedException("Cannot insert a deleted object!");
    }

    public void bulkInsert(MithraTransactionalObject obj)
    {
        throw new MithraDeletedException("Cannot bulk-insert a deleted object!");
    }

    public void delete(MithraTransactionalObject obj)
    {
        throw new MithraDeletedException("Cannot delete a deleted object!");
    }

    public void deleteForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        throw new MithraDeletedException("Cannot delete a deleted object!");
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject mithraTransactionalObject, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
         throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        throw new MithraDeletedException("Cannot update a deleted object!");
    }

    public MithraTransactionalObject getDetachedCopy(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Can't detach a deleted object!");
    }

    public void remoteUpdate(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("Should never get here");
    }

    public void remoteUpdateForBatch(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("Should never get here");
    }

    public MithraTransactionalObject updateOriginalOrInsert(MithraTransactionalObject obj)
    {
        MithraTransactionalObject original = obj.zFindOriginal();
        if (original != null)
        {
            original.cascadeDelete();
        }
        return original;
    }

    public void persistChildDelete(MithraTransactionalObject obj)
    {
        // nothing to do
    }

    public boolean isModifiedSinceDetachment(MithraTransactionalObject obj)
    {
        return true;
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        throw new RuntimeException("Should never get here");
//        MithraObjectUtils.zAddToNavigationStats(parentFinder, true, navigationStats);
//        obj.zAddNavigatedRelationshipsStatsForDelete(parentFinder, navigationStats);
//        return navigationStats;
    }
}
