
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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistedState;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import com.gs.fw.common.mithra.util.Time;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class PersistedNonTransactionalBehavior extends TransactionalBehavior
{
    public PersistedNonTransactionalBehavior()
    {
        super(true, false, false, false, NON_TRANSACTIONAL, null);
    }

    public boolean maySetPrimaryKey()
    {
        return false;
    }

    public void insert(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Cannot insert an object that is already in the database!");
    }

    public void insertForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        throw new RuntimeException("should not get here");
    }

    public void deleteForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        throw new RuntimeException("should not get here");
    }

    public void bulkInsert(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Cannot bulk-insert an object that is already in the database!");
    }

    public void remoteUpdateForBatch(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("Should only be called in persisted same tx behavior");
    }

    public MithraTransactionalObject getDetachedCopy(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException(obj.getClass().getName()+" is configured as read-only. detaching is not sensible. use a non-persistent copy instead.");
    }

    public MithraTransactionalObject updateOriginalOrInsert(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Only detached objects can be updated.");
    }

    public void persistChildDelete(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Only detached objects can be updated.");
    }

    public boolean isModifiedSinceDetachment(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Object is not detached!");
    }

    public MithraDataObject getDataForPrimaryKey(MithraTransactionalObject mithraObject)
    {
        return this.getCurrentDataForRead(mithraObject);
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        // Do nothing
        return navigationStats;
    }

    public MithraDataObject getCurrentDataForWrite(MithraTransactionalObject mithraObject)
    {
        throw new RuntimeException("should not get here");
    }

    public TransactionalBehavior enrollInTransactionForDelete(MithraTransactionalObject mithraObject, MithraTransaction tx, TransactionalState prevState)
    {
        return this; // no tx to enroll in
    }

    public TransactionalBehavior enrollInTransactionForRead(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        return this;
    }

    public TransactionalBehavior enrollInTransactionForWrite(MithraTransactionalObject mto, MithraTransaction threadTx, TransactionalState prevState)
    {
        return this;
    }

    public void setData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        mithraObject.zSetNonTxData(newData);
    }

    public void possiblyEnrollAndSetData(MithraTransactionalObject mithraObject, MithraDataObject newData)
    {
        mithraObject.zSetNonTxData(newData);
    }

    public void delete(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException(obj.getClass().getName()+" is configured as read-only. delete is not allowed.");
    }

    public void update(MithraTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    public MithraDataObject update(MithraTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        throw newUpdateException(obj);
    }

    private MithraBusinessException newUpdateException(MithraTransactionalObject obj)
    {
        return new MithraBusinessException(obj.getClass().getName()+" is configured as read-only. update is not allowed.");
    }

    private MithraDataObject throwReadOnlyException()
    {
        throw new MithraBusinessException("cannot change a readonly or primary key attribute which is already persisted in database");
    }

    public void remoteUpdate(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("shouldn't get here");
    }

    public void clearTempTransaction(MithraTransactionalObject obj)
    {
        obj.zClearTempTransaction();
    }
}
