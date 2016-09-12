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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.PrintablePrimaryKeyMessageBuilder;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;

import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import java.sql.Timestamp;
import java.util.Map;



public abstract class DatedPersistedBehavior extends DatedTransactionalBehavior
{
    protected DatedPersistedBehavior(byte readDataMode, byte writeDatMode, boolean isDirectReferenceAllowed)
    {
        super((byte) DatedPersistenceState.PERSISTED, readDataMode, writeDatMode, true, false, false, false, isDirectReferenceAllowed, false);
    }

    public boolean isDeleted(MithraDatedTransactionalObject mithraObject)
    {
        return false;
    }

    public void insert(MithraDatedTransactionalObject obj)
    {
        throw newInsertException(obj);
    }

    private MithraBusinessException newInsertException(MithraDatedTransactionalObject obj)
    {
        return new MithraBusinessException("Cannot insert an object that is already in the database! Object: " + PrintablePrimaryKeyMessageBuilder.createMessage(obj, this.getCurrentDataForRead(obj)));
    }

    public void insertForRecovery(MithraDatedTransactionalObject obj)
    {
        throw newInsertException(obj);
    }

    public void insertWithIncrement(MithraDatedTransactionalObject obj)
    {
        throw newInsertException(obj);
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw newInsertException(obj);
    }

    public void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw newInsertException(obj);
    }

    public MithraDatedTransactionalObject getDetachedCopy(MithraDatedTransactionalObject obj, Timestamp[] asOfAttributes)
    {
        MithraDataObject data = this.getCurrentDataForRead(obj);
        MithraDataObject newData = data.copy(false);
        MithraDatedTransactionalObject result = (MithraDatedTransactionalObject)
                obj.zGetPortal().getMithraDatedObjectFactory().createObject(newData, asOfAttributes);
        result.zSetNonTxPersistenceState(DatedPersistenceState.DETACHED);
        return result;
    }

    public MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Only detached objects can update original objects.");
    }

    public void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Only detached objects can update original objects.");
    }

    public MithraDatedTransactionalObject updateOriginalOrInsertUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new MithraBusinessException("Only detached objects can update original objects.");
    }

    public boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Only detached objects can be interrogated.");
    }

    public MithraDatedTransactionalObject copyThenInsert(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Cannot insert a persisted object!");
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
