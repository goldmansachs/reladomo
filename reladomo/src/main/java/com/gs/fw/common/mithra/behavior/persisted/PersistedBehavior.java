
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
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import java.util.List;
import java.util.Map;


public abstract class PersistedBehavior extends TransactionalBehavior
{

    protected PersistedBehavior(short dataReadType, String readDataExceptionMessage)
    {
        super(true, false, false, false, dataReadType, readDataExceptionMessage);
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
        MithraDataObject data = obj.zGetTxDataForRead();
        data.zGetMithraObjectPortal(hierarchyDepth).getMithraObjectPersister().insert(data);
        obj.zSetInserted();
        data.clearRelationships();
        obj.zGetPortal().incrementClassUpdateCount();
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
        MithraDataObject data = this.getCurrentDataForRead(obj);
        MithraDataObject newData = data.copy(false);
        MithraTransactionalObject result = (MithraTransactionalObject)
                obj.zGetPortal().getMithraObjectFactory().createObject(newData);
        result.zSetNonTxPersistenceState(PersistenceState.DETACHED);
        return result;

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
}
