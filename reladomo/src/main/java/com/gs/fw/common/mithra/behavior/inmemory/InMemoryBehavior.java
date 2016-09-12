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
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import java.util.List;
import java.util.Map;



public abstract class InMemoryBehavior extends TransactionalBehavior
{

    protected InMemoryBehavior(short dataReadType, String readDataExceptionMessage)
    {
        super(false, false, true, false, dataReadType, readDataExceptionMessage);
    }

    protected InMemoryBehavior(boolean detached, short dataReadType, String readDataExceptionMessage)
    {
        super(false, false, true, detached, dataReadType, readDataExceptionMessage);
    }

    public void insertForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        throw new RuntimeException("should not get here");
    }

    public void delete(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("Cannot delete an object that is not in the database!");
    }

    public void deleteForRemote(MithraTransactionalObject obj, int hierarchyDepth)
    {
        throw new MithraBusinessException("Cannot delete an object that is not in the database!");
    }

    public MithraTransactionalObject updateOriginalOrInsert(MithraTransactionalObject obj)
    {
        return obj.zCascadeCopyThenInsert();
    }

    public void persistChildDelete(MithraTransactionalObject obj)
    {
        // nothing to do
    }

    public MithraTransactionalObject getDetachedCopy(MithraTransactionalObject obj)
    {
        throw new RuntimeException("Only persisted objects may be detached");
    }

    public void remoteUpdate(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("should never get here");
    }

    public void remoteUpdateForBatch(MithraTransactionalObject obj, List updateWrappers)
    {
        throw new RuntimeException("should never get here");
    }

    public boolean maySetPrimaryKey()
    {
        return true;
    }

    public MithraDataObject getOrAllocateNonTxDataForRead(MithraTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetNonTxData();
        if (data == null)
        {
            data = mithraObject.zAllocateData();
            mithraObject.zSetNonTxData(data);
        }
        return data;
    }

    public boolean isModifiedSinceDetachment(MithraTransactionalObject obj)
    {
        return true;
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
