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
import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.behavior.inmemory.InMemorySameTxBehavior;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;
import java.util.Map;



public class DetachedSameTxBehavior extends InMemorySameTxBehavior
{

    public DetachedSameTxBehavior()
    {
        super(true);
    }

    public void insert(MithraTransactionalObject obj)
    {
        throw new MithraBusinessException("a detached object may not be inserted!");
    }

    public void delete(MithraTransactionalObject obj)
    {
        obj.zSetTxDetachedDeleted();
    }

    public boolean maySetPrimaryKey()
    {
        return false;
    }

    @Override
    public MithraTransactionalObject copyThenInsert(MithraTransactionalObject obj)
    {
        MithraTransactionalObject original = obj.zFindOriginal();
        if (original == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        MithraDataObject currentDataForRead = this.getCurrentDataForRead(obj);
        original.zCopyAttributesFrom(currentDataForRead);
        return original;
    }

    public MithraTransactionalObject updateOriginalOrInsert(MithraTransactionalObject obj)
    {
        MithraTransactionalObject original = obj.zFindOriginal();
        if (original == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        MithraDataObject currentDataForRead = this.getCurrentDataForRead(obj);
        original.zCopyAttributesFrom(currentDataForRead);
        original.zPersistDetachedRelationships(currentDataForRead);
        return original;
    }

    public void persistChildDelete(MithraTransactionalObject obj)
    {
        MithraTransactionalObject original = obj.zFindOriginal();
        if (original == null)
        {
            throw new MithraDeletedException("original object was deleted and cannot be updated");
        }
        original.zPersistDetachedChildDelete(this.getCurrentDataForRead(obj));
    }

    public boolean isModifiedSinceDetachment(MithraTransactionalObject obj)
    {
        MithraTransactionalObject mithraTransactionalObject = obj.zFindOriginal();
        if (mithraTransactionalObject == null) return true;
        return mithraTransactionalObject.zIsDataChanged(this.getCurrentDataForRead(obj));
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> addNavigatedRelationshipsStats(MithraTransactionalObject obj, RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        return obj.zAddNavigatedRelationshipsStatsForUpdate(parentFinder, navigationStats);
    }
}
