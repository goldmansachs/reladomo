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

package com.gs.fw.common.mithra.cache.offheap;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedObjectFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.notification.listener.MithraNotificationListener;
import com.gs.fw.common.mithra.notification.listener.OffHeapDatedMithraNotificationListener;

import java.util.List;


public class OffHeapFullDatedTransactionalCache extends AbstractDatedTransactionalCache implements OffHeapSyncableCache
{
    private OffHeapDatedMithraNotificationListener notificationListener;
    private OffHeapDataStorage dataStorage;
    private boolean replicationMode;

    public OffHeapFullDatedTransactionalCache(Attribute[] nonDatedPkAttributes, AsOfAttribute[] asOfAttributes,
            MithraDatedObjectFactory factory, Attribute[] immutableAttributes, OffHeapDataStorage dataStorage)
    {
        super(nonDatedPkAttributes, asOfAttributes, factory, immutableAttributes, 0, 0, dataStorage);
        this.dataStorage = dataStorage;
        this.dataStorage.setReadWriteLock(this.getCacheLock());
    }

    @Override
    protected Index createIndex(String indexName, Extractor[] extractors, OffHeapDataStorage dataStorage)
    {
        Index mainIndex = new NonUniqueOffHeapIndex(extractors, 16, dataStorage);
        return new TransactionalDatedNonUniqueIndex(indexName, getPrimaryKeyAttributes(), extractors, mainIndex);
    }

    @Override
    protected SemiUniqueDatedIndex createSemiUniqueDatedIndex(String indexName, Extractor[] extractors,
            AsOfAttribute[] asOfAttributes, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage)
    {
        return new TransactionalOffHeapSemiUniqueDatedIndex(indexName, extractors, asOfAttributes, dataStorage);
    }

    public boolean isFullCache()
    {
        return true;
    }

    public boolean isPartialCache()
    {
        return false;
    }

    @Override
    public boolean isOffHeap()
    {
        return true;
    }

    @Override
    public long getOffHeapAllocatedDataSize()
    {
        return dataStorage.getAllocatedSize();
    }

    @Override
    public long getOffHeapUsedDataSize()
    {
        return dataStorage.getUsedSize();
    }

    @Override
    public long getOffHeapAllocatedIndexSize()
    {
        long sum = 0;
        try
        {
            this.getCacheLock().acquireReadLock();
            Index[] indices = this.getIndices();
            for(int i=1;i<indices.length;i++) // start from 1 so we don't double count the semi unique PK index
            {
                sum += indices[i].getOffHeapAllocatedIndexSize();
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
        return sum;
    }

    @Override
    public long getOffHeapUsedIndexSize()
    {
        long sum = 0;
        try
        {
            this.getCacheLock().acquireReadLock();
            Index[] indices = this.getIndices();
            for(int i=1;i<indices.length;i++) // start from 1 so we don't double count the semi unique PK index
            {
                sum += indices[i].getOffHeapUsedIndexSize();
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
        return sum;
    }

    public MithraNotificationListener createNotificationListener(MithraObjectPortal portal)
    {
        OffHeapDatedMithraNotificationListener local = this.notificationListener;
        if (local == null || local.getMithraObjectPortal() != portal)
        {
            local = new OffHeapDatedMithraNotificationListener(this, portal);
            notificationListener = local;
        }
        return local;
    }

    @Override
    protected MithraDataObject copyDataForCacheIgnoringTransaction(MithraDataObject dataObject)
    {
        return dataObject.zCopyOffHeap();
    }

    @Override
    protected MithraDataObject copyDataForCacheIfNotInTransaction(MithraDataObject dataObject)
    {
        return dataObject.zCopyOffHeap();
    }

    @Override
    protected List copyDataForCacheIgnoringTransaction(List dataObjects)
    {
        this.dataStorage.ensureExtraCapacity(dataObjects.size());
        List result = FastList.newList(dataObjects.size());
        for(int i=0;i<dataObjects.size();i++)
        {
            result.add(((MithraDataObject)dataObjects.get(i)).zCopyOffHeap());
        }
        return result;
    }

    @Override
    protected void releaseCacheData(MithraDataObject removed)
    {
        if (removed instanceof MithraOffHeapDataObject)
        {
            MithraOffHeapDataObject data = (MithraOffHeapDataObject) removed;
            dataStorage.free(data.zGetOffset());
        }
    }

    @Override
    public void evictCollectedReferences()
    {
        super.evictCollectedReferences();
        dataStorage.evictCollectedReferences();
    }

    @Override
    protected void ensureExtraCapacity(int size)
    {
        super.ensureExtraCapacity(size);
        this.dataStorage.ensureExtraCapacity(size);
    }

    @Override
    protected void reportSpaceUsage()
    {
        String className = this.getPrimaryKeyAttributes()[0].zGetTopOwnerClassName();
        this.dataStorage.reportSpaceUsage(this.getLogger(), className);
        Index[] indexes = this.getIndices();
        for(Index index: indexes)
        {
            index.reportSpaceUsage(this.getLogger(), className);
        }
    }

    @Override
    public boolean syncWithMasterCache(MasterCacheUplink uplink)
    {
        return this.dataStorage.syncWithMasterCache(uplink, this);
    }

    @Override
    public MasterSyncResult sendSyncResult(long maxReplicatedPageVersion)
    {
        return this.dataStorage.sendSyncResult(maxReplicatedPageVersion);
    }

    @Override
    public void setReplicationMode()
    {
        this.replicationMode = true;
    }

    @Override
    public void syncDataRemove(Object data)
    {
        this.zSyncDataRemove(data);
    }

    @Override
    public void syncDataAdd(Object data)
    {
        this.zSyncDataAdd(data);
    }

    @Override
    public boolean isReplicated()
    {
        return this.replicationMode;
    }
}
