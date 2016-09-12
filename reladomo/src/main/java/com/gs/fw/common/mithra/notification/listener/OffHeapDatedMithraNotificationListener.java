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

package com.gs.fw.common.mithra.notification.listener;


import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.offheap.OffHeapSyncableCache;
import com.gs.fw.common.mithra.finder.Operation;

public class OffHeapDatedMithraNotificationListener implements MithraNotificationListener
{
    private final OffHeapSyncableCache offHeapSyncableCache;
    private final FullDatedCacheMithraNotificationListener delegate;

    public OffHeapDatedMithraNotificationListener(OffHeapSyncableCache offHeapSyncableCache, MithraObjectPortal mithraObjectPortal)
    {
        this.offHeapSyncableCache = offHeapSyncableCache;
        this.delegate = new FullDatedCacheMithraNotificationListener(mithraObjectPortal);
    }

    @Override
    public void onInsert(MithraDataObject[] mithraDataObjects, Object sourceAttributeValue)
    {
        if (!offHeapSyncableCache.isReplicated())
        {
            delegate.onInsert(mithraDataObjects, sourceAttributeValue);
        }
    }

    @Override
    public void onUpdate(MithraDataObject[] mithraDataObjects, Attribute[] updatedAttributes, Object sourceAttributeValue)
    {
        if (!offHeapSyncableCache.isReplicated())
        {
            delegate.onUpdate(mithraDataObjects, updatedAttributes, sourceAttributeValue);
        }
    }

    @Override
    public void onDelete(MithraDataObject[] mithraDataObjects)
    {
        if (!offHeapSyncableCache.isReplicated())
        {
            delegate.onDelete(mithraDataObjects);
        }
    }

    @Override
    public void onMassDelete(Operation op)
    {
        if (!offHeapSyncableCache.isReplicated())
        {
            delegate.onMassDelete(op);
        }
    }

    @Override
    public String getFinderClassname()
    {
        return delegate.getFinderClassname();
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return delegate.getMithraObjectPortal();
    }
}
