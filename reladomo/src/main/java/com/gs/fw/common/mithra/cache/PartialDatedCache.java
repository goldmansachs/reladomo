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

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.MithraDatedObjectFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDataStorage;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.notification.listener.MithraNotificationListener;
import com.gs.fw.common.mithra.notification.listener.PartialDatedCacheMithraNotificationListener;



public class PartialDatedCache extends AbstractDatedCache implements ReferenceListener
{
    private PartialDatedCacheMithraNotificationListener notificationListener;
    
    public PartialDatedCache(Attribute[] nonDatedPkAttributes, AsOfAttribute[] asOfAttributes,
            MithraDatedObjectFactory factory, Attribute[] immutableAttributes, long timeToLive, long relationshipTimeToLive)
    {
        super(nonDatedPkAttributes, asOfAttributes, factory, immutableAttributes, timeToLive, relationshipTimeToLive, null);
    }

    @Override
    protected Index createIndex(String indexName, Extractor[] extractors, OffHeapDataStorage dataStorage)
    {
        return null; // this is a partial cache
    }

    @Override
    protected SemiUniqueDatedIndex createSemiUniqueDatedIndex(String indexName, Extractor[] extractors, AsOfAttribute[] asOfAttributes, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage)
    {
        return new PartialSemiUniqueDatedIndex(indexName, extractors, asOfAttributes, timeToLive, relationshipTimeToLive);
    }

    public boolean isFullCache()
    {
        return false;
    }

    public boolean isPartialCache()
    {
        return true;
    }

    public MithraNotificationListener createNotificationListener(MithraObjectPortal portal)
    {
        PartialDatedCacheMithraNotificationListener local = this.notificationListener;
        if (local == null || local.getMithraObjectPortal() != portal)
        {
            local = new PartialDatedCacheMithraNotificationListener(portal);
            notificationListener = local;
        }
        return local;
    }
}
