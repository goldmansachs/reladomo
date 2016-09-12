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

import com.gs.fw.common.mithra.MithraObjectFactory;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.notification.listener.FullCacheMithraNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraNotificationListener;



public class FullNonDatedTransactionalCache extends AbstractNonDatedTransactionalCache
{
    private FullCacheMithraNotificationListener notificationListener;
    
    public FullNonDatedTransactionalCache(Attribute[] pkAttributes, MithraObjectFactory factory)
    {
        super(pkAttributes, factory, 0, 0);
    }

    public FullNonDatedTransactionalCache(Attribute[] pkAttributes, MithraObjectFactory factory, Attribute[] immutableAttributes)
    {
        super(pkAttributes, factory, immutableAttributes, 0, 0);
    }

//    public void clear()
//    {
//        this.getLogger().error("clearing full cache not yet implemented");
//    }

    @Override
    protected Index createIndex(String indexName, Extractor[] extractors)
    {
        return new TransactionalNonUniqueIndex(indexName, this.getPrimaryKeyAttributes(), extractors);
    }

    @Override
    protected PrimaryKeyIndex createPrimaryKeyIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive)
    {
        return new TransactionalFullUniqueIndex(indexName, extractors);
    }

    @Override
    protected Index createUniqueIndex(String indexName, Extractor[] extractors, long timeToLive, long relationshipTimeToLive)
    {
        return new TransactionalFullUniqueIndex(indexName, extractors);
    }

    public boolean isFullCache()
    {
        return true;
    }

    public boolean isPartialCache()
    {
        return false;
    }

    public MithraNotificationListener createNotificationListener(MithraObjectPortal portal)
    {
        FullCacheMithraNotificationListener local = this.notificationListener;
        if (local == null || local.getMithraObjectPortal() != portal)
        {
            local = new FullCacheMithraNotificationListener(portal);
            notificationListener = local;
        }
        return local;
    }
    
}
