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

package com.gs.fw.common.mithra.portal;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObjectDeserializer;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheUpdateNotAllowedTxParticipationMode;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.AnalyzedOperation;
import com.gs.fw.common.mithra.finder.NonTransactionalUpdateCountHolder;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.tempobject.MithraTuplePersister;

import java.util.List;
import java.util.Set;



public class MithraReadOnlyPortal extends MithraAbstractObjectPortal
{

    public TxParticipationMode getTxParticipationMode(MithraTransaction tx)
    {
        return ReadCacheUpdateNotAllowedTxParticipationMode.getInstance();
    }

    public TxParticipationMode getTxParticipationMode()
    {
        return ReadCacheUpdateNotAllowedTxParticipationMode.getInstance();
    }

    public void clearTxParticipationMode(MithraTransaction tx)
    {
        throw new RuntimeException("should never get here!");
    }

    public void setTxParticipationMode(TxParticipationMode txParticipationMode, MithraTransaction tx)
    {
        throw new RuntimeException("read only objects only support read cache participation mode!");
    }

    public MithraReadOnlyPortal(MithraObjectDeserializer databaseObject, Cache cache,
            RelatedFinder finder, int relationshipCacheSize, int minQueriesToKeep,
            RelatedFinder[] superClassFinders, RelatedFinder[] subClassFinders, String uniqueAlias,
            int hierarchyDepth, MithraObjectReader mithraObjectPersister)
    {
        super(databaseObject, cache, finder, relationshipCacheSize, minQueriesToKeep, hierarchyDepth,
                new NonTransactionalUpdateCountHolder(), mithraObjectPersister, (MithraTuplePersister) mithraObjectPersister, false);
        this.setSuperClassFinders(superClassFinders);
        this.setSubClassFinders(subClassFinders);
        this.setUniqueAlias(uniqueAlias);
    }

    @Override
    protected CachedQuery findInCache(Operation op, AnalyzedOperation analyzedOperation, OrderBy orderby, boolean forRelationship)
    {
        return findInCacheForNoTransaction(op, analyzedOperation, orderby, forRelationship);
    }

    public List findForMassDeleteInMemory(Operation op, MithraTransaction tx)
    {
        throw new RuntimeException("should never get here. a read-only objects cannot be deleted");
    }

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin)
    {
        throw new RuntimeException("can't delete read only objects");
    }

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin)
    {
        throw new RuntimeException("can't purge read only objects");
    }

    public void registerForApplicationNotification(String subject, MithraApplicationNotificationListener listener,
                                                   List mithraObjectList, Operation operation)
    {

    }

    public void registerForApplicationClassLevelNotification(MithraApplicationClassLevelNotificationListener listener)
    {
        throw new RuntimeException("can't register for notification on read-only objects");
    }

    public void registerForApplicationClassLevelNotification(Set sourceAttributeValueSet, MithraApplicationClassLevelNotificationListener listener)
    {
        throw new RuntimeException("can't register for notification on read-only objects");
    }

    public void setDefaultTxParticipationMode(TxParticipationMode mode)
    {
        throw new MithraBusinessException("Transaction participation modes are only valid for transactional objects");
    }
}
