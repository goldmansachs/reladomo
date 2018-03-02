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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.state.PersistedState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class RemoteBatchInsertResult extends MithraRemoteResult
{

    private transient List mithraDataObjects;
    private transient int hierarchyDepth;
    private transient int bulkInsertThreshold;
    private Map databaseIdentifierMap;

    public RemoteBatchInsertResult()
    {
        // for externalizable
    }

    public RemoteBatchInsertResult(List mithraDataObjects, int hierarchyDepth, int bulkInsertThreshold)
    {
        this.mithraDataObjects = mithraDataObjects;
        this.hierarchyDepth = hierarchyDepth;
        this.bulkInsertThreshold = bulkInsertThreshold;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        Set databaseIdentifierKeySet = databaseIdentifierMap.keySet();
        out.writeInt(databaseIdentifierKeySet.size());
        for(Iterator it = databaseIdentifierKeySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey) it.next();
            String classname = key.getFinder().getClass().getName();
            Object sourceAttributeValue = key.getSourceAttributeValue();
            String databaseIdentifier = (String) databaseIdentifierMap.get(key);
            out.writeObject(classname);
            out.writeObject(sourceAttributeValue);
            out.writeObject(databaseIdentifier);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        int dbIdSize = in.readInt();
        databaseIdentifierMap = new UnifiedMap(dbIdSize);
        for(int i = 0; i < dbIdSize; i++)
        {
            String finderClassname = (String)in.readObject();
            RelatedFinder finderClass = this.instantiateRelatedFinder(finderClassname);
            Object sourceAttributeValue = in.readObject();
            String databaseIdentifier = (String) in.readObject();

            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(sourceAttributeValue, finderClass);
            databaseIdentifierMap.put(key, databaseIdentifier);
        }
    }

    public void run()
    {
        MithraDataObject mithraDataObject = (MithraDataObject) mithraDataObjects.get(0);
        MithraObjectPortal mithraObjectPortal = mithraDataObject.zGetMithraObjectPortal(this.hierarchyDepth);
        Cache cache = mithraObjectPortal.getCache();
        boolean isDated = mithraObjectPortal.getFinder().getAsOfAttributes() != null;
        List txObjects = new ArrayList(mithraDataObjects.size());
        MithraTransactionalObject[] objectsToSetInserted = new MithraTransactionalObject[mithraDataObjects.size()];
        if (isDated)
        {
            for(int i=0;i<mithraDataObjects.size();i++)
            {
                mithraDataObject = (MithraDataObject) mithraDataObjects.get(i);
                TemporalContainer container = cache.getOrCreateContainer(mithraDataObject);
                InTransactionDatedTransactionalObject inTxObject = container.makeUninsertedDataActiveAndCreateObject(mithraDataObject);
                cache.putDatedData(mithraDataObject);
                txObjects.add(inTxObject);
                objectsToSetInserted[i] = inTxObject;
            }
        }
        else
        {
            for(int i=0;i<mithraDataObjects.size();i++)
            {
                mithraDataObject = (MithraDataObject) mithraDataObjects.get(i);
                MithraTransactionalObject txObject = (MithraTransactionalObject)
                        mithraObjectPortal.getMithraObjectFactory().createObject(mithraDataObject);
                txObject.zPrepareForRemoteInsert();
                txObject.zSetTxPersistenceState(PersistedState.PERSISTED);
                cache.put(txObject);
                txObjects.add(txObject);
                objectsToSetInserted[i] = txObject;
            }
        }
        mithraObjectPortal.incrementClassUpdateCount();
        mithraObjectPortal.getMithraObjectPersister().batchInsert(txObjects, this.bulkInsertThreshold);

        Attribute sourceAttribute = mithraObjectPortal.getFinder().getSourceAttribute();
        Set sourceAttributeValueSet = new UnifiedSet();
        Object sourceAttributeValue = null;
        if(sourceAttribute == null)
        {
            sourceAttributeValueSet.add(sourceAttributeValue);
        }
        for(int i=0;i<objectsToSetInserted.length;i++)
        {
            objectsToSetInserted[i].zSetInserted();
            if(sourceAttribute != null)
            {
                sourceAttributeValue = sourceAttribute.valueOf(mithraDataObjects.get(i));
                sourceAttributeValueSet.add(sourceAttributeValue);
            }
        }
        databaseIdentifierMap = mithraObjectPortal.extractDatabaseIdentifiers(sourceAttributeValueSet);
    }

    public Map getDatabaseIdentifierMap()
    {
        return this.databaseIdentifierMap;
    }
}
