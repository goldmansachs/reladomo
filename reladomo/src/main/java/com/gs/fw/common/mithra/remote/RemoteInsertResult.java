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
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;



public class RemoteInsertResult extends MithraRemoteResult
{

    private transient MithraDataObject mithraDataObject;
    private transient int hierarchyDepth;
    private Map<MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey, String> databaseIdentifierMap;

    public RemoteInsertResult(MithraDataObject mithraDataObject, int hierarchyDepth)
    {
        this.mithraDataObject = mithraDataObject;
        this.hierarchyDepth = hierarchyDepth;
    }

    public RemoteInsertResult()
    {
        // for externalizable
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);

        Set<MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey> databaseIdentifierKeySet = databaseIdentifierMap.keySet();
        out.writeInt(databaseIdentifierKeySet.size());
        for(Iterator<MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey> it = databaseIdentifierKeySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key = it.next();
            String classname = key.getFinder().getClass().getName();
            Object sourceAttributeValue = key.getSourceAttributeValue();
            String databaseIdentifier = databaseIdentifierMap.get(key);
            out.writeObject(classname);
            out.writeObject(sourceAttributeValue);
            out.writeObject(databaseIdentifier);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        int dbIdSize = in.readInt();
        this.databaseIdentifierMap = new UnifiedMap<MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey, String>(dbIdSize);
        for(int i = 0; i < dbIdSize; i++)
        {
            String finderClassname = (String)in.readObject();
            RelatedFinder finderClass = this.instantiateRelatedFinder(finderClassname);
            Object sourceAttributeValue = in.readObject();
            String databaseIdentifier = (String) in.readObject();

            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(sourceAttributeValue, finderClass);
            this.databaseIdentifierMap.put(key, databaseIdentifier);
        }
    }

    public void run()
    {
        MithraObjectPortal mithraObjectPortal = this.mithraDataObject.zGetMithraObjectPortal(this.hierarchyDepth);
        boolean isDated = mithraObjectPortal.getFinder().getAsOfAttributes() != null;
        if (isDated)
        {
            Cache cache = mithraObjectPortal.getCache();
            TemporalContainer container = cache.getOrCreateContainer(this.mithraDataObject);
            InTransactionDatedTransactionalObject inTxObject = container.makeUninsertedDataActiveAndCreateObject(this.mithraDataObject);
            cache.putDatedData(this.mithraDataObject);
            mithraObjectPortal.getMithraObjectPersister().insert(this.mithraDataObject);
            inTxObject.zSetInserted();
            mithraObjectPortal.incrementClassUpdateCount();
        }
        else
        {
            MithraTransactionalObject txObject = (MithraTransactionalObject)
                    mithraObjectPortal.getMithraObjectFactory().createObject(this.mithraDataObject);
            txObject.zPrepareForRemoteInsert();
            txObject.zInsertForRemote(this.hierarchyDepth);
        }
        Attribute sourceAttribute = mithraObjectPortal.getFinder().getSourceAttribute();
        Object sourceAttributeValue = null;
        if(sourceAttribute != null)
        {
            sourceAttributeValue = sourceAttribute.valueOf(this.mithraDataObject);
        }
        Set sourceAttributeValueSet = new UnifiedSet(1);
        sourceAttributeValueSet.add(sourceAttributeValue);
        this.databaseIdentifierMap = mithraObjectPortal.extractDatabaseIdentifiers(sourceAttributeValueSet);
    }

    public Number getIdentityValue()
    {
        return this.mithraDataObject.zGetIdentityValue();
    }
    
    public Map getDatabaseIdentifierMap()
    {
        return this.databaseIdentifierMap;
    }
}
