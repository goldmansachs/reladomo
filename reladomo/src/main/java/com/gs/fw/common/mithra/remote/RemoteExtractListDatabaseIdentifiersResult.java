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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;



public class RemoteExtractListDatabaseIdentifiersResult extends MithraRemoteResult
{
    private String finderClassname;
    private Set sourceAttributeValueSet;
    private Map databaseIdentifierMap;

    public RemoteExtractListDatabaseIdentifiersResult(String finderClassname, Set sourceAttributeValueSet)
    {
        this.finderClassname = finderClassname;
        this.sourceAttributeValueSet = sourceAttributeValueSet;
    }

    public RemoteExtractListDatabaseIdentifiersResult()
    {
        //for externalizable
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int dbIdSize = in.readInt();
        databaseIdentifierMap = new UnifiedMap(dbIdSize);
        for(int i = 0; i < dbIdSize; i++)
        {
            String finderClassname = (String)in.readObject();
            RelatedFinder finderClass = instantiateRelatedFinder(finderClassname);
            Object sourceAttributeValue = in.readObject();
            String databaseIdentifier = (String) in.readObject();

            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(sourceAttributeValue, finderClass);
            databaseIdentifierMap.put(key, databaseIdentifier);
        }

    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
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

    public void run()
    {
        RelatedFinder finder = this.instantiateRelatedFinder(finderClassname);
        MithraObjectPortal portal = finder.getMithraObjectPortal();
        databaseIdentifierMap = portal.extractDatabaseIdentifiers(sourceAttributeValueSet);
    }

    public Map getDatabaseIdentifierMap()
    {
        return databaseIdentifierMap;
    }
}
