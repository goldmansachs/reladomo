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
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;



public class RemoteExtractOperationDatabaseIdentifiersResult extends MithraRemoteResult
{

    private transient Operation operation;
    private Map databaseIdentifierMap;

    public RemoteExtractOperationDatabaseIdentifiersResult(Operation op)
    {
        this.operation = op;
    }

    public RemoteExtractOperationDatabaseIdentifiersResult()
    {
        //for externalizable
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int dbIdSize = in.readInt();
        databaseIdentifierMap = new UnifiedMap(dbIdSize);
        for(int i = 0; i < dbIdSize; i++)
        {
            String finderClassname = in.readUTF();
            RelatedFinder finderClass = null;
            try
            {
                finderClass = (RelatedFinder) Class.forName(finderClassname).newInstance();
            }
            catch (InstantiationException e)
            {
                throw new RuntimeException();
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException();
            }
            Object sourceAttributeValue = in.readObject();
            String databaseIdentifier = in.readUTF();

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
            out.writeUTF(classname);
            out.writeObject(sourceAttributeValue);
            out.writeUTF(databaseIdentifier);
        }
    }

    public void run()
    {
        MithraObjectPortal mithraObjectPortal = operation.getResultObjectPortal();
        databaseIdentifierMap = mithraObjectPortal.extractDatabaseIdentifiers(operation);
    }

    public Map getDatabaseIdentifierMap()
    {
        return databaseIdentifierMap;
    }
}
