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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.HavingOperation;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.*;

import com.gs.collections.impl.map.mutable.UnifiedMap;


public class RemoteAggregateResult  extends MithraRemoteResult
{
    private Operation op;
    private Map<String, MithraAggregateAttribute> aggregateAttributes;
    private Map<String, MithraGroupByAttribute> groupByAttributes;
    private com.gs.fw.common.mithra.HavingOperation havingOperation;
    private List<AggregateData> serverSideList;
    private Map databaseIdentifierMap;
    private boolean bypassCache;

    public RemoteAggregateResult(Operation op, Map<String, MithraAggregateAttribute> aggregateAttributes,
                                 Map<String,MithraGroupByAttribute> groupByAttributes, HavingOperation havingOperation, boolean bypassCache)
    {
        this.op = op;
        this.aggregateAttributes = aggregateAttributes;
        this.groupByAttributes = groupByAttributes;
        this.havingOperation = havingOperation;
        this.bypassCache = bypassCache;
    }

    public RemoteAggregateResult()
    {
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        out.writeObject(op);

        out.writeInt(serverSideList.size());
        for(int i=0;i < serverSideList.size();i++)
        {
            AggregateData aggregateDataObject =  serverSideList.get(i);
            aggregateDataObject.writeExternal(out);
        }

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
        out.writeBoolean(this.bypassCache);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        this.op = (Operation) in.readObject();

        int size = in.readInt();
        this.serverSideList = new ArrayList<AggregateData>(size);
        for(int i = 0; i < size; i++)
        {
            AggregateData data = new AggregateData();
            data.readExternal(in);
            serverSideList.add(data);
        }

        int dbIdSize = in.readInt();
        databaseIdentifierMap = new UnifiedMap(dbIdSize);
        for(int i = 0; i < dbIdSize; i++)
        {
            String finderClassname = (String)in.readObject();
            RelatedFinder finderClass;
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
            String databaseIdentifier = (String) in.readObject();

            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    new MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey(sourceAttributeValue, finderClass);
            databaseIdentifierMap.put(key, databaseIdentifier);
        }
        this.bypassCache = in.readBoolean();
    }

    public void run()
    {
        serverSideList = op.getResultObjectPortal().findAggregatedData(op, aggregateAttributes, groupByAttributes, havingOperation, null, this.bypassCache);
        databaseIdentifierMap = op.getResultObjectPortal().extractDatabaseIdentifiers(op);
    }

    public int getServerSideSize()
    {
        return serverSideList.size();
    }

    public List<AggregateData> getAggregateList()
    {
        return serverSideList;
    }

    public Map getDatabaseIdentifierMap()
    {
        return databaseIdentifierMap;
    }

    public void registerForNotification()
    {
        Map databaseIdentifierMap = this.getDatabaseIdentifierMap();

        Set keySet = databaseIdentifierMap.keySet();
        for(Iterator it = keySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey)it.next();

            RelatedFinder finder = key.getFinder();
            MithraObjectPortal portal = finder.getMithraObjectPortal();
            portal.registerForNotification((String)databaseIdentifierMap.get(key));
        }
    }
}
