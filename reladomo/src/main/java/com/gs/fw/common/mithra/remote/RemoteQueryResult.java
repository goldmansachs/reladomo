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

import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.querycache.CachedQuery;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;



public class RemoteQueryResult extends MithraRemoteResult
{
    private Operation op;
    private transient OrderBy orderBy;
    private transient boolean bypassCache;
    private transient boolean forRelationship;
    private transient int maxObjectsToRetrieve;
    private transient ServerContext serverContext;
    private transient boolean forceImplicitJoin;

    private List deserializedResult;
    private List serverSideList;
    private boolean reachedMaxRetrieveCount;
   // private Set databaseIdentifiers;
    private Map databaseIdentifierMap;

    public RemoteQueryResult(Operation op, OrderBy orderBy, boolean bypassCache, boolean forRelationship,
            int maxObjectsToRetrieve, ServerContext serverContext, boolean forceImplicitJoin)
    {
        this.op = op;
        this.orderBy = orderBy;
        this.bypassCache = bypassCache;
        this.forRelationship = forRelationship;
        this.maxObjectsToRetrieve = maxObjectsToRetrieve;
        this.serverContext = serverContext;
        this.forceImplicitJoin = forceImplicitJoin;
    }

    public RemoteQueryResult()
    {
        // for externalizable
    }

    public void run()
    {
        CachedQuery resolved = op.getResultObjectPortal().findAsCachedQuery(op,
                orderBy, bypassCache, forRelationship, maxObjectsToRetrieve, forceImplicitJoin);
        serverSideList = resolved.getResult();
        int lastIndex = serverSideList.size();
        reachedMaxRetrieveCount = resolved.reachedMaxRetrieveCount();
        if (maxObjectsToRetrieve > 0 && lastIndex > maxObjectsToRetrieve)
        {
            serverSideList = serverSideList.subList(0, maxObjectsToRetrieve);
            reachedMaxRetrieveCount = true;
        }

         databaseIdentifierMap = op.getResultObjectPortal().extractDatabaseIdentifiers(op);
    }

    public boolean isReachedMaxRetrieveCount()
    {
        return reachedMaxRetrieveCount;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        this.op = (Operation) in.readObject();
        this.reachedMaxRetrieveCount = in.readBoolean();
        MithraObjectPortal mithraObjectPortal = op.getResultObjectPortal();
        int serverVersion = in.readInt();
        int localVersion = mithraObjectPortal.getFinder().getSerialVersionId();
        if (serverVersion != localVersion)
        {
            throw new IOException("version of the object "+mithraObjectPortal.getFinder().getClass().getName()+
                    " does not match this version. Server version "+serverVersion+" local version "+localVersion);
        }
        this.deserializedResult = mithraObjectPortal.getMithraObjectDeserializer().deserializeList(op, in, false);

        this.databaseIdentifierMap = readDatabaseIdentifierMap(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        out.writeObject(op);
        out.writeBoolean(this.reachedMaxRetrieveCount);
        out.writeInt(op.getResultObjectPortal().getFinder().getSerialVersionId());
        out.writeInt(serverSideList.size());
        for(int i=0;i<serverSideList.size();i++)
        {
            MithraObject mithraObject = (MithraObject) serverSideList.get(i);
            serverContext.serializeFullData(mithraObject, out);
        }
        writeDatabaseIdentifierMap(out, this.databaseIdentifierMap);
    }

    public List getDeserializedResult()
    {
        return deserializedResult;
    }

    public int getServerSideSize()
    {
        return serverSideList.size();
    }

    public Map getDatabaseIdentifierMap()
    {
        return databaseIdentifierMap;
    }

    public void registerForNotification()
    {
        registerForNotification(this.getDatabaseIdentifierMap());
    }

}
