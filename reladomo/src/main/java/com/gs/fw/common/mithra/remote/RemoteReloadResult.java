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
import com.gs.fw.common.mithra.querycache.CachedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RemoteReloadResult extends MithraRemoteResult
{
    static private Logger logger = LoggerFactory.getLogger(RemoteReloadResult.class.getName());

    private transient ServerContext serverContext;
    private transient List operations;
    private transient List serverSideList;
    private transient Map databaseIdentifierMap;

    public RemoteReloadResult(List operations, ServerContext serverContext)
    {
        this.operations = operations;
        this.serverContext = serverContext;
    }

    public RemoteReloadResult()
    {
        // for externalizable
    }

    public void run()
    {
        Operation op = (Operation) operations.get(0);
        CachedQuery resolved = op.getResultObjectPortal().findAsCachedQuery(op,
                null, false, false, 0, false);
        serverSideList = new ArrayList(resolved.getResult());
        this.databaseIdentifierMap = new HashMap(op.getResultObjectPortal().extractDatabaseIdentifiers(op));
        for(int i=1;i<operations.size();i++)
        {
            op = (Operation) operations.get(i);
            resolved = op.getResultObjectPortal().findAsCachedQuery(op,
                    null, false, false, 0, false);
            serverSideList.addAll(resolved.getResult());

            this.databaseIdentifierMap.putAll(op.getResultObjectPortal().extractDatabaseIdentifiers(op));
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        Operation op = (Operation) in.readObject();
        MithraObjectPortal mithraObjectPortal = op.getResultObjectPortal();
        int serverVersion = in.readInt();
        int localVersion = mithraObjectPortal.getFinder().getSerialVersionId();
        if (serverVersion != localVersion)
        {
            throw new IOException("version of the object "+mithraObjectPortal.getFinder().getClass().getName()+
                    " does not match this version. Server version "+serverVersion+" local version "+localVersion);
        }
        mithraObjectPortal.getMithraObjectDeserializer().deserializeForReload(in);

        this.databaseIdentifierMap = readDatabaseIdentifierMap(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        Operation op = (Operation) operations.get(0);
        out.writeObject(op);
        out.writeInt(op.getResultObjectPortal().getFinder().getSerialVersionId());
        out.writeInt(serverSideList.size());
        for(int i=0;i<serverSideList.size();i++)
        {
            MithraObject mithraObject = (MithraObject) serverSideList.get(i);
            serverContext.serializeFullData(mithraObject, out);
        }
        writeDatabaseIdentifierMap(out, this.databaseIdentifierMap);
    }

    public int getServerSideSize()
    {
        return serverSideList.size();
    }

    public void registerForNotification()
    {
        registerForNotification(this.getDatabaseIdentifierMap());
    }

    public Map getDatabaseIdentifierMap()
    {
        return databaseIdentifierMap;
    }
}
