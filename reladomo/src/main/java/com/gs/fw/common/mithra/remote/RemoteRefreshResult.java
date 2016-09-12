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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;



public class RemoteRefreshResult extends MithraRemoteResult
{

    static private Logger logger = LoggerFactory.getLogger(RemoteRefreshResult.class.getName());
    private MithraDataObject mithraDataObject;
    private transient boolean lockInDatabase;

    public RemoteRefreshResult()
    {
        // for externalizable
    }

    public RemoteRefreshResult(MithraDataObject mithraDataObject, boolean lockInDatabase)
    {
        this.mithraDataObject = mithraDataObject;
        this.lockInDatabase = lockInDatabase;
    }

    public MithraDataObject getMithraDataObject()
    {
        return mithraDataObject;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        if (mithraDataObject == null)
        {
            out.writeBoolean(true);
        }
        else
        {
            out.writeBoolean(false);
            out.writeObject(MithraSerialUtil.getDataClassNameToSerialize(mithraDataObject));
            mithraDataObject.zSerializeFullData(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        boolean isNull = in.readBoolean();
        if (!isNull)
        {
            Class dataClass = MithraSerialUtil.getDataClassToInstantiate((String) in.readObject());
            this.mithraDataObject = MithraSerialUtil.instantiateData(dataClass);
            this.mithraDataObject.zDeserializeFullData(in);
        }
    }

    public void run()
    {
        Cache cache = mithraDataObject.zGetMithraObjectPortal().getCache();
        MithraDataObject txData = mithraDataObject.zGetMithraObjectPortal().refresh(mithraDataObject, lockInDatabase);
        MithraTransactionalObject mithraObject = (MithraTransactionalObject) cache.getObjectFromData(txData);
        mithraDataObject = mithraObject.zGetTxDataForRead();
    }
}
