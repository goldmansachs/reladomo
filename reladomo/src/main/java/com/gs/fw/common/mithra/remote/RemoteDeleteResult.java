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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;



public class RemoteDeleteResult extends MithraRemoteResult
{

    private transient MithraDataObject mithraDataObject;
    private transient int hierarchyDepth;

    public RemoteDeleteResult(MithraDataObject mithraDataObject, int hierarchyDepth)
    {
        this.mithraDataObject = mithraDataObject;
        this.hierarchyDepth = hierarchyDepth;
    }

    public RemoteDeleteResult()
    {
        // for externalizable
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
    }

    public void run()
    {
        MithraObjectPortal mithraObjectPortal = mithraDataObject.zGetMithraObjectPortal(this.hierarchyDepth);
        Cache cache = mithraObjectPortal.getCache();
        boolean isDated = mithraObjectPortal.getFinder().getAsOfAttributes() != null;
        if (isDated)
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            TemporalContainer container = cache.getOrCreateContainer(mithraDataObject);
            InTransactionDatedTransactionalObject txObject = container.getObjectForTx(mithraDataObject);
            MithraDataObject dataToRemove;
            if (txObject != null)
            {
                dataToRemove = txObject.zGetTxDataForRead();
            }
            else
            {
                dataToRemove = mithraDataObject;
                txObject = new InTransactionDatedTransactionalObject(mithraObjectPortal, dataToRemove, null, InTransactionDatedTransactionalObject.DELETED_STATE);
                tx.enrollObject(txObject, txObject.zGetCache());
            }
            if (dataToRemove != null) cache.removeDatedData(dataToRemove);
            tx.enrollObject(txObject, cache);
            txObject.zSetDeleted();
            tx.delete(txObject);
            mithraObjectPortal.incrementClassUpdateCount();
        }
        else
        {

            MithraTransactionalObject mithraObject = (MithraTransactionalObject) cache.getObjectByPrimaryKey(mithraDataObject, true);
            if (mithraObject == null)
            {
                // must've fallen off the cache
                MithraDataObject txData = mithraObjectPortal.refresh(mithraDataObject, false);
                mithraObject = (MithraTransactionalObject) cache.getObjectFromData(txData);
            }
            mithraObject.zDeleteForRemote(this.hierarchyDepth);
        }
    }
}
