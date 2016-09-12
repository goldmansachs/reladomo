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
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;



public class RemoteBatchDeleteResult extends MithraRemoteResult
{

    private transient List mithraDataObjects;
    private transient int hierarchyDepth;

    public RemoteBatchDeleteResult(List mithraDataObjects, int hierarchyDepth)
    {
        this.mithraDataObjects = mithraDataObjects;
        this.hierarchyDepth = hierarchyDepth;
    }

    public RemoteBatchDeleteResult()
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
        MithraDataObject mithraDataObject = (MithraDataObject) mithraDataObjects.get(0);
        MithraObjectPortal mithraObjectPortal = mithraDataObject.zGetMithraObjectPortal(this.hierarchyDepth);
        Cache cache = mithraObjectPortal.getCache();
        AsOfAttribute[] asOfAttributes = mithraObjectPortal.getFinder().getAsOfAttributes();
        boolean isDated = asOfAttributes != null;
        List txObjects = new ArrayList(mithraDataObjects.size());
        if (isDated)
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            for(int i=0;i<mithraDataObjects.size();i++)
            {
                mithraDataObject = (MithraDataObject) mithraDataObjects.get(i);
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
                if (dataToRemove == null)
                {
                    dataToRemove = txObject.zGetTxDataForRead();
                }
                if (dataToRemove != null) cache.removeDatedData(dataToRemove);
                tx.enrollObject(txObject, cache);
                txObject.zSetDeleted();
                txObjects.add(txObject);
            }
        }
        else
        {
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            for(int i=0;i<mithraDataObjects.size();i++)
            {
                mithraDataObject = (MithraDataObject) mithraDataObjects.get(i);
                MithraTransactionalObject mithraObject = (MithraTransactionalObject) cache.getObjectByPrimaryKey(mithraDataObject, true);
                if (mithraObject == null)
                {
                    // must've fallen off the cache
                    MithraDataObject txData = mithraObjectPortal.refresh(mithraDataObject, true);
                    mithraObject = (MithraTransactionalObject) cache.getObjectFromData(txData);
                }
                if (!mithraObject.zIsParticipatingInTransaction(tx))
                {
                    throw new MithraTransactionException("object should already be in transaction!");
                }
                if (this.hierarchyDepth == 0)
                {
                    cache.remove(mithraObject);
                }
                mithraObject.zPrepareForDelete();
                mithraObject.zSetTxPersistenceState(PersistenceState.DELETED);
                txObjects.add(mithraObject);
            }

        }
        mithraObjectPortal.incrementClassUpdateCount();
        mithraObjectPortal.getMithraObjectPersister().batchDelete(txObjects);
    }
}
