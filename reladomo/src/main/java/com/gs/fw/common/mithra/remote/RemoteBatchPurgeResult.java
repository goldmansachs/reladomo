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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;


public class RemoteBatchPurgeResult extends MithraRemoteResult
{

    private transient List mithraDataObjects;
    private transient int hierarchyDepth;

    public RemoteBatchPurgeResult(List mithraDataObjects, int hierarchyDepth)
    {
        this.mithraDataObjects = mithraDataObjects;
        this.hierarchyDepth = hierarchyDepth;
    }

    public RemoteBatchPurgeResult()
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
        boolean isDated = mithraObjectPortal.getFinder().getAsOfAttributes() != null;
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

                //Set the state for all the intransaction objects to deleted
                InternalList inTxObjects = container.getInTxObjects();
                if (inTxObjects != null)
                {
                    for(int j=0; j<inTxObjects.size(); j++)
                    {
                        InTransactionDatedTransactionalObject txObjectToRemove = (InTransactionDatedTransactionalObject) inTxObjects.get(j);
                        txObjectToRemove.zSetDeleted();
                    }
                }

                //Remove all other dates from cache
                List allDataIgnoringDates = cache.getDatedDataIgnoringDates(dataToRemove);
                for (int j=0; j<allDataIgnoringDates.size(); j++)
                {
                    MithraDataObject dataObject = (MithraDataObject) allDataIgnoringDates.get(j);

                    InTransactionDatedTransactionalObject otherDatedObject = new InTransactionDatedTransactionalObject(mithraObjectPortal,
                        dataObject, null, InTransactionDatedTransactionalObject.DELETED_STATE);
                    tx.enrollObject(otherDatedObject, otherDatedObject.zGetCache());
                    cache.removeDatedData(dataObject);
                }

                txObjects.add(txObject);
                container.clearAllObjects();
            }
        }
        else
        {
            throw new RuntimeException("Cannot purge non-dated objects");
        }
        mithraObjectPortal.incrementClassUpdateCount();
        mithraObjectPortal.getMithraObjectPersister().batchPurge(txObjects);
    }
}
