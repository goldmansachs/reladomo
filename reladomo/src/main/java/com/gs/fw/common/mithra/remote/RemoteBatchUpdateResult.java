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
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.transaction.UpdateOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;



public class RemoteBatchUpdateResult extends MithraRemoteResult
{

    private MithraDataObject[] mithraDataObjects;
    private List[] updateWrappers;

    public RemoteBatchUpdateResult()
    {
        // for externalizable
    }

    public RemoteBatchUpdateResult(MithraDataObject[] mithraDataObjects, List[] updateWrappers)
    {
        this.mithraDataObjects = mithraDataObjects;
        this.updateWrappers = updateWrappers;
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
        MithraObjectPortal mithraObjectPortal = ((AttributeUpdateWrapper) updateWrappers[0].get(0)).getAttribute().getOwnerPortal();
        MithraRemoteTransactionProxy tx = (MithraRemoteTransactionProxy) MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean isOptimistic = mithraObjectPortal.getTxParticipationMode(tx).isOptimisticLocking();
        VersionAttribute versionAttribute = null;
        if (isOptimistic)
        {
            versionAttribute = mithraObjectPortal.getFinder().getVersionAttribute();
        }
        Cache cache = mithraObjectPortal.getCache();
        AsOfAttribute[] asOfAttributes = mithraObjectPortal.getFinder().getAsOfAttributes();
        boolean isDated = asOfAttributes != null;
        ArrayList updateOperations = new ArrayList(mithraDataObjects.length);
        MithraTransactionalObject[] objectsToSetUpdated = new MithraTransactionalObject[mithraDataObjects.length];
        MithraRemoteOptimisticLockException optimException = null;
        if (isDated)
        {
            for(int i=0;i<mithraDataObjects.length;i++)
            {
                TemporalContainer container = cache.getOrCreateContainer(mithraDataObjects[i]);
                InTransactionDatedTransactionalObject txObject = container.getObjectForTx(mithraDataObjects[i]);
                if (txObject == null && asOfAttributes.length == 2)
                {
                    container.getForDateRange(mithraDataObjects[i], asOfAttributes[0].getFromAttribute().timestampValueOf(mithraDataObjects[i]),
                            asOfAttributes[0].getToAttribute().timestampValueOf(mithraDataObjects[i]));
                    txObject = container.getObjectForTx(mithraDataObjects[i]);
                }
                if (txObject == null)
                {
                    if (mithraObjectPortal.getTxParticipationMode(tx).isOptimisticLocking())
                    {
                        optimException = createOrAddDirtyData(optimException, mithraDataObjects[i]);
                    }
                    else
                    {
                        throw new RuntimeException("could not find tx object for "+mithraDataObjects[i].getClass().getName()+" with pk "+
                            mithraDataObjects[i].zGetPrintablePrimaryKey());
                    }
                }
                else
                {
                    optimException = checkDatedOptimisticLocking(isOptimistic, asOfAttributes, optimException, txObject, mithraDataObjects[i]);
                    MithraDataObject dataToUpdate = txObject.zGetTxDataForWrite();
                    MithraDataObject dataToRemove = txObject.zGetCurrentData();
                    if (dataToRemove == null)
                    {
                        dataToRemove = txObject.zGetTxDataForRead();
                    }
                    cache.removeDatedData(dataToRemove);
                    for(int j=0;j<updateWrappers[i].size();j++)
                    {
                        AttributeUpdateWrapper uw = (AttributeUpdateWrapper) updateWrappers[i].get(j);
                        uw.updateData(dataToUpdate);
                        uw.incrementUpdateCount();
                        tx.enrollAttribute(uw.getAttribute());
                    }
                    cache.putDatedData(dataToUpdate);
                    objectsToSetUpdated[i] = txObject;
                    UpdateOperation updateOperation = new UpdateOperation(txObject, updateWrappers[i]);
                    updateOperations.add(updateOperation);
                }
            }
            mithraObjectPortal.incrementClassUpdateCount(); // necessary because of termination that happens via updates
        }
        else
        {
            for(int i=0;i<mithraDataObjects.length;i++)
            {
                MithraTransactionalObject mithraObject = (MithraTransactionalObject) cache.getObjectByPrimaryKey(mithraDataObjects[i], true);
                if (mithraObject == null)
                {
                    // must've fallen off the cache
                    MithraDataObject txData = mithraObjectPortal.refresh(mithraDataObjects[i], false);
                    mithraObject = (MithraTransactionalObject) cache.getObjectFromData(txData);
                }
                optimException = checkOptimisticLocking(isOptimistic, versionAttribute, optimException, mithraDataObjects[i], mithraObject);
                // now update the object in memory (object, cache, update counter) and send the update to the database
                mithraObject.zApplyUpdateWrappersForBatch(updateWrappers[i]);
                for(int j=0;j<updateWrappers[i].size();j++)
                {
                    AttributeUpdateWrapper uw = (AttributeUpdateWrapper) updateWrappers[i].get(j);
                    tx.enrollAttribute(uw.getAttribute());
                }
                UpdateOperation updateOperation = new UpdateOperation(mithraObject, updateWrappers[i]);
                updateOperations.add(updateOperation);
                objectsToSetUpdated[i] = mithraObject;
            }
        }
        if (optimException != null)
        {
            throw optimException;
        }
        BatchUpdateOperation batchUpdateOperation = new BatchUpdateOperation(updateOperations);
        mithraObjectPortal.getMithraObjectPersister().batchUpdate(batchUpdateOperation);
        batchUpdateOperation.setUpdated();
    }

}
