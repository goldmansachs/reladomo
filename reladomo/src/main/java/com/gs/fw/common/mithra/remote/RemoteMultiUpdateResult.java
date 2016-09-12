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
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.VersionAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;



public class RemoteMultiUpdateResult extends MithraRemoteResult
{

    private MithraDataObject mithraDataObject;
    private List updateWrappers;

    public RemoteMultiUpdateResult()
    {
        // for externalizable
    }

    public RemoteMultiUpdateResult(MithraDataObject mithraDataObject, List updateWrappers)
    {
        this.mithraDataObject = mithraDataObject;
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
        MithraObjectPortal mithraObjectPortal = ((AttributeUpdateWrapper) updateWrappers.get(0)).getAttribute().getOwnerPortal();
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
        if (isDated)
        {
            TemporalContainer container = cache.getOrCreateContainer(mithraDataObject);
            InTransactionDatedTransactionalObject txObject = container.getObjectForTx(mithraDataObject);
            if (txObject == null && asOfAttributes.length == 2)
            {
                container.getForDateRange(mithraDataObject, asOfAttributes[0].getFromAttribute().timestampValueOf(mithraDataObject),
                        asOfAttributes[0].getToAttribute().timestampValueOf(mithraDataObject));
                txObject = container.getObjectForTx(mithraDataObject);
            }
            if (txObject == null)
            {
                if (isOptimistic)
                {
                    throw createOrAddDirtyData(null, mithraDataObject);
                }
                else
                {
                    throw new RuntimeException("could not find tx object for "+mithraDataObject.getClass().getName()+" with pk "+
                        mithraDataObject.zGetPrintablePrimaryKey());
                }
            }
            MithraDataObject dataToUpdate = txObject.zGetTxDataForWrite();
            MithraDataObject dataToRemove = txObject.zGetCurrentData();
            if (dataToRemove == null)
            {
                dataToRemove = txObject.zGetTxDataForRead();
            }
            cache.removeDatedData(dataToRemove);
            for(int i=0;i<updateWrappers.size();i++)
            {
                AttributeUpdateWrapper uw = (AttributeUpdateWrapper) updateWrappers.get(i);
                uw.updateData(dataToUpdate);
                uw.incrementUpdateCount();
                tx.enrollAttribute(uw.getAttribute());
            }
            cache.putDatedData(dataToUpdate);
            mithraObjectPortal.getMithraObjectPersister().update(txObject, updateWrappers);
            txObject.zSetUpdated(updateWrappers);
            mithraObjectPortal.incrementClassUpdateCount(); // necessary because of termination that happens via updates
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
            MithraRemoteOptimisticLockException optimException =
                    checkOptimisticLocking(isOptimistic, versionAttribute, null, mithraDataObject, mithraObject);
            if (optimException != null) throw optimException;
            // now update the object in memory (object, cache, update counter) and send the update to the database
            mithraObject.zApplyUpdateWrappers(updateWrappers);
            if (tx != null)
            {
                for(int i=0;i<updateWrappers.size();i++)
                {
                    AttributeUpdateWrapper uw = (AttributeUpdateWrapper) updateWrappers.get(i);
                    tx.enrollAttribute(uw.getAttribute());
                }
            }
        }
    }
}
