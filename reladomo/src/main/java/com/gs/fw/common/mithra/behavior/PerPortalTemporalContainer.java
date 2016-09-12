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

package com.gs.fw.common.mithra.behavior;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.collections.impl.map.mutable.UnifiedMap;


public class PerPortalTemporalContainer
{

    private final AsOfAttribute processingAsOfAttribute;
    private final MithraTransaction transaction;
    private final MithraObjectPortal portal;
    private UnifiedMap<MithraDataObject, InternalList> readEnrolled = null;

    public PerPortalTemporalContainer(MithraTransaction transaction, MithraObjectPortal portal, AsOfAttribute processingAsOfAttribute)
    {
        this.transaction = transaction;
        this.portal = portal;
        this.processingAsOfAttribute = processingAsOfAttribute;
    }

    public MithraObjectPortal getPortal()
    {
        return portal;
    }

    public AsOfAttribute getProcessingAsOfAttribute()
    {
        return processingAsOfAttribute;
    }

    public UnifiedMap<MithraDataObject, InternalList> getReadEnrolled()
    {
        return readEnrolled;
    }

    public MithraTransaction getTransaction()
    {
        return transaction;
    }

    public boolean enrollReadOnly(MithraDatedTransactionalObject businessObject,
            MithraDataObject committedData, DatedTransactionalState prevState)
    {
        addToReadEnrolled(businessObject, committedData);
        return businessObject.zEnrollInTransactionForRead(prevState, this.transaction, DatedPersistenceState.PERSISTED);

    }

    private void addToReadEnrolled(MithraDatedTransactionalObject businessObject, MithraDataObject committedData)
    {
        if (processingAsOfAttribute == null ||
                processingAsOfAttribute.getToAttribute().timestampValueOfAsLong(committedData) == processingAsOfAttribute.getInfinityDate().getTime())
        {
            if (readEnrolled == null) readEnrolled = new UnifiedMap<MithraDataObject, InternalList>();
            InternalList list = readEnrolled.get(committedData);
            if (list == null)
            {
                list = new InternalList(4);
                readEnrolled.put(committedData, list);
            }
            list.add(businessObject);
        }
    }

    public void enrollInWrite(MithraDataObject committedData, TemporalContainer container)
    {
        if (committedData == null || this.readEnrolled == null || this.readEnrolled.isEmpty()) return;
        InternalList list = this.readEnrolled.remove(committedData);
        if (list != null)
        {
            for(int i=0;i<list.size();i++)
            {
                ((MithraDatedTransactionalObject)list.get(i)).zEnrollInTransactionForWrite(container, null, this.transaction);
            }
        }
    }

    public void lockForTransaction(MithraDatedTransactionalObject businessObject, MithraDataObject transactionalData,
            MithraDataObject committedData, boolean setAsCurrent, TemporalContainer container)
    {
        if ((transactionalData == null || setAsCurrent ) && committedData != null)
        {
            businessObject.zSetCurrentData(committedData);
            businessObject.zSetNonTxPersistenceState(DatedPersistenceState.PERSISTED);
            addToReadEnrolled(businessObject, committedData);
        }
        if (transactionalData == null || transactionalData == committedData)
        {
            businessObject.zLockForTransaction();
        }
        else
        {
            businessObject.zEnrollInTransactionForWrite(container, transactionalData, this.transaction);
        }
    }

    public boolean possiblyEnroll(MithraDatedTransactionalObject businessObject, MithraDataObject transactionalData,
            MithraDataObject committedData, DatedTransactionalState prevState, TemporalContainer container)
    {
        if (transactionalData == null && committedData != null)
        {
            businessObject.zSetCurrentData(committedData);
            businessObject.zSetNonTxPersistenceState(DatedPersistenceState.PERSISTED);
            addToReadEnrolled(businessObject, committedData);
        }
        if (transactionalData == null || transactionalData == committedData)
        {
            return businessObject.zEnrollInTransactionForRead(prevState, this.transaction, DatedPersistenceState.PERSISTED);
        }
        else
        {
            businessObject.zEnrollInTransactionForWrite(container, transactionalData, this.transaction);
            return true;
        }
    }

}
