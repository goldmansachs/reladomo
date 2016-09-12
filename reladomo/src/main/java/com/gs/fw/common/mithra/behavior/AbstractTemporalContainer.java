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

import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;


public abstract class AbstractTemporalContainer implements TemporalContainer
{

    protected MithraDataObject anyData;
    protected InternalList committedDataList;
    protected InternalList inTxObjects;
    private PerPortalTemporalContainer perPortalTemporalContainer;

    public AbstractTemporalContainer(PerPortalTemporalContainer perPortalTemporalContainer)
    {
        this.perPortalTemporalContainer = perPortalTemporalContainer;
        inTxObjects = new InternalList(4);
    }

    protected PerPortalTemporalContainer getPerPortalTemporalContainer()
    {
        return perPortalTemporalContainer;
    }

    public InternalList getInTxObjects()
    {
        return inTxObjects;
    }

    public void setAnyData(MithraDataObject anyData)
    {
        this.anyData = anyData;
    }

    // used for the pk
    public MithraDataObject getAnyData()
    {
        return this.anyData;
    }

    public MithraTransaction getTransaction()
    {
        return this.perPortalTemporalContainer.getTransaction();
    }

    public boolean enrollReadOnly(MithraDatedTransactionalObject businessObject,
            MithraDataObject committedData, DatedTransactionalState prevState)
    {
        businessObject.zSetCurrentData(committedData);
        return this.perPortalTemporalContainer.enrollReadOnly(businessObject, committedData, prevState);
    }

    public boolean enrollForWrite(MithraDatedTransactionalObject businessObject,
            MithraDataObject committedData, DatedTransactionalState prevState)
    {
        businessObject.zSetCurrentData(committedData);
        enrollInWrite(committedData);
        return businessObject.zEnrollInTransactionForWrite(prevState, this, committedData.copy(false), this.perPortalTemporalContainer.getTransaction());
    }

    public void lockForTransaction(MithraDatedTransactionalObject businessObject, MithraDataObject transactionalData,
            MithraDataObject committedData, boolean setAsCurrent)
    {
        this.perPortalTemporalContainer.lockForTransaction(businessObject, transactionalData, committedData, setAsCurrent, this);
    }

    public boolean possiblyEnroll(MithraDatedTransactionalObject businessObject, MithraDataObject transactionalData,
            MithraDataObject committedData, DatedTransactionalState prevState)
    {
        return this.perPortalTemporalContainer.possiblyEnroll(businessObject, transactionalData, committedData, prevState, this);
    }

    public void enrollInWrite(MithraDataObject committedData)
    {
        this.perPortalTemporalContainer.enrollInWrite(committedData, this);
    }

    protected AsOfAttribute getProcessingAsOfAttribute()
    {
        return this.getPerPortalTemporalContainer().getProcessingAsOfAttribute();
    }

    protected boolean isCurrent(MithraDataObject data)
    {
        AsOfAttribute processingAsOfAttribute = this.getProcessingAsOfAttribute();
        if (processingAsOfAttribute != null)
        {
            TimestampAttribute toAttribute = processingAsOfAttribute.getToAttribute();
            return toAttribute.timestampValueOfAsLong(data) == processingAsOfAttribute.getInfinityDate().getTime();
        }
        return true;
    }
}
