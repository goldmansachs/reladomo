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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;

import java.sql.Timestamp;



public class BiTemporalTransactionalDataContainer extends AbstractTemporalContainerWithBusinessDate
{

    public BiTemporalTransactionalDataContainer(PerPortalTemporalContainer perPortalTemporalContainer, AsOfAttribute busAsOfAttribute)
    {
        super(perPortalTemporalContainer, busAsOfAttribute);
    }

    public void addCommittedData(MithraDataObject data)
    {
        if (this.getProcessingAsOfAttribute().getToAttribute().timestampValueOfAsLong(data) == this.getProcessingAsOfAttribute().getInfinityDate().getTime())
        {
            super.addCommittedData(data);
        }
        this.setAnyData(data);
    }

    protected MithraDataObject getDataFromList(InternalList list, Timestamp businessDate, Timestamp processingDate)
    {
        if (list != null)
        {
            for(int i=0;i<list.size();i++)
            {
                MithraDataObject data = (MithraDataObject) list.get(i);
                if (this.businessAsOfAttribute.dataMatches(data, businessDate)
                        && this.getProcessingAsOfAttribute().dataMatches(data, processingDate))
                {
                    return data;
                }
            }
        }
        return null;
    }


    public MithraDataObject getTxDataFor(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp processingDate = this.getProcessingAsOfAttribute().timestampValueOf(mithraObject);
        Timestamp businessDate = this.businessAsOfAttribute.timestampValueOf(mithraObject);
        if (processingDate.equals(this.getProcessingAsOfAttribute().getInfinityDate()))
        {
            return this.getActiveDataFor(businessDate);
        }
        else
        {
            return getDataFromList(this.inactiveDataList, businessDate, processingDate);
        }
    }

    public MithraDataObject getCommitedDataFor(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp businessDate = this.businessAsOfAttribute.timestampValueOf(mithraObject);
        Timestamp processingDate = this.getProcessingAsOfAttribute().timestampValueOf(mithraObject);
        return this.getDataFromList(this.committedDataList, businessDate, processingDate);
    }

    protected boolean matchesOnFromAttributes(MithraDataObject data, MithraDataObject second)
    {
        return this.businessAsOfAttribute.getFromAttribute().valueEquals(data, second)
                        && this.getProcessingAsOfAttribute().getFromAttribute().valueEquals(data, second);
    }

    @Override
    protected boolean matches(MithraDataObject data, Timestamp[] asOfDates)
    {
        return this.businessAsOfAttribute.dataMatches(data, asOfDates[0]) && this.getProcessingAsOfAttribute().dataMatches(data, asOfDates[1]);
    }

    public MithraDataObject getDataForTxByDates(MithraDataObject mithraDataObject, Timestamp[] asOfDates)
    {
        for(int i=this.inTxObjects.size()-1;i>=0;i--)
        {
            InTransactionDatedTransactionalObject obj = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject data = obj.zGetTxDataForRead();
            if (this.businessAsOfAttribute.dataMatches(data, asOfDates[0])
                        && this.getProcessingAsOfAttribute().dataMatches(data, asOfDates[1]))
            {
                return data;
            }
        }

        return null;
    }
}
