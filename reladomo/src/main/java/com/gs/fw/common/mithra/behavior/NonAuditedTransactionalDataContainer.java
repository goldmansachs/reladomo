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
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;

import java.sql.Timestamp;



public class NonAuditedTransactionalDataContainer extends AbstractTemporalContainerWithBusinessDate
{

    public NonAuditedTransactionalDataContainer(PerPortalTemporalContainer perPortalTemporalContainer, AsOfAttribute busAsOfAttribute)
    {
        super(perPortalTemporalContainer, busAsOfAttribute);
    }

    public MithraDataObject getTxDataFor(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp businessDate = this.businessAsOfAttribute.timestampValueOf(mithraObject);
        return this.getActiveDataFor(businessDate);
    }

    public MithraDataObject getCommitedDataFor(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp businessDate = this.businessAsOfAttribute.timestampValueOf(mithraObject);
        return this.getDataFromOneDimensionalList(this.committedDataList, businessDate);
    }

    protected boolean matchesOnFromAttributes(MithraDataObject data, MithraDataObject second)
    {
        return this.businessAsOfAttribute.getFromAttribute().valueEquals(data, second);
    }

    @Override
    protected boolean matches(MithraDataObject data, Timestamp[] asOfDates)
    {
        return this.businessAsOfAttribute.dataMatches(data, asOfDates[0]);
    }

    public MithraDataObject getDataForTxByDates(MithraDataObject mithraDataObject, Timestamp[] asOfDates)
    {
        for(int i=this.inTxObjects.size()-1;i>=0;i--)
        {
            InTransactionDatedTransactionalObject obj = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject data = obj.zGetTxDataForRead();
            if (this.businessAsOfAttribute.dataMatches(data, asOfDates[0]))
            {
                return data;
            }
        }

        return null;
    }
}
