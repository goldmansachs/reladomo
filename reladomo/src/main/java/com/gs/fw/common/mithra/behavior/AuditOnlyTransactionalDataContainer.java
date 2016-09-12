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
import com.gs.fw.common.mithra.MithraDeletedException;
import com.gs.fw.common.mithra.PrintablePrimaryKeyMessageBuilder;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;

import java.sql.Timestamp;
import java.util.List;



public class AuditOnlyTransactionalDataContainer extends AbstractTemporalContainer
{

    private MithraDataObject activeData;
    private boolean inactivated = false;

    public AuditOnlyTransactionalDataContainer(PerPortalTemporalContainer perPortalTemporalContainer)
    {
        super(perPortalTemporalContainer);
    }

    public InTransactionDatedTransactionalObject makeUninsertedDataActiveAndCreateObject(MithraDataObject data)
    {
        InTransactionDatedTransactionalObject result = new InTransactionDatedTransactionalObject(this.getPerPortalTemporalContainer().getPortal(),
                null, data, InTransactionDatedTransactionalObject.TO_BE_INSERTED_STATE);
        this.addObjectForTx(result);
        return result;
    }

    public InternalList getObjectsForRange(MithraDatedTransactionalObject mithraObject, Timestamp startRange, Timestamp endRange)
    {
        InternalList result = new InternalList(1);
        InTransactionDatedTransactionalObject inTxObject = this.getObjectForTx(mithraObject.zGetTxDataForRead());

        if (inTxObject == null)
        {
            inTxObject = new InTransactionDatedTransactionalObject(this.getPerPortalTemporalContainer().getPortal(), mithraObject.zGetCurrentData(),
                    null,
                    InTransactionDatedTransactionalObject.COMMITTED_STATE);
           this.addObjectForTx(inTxObject);
        }
        result.add(inTxObject);
        return result;
    }

    public void inactivateObject(InTransactionDatedTransactionalObject obj)
    {
        this.inTxObjects.remove(obj);
        this.activeData = null;
        this.inactivated = true;
    }

    public void clearAllObjects()
    {
        this.inTxObjects.clear();
        this.activeData = null;
        this.inactivated = true;
    }

    public void addObjectForTx(InTransactionDatedTransactionalObject toInsert)
    {
        if(!inTxObjects.contains(toInsert))
        {
            this.inTxObjects.add(toInsert);
        }
        this.getTransaction().enrollObject(toInsert, toInsert.zGetCache());

        //Determine if its active
        MithraDataObject data = toInsert.zGetTxDataForRead();
        Timestamp processingTo = getProcessingAsOfAttribute().getToAttribute().timestampValueOf(data);

        if (processingTo.getTime() == getPerPortalTemporalContainer().getProcessingAsOfAttribute().getInfinityDate().getTime())
        {
            this.activeData = toInsert.zGetTxDataForRead();
            this.inactivated = false;
        }
    }

    public MithraDataObject getActiveDataFor(Timestamp businessDate)
    {
        return activeData;
    }

    public MithraDataObject getDataForTxByDates(MithraDataObject mithraDataObject, Timestamp[] asOfDates)
    {
        for(int i=this.inTxObjects.size()-1;i>=0;i--)
        {
            InTransactionDatedTransactionalObject obj = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject data = obj.zGetTxDataForRead();
            if (getProcessingAsOfAttribute().dataMatches(data, asOfDates[0]))
            {
                return data;
            }
        }

        return null;
    }

    protected MithraDataObject getDataFromOneDimensionalList(InternalList list, Timestamp processingDate)
    {
        if (list != null)
        {
            for(int i=list.size()-1;i>=0;i--)
            {
                MithraDataObject data = (MithraDataObject) list.get(i);
                if (getProcessingAsOfAttribute().dataMatches(data, processingDate))
                {
                    return data;
                }
            }
        }
        return null;
    }

    public MithraDataObject getCommitedDataFor(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp processingDate = getProcessingAsOfAttribute().timestampValueOf(mithraObject);
        return this.getDataFromOneDimensionalList(this.committedDataList, processingDate);
    }

    public boolean isInactivatedOrSplit(Timestamp businessDate)
    {
        return this.inactivated;
    }

    public void deleteInTxObject(InTransactionDatedTransactionalObject inTxObject)
    {
        this.activeData = null;
        this.inactivated = true;
    }

    public void voidData(MithraDataObject splitData)
    {
        if (this.isCurrent(splitData))
        {
            this.activeData = null;
            this.inactivated = true;
        }
    }

    public void updateInPlaceData(MithraDataObject activeData, MithraDataObject updatedData)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject getCommittedDataFromData(MithraDataObject data)
    {
        if (committedDataList == null) return null;
        for(int i=0;i<committedDataList.size();i++)
        {
            MithraDataObject second = (MithraDataObject) committedDataList.get(i);
            if (getProcessingAsOfAttribute().getFromAttribute().valueEquals(data, second))
            {
                return second;
            }
        }
        return null;
    }

    public MithraDataObject getCommittedDataFromDates(Timestamp[] asOfDates)
    {
        if (committedDataList == null) return null;
        for(int i=0;i<committedDataList.size();i++)
        {
            MithraDataObject second = (MithraDataObject) committedDataList.get(i);
            if (getProcessingAsOfAttribute().dataMatches(second, asOfDates[0]))
            {
                return second;
            }
        }
        return null;
    }

    public MithraDataObject getActiveDataFromData(MithraDataObject data)
    {
        if (getProcessingAsOfAttribute().getInfinityDate().equals(getProcessingAsOfAttribute().getToAttribute().timestampValueOf(data)))
        {
            return this.activeData;
        }
        return null;
    }

    public MithraDataObject getActiveOrInactiveDataFromData(MithraDataObject data)
    {
        return this.getActiveDataFromData(data);
    }

    public void addCommittedData(MithraDataObject data)
    {
        if (committedDataList == null)
        {
            this.committedDataList = new InternalList(2);
        }
        if (!this.committedDataList.contains(data))
        {
            this.committedDataList.add(data);
            if (getProcessingAsOfAttribute().getInfinityDate().equals(getProcessingAsOfAttribute().getToAttribute().timestampValueOf(data)))
            {
                InTransactionDatedTransactionalObject obj = new InTransactionDatedTransactionalObject(this.getPerPortalTemporalContainer().getPortal(), data, null,
                        InTransactionDatedTransactionalObject.COMMITTED_STATE);
                this.addObjectForTx(obj);
            }
        }
        this.setAnyData(data);
    }

    public MithraDataObject getTxDataFor(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp to = this.getProcessingAsOfAttribute().getToAttribute().timestampValueOf(mithraObject.zGetCurrentOrTransactionalData());
        if (to == null || to.getTime() == this.getProcessingAsOfAttribute().getInfinityDate().getTime())
        {
            return activeData;
        }
        return null;
    }

    public MithraDataObject getAnyData()
    {
        if (activeData != null)
        {
            return this.activeData;
        }
        return this.anyData;
    }

    public List getForDateRange(MithraDataObject mithraDataObject, Timestamp start, Timestamp end)
    {
        throw new RuntimeException("not implemented");
    }

    public InTransactionDatedTransactionalObject getObjectForTx(MithraDataObject mithraDataObject)
    {
        for(int i=this.inTxObjects.size() - 1;i >= 0;i--)
        {
            InTransactionDatedTransactionalObject obj = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject data = obj.zGetTxDataForRead();
            if (mithraDataObject.zAsOfAttributesFromEquals(data))
            {
                return obj;
            }
        }

        return null;
    }

    public void addActiveData(MithraDataObject data)
    {
        this.activeData = data;
    }

    public void setInfiniteRange()
    {
    }

    public void checkInactivatedForDelete(MithraDatedTransactionalObject mithraObject)
    {
        // nothing to do
    }

    public void checkInactivated(MithraDatedTransactionalObject mithraObject)
    {
        if (this.getTransaction().isInFuture(getProcessingAsOfAttribute().timestampValueOf(mithraObject).getTime()) && inactivated)
        {
            MithraDataObject data = mithraObject.zGetCurrentData();
            if (data == null) data = this.getAnyData();
            throw new MithraDeletedException("Cannot access deleted object " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }
    }
}
