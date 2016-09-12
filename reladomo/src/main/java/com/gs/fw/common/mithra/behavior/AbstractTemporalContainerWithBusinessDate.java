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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.transaction.MithraDatedObjectPersister;
import com.gs.fw.common.mithra.util.InternalList;

import java.sql.Timestamp;
import java.util.List;



public abstract class AbstractTemporalContainerWithBusinessDate extends AbstractTemporalContainer
{

    private static final Timestamp DAWN_OF_TIME = new Timestamp(Long.MIN_VALUE/2);
    private static final Timestamp END_OF_TIME = new Timestamp(Long.MAX_VALUE/2);

    private static final int COMMITTED_STATUS = 10;
    private static final int ACTIVE_STATUS = 20;
    private static final int INACTIVE_STATUS = 30;
    private static final int VOID_STATUS = 40;

    protected AsOfAttribute businessAsOfAttribute;
    protected InternalList activeDataList;
    protected InternalList inactiveDataList;
    private InternalList rangeStatusList;
    private Timestamp loadedRangeStart;
    private Timestamp loadedRangeEnd;


    protected AbstractTemporalContainerWithBusinessDate(PerPortalTemporalContainer perPortalTemporalContainer, AsOfAttribute businessAsOfAttribute)
    {
        super(perPortalTemporalContainer);
        this.businessAsOfAttribute = businessAsOfAttribute;
        rangeStatusList = new InternalList(3);
        committedDataList = new InternalList(3);
    }

    public void setInfiniteRange()
    {
        this.loadedRangeStart = DAWN_OF_TIME;
        this.loadedRangeEnd = END_OF_TIME;
    }

    public void checkInactivated(MithraDatedTransactionalObject mithraObject)
    {
        if (this.getProcessingAsOfAttribute() != null)
        {
            if (!this.getTransaction().isInFuture(this.getProcessingAsOfAttribute().timestampValueOfAsLong(mithraObject))) return;
        }
        if (this.isInactivatedOrSplit(businessAsOfAttribute.timestampValueOf(mithraObject)))
        {
            MithraDataObject data = mithraObject.zGetCurrentData();
            if (data == null) data = this.getAnyData();
            throw new MithraDeletedException("Cannot access deleted object " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }
    }

    public void checkInactivatedForDelete(MithraDatedTransactionalObject mithraObject)
    {
        if (this.getProcessingAsOfAttribute() != null)
        {
            return;
        }
        if (this.isInactivatedOrSplit(businessAsOfAttribute.timestampValueOf(mithraObject)))
        {
            MithraDataObject data = mithraObject.zGetCurrentData();
            if (data == null) data = this.getAnyData();
            throw new MithraDeletedException("Cannot access deleted object " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }
    }

    public List getForDateRange(MithraDataObject mithraDataObject, Timestamp start, Timestamp end)
    {
        // todo: this is not optimal, when two separate ranges are asked for in the same tx, but that's rare
        this.stichToCurrentRange(start, end);
        MithraDatedObjectPersister objectPersister = (MithraDatedObjectPersister) mithraDataObject.zGetMithraObjectPortal().getMithraObjectPersister();

        // the following has the side effect that these data items get added to this container
        return objectPersister.getForDateRange(mithraDataObject, start, end);
    }

    public InTransactionDatedTransactionalObject getObjectForTx(MithraDataObject mithraDataObject)
    {
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject obj = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject data = obj.zGetCurrentData();
            if (data == null)
            {
                data = obj.zGetTxDataForRead();
            }
            if (mithraDataObject.zAsOfAttributesFromEquals(data))
            {
                return obj;
            }
        }
        return null;
    }

    public void addObjectForTx(InTransactionDatedTransactionalObject toInsert)
    {
        if(!inTxObjects.contains(toInsert))
        {
            this.inTxObjects.add(toInsert);
        }        
        this.getTransaction().enrollObject(toInsert, toInsert.zGetCache());

        //Determine if its active
        AsOfAttribute processingAsOfAttribute = this.getProcessingAsOfAttribute();
        if (processingAsOfAttribute != null)
        {
            MithraDataObject data = toInsert.zGetTxDataForRead();
            Timestamp processingTo = processingAsOfAttribute.getToAttribute().timestampValueOf(data);

            if (processingTo.getTime() == processingAsOfAttribute.getInfinityDate().getTime())
            {
                this.addActiveData(toInsert.zGetTxDataForRead());
            }
        }
        else
        {
            this.addActiveData(toInsert.zGetTxDataForRead());
        }
    }

    protected MithraDataObject getDataFromOneDimensionalList(InternalList list, Timestamp businessDate)
    {
        if (list != null)
        {
            for(int i=list.size()-1;i>=0;i--)
            {
                MithraDataObject data = (MithraDataObject) list.get(i);
                if (this.businessAsOfAttribute.dataMatches(data, businessDate))
                {
                    return data;
                }
            }
        }
        return null;
    }

    public MithraDataObject getActiveDataFor(Timestamp businessDate)
    {
        return this.getDataFromOneDimensionalList(this.activeDataList, businessDate);
    }

    public boolean isInactivatedOrSplit(Timestamp businessDate)
    {
        for(int i=rangeStatusList.size() - 1; i >= 0; i--)
        {
            RangeStatus rangeStatus = (RangeStatus) rangeStatusList.get(i);
            if (rangeStatus.matchesDate(businessDate))
            {
                return rangeStatus.getStatus() == INACTIVE_STATUS || rangeStatus.getStatus() == VOID_STATUS;
            }
        }
        return false;
    }

    public void updateInPlaceData(MithraDataObject activeData, MithraDataObject updatedData)
    {
        if (this.activeDataList == null)
        {
            this.activeDataList = new InternalList(3);
        }
        else
        {
            this.activeDataList.remove(activeData);
        }
        this.activeDataList.add(updatedData);
        this.rangeStatusList.add(new RangeStatus(updatedData, ACTIVE_STATUS));
    }

    public void voidData(MithraDataObject activeData)
    {
        if (this.isCurrent(activeData))
        {
            if (this.activeDataList != null)
            {
                this.activeDataList.remove(activeData);
            }
            this.rangeStatusList.add(new RangeStatus(activeData, VOID_STATUS));
        }        
    }

    public void inactivateObject(InTransactionDatedTransactionalObject obj)
    {
        MithraDataObject inactivatedData = obj.zGetTxDataForRead();
        MithraDataObject committed = obj.zGetCurrentData();
        if (committed != null) this.inTxObjects.remove(obj);
        this.inTxObjects.remove(obj);
        this.rangeStatusList.add(new RangeStatus(inactivatedData, INACTIVE_STATUS));
        if (this.inactiveDataList == null)
        {
            this.inactiveDataList = new InternalList(2);
        }
        this.inactiveDataList.add(inactivatedData);
    }

    public void clearAllObjects()
    {
        if (this.activeDataList != null)
        {
            this.activeDataList.clear();
        }
        this.rangeStatusList.clear();
        this.rangeStatusList.add(new RangeStatus(new Timestamp(0), this.businessAsOfAttribute.getInfinityDate(), VOID_STATUS));
        this.inTxObjects.clear();
    }

    protected void stichToCurrentRange(Timestamp start, Timestamp end)
    {
        if (this.loadedRangeStart == null)
        {
            this.loadedRangeStart = start;
            this.loadedRangeEnd = end;
        }
        else
        {
            long startLong = start.getTime();
            long endLong = end.getTime();
            long loadedStart = loadedRangeStart.getTime();
            long loadedEnd = loadedRangeEnd.getTime();
            if (endLong < loadedStart || startLong > loadedEnd)
            {
                // disjoint case:
                this.loadedRangeStart = start;
                this.loadedRangeEnd = end;
            }
            else
            {
                if (startLong < loadedStart)
                {
                    this.loadedRangeStart = start;
                }
                if (endLong > loadedEnd)
                {
                    this.loadedRangeEnd = end;
                }
            }
        }
    }

    public void addCommittedData(MithraDataObject data)
    {
        if (!this.committedDataList.contains(data))
        {
            this.committedDataList.add(data);
            Timestamp start = this.businessAsOfAttribute.getFromAttribute().timestampValueOf(data);
            Timestamp end = this.businessAsOfAttribute.getToAttribute().timestampValueOf(data);
            this.rangeStatusList.add(new RangeStatus(start, end, COMMITTED_STATUS));
            stichToCurrentRange(start, end);
            InTransactionDatedTransactionalObject obj = new InTransactionDatedTransactionalObject(this.getPerPortalTemporalContainer().getPortal(), data, null,
                    InTransactionDatedTransactionalObject.COMMITTED_STATE);
            this.getTransaction().enrollObject(obj, obj.zGetCache());
            this.inTxObjects.add(obj);
        }
        this.setAnyData(data);
    }

    public void addActiveData(MithraDataObject data)
    {
        if (this.activeDataList == null)
        {
            this.activeDataList = new InternalList(3);
        }
        this.activeDataList.add(data);
        this.setAnyData(data);
        Timestamp start = this.businessAsOfAttribute.getFromAttribute().timestampValueOf(data);
        Timestamp end = this.businessAsOfAttribute.getToAttribute().timestampValueOf(data);
        this.rangeStatusList.add(new RangeStatus(start, end, ACTIVE_STATUS));
        stichToCurrentRange(start, end);
    }

    public InternalList getObjectsForRange(MithraDatedTransactionalObject mithraObject, Timestamp startRange, Timestamp endRange)
    {
        long startTime = startRange.getTime();
        long endTime = endRange.getTime();
        if (!haveCompleteRange(startTime, endTime))
        {
            MithraDataObject mithraDataObject = mithraObject.zGetTxDataForRead();
            this.getForDateRange(mithraDataObject,  startRange, endRange);
        }
        InternalList result = new InternalList();

        for(int i = 0; i < inTxObjects.size(); i++)
        {
            InTransactionDatedTransactionalObject obj = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject data = obj.zGetTxDataForRead();
            long dataStart = this.businessAsOfAttribute.getFromAttribute().timestampValueOfAsLong(data);
            long dataEnd = this.businessAsOfAttribute.getToAttribute().timestampValueOfAsLong(data);
            if (dataStart < endTime && dataEnd > startTime)
            {
                result.add(obj);
                enrollInWrite(obj.zGetCurrentData());
            }

        }
        return result;
    }

    private boolean haveCompleteRange(long startTime, long endTime)
    {
        if (loadedRangeStart != null && startTime >= this.loadedRangeStart.getTime() && endTime <= this.loadedRangeEnd.getTime())
        {
            return true;
        }
        if (rangeStatusList.size() < 2) return false; // loaded range would already be exact
        for(int i=this.rangeStatusList.size() - 1; i >= 0; i--)
        {
            RangeStatus rangeStatus = (RangeStatus) rangeStatusList.get(i);
            if (rangeStatus.matchesRange(startTime, endTime))
            {
                if (rangeStatus.getStatus() == VOID_STATUS || rangeStatus.getStatus() == INACTIVE_STATUS)
                {
                    return false;
                }
                this.loadedRangeStart = rangeStatus.startDate;
                this.loadedRangeEnd = rangeStatus.endDate;
                return true;
            }
        }
        return false;
    }

    public InTransactionDatedTransactionalObject makeUninsertedDataActiveAndCreateObject(MithraDataObject data)
    {
        InTransactionDatedTransactionalObject result = new InTransactionDatedTransactionalObject(this.getPerPortalTemporalContainer().getPortal(), null, data,
                InTransactionDatedTransactionalObject.TO_BE_INSERTED_STATE);
        this.addObjectForTx(result);
        return result;
    }

    protected abstract boolean matchesOnFromAttributes(MithraDataObject data, MithraDataObject second);

    public MithraDataObject getCommittedDataFromData(MithraDataObject data)
    {
        for(int i=0;i<committedDataList.size();i++)
        {
            MithraDataObject second = (MithraDataObject) committedDataList.get(i);
            if (matchesOnFromAttributes(data, second))
            {
                return second;
            }
        }
        return null;
    }

    protected abstract boolean matches(MithraDataObject data, Timestamp[] asOfDates);

    public MithraDataObject getCommittedDataFromDates(Timestamp[] asOfDates)
    {
        for(int i=0;i<committedDataList.size();i++)
        {
            MithraDataObject second = (MithraDataObject) committedDataList.get(i);
            if (matches(second, asOfDates))
            {
                return second;
            }
        }
        return null;
    }

    public MithraDataObject getActiveDataFromData(MithraDataObject data)
    {
        if (activeDataList != null)
        {
            for(int i=activeDataList.size()-1;i>=0;i--)
            {
                MithraDataObject second = (MithraDataObject) activeDataList.get(i);
                if (matchesOnFromAttributes(data, second))
                {
                    return second;
                }
            }
        }
        return null;
    }

    public MithraDataObject getActiveOrInactiveDataFromData(MithraDataObject data)
    {
        MithraDataObject result = this.getActiveDataFromData(data);
        if (result == null && this.inactiveDataList != null)
        {
            for(int i=inactiveDataList.size()-1;i>=0;i--)
            {
                MithraDataObject second = (MithraDataObject) inactiveDataList.get(i);
                if (matchesOnFromAttributes(data, second))
                {
                    return second;
                }
            }
        }
        return result;
    }

    private class RangeStatus
    {
        private Timestamp startDate;
        private Timestamp endDate;
        private int status;

        public RangeStatus(Timestamp startDate, Timestamp endDate, int status)
        {
            this.startDate = startDate;
            this.endDate = endDate;
            this.status = status;
        }

        public RangeStatus(MithraDataObject databaseData, int status)
        {
            this.startDate = businessAsOfAttribute.getFromAttribute().timestampValueOf(databaseData);
            this.endDate = businessAsOfAttribute.getToAttribute().timestampValueOf(databaseData);
            this.status = status;
        }

        public int getStatus()
        {
            return status;
        }

        public boolean matchesDate(Timestamp businessDate)
        {
            return businessAsOfAttribute.asOfDateMatchesRange(businessDate, this.startDate, this.endDate);
        }

        public boolean matchesRange(long startTime, long endTime)
        {
            return startTime >= startDate.getTime() && endTime <= endDate.getTime();
        }
    }

    public void deleteInTxObject(InTransactionDatedTransactionalObject inTxObject)
    {
        if (this.activeDataList != null)
        {
            this.activeDataList.remove(inTxObject.zGetTxDataForRead());
        }
        this.rangeStatusList.add(new RangeStatus(inTxObject.zGetTxDataForRead(), VOID_STATUS));
        this.inTxObjects.remove(inTxObject);
    }
}
