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

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.math.BigDecimal;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.update.*;
import com.gs.fw.common.mithra.behavior.state.DatedPersistedState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;



public class GenericNonAuditedTemporalDirector implements TemporalDirector
{

    private AsOfAttribute businessDateAttribute;
    protected static final long DAY_IN_MS = 24 * 3600 * 1000;
    private ByBusinessFromComparator byBusinessFromComparator = new ByBusinessFromComparator();
    private DoubleAttribute[] doubleAttributes;
    private BigDecimalAttribute[] bigDecimalAttributes;

    public GenericNonAuditedTemporalDirector(AsOfAttribute businessDateAttribute, DoubleAttribute[] doubleAttributes,
                                             BigDecimalAttribute[] bigDecimalAttributes)
    {
        this.businessDateAttribute = businessDateAttribute;
        this.doubleAttributes = doubleAttributes;
        this.bigDecimalAttributes = bigDecimalAttributes;
    }

    public void insert(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityDate(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        Timestamp businessDate = businessDateAttribute.timestampValueOf(mithraObject);
        if (container.getActiveDataFor(businessDate) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists: "+ PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }

        //todo: rezaem: check the database to ensure nothing matches this new object

        MithraDataObject containerData = container.getActiveDataFor(businessDate);
        if (containerData != null)
        {
            throw new MithraTransactionException("can't insert a value for business date " + businessDate + " on " +
                                                 PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }
        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        if (businessToAttribute.isAttributeNull(data))
        {
            businessToAttribute.setValue(data, businessDateAttribute.getInfinityDate());
        }
        TimestampAttribute businessFromAttribute = businessDateAttribute.getFromAttribute();
        if (businessFromAttribute.isAttributeNull(data))
        {
            businessFromAttribute.setValue(data, this.getBusinessFromDateForBusinessDate(businessDate));
        }
        InTransactionDatedTransactionalObject objectForInsert = container.makeUninsertedDataActiveAndCreateObject(data);
        container.getTransaction().insert(objectForInsert);

        mithraObject.zSetTxPersistenceState(DatedPersistedState.PERSISTED);
        mithraObject.zGetCache().put(mithraObject);
        mithraObject.zGetPortal().incrementClassUpdateCount();
    }

    private void checkInfinityDate(MithraDatedTransactionalObject mithraObject)
    {
        long busTimestamp = this.businessDateAttribute.timestampValueOfAsLong(mithraObject);
        if (busTimestamp == this.businessDateAttribute.getInfinityDate().getTime())
        {
            throw new MithraTransactionException("Dated objects must not have an infinity date when being inserted/modified");
        }
    }

    public void insertWithIncrement(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityDate(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = this.getBusinessFromDateForBusinessDate(asOfDate);
        if (container.getActiveDataFor(asOfDate) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists");
        }
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getInfinityDate()));
        if (inTxObjects.size() == 0)
        {
            this.insert(mithraObject, container);
            return;
        }

        //todo: rezaem: check the database to ensure nothing matches this new object

        TimestampAttribute businessFromAttribute = businessDateAttribute.getFromAttribute();
        if (businessFromAttribute.isAttributeNull(data))
        {
            businessFromAttribute.setValue(data, this.getBusinessFromDateForBusinessDate(asOfDate));
        }

        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        InTransactionDatedTransactionalObject nextSegment = (InTransactionDatedTransactionalObject) inTxObjects.get(inTxObjects.size() - 1);
        Timestamp nextSegmentFromDate = this.getBusinessFromDateForBusinessDate(businessFromAttribute.timestampValueOf(nextSegment.zGetTxDataForRead()));
        businessToAttribute.setTimestampValue(data, nextSegmentFromDate);

        InternalList toIncrement = new InternalList(this.doubleAttributes.length);
        for(int i=0;i<doubleAttributes.length;i++)
        {
            double newValue = doubleAttributes[i].doubleValueOf(data);
            if (newValue != 0)
            {
                DoubleIncrementUpdateWrapper incrementUpdateWrapper = new DoubleIncrementUpdateWrapper(doubleAttributes[i], null, newValue);
                toIncrement.add(incrementUpdateWrapper);
            }
        }

        for(int i=0;i<bigDecimalAttributes.length;i++)
        {
            BigDecimal newValue = bigDecimalAttributes[i].bigDecimalValueOf(data);
            if (!newValue.equals(BigDecimal.ZERO))
            {
                BigDecimalIncrementUpdateWrapper incrementUpdateWrapper = new BigDecimalIncrementUpdateWrapper(bigDecimalAttributes[i], null, newValue);
                toIncrement.add(incrementUpdateWrapper);
            }
        }

        if (toIncrement.size() > 0)
        {
            AttributeUpdateWrapper[] updateWrappers = new AttributeUpdateWrapper[toIncrement.size()];
            toIncrement.toArray(updateWrappers);
            this.incrementMultipleWrappers(inTxObjects, nextSegmentFromDate, tx, updateWrappers, container, mithraObject.zGetCache());
        }

        InTransactionDatedTransactionalObject objectForInsert = container.makeUninsertedDataActiveAndCreateObject(data);

        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        currentTransaction.insert(objectForInsert);

        mithraObject.zSetTxPersistenceState(DatedPersistedState.PERSISTED);
        mithraObject.zGetCache().put(mithraObject);
        mithraObject.zGetPortal().incrementClassUpdateCount();

    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container, Timestamp exclusiveUntil)
    {
        checkInfinityDate(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = this.getBusinessFromDateForBusinessDate(asOfDate);
        if (container.getActiveDataFor(asOfDate) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists");
        }
        Timestamp endDate = this.getBusinessFromDateForBusinessDate(exclusiveUntil);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, endDate));
        if (inTxObjects.size() == 0)
        {
            this.businessDateAttribute.getToAttribute().setTimestampValue(mithraObject, endDate);
            this.insert(mithraObject, container);
            return;
        }

        //todo: rezaem: check the database to ensure nothing matches this new object

        TimestampAttribute businessFromAttribute = businessDateAttribute.getFromAttribute();
        if (businessFromAttribute.isAttributeNull(data))
        {
            businessFromAttribute.setValue(data, this.getBusinessFromDateForBusinessDate(asOfDate));
        }

        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        InTransactionDatedTransactionalObject nextSegment = (InTransactionDatedTransactionalObject) inTxObjects.get(inTxObjects.size() - 1);
        Timestamp nextSegmentFromDate = businessFromAttribute.timestampValueOf(nextSegment.zGetTxDataForRead());
        businessToAttribute.setTimestampValue(data, nextSegmentFromDate);

        InternalList toIncrement = new InternalList(this.doubleAttributes.length);
        for(int i=0;i<doubleAttributes.length;i++)
        {
            double newValue = doubleAttributes[i].doubleValueOf(data);
            if (newValue != 0)
            {
                DoubleIncrementUpdateWrapper incrementUpdateWrapper = new DoubleIncrementUpdateWrapper(doubleAttributes[i], null, newValue);
                toIncrement.add(incrementUpdateWrapper);
            }
        }

        for(int i=0;i<bigDecimalAttributes.length;i++)
        {
            BigDecimal newValue = bigDecimalAttributes[i].bigDecimalValueOf(data);
            if (!newValue.equals(BigDecimal.ZERO))
            {
                BigDecimalIncrementUpdateWrapper incrementUpdateWrapper = new BigDecimalIncrementUpdateWrapper(bigDecimalAttributes[i], null, newValue);
                toIncrement.add(incrementUpdateWrapper);
            }
        }

        if (toIncrement.size() > 0)
        {
            AttributeUpdateWrapper[] updateWrappers = new AttributeUpdateWrapper[toIncrement.size()];
            toIncrement.toArray(updateWrappers);
            this.incrementMultipleWrappersUntil(inTxObjects, fromDate, endDate, mithraObject.zGetCache(), tx, updateWrappers, container);
        }

        InTransactionDatedTransactionalObject objectForInsert = container.makeUninsertedDataActiveAndCreateObject(data);

        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        currentTransaction.insert(objectForInsert);

        mithraObject.zSetTxPersistenceState(DatedPersistedState.PERSISTED);
        mithraObject.zGetCache().put(mithraObject);
        mithraObject.zGetPortal().incrementClassUpdateCount();
    }

    public void insertUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container, Timestamp exclusiveUntil)
    {
        Timestamp endDate = getBusinessFromDateForBusinessDate(exclusiveUntil);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        if (businessToAttribute.isAttributeNull(data))
        {
            businessToAttribute.setValue(data, endDate);
        }
        else
        {
            Timestamp currentToDate = businessToAttribute.timestampValueOf(data);
            if (endDate.getTime() != currentToDate.getTime())
            {
                throw new MithraBusinessException("until date set incorrectly for " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data) +
                        " expecting "+ PrintablePreparedStatement.timestampFormat.print(endDate.getTime())+" but was set to "+
                PrintablePreparedStatement.timestampFormat.print(currentToDate.getTime()));
            }
        }
        this.insert(mithraObject, container);
    }

    protected void split(InTransactionDatedTransactionalObject inTxObject, Timestamp fromDate,
            TemporalContainer container, Cache cache,
            MithraTransaction tx)
    {
        MithraDataObject splitData = inTxObject.zGetTxDataForWrite();
        container.voidData(splitData);
        if (inTxObject.isNewInThisTransaction() && !inTxObject.isInserted())
        {
            // just update in place
            this.businessDateAttribute.getToAttribute().setTimestampValue(splitData, fromDate);
        }
        else
        {
            MithraDataObject committedData = inTxObject.zGetCurrentData();
            if (committedData != null) cache.removeDatedData(committedData);
            TimestampUpdateWrapper updateBusinessToWrapper = new TimestampUpdateWrapper(this.businessDateAttribute.getToAttribute(),
                    splitData, fromDate);
            updateBusinessToWrapper.updateData();

            tx.update(inTxObject, updateBusinessToWrapper);
        }
        container.addActiveData(splitData);
        cache.putDatedData(splitData);
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();
    }

    protected void delete(TemporalContainer container, MithraTransaction tx, Cache cache, InTransactionDatedTransactionalObject inTxObject)
    {
        MithraDataObject oldData = inTxObject.zGetCurrentData();
        if (oldData != null) cache.removeDatedData(oldData);
        MithraDataObject activeData = inTxObject.zGetTxDataForRead();
        cache.removeDatedData(activeData);

        activeData.zIncrementDataVersion();

        container.deleteInTxObject(inTxObject);
        tx.delete(inTxObject);
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();
    }


    public void update(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getInfinityDate()));
        long fromDateAsLong = fromDate.getTime();
        MithraDataObject txDataToInsert = null;
        Cache cache = mithraObject.zGetCache();
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                txDataToInsert = inTxObject.zGetTxDataForRead().copy(false);
                split(inTxObject, fromDate, container, cache, tx);
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData)  == businessDateAttribute.getInfinityDate().getTime())
                {
                    MithraDataObject txData = inTxObject.zGetTxDataForWrite();
                    updateWrapper.updateData(txData);
                    if (txData != activeData)
                    {
                        cache.removeDatedData(activeData);
                        cache.putDatedData(txData);
                        container.addActiveData(txData);
                    }
                    tx.update(inTxObject, updateWrapper);
                    activeData.zIncrementDataVersion();
                }
                else
                {
                    txDataToInsert = inTxObject.zGetTxDataForRead().copy(false);
                    delete(container, tx, cache, inTxObject);
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                txDataToInsert = inTxObject.zGetTxDataForRead().copy(false);
                delete(container, tx, cache, inTxObject);
            }
        }
        if (txDataToInsert != null)
        {
            insertForUpdate(updateWrapper, mithraObject, txDataToInsert, fromDate, container, tx, cache);
        }
        mithraObject.zClearTxData();
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        throw new MithraBusinessException("Not implemented yet!");
    }


    private void insertForUpdate(AttributeUpdateWrapper updateWrapper, MithraDatedTransactionalObject mithraObject,
            MithraDataObject txData, Timestamp fromDate, TemporalContainer container, MithraTransaction tx, Cache cache)
    {
        InTransactionDatedTransactionalObject toInsert = new InTransactionDatedTransactionalObject(mithraObject.zGetPortal(),
                mithraObject.zGetCurrentData(), txData, InTransactionDatedTransactionalObject.TO_BE_INSERTED_STATE);
        updateWrapper.updateData(txData);
        this.businessDateAttribute.getFromAttribute().setTimestampValue(txData, fromDate);
        this.businessDateAttribute.getToAttribute().setTimestampValue(txData, this.businessDateAttribute.getInfinityDate());
        container.addObjectForTx(toInsert);
        cache.putDatedData(txData);
        tx.insert(toInsert);

        this.businessDateAttribute.getToAttribute().incrementUpdateCount();
    }

    public void increment(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getInfinityDate()));
        Cache cache = mithraObject.zGetCache();
        incrementMultipleWrappers(inTxObjects, fromDate, tx, new AttributeUpdateWrapper[] { updateWrapper } , container, cache);
        mithraObject.zClearTxData();
    }

    protected void incrementMultipleWrappers(InternalList inTxObjects, Timestamp fromDate, MithraTransaction tx, AttributeUpdateWrapper[] updateWrappers, TemporalContainer container, Cache cache)
    {
        InternalList toInsert = new InternalList(inTxObjects.size());
        long fromDateAsLong = fromDate.getTime();
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                MithraDatedTransactionalObject objectToInsertForIncrement = getObjectToInsertForIncrement(inTxObject,
                                        updateWrappers);
                toInsert.add(objectToInsertForIncrement);
                this.businessDateAttribute.getFromAttribute().setTimestampValue(objectToInsertForIncrement.zGetTxDataForWrite(), fromDate);
                cutTail(inTxObject, fromDate, container, cache, tx);
            }
            else
            {
                applyUpateWrappers(tx, updateWrappers, container, cache, inTxObject);
            }
        }
        for(int i=0;i<toInsert.size();i++)
        {
            InTransactionDatedTransactionalObject objectToInsert = (InTransactionDatedTransactionalObject) toInsert.get(i);
            MithraDataObject mithraDataObject = objectToInsert.zGetTxDataForWrite(); // this is a copy of the activeData
            container.addObjectForTx(objectToInsert);
            cache.putDatedData(mithraDataObject);
            tx.insert(objectToInsert);
        }
        this.businessDateAttribute.getToAttribute().incrementUpdateCount();
    }

    private void applyUpateWrappers(MithraTransaction tx, AttributeUpdateWrapper[] updateWrappers, TemporalContainer container, Cache cache, InTransactionDatedTransactionalObject inTxObject)
    {
        MithraDataObject data = inTxObject.zGetTxDataForWrite();
        inTxObject.setToBeUpdated();
        cache.removeDatedData(data);
        for (int k = 0; k < updateWrappers.length; k++)
        {
            updateWrappers[k].updateData(data);
            tx.update(inTxObject, updateWrappers[k]);
            updateWrappers[k].incrementUpdateCount();
        }
        cache.putDatedData(data);
        container.addActiveData(data);
    }

    private MithraDatedTransactionalObject getObjectToInsertForIncrement(
            InTransactionDatedTransactionalObject inTxObject,
            AttributeUpdateWrapper[] updateWrappers)
    {
        InTransactionDatedTransactionalObject result = null;
        inTxObject.zGetTxDataForRead().zIncrementDataVersion();
        result = inTxObject.copyForInsert();
        MithraDataObject data = result.zGetTxDataForWrite();

        for(int i=0;i<updateWrappers.length;i++)
        {
            updateWrappers[i].updateData(data);
        }
        return result;
    }


    public void terminate(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getInfinityDate()));
        long fromDateAsLong = fromDate.getTime();
        Cache cache = mithraObject.zGetCache();
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                // thruz the existing data
                split(inTxObject, fromDate, container, cache, tx);
            }
            else
            {
                delete(container, tx, cache, inTxObject);
            }
        }
        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
    }

    public void purge(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = container.getTransaction();

        Cache cache = mithraObject.zGetCache();
        MithraDataObject passedInData = mithraObject.zGetTxDataForRead();

        //Purge the current object
        InTransactionDatedTransactionalObject currentObject = new InTransactionDatedTransactionalObject(mithraObject.zGetPortal(),
                    passedInData, null, InTransactionDatedTransactionalObject.DELETED_STATE);
        tx.enrollObject(currentObject, currentObject.zGetCache());
        cache.removeDatedData(passedInData);

        //Set the state for all the intransaction objects to deleted
        InternalList inTxObjects = container.getInTxObjects();
        if (inTxObjects != null)
        {
            for(int i=0;i<inTxObjects.size();i++)
            {
                InTransactionDatedTransactionalObject txObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
                txObject.zSetDeleted();
            }
        }

        //Remove all other dates from cache
        List allDataIgnoringDates = cache.getDatedDataIgnoringDates(passedInData);
        for (int i=0; i<allDataIgnoringDates.size(); i++)
        {
            MithraDataObject dataObject = (MithraDataObject) allDataIgnoringDates.get(i);

            InTransactionDatedTransactionalObject otherDatedObject = new InTransactionDatedTransactionalObject(mithraObject.zGetPortal(),
                dataObject, null, InTransactionDatedTransactionalObject.DELETED_STATE);
            tx.enrollObject(otherDatedObject, otherDatedObject.zGetCache());
            cache.removeDatedData(dataObject);
        }
        container.clearAllObjects();

        tx.purge(currentObject);

        mithraObject.zClearTxData();
        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
    }

    public void insertForRecovery(MithraDatedTransactionalObject obj, TemporalContainer container)
    {
        checkDatesAreAllSet(obj);
        this.insert(obj, container);
    }

    private void checkDatesAreAllSet(MithraDatedTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetTxDataForRead();

        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        TimestampAttribute businessFromAttribute = businessDateAttribute.getFromAttribute();

        if (businessFromAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify businessFrom attribute");
        }
        if (businessToAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify businessTo attribute");
        }
    }

    public void incrementUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container,
            AttributeUpdateWrapper updateWrapper, Timestamp until)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        Cache cache = mithraObject.zGetCache();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = this.getBusinessFromDateForBusinessDate(asOfDate);
        Timestamp endDate = getBusinessFromDateForBusinessDate(until);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, endDate));
        incrementMultipleWrappersUntil(inTxObjects, fromDate, endDate, cache, tx,
                new AttributeUpdateWrapper[] { updateWrapper} , container);
        mithraObject.zClearTxData();
    }

    private void incrementMultipleWrappersUntil(InternalList inTxObjects, Timestamp fromDate, Timestamp endDate, Cache cache,
            MithraTransaction tx, AttributeUpdateWrapper[] updateWrappers, TemporalContainer container)
    {
        InternalList toInsert = new InternalList(inTxObjects.size());
        long fromDateAsLong = fromDate.getTime();
        long endDateAsLong = endDate.getTime();
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);
            long activeTo = businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData);

            if (activeTo > endDateAsLong)
            {
                // split the tail end
                splitTailEnd(inTxObject, endDate, activeData, toInsert);
                MithraDataObject splitData = inTxObject.zGetTxDataForWrite();
                cache.removeDatedData(splitData);
                TimestampUpdateWrapper updateBusinessToWrapper = new TimestampUpdateWrapper(this.businessDateAttribute.getToAttribute(),
                        splitData, endDate);
                updateBusinessToWrapper.updateData();
                cache.putDatedData(splitData);
                inTxObject.setToBeUpdated();
                tx.update(inTxObject, updateBusinessToWrapper);
            }
            if (activeFrom < fromDateAsLong)
            {
                MithraDatedTransactionalObject objectToInsertForIncrement = getObjectToInsertForIncrement(inTxObject,
                                        updateWrappers);
                toInsert.add(objectToInsertForIncrement);
                this.businessDateAttribute.getFromAttribute().setTimestampValue(objectToInsertForIncrement.zGetTxDataForWrite(), fromDate);
                cutTail(inTxObject, fromDate, container, cache, tx);
            }
            else
            {
                applyUpateWrappers(tx, updateWrappers, container, cache, inTxObject);
            }
        }
        for(int i=0;i<toInsert.size();i++)
        {
            InTransactionDatedTransactionalObject objectToInsert = (InTransactionDatedTransactionalObject) toInsert.get(i);
            MithraDataObject mithraDataObject = objectToInsert.zGetTxDataForWrite();
            container.addObjectForTx(objectToInsert);
            cache.putDatedData(mithraDataObject);
            tx.insert(objectToInsert);
        }
        this.businessDateAttribute.getToAttribute().incrementUpdateCount();
    }

    private void splitTailEnd(InTransactionDatedTransactionalObject inTxObject, Timestamp endDate,
            MithraDataObject activeData, InternalList toInsert)
    {
        InTransactionDatedTransactionalObject splitEnd = inTxObject.copyForInsert();
        MithraDataObject data = splitEnd.zGetTxDataForWrite();

        businessDateAttribute.getFromAttribute().setTimestampValue(data, endDate);
        activeData.zIncrementDataVersion();
        toInsert.add(splitEnd);
    }

    protected void cutTail(InTransactionDatedTransactionalObject inTxObject, Timestamp fromDate,
            TemporalContainer container, Cache cache,
            MithraTransaction tx)
    {
        container.voidData(inTxObject.zGetTxDataForRead());
        TimestampUpdateWrapper updateWrapper = this.cutTailWithoutUpdate(inTxObject, fromDate, container, cache, tx);
        container.addActiveData(inTxObject.zGetTxDataForRead());
        tx.update(inTxObject, updateWrapper);
    }

    protected TimestampUpdateWrapper cutTailWithoutUpdate(InTransactionDatedTransactionalObject inTxObject, Timestamp fromDate,
            TemporalContainer container, Cache cache,
            MithraTransaction tx)
    {
        MithraDataObject splitData = inTxObject.zGetTxDataForWrite();
        cache.removeDatedData(splitData);
        TimestampUpdateWrapper updateBusinessToWrapper = new TimestampUpdateWrapper(this.businessDateAttribute.getToAttribute(),
                splitData, fromDate);
        updateBusinessToWrapper.updateData();
        cache.putDatedData(splitData);
        MithraDataObject committed = inTxObject.zGetCurrentData();
        if (committed != null)
            committed.zIncrementDataVersion();
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();
        return updateBusinessToWrapper;
    }

    public void updateUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper, Timestamp until)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        Timestamp endDate = getBusinessFromDateForBusinessDate(until);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, endDate));
        long fromDateAsLong = fromDate.getTime();
        long endDateAsLong = endDate.getTime();
        boolean mustInsert = false;
        InternalList toInsert = new InternalList(2);
        Cache cache = mithraObject.zGetCache();
        InTransactionDatedTransactionalObject forInsert = new InTransactionDatedTransactionalObject(mithraObject.zGetPortal(),
                mithraObject.zGetCurrentData(), mithraObject.zGetTxDataForRead().copy(false),
                InTransactionDatedTransactionalObject.TO_BE_INSERTED_STATE);
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);
            long activeTo = businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                if (activeTo > endDateAsLong)
                {
                    splitTailEnd(inTxObject, endDate, activeData, toInsert);
                }
                cutTail(inTxObject, fromDate, container, cache, tx);
                mustInsert = true;
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (activeTo < endDateAsLong)
                {
                    delete(container, tx, cache, inTxObject);
                    mustInsert = true;
                }
                else if (activeTo == endDateAsLong)
                {
                    // exact match
                    MithraDataObject toUpdateData = inTxObject.zGetTxDataForWrite();
                    container.addActiveData(toUpdateData);
                    cache.removeDatedData(toUpdateData);
                    updateWrapper.updateData(toUpdateData);
                    cache.putDatedData(toUpdateData);
                    tx.update(inTxObject, updateWrapper);
                }
                else if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, toInsert);
                    MithraDataObject toUpdateData = inTxObject.zGetTxDataForWrite();
                    cache.removeDatedData(toUpdateData);
                    updateWrapper.updateData(toUpdateData);
                    cache.putDatedData(toUpdateData);
                    tx.update(inTxObject, updateWrapper);
                    cutTail(inTxObject, endDate, container, cache, tx);
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, toInsert);
                }
                delete(container, tx, cache, inTxObject);
                mustInsert = true;
            }
        }
        for(int i=0;i<toInsert.size();i++)
        {
            InTransactionDatedTransactionalObject objectToInsert = (InTransactionDatedTransactionalObject) toInsert.get(i);
            MithraDataObject mithraDataObject = objectToInsert.zGetTxDataForWrite();
            container.addObjectForTx(objectToInsert);
            cache.putDatedData(mithraDataObject);
            tx.insert(objectToInsert);
        }
        if (mustInsert)
        {
            MithraDataObject mithraDataObject = forInsert.zGetTxDataForWrite();
            updateWrapper.updateData(mithraDataObject);
            this.businessDateAttribute.getFromAttribute().setTimestampValue(mithraDataObject, fromDate);
            this.businessDateAttribute.getToAttribute().setTimestampValue(mithraDataObject, endDate);
            container.addObjectForTx(forInsert);
            cache.putDatedData(mithraDataObject);
            tx.insert(forInsert);

            this.businessDateAttribute.getToAttribute().incrementUpdateCount();
        }
        mithraObject.zClearTxData();
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new RuntimeException("should never get here");
    }

    public void terminateUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container, Timestamp until)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        Timestamp endDate = getBusinessFromDateForBusinessDate(until);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, endDate));
        long fromDateAsLong = fromDate.getTime();
        long endDateAsLong = endDate.getTime();
        InternalList toInsert = new InternalList(2);
        Cache cache = mithraObject.zGetCache();
        InTransactionDatedTransactionalObject forInsert = new InTransactionDatedTransactionalObject(mithraObject.zGetPortal(),
                mithraObject.zGetCurrentData(), mithraObject.zGetTxDataForRead().copy(false),
                InTransactionDatedTransactionalObject.TO_BE_INSERTED_STATE);
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);
            long activeTo = businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                if (activeTo > endDateAsLong)
                {
                    splitTailEnd(inTxObject, endDate, activeData, toInsert);
                }
                cutTail(inTxObject, fromDate, container, cache, tx);
            }
            else
            {
                if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, toInsert);
                }
                delete(container, tx, cache, inTxObject);
            }
        }
        for(int i=0;i<toInsert.size();i++)
        {
            InTransactionDatedTransactionalObject objectToInsert = (InTransactionDatedTransactionalObject) toInsert.get(i);
            MithraDataObject mithraDataObject = objectToInsert.zGetTxDataForWrite();
            container.addObjectForTx(objectToInsert);
            cache.putDatedData(mithraDataObject);
            tx.insert(objectToInsert);
        }
        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
    }

    protected InternalList sortByBusinessFrom(InternalList dataList)
    {
        dataList.sort(byBusinessFromComparator);
        return dataList;
    }

    public Timestamp getBusinessFromDateForBusinessDate(Timestamp businessDate)
    {
        if (businessDateAttribute.isToIsInclusive())
        {
            return new Timestamp(businessDate.getTime() - DAY_IN_MS);
        }
        return businessDate;
    }

    private class ByBusinessFromComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            InTransactionDatedTransactionalObject left = (InTransactionDatedTransactionalObject) o1;
            InTransactionDatedTransactionalObject right = (InTransactionDatedTransactionalObject) o2;
            return businessDateAttribute.getFromAttribute().descendingOrderBy().compare(left.zGetTxDataForRead(), right.zGetTxDataForRead());
        }
    }
}
