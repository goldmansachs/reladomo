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
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.BigDecimalIncrementUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.DoubleIncrementUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.TimestampUpdateWrapper;
import com.gs.fw.common.mithra.behavior.state.DatedPersistedState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;



public class GenericBiTemporalDirector implements TemporalDirector
{
    private static int DAYS_TO_ADD = 1;
    private static Logger logger = LoggerFactory.getLogger(GenericBiTemporalDirector.class.getName());

    private AsOfAttribute businessDateAttribute;
    private AsOfAttribute processingDateAttribute;
    private DoubleAttribute[] doubleAttributes;
    private BigDecimalAttribute[] bigDecimalAttributes;
    private ByBusinessFromComparator byBusinessFromComparator = new ByBusinessFromComparator();
    private static ThreadLocal threadLocalCalendar = new ThreadLocal();

    public GenericBiTemporalDirector(AsOfAttribute businessDateAttribute, AsOfAttribute processingDateAttribute,
            DoubleAttribute[] doubleAttributes, BigDecimalAttribute[] bigDecimalAttributes)
    {
        this.businessDateAttribute = businessDateAttribute;
        this.processingDateAttribute = processingDateAttribute;
        this.doubleAttributes = doubleAttributes;
        this.bigDecimalAttributes = bigDecimalAttributes;
    }

    public static void setDaysToAddToBusinessDateToForInactivation(int daysToAdd)
    {
        if (daysToAdd <= 0) throw new IllegalArgumentException("must be at least 1");
        DAYS_TO_ADD = daysToAdd;
    }

    public void insert(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityDate(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp businessDate = businessDateAttribute.timestampValueOf(mithraObject);
        if (container.getActiveDataFor(businessDate) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists: "+data.zGetPrintablePrimaryKey());
        }

        //todo: rezaem: check the database to ensure nothing matches this new object

        processingDateAttribute.getFromAttribute().setValue(data, createProcessingTimestamp(tx));
        processingDateAttribute.getToAttribute().setValue(data, processingDateAttribute.getInfinityDate());
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

        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        currentTransaction.insert(objectForInsert);

        mithraObject.zSetTxPersistenceState(DatedPersistedState.PERSISTED);
        mithraObject.zGetCache().put(mithraObject);
        mithraObject.zGetPortal().incrementClassUpdateCount();
    }

    private Timestamp createProcessingTimestamp(MithraTransaction tx)
    {
        return new Timestamp(tx.getProcessingStartTime()/10*10); // clamp for sybase
    }

    public void insertWithIncrement(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityDate(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        if (container.getActiveDataFor(asOfDate) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists: " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getInfinityDate()));
        if (inTxObjects.size() == 0)
        {
            this.insert(mithraObject, container);
            return;
        }

        //todo: rezaem: check the database to ensure nothing matches this new object

        processingDateAttribute.getFromAttribute().setValue(data, createProcessingTimestamp(tx));
        processingDateAttribute.getToAttribute().setValue(data, processingDateAttribute.getInfinityDate());
        TimestampAttribute businessFromAttribute = businessDateAttribute.getFromAttribute();
        if (businessFromAttribute.isAttributeNull(data))
        {
            businessFromAttribute.setValue(data, this.getBusinessFromDateForBusinessDate(asOfDate));
        }

        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        InTransactionDatedTransactionalObject nextSegment = (InTransactionDatedTransactionalObject) inTxObjects.get(inTxObjects.size() - 1);
        Timestamp nextSegmentFromDate = businessFromAttribute.timestampValueOf(nextSegment.zGetTxDataForRead());
        businessToAttribute.setTimestampValue(data, nextSegmentFromDate);

        InternalList toIncrement = new InternalList(this.doubleAttributes.length + this.bigDecimalAttributes.length);
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
            this.incrementMultipleWrappers(inTxObjects, asOfDate, nextSegmentFromDate, tx, updateWrappers, container, mithraObject.zGetCache());
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
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        if (container.getActiveDataFor(asOfDate) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists: " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
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

        processingDateAttribute.getFromAttribute().setValue(data, createProcessingTimestamp(tx));
        processingDateAttribute.getToAttribute().setValue(data, processingDateAttribute.getInfinityDate());
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
            this.incrementMultipleWrappersUntil(inTxObjects, fromDate, endDate, tx, mithraObject.zGetCache(),
                    updateWrappers, container, !inactivateOnSameDayUpdate(asOfDate));
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
            long currentToDate = businessToAttribute.timestampValueOfAsLong(data);
            if (endDate.getTime() != currentToDate)
            {
                throw new MithraBusinessException("until date set incorrectly for " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data) +
                        " expecting "+ PrintablePreparedStatement.timestampFormat.print(endDate.getTime())+" but was set to "+
                PrintablePreparedStatement.timestampFormat.print(currentToDate));
            }
        }
        this.insert(mithraObject, container);
    }

    protected void delete(TemporalContainer container, MithraTransaction tx, Cache cache, InTransactionDatedTransactionalObject inTxObject)
    {
        MithraDataObject oldData = inTxObject.zGetCurrentData();
        if (oldData != null)
        {
            cache.removeDatedData(oldData);
            oldData.zIncrementDataVersion();
        }
        MithraDataObject activeData = inTxObject.zGetTxDataForRead();
        cache.removeDatedData(activeData);
        // create a new object with this tx's processing date, pointing to the deleted data object

        activeData.zIncrementDataVersion();

        container.deleteInTxObject(inTxObject);
        tx.delete(inTxObject);
        activeData.zGetMithraObjectPortal().incrementClassUpdateCount();
    }

    protected void inactivateObject(TemporalContainer container, MithraTransaction tx, Cache cache,
        Timestamp txStartTime, InTransactionDatedTransactionalObject inTxObject)
    {
        if (inTxObject.isNewInThisTransaction())
        {
            this.delete(container, tx, cache, inTxObject);
        }
        else
        {
            MithraDataObject oldData = inTxObject.zGetCurrentData();
            if (this.processingDateAttribute.getFromAttribute().timestampValueOfAsLong(oldData) == txStartTime.getTime())
            {
                this.logger.warn("The object " + PrintablePrimaryKeyMessageBuilder.createMessage(inTxObject, oldData) + " has changed too fast. " +
                                 "Deleting, instead of inactivating");
                this.delete(container, tx, cache, inTxObject);
            }
            else
            {
                MithraDataObject inactivatedData = inTxObject.zGetTxDataForWrite();
                cache.removeDatedData(oldData);
                TimestampUpdateWrapper updateProcessingToWrapper = new TimestampUpdateWrapper(this.processingDateAttribute.getToAttribute(),
                        inactivatedData, txStartTime);
                updateProcessingToWrapper.updateData();
                container.inactivateObject(inTxObject);
                long currentFromDate = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(inactivatedData);
                long currentToDate = businessDateAttribute.getToAttribute().timestampValueOfAsLong(inactivatedData);
                if (this.businessDateAttribute.isFutureExpiringRowsExist()
                        || currentToDate != this.businessDateAttribute.getInfinityDate().getTime()
                        || currentFromDate > this.getCurrentBusinessDate().getTime())
                {
                    // in this case, we should not set the businessTo attribute, as we do below
                }
                else
                {
                    TimestampUpdateWrapper updateBusinessToWrapper = new TimestampUpdateWrapper(this.businessDateAttribute.getToAttribute(),
                    inactivatedData, this.addDays(this.getCurrentBusinessDate(), DAYS_TO_ADD));
                    updateBusinessToWrapper.updateData();
                    tx.update(inTxObject, updateBusinessToWrapper);
                }
                cache.putDatedData(inactivatedData);
                tx.update(inTxObject, updateProcessingToWrapper);
            }
        }
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();
    }

    protected void cutTail(InTransactionDatedTransactionalObject inTxObject, Timestamp fromDate,
            TemporalContainer container, Cache cache,
            MithraTransaction tx)
    {
        container.voidData(inTxObject.zGetTxDataForRead());
        TimestampUpdateWrapper updateWrapper = this.cutTailWithoutUpdate(inTxObject, fromDate, container, cache, tx, null);
        container.addActiveData(inTxObject.zGetTxDataForRead());
        if (inTxObject.needsTransactionalUpdate())
        {
            tx.update(inTxObject, updateWrapper);
        }
    }

    protected TimestampUpdateWrapper cutTailWithoutUpdate(InTransactionDatedTransactionalObject inTxObject, Timestamp fromDate,
            TemporalContainer container, Cache cache,
            MithraTransaction tx, Timestamp txStartTime)
    {
        MithraDataObject splitData = inTxObject.zGetTxDataForWrite();
//        this.processingDateAttribute.getToAttribute().setTimestampValue(splitData,
//                this.processingDateAttribute.getInfinityDate());
        if (txStartTime != null)
        {
            cache.removeDatedData(splitData.copy());
        }
        else
        {
            cache.removeDatedData(splitData);
        }
        TimestampUpdateWrapper updateBusinessToWrapper = new TimestampUpdateWrapper(this.businessDateAttribute.getToAttribute(),
                splitData, fromDate);
        updateBusinessToWrapper.updateData();
        if (txStartTime != null)
        {
            this.processingDateAttribute.getFromAttribute().setTimestampValue(splitData, txStartTime);
        }
        cache.putDatedData(splitData);
        MithraDataObject committed = inTxObject.zGetCurrentData();
        if (committed != null)
            committed.zIncrementDataVersion();
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();
        return updateBusinessToWrapper;
    }

    private void insertForUpdate(AttributeUpdateWrapper updateWrapper,
            MithraDataObject txData, Timestamp txStartTime, Timestamp fromDate, TemporalContainer container, MithraTransaction tx, Cache cache)
    {
        updateWrapper.updateData(txData);
        this.processingDateAttribute.getFromAttribute().setTimestampValue(txData, txStartTime);
        this.businessDateAttribute.getFromAttribute().setTimestampValue(txData, fromDate);
        this.businessDateAttribute.getToAttribute().setTimestampValue(txData, this.businessDateAttribute.getInfinityDate());
        InTransactionDatedTransactionalObject inTxObject = container.makeUninsertedDataActiveAndCreateObject(txData);
        cache.putDatedData(txData);
        tx.insert(inTxObject);

        this.businessDateAttribute.getToAttribute().incrementUpdateCount();
        this.processingDateAttribute.getToAttribute().incrementUpdateCount();
    }

    public void update(MithraDatedTransactionalObject mithraObject,
                       TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getInfinityDate()));
        long fromDateAsLong = fromDate.getTime();
        MithraDataObject txDataForInsert = null;
        Timestamp txStartTime = createProcessingTimestamp(tx);
        Cache cache = mithraObject.zGetCache();
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                txDataForInsert = inTxObject.zGetTxDataForRead().copy(false);
                if (mayCutTailWithoutInactivation(inTxObject, activeData, asOfDate) && !this.businessDateAttribute.isFutureExpiringRowsExist())
                {
                    // thruz the existing data
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else
                {
                    InTransactionDatedTransactionalObject toInsert = inTxObject.copyForInsert();
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    cutTailWithoutUpdate(toInsert, fromDate, container, cache, tx, txStartTime);
                    container.addObjectForTx(toInsert);
                    tx.insert(toInsert);
                }
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData) == businessDateAttribute.getInfinityDate().getTime())
                {
                    if (inTxObject.isNewInThisTransaction())
                    {
                        // no need to worry about inactivation
                        MithraDataObject toUpdateData = inTxObject.zGetTxDataForRead();
                        cache.removeDatedData(toUpdateData);
                        updateWrapper.updateData(toUpdateData);
                        cache.putDatedData(toUpdateData);
                        if (inTxObject.needsTransactionalUpdate())
                        {
                            tx.update(inTxObject, updateWrapper);
                        }
                    }
                    else
                    {
                        if (this.inactivateOnSameDayUpdate(asOfDate))
                        {
                            txDataForInsert = inTxObject.zGetTxDataForRead().copy(false);
                            inactivateObject(container, tx, cache, txStartTime, inTxObject);
                        }
                        else
                        {
                            // in place update: must set processingFrom: delete and reinsert (logically)
                            inTxObject.setToBeUpdated();
                            MithraDataObject updatedData = inTxObject.zGetTxDataForWrite();
                            cache.removeDatedData(inTxObject.zGetCurrentData());
                            tx.enrollObject(inTxObject.copyForDelete(), inTxObject.zGetCache());
                            TimestampUpdateWrapper updateProcessingFromWrapper = new TimestampUpdateWrapper(this.processingDateAttribute.getFromAttribute(),
                                    updatedData, txStartTime);
                            updateProcessingFromWrapper.updateData();
                            updateWrapper.updateData(updatedData);
                            container.addObjectForTx(inTxObject);
                            cache.putDatedData(updatedData);
                            container.updateInPlaceData(inTxObject.zGetCurrentData(), updatedData);
                            if (inTxObject.needsTransactionalUpdate())
                            {
                                tx.update(inTxObject, updateProcessingFromWrapper);
                                tx.update(inTxObject, updateWrapper);
                            }
                        }
                    }
                }
                else
                {
                    txDataForInsert = inTxObject.zGetTxDataForRead().copy(false);
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                txDataForInsert = inTxObject.zGetTxDataForRead().copy(false);
                inactivateObject(container, tx, cache, txStartTime, inTxObject);
            }
        }
        if (txDataForInsert != null)
        {
            insertForUpdate(updateWrapper, txDataForInsert, txStartTime, fromDate, container, tx, cache);
        }
        mithraObject.zClearTxData();
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getToAttribute().timestampValueOf(mithraObject)));
        Cache cache = mithraObject.zGetCache();

        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);

            MithraDataObject toUpdateData = inTxObject.zGetTxDataForWrite();
            cache.removeDatedData(toUpdateData);
            updateWrapper.updateData(toUpdateData);
            cache.putDatedData(toUpdateData);
            tx.update(inTxObject, updateWrapper);
        }
        mithraObject.zClearTxData();
    }

    protected boolean mayCutTailWithoutInactivation(InTransactionDatedTransactionalObject inTxObject,
                                                    MithraDataObject activeData, Timestamp asOfDate)
    {
        return inTxObject.isNewInThisTransaction() ||
                (businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData) == businessDateAttribute.getInfinityDate().getTime()
                 && !this.inactivateOnSameDayUpdate(asOfDate));
    }

    private MithraDatedTransactionalObject getObjectToInsertForIncrement(
            InTransactionDatedTransactionalObject inTxObject,
            AttributeUpdateWrapper[] updateWrappers, Timestamp fromDate, Timestamp txStartTime,
            TemporalContainer container)
    {
        InTransactionDatedTransactionalObject result = null;
        inTxObject.zGetTxDataForRead().zIncrementDataVersion();
        result = inTxObject.copyForInsert();
        MithraDataObject data = result.zGetTxDataForWrite();

        for(int i=0;i<updateWrappers.length;i++)
        {
            updateWrappers[i].updateData(data);
        }
        this.processingDateAttribute.getFromAttribute().setTimestampValue(data, txStartTime);
        return result;
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
        incrementMultipleWrappers(inTxObjects, asOfDate, fromDate, tx, new AttributeUpdateWrapper[] { updateWrapper } , container, cache);
        mithraObject.zClearTxData();
    }

    private void incrementMultipleWrappers(InternalList inTxObjects, Timestamp asOfDate, Timestamp fromDate, MithraTransaction tx, AttributeUpdateWrapper[] updateWrappers, TemporalContainer container, Cache cache)
    {
        InternalList toInsert = new InternalList(inTxObjects.size());
        long fromDateAsLong = fromDate.getTime();
        Timestamp txStartTime = createProcessingTimestamp(tx);
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                MithraDatedTransactionalObject objectToInsertForIncrement = getObjectToInsertForIncrement(inTxObject,
                                        updateWrappers, fromDate, txStartTime, container);
                toInsert.add(objectToInsertForIncrement);
                this.businessDateAttribute.getFromAttribute().setTimestampValue(objectToInsertForIncrement.zGetTxDataForWrite(), fromDate);
                if (mayCutTailWithoutInactivation(inTxObject, activeData, asOfDate) && !this.businessDateAttribute.isFutureExpiringRowsExist())
                {
                    // thruz the existing data
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else
                {
                    InTransactionDatedTransactionalObject head = inTxObject.copyForInsert();
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    cutTailWithoutUpdate(head, fromDate, container, cache, tx, txStartTime);
                    toInsert.add(head);
                }
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (inTxObject.isNewInThisTransaction())
                {
                    // no need to worry about inactivation
                    MithraDataObject data = inTxObject.zGetTxDataForWrite();
                    cache.removeDatedData(data);
                    applyUpdateWrappers(updateWrappers, data, tx, inTxObject);
                    cache.putDatedData(data);
                }
                else
                {
                    if (businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData) == businessDateAttribute.getInfinityDate().getTime() )
                    {
                        if (this.inactivateOnSameDayUpdate(asOfDate))
                        {
                            toInsert.add(getObjectToInsertForIncrement(inTxObject,
                                    updateWrappers, fromDate, txStartTime, container));
                            inactivateObject(container, tx, cache, txStartTime, inTxObject);
                        }
                        else
                        {
                            // in place update: must set processingFrom: delete and reinsert (logically)
                            inTxObject.setToBeUpdated();
                            MithraDataObject updatedData = inTxObject.zGetTxDataForWrite();
                            cache.removeDatedData(inTxObject.zGetCurrentData());
                            tx.enrollObject(inTxObject.copyForDelete(), inTxObject.zGetCache());
                            TimestampUpdateWrapper updateProcessingFromWrapper = new TimestampUpdateWrapper(this.processingDateAttribute.getFromAttribute(),
                                    updatedData, txStartTime);
                            updateProcessingFromWrapper.updateData();
                            applyUpdateWrappers(updateWrappers, updatedData, tx, inTxObject);
                            cache.putDatedData(updatedData);
                            container.updateInPlaceData(inTxObject.zGetCurrentData(), updatedData);
                            tx.update(inTxObject, updateProcessingFromWrapper);
                        }
                    }
                    else
                    {
                        toInsert.add(getObjectToInsertForIncrement(inTxObject,
                                updateWrappers, fromDate, txStartTime, container));
                        inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    }
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                if (inTxObject.isNewInThisTransaction())
                {
                    // no need to worry about inactivation
                    MithraDataObject data = inTxObject.zGetTxDataForWrite();
                    cache.removeDatedData(data);
                    applyUpdateWrappers(updateWrappers, data, tx, inTxObject);
                    cache.putDatedData(data);
                }
                else
                {
                    toInsert.add(getObjectToInsertForIncrement(inTxObject,
                            updateWrappers, fromDate, txStartTime, container));
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
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
        this.processingDateAttribute.getToAttribute().incrementUpdateCount();
    }

    private void applyUpdateWrappers(AttributeUpdateWrapper[] updateWrappers, MithraDataObject data, MithraTransaction tx, InTransactionDatedTransactionalObject inTxObject)
    {
        for(int k=0;k<updateWrappers.length;k++)
        {
            updateWrappers[k].updateData(data);
            if (inTxObject.needsTransactionalUpdate())
            {
                tx.update(inTxObject, updateWrappers[k]);
            }
            updateWrappers[k].incrementUpdateCount();
        }
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
        Timestamp txStartTime = createProcessingTimestamp(tx);
        Cache cache = mithraObject.zGetCache();
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForRead();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);

            if (activeFrom < fromDateAsLong)
            {
                if (mayCutTailWithoutInactivation(inTxObject, activeData, asOfDate) && !this.businessDateAttribute.isFutureExpiringRowsExist())
                {
                    // thruz the existing data
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else
                {
                    InTransactionDatedTransactionalObject head = inTxObject.copyForInsert();
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    cutTailWithoutUpdate(head, fromDate, container, cache, tx, txStartTime);
                    container.addObjectForTx(head);
                    cache.putDatedData(head.zGetTxDataForRead());
                    tx.insert(head);
                }
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData) ==  businessDateAttribute.getInfinityDate().getTime())
                {
                    if (inTxObject.isNewInThisTransaction())
                    {
                        delete(container, tx, cache, inTxObject);
                    }
                    else
                    {
                        if (this.inactivateOnSameDayUpdate(asOfDate))
                        {
                            inactivateObject(container, tx, cache, txStartTime, inTxObject);
                        }
                        else
                        {
                            // delete!
                            delete(container, tx, cache, inTxObject);
                        }
                    }
                }
                else
                {
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                inactivateObject(container, tx, cache, txStartTime, inTxObject);
            }
        }
        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
    }

    public void purge(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
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

    public void insertForRecovery(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkNotInfinityBusinessDate(mithraObject);
        checkDatesAreAllSet(mithraObject);
        checkDatesAreWithinRange(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();

        Timestamp businessDate = businessDateAttribute.timestampValueOf(mithraObject);
        long processingDateTo = processingDateAttribute.getToAttribute().timestampValueOfAsLong(mithraObject);

        if (container.getActiveDataFor(businessDate) != null &&
            processingDateTo == processingDateAttribute.getInfinityDate().getTime())
        {
            throw new MithraTransactionException("cannot insert data. data already exists: " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }

        //todo: rezaem: check the database to ensure nothing matches this new object
        InTransactionDatedTransactionalObject objectForInsert = container.makeUninsertedDataActiveAndCreateObject(data);

        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        currentTransaction.insert(objectForInsert);

        mithraObject.zSetTxPersistenceState(DatedPersistedState.PERSISTED);
        mithraObject.zGetCache().put(mithraObject);
        mithraObject.zGetPortal().incrementClassUpdateCount();
    }

    private void checkDatesAreAllSet(MithraDatedTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetTxDataForRead();

        TimestampAttribute businessToAttribute = businessDateAttribute.getToAttribute();
        TimestampAttribute businessFromAttribute = businessDateAttribute.getFromAttribute();
        TimestampAttribute processingToAttribute = processingDateAttribute.getToAttribute();
        TimestampAttribute processingFromAttribute = processingDateAttribute.getFromAttribute();

        if (businessFromAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify businessFrom attribute");
        }
        if (!businessToAttribute.isInfiniteNull() && businessToAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify businessTo attribute");
        }
        if (processingFromAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify processingFrom attribute");
        }
        if (!processingToAttribute.isInfiniteNull() && processingToAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify processingTo attribute");
        }
    }

    public void incrementUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container,
                               AttributeUpdateWrapper updateWrapper, Timestamp until)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp asOfDate = businessDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromDate = getBusinessFromDateForBusinessDate(asOfDate);
        Timestamp endDate = getBusinessFromDateForBusinessDate(until);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, endDate));
        Cache cache = mithraObject.zGetCache();
        incrementMultipleWrappersUntil(inTxObjects, fromDate, endDate, tx, cache,
                new AttributeUpdateWrapper[] { updateWrapper }, container, !inactivateOnSameDayUpdate(asOfDate));
        mithraObject.zClearTxData();
    }

    private void incrementMultipleWrappersUntil(InternalList inTxObjects, Timestamp fromDate, Timestamp endDate,
            MithraTransaction tx, Cache cache, AttributeUpdateWrapper[] updateWrappers, TemporalContainer container,
            boolean updateInPlace)
    {
        InternalList toInsert = new InternalList(inTxObjects.size());
        long fromDateAsLong = fromDate.getTime();
        long endDateAsLong = endDate.getTime();
        Timestamp txStartTime = createProcessingTimestamp(tx);
        for(int i=0;i<inTxObjects.size();i++)
        {
            InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(i);
            MithraDataObject activeData = inTxObject.zGetTxDataForWrite();
            long activeFrom = businessDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData);
            long activeTo = businessDateAttribute.getToAttribute().timestampValueOfAsLong(activeData);

            if (inTxObject.isNewInThisTransaction() || updateInPlace)
            {
                // no need to worry about inactivation
                if (activeFrom >= fromDateAsLong && activeTo <= endDateAsLong)
                {
                    // just update in place
                    updateInPlaceForIncrementUntil(cache, activeData, updateWrappers, inTxObject, tx, txStartTime, container);
                }
                else if (activeFrom < fromDateAsLong && activeTo > endDateAsLong)
                {
                    if (activeTo > endDateAsLong)
                    {
                        splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                    }
                    insertIncrementUntilSegment(inTxObject, updateWrappers, fromDate, txStartTime, container, endDate, toInsert);
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else if (activeFrom < fromDateAsLong)
                {
                    insertIncrementUntilSegment(inTxObject, updateWrappers, fromDate, txStartTime, container, endDate, toInsert);
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else if (activeTo > endDateAsLong)
                {
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                    cutTail(inTxObject, endDate, container, cache, tx);
                    updateInPlaceForIncrementUntil(cache, activeData, updateWrappers, inTxObject, tx, txStartTime, container);
                }
            }
            else
            {
                if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                }

                insertIncrementUntilSegment(inTxObject, updateWrappers, fromDate, txStartTime, container, endDate, toInsert);
                // thruz the existing data

                if (activeFrom < fromDateAsLong)
                {
                    InTransactionDatedTransactionalObject tail = inTxObject.copyForInsert();
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    cutTailWithoutUpdate(tail, fromDate, container, cache, tx, txStartTime);
                    toInsert.add(tail);
                }

                inactivateObject(container, tx, cache, txStartTime, inTxObject);
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
        this.processingDateAttribute.getToAttribute().incrementUpdateCount();
    }

    private void insertIncrementUntilSegment(InTransactionDatedTransactionalObject inTxObject, AttributeUpdateWrapper[] updateWrappers, Timestamp fromDate, Timestamp txStartTime, TemporalContainer container, Timestamp endDate, InternalList toInsert)
    {
        InTransactionDatedTransactionalObject toInsertObject = getObjectToInsertForIncrementUntil(inTxObject,
                                updateWrappers[0], fromDate, txStartTime, container, endDate);
        toInsert.add(toInsertObject);
        MithraDataObject toInsertData = toInsertObject.zGetTxDataForWrite();
        for(int k=1; k < updateWrappers.length; k++)
        {
            updateWrappers[k].updateData(toInsertData);
            updateWrappers[k].incrementUpdateCount();
        }
    }

    private void updateInPlaceForIncrementUntil(Cache cache, MithraDataObject activeData,
            AttributeUpdateWrapper[] updateWrappers, InTransactionDatedTransactionalObject inTxObject,
            MithraTransaction tx, Timestamp txStartTime, TemporalContainer container)
    {
        cache.removeDatedData(activeData);
        if (this.processingDateAttribute.getFromAttribute().timestampValueOfAsLong(activeData) != txStartTime.getTime())
        {
            inTxObject.setToBeUpdated();
            MithraDataObject updatedData = inTxObject.zGetTxDataForWrite();
            cache.removeDatedData(inTxObject.zGetCurrentData());
            tx.enrollObject(inTxObject.copyForDelete(), inTxObject.zGetCache());
            TimestampUpdateWrapper updateProcessingFromWrapper = new TimestampUpdateWrapper(this.processingDateAttribute.getFromAttribute(),
                    updatedData, txStartTime);
            updateProcessingFromWrapper.updateData();
            for(int k=0; k < updateWrappers.length; k++)
            {
                updateWrappers[k].updateData(activeData);
                updateWrappers[k].incrementUpdateCount();
            }
            cache.putDatedData(updatedData);
            container.updateInPlaceData(inTxObject.zGetCurrentData(), updatedData);
            if (inTxObject.needsTransactionalUpdate())
            {
                tx.update(inTxObject, updateProcessingFromWrapper);
                for(int k=0; k < updateWrappers.length; k++)
                {
                    tx.update(inTxObject, updateWrappers[k]);
                }
            }
        }
        else
        {
            for(int k=0; k < updateWrappers.length; k++)
            {
                updateWrappers[k].updateData(activeData);
                updateWrappers[k].incrementUpdateCount();
                if (inTxObject.needsTransactionalUpdate())
                {
                    tx.update(inTxObject, updateWrappers[k]);
                }
            }
        }
        cache.putDatedData(activeData);
    }

    public void updateUntil(MithraDatedTransactionalObject mithraObject, TemporalContainer container,
            AttributeUpdateWrapper updateWrapper, Timestamp until)
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
        Timestamp txStartTime = createProcessingTimestamp(tx);
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
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                }
                if (inTxObject.isNewInThisTransaction())
                {
                    // thruz the existing data
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else
                {
                    InTransactionDatedTransactionalObject head = inTxObject.copyForInsert();
                    cutTailWithoutUpdate(head, fromDate, container, cache, tx, txStartTime);
                    toInsert.add(head);
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
                mustInsert = true;
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (activeTo < endDateAsLong)
                {
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    mustInsert = true;
                }
                else if (activeTo == endDateAsLong)
                {
                    // exact match
                    if (inTxObject.isNewInThisTransaction())
                    {
                        MithraDataObject toUpdateData = inTxObject.zGetTxDataForWrite();
                        cache.removeDatedData(toUpdateData);
                        updateWrapper.updateData(toUpdateData);
                        cache.putDatedData(toUpdateData);
                        if (inTxObject.isInserted())
                        {
                            tx.update(inTxObject, updateWrapper);
                        }
                    }
                    else
                    {
                        inactivateObject(container, tx, cache, txStartTime, inTxObject);
                        mustInsert = true;
                    }
                }
                else if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                    mustInsert = true;
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                }
                inactivateObject(container, tx, cache, txStartTime, inTxObject);
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
            this.processingDateAttribute.getFromAttribute().setTimestampValue(mithraDataObject, txStartTime);
            this.businessDateAttribute.getFromAttribute().setTimestampValue(mithraDataObject, fromDate);
            this.businessDateAttribute.getToAttribute().setTimestampValue(mithraDataObject, endDate);
            container.addObjectForTx(forInsert);
            cache.putDatedData(mithraDataObject);
            tx.insert(forInsert);

            this.businessDateAttribute.getToAttribute().incrementUpdateCount();
            this.processingDateAttribute.getToAttribute().incrementUpdateCount();
        }
        mithraObject.zClearTxData();
    }

    private void splitTailEnd(InTransactionDatedTransactionalObject inTxObject, Timestamp endDate, MithraDataObject activeData, Timestamp txStartTime, InternalList toInsert)
    {
        InTransactionDatedTransactionalObject splitEnd = inTxObject.copyForInsert();
        MithraDataObject data = splitEnd.zGetTxDataForWrite();

        businessDateAttribute.getFromAttribute().setTimestampValue(data, endDate);
        activeData.zIncrementDataVersion();
        this.processingDateAttribute.getFromAttribute().setTimestampValue(data, txStartTime);
        toInsert.add(splitEnd);
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject mithraObject, TemporalContainer container, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        checkInfinityDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());

        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        Timestamp fromDate = businessDateAttribute.getFromAttribute().timestampValueOf(mithraObject);
        InternalList inTxObjects = this.sortByBusinessFrom(container.getObjectsForRange(mithraObject,
                fromDate, businessDateAttribute.getToAttribute().timestampValueOf(mithraObject)));
        long fromDateAsLong = fromDate.getTime();
        Cache cache = mithraObject.zGetCache();
        InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(0);
        if (inTxObjects.size() != 1 || businessDateAttribute.getFromAttribute().timestampValueOfAsLong(inTxObject.zGetTxDataForRead()) != fromDateAsLong)
        {
            throw new MithraBusinessException("should not get here: stale data in object");
        }
        if (inTxObject.isNewInThisTransaction())
        {
            this.delete(container, tx, cache, inTxObject);
        }
        else
        {
            MithraDataObject oldData = inTxObject.zGetCurrentData();
            MithraDataObject inactivatedData = inTxObject.zGetTxDataForWrite();
            cache.removeDatedData(oldData);
            if (businessDateTo != null)
            {
                TimestampUpdateWrapper updateBusinessToWrapper = new TimestampUpdateWrapper(this.businessDateAttribute.getToAttribute(),
                        inactivatedData, businessDateTo);
                updateBusinessToWrapper.updateData();
                tx.update(inTxObject, updateBusinessToWrapper);
            }
            TimestampUpdateWrapper updateProcessingToWrapper = new TimestampUpdateWrapper(this.processingDateAttribute.getToAttribute(),
                    inactivatedData, processingDateTo);
            updateProcessingToWrapper.updateData();
            container.inactivateObject(inTxObject);
            cache.putDatedData(inactivatedData);
            tx.update(inTxObject, updateProcessingToWrapper);
        }
        this.businessDateAttribute.getOwnerPortal().incrementClassUpdateCount();

        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
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
        Timestamp txStartTime = createProcessingTimestamp(tx);
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
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                }
                if (inTxObject.isNewInThisTransaction())
                {
                    // thruz the existing data
                    cutTail(inTxObject, fromDate, container, cache, tx);
                }
                else
                {
                    InTransactionDatedTransactionalObject head = inTxObject.copyForInsert();
                    cutTailWithoutUpdate(head, fromDate, container, cache, tx, txStartTime);
                    toInsert.add(head);
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
            }
            else if (activeFrom == fromDateAsLong)
            {
                if (activeTo <= endDateAsLong)
                {
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
                else
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                    inactivateObject(container, tx, cache, txStartTime, inTxObject);
                }
            }
            else if (activeFrom > fromDateAsLong)
            {
                if (activeTo > endDateAsLong)
                {
                    // split the tail end
                    splitTailEnd(inTxObject, endDate, activeData, txStartTime, toInsert);
                }
                inactivateObject(container, tx, cache, txStartTime, inTxObject);
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

    private InTransactionDatedTransactionalObject getObjectToInsertForIncrementUntil(InTransactionDatedTransactionalObject inTxObject,
            AttributeUpdateWrapper updateWrapper, Timestamp fromDate,
            Timestamp txStartTime, TemporalContainer container, Timestamp endDate)
    {
        Timestamp currentEnd = endDate;
        Timestamp currentStart = fromDate;
        MithraDataObject activeData = inTxObject.zGetTxDataForRead();
        Timestamp activeDataEnd = this.businessDateAttribute.getToAttribute().timestampValueOf(activeData);
        if (activeDataEnd.getTime() < currentEnd.getTime())
        {
            currentEnd = activeDataEnd;
        }
        Timestamp activeDataStart = this.businessDateAttribute.getFromAttribute().timestampValueOf(activeData);
        if (activeDataStart.getTime() > fromDate.getTime())
        {
            currentStart = activeDataStart;
        }
        InTransactionDatedTransactionalObject result = inTxObject.copyForInsert();
        MithraDataObject data = result.zGetTxDataForWrite();

        this.processingDateAttribute.getFromAttribute().setTimestampValue(data, txStartTime);
        this.businessDateAttribute.getFromAttribute().setTimestampValue(data, currentStart);
        this.businessDateAttribute.getToAttribute().setTimestampValue(data, currentEnd);

        activeData.zIncrementDataVersion();
        updateWrapper.updateData(data);
        return result;
    }

    public Timestamp getBusinessToDateForBusinessDate(Timestamp asOfDate)
    {
//        return new Timestamp(asOfDate.getTime()+DAY_IN_MS);
        throw new RuntimeException("not implemented");
    }

    protected InternalList sortByBusinessFrom(InternalList dataList)
    {
        dataList.sort(byBusinessFromComparator);
        return dataList;
    }

    protected void checkInfinityDate(MithraDatedTransactionalObject mithraObject)
    {
        long processingDate = processingDateAttribute.timestampValueOfAsLong(mithraObject);
        if (processingDate != processingDateAttribute.getInfinityDate().getTime())
        {
            throw new MithraTransactionException("processing date must be infinity when creating/modifying an object");
        }
        this.checkNotInfinityBusinessDate(mithraObject);
    }

    protected void checkNotInfinityBusinessDate(MithraDatedTransactionalObject mithraObject)
    {
        long businessDate = businessDateAttribute.timestampValueOfAsLong(mithraObject);
        if (businessDate == businessDateAttribute.getInfinityDate().getTime())
        {
            throw new MithraTransactionException("business date must not be infinity when creating/modifying an object");
        }
    }

    protected void checkDatesAreWithinRange(MithraDatedTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetTxDataForRead();

        Timestamp processingDate = processingDateAttribute.timestampValueOf(mithraObject);
        Timestamp businessDate = businessDateAttribute.timestampValueOf(mithraObject);

        if (!processingDateAttribute.dataMatches(data, processingDate))
        {
             throw new MithraTransactionException("processing date must be valid for to and from processing dates");
        }
        if (!businessDateAttribute.dataMatches(data, businessDate))
        {
             throw new MithraTransactionException("business date must be valid for to and from business dates");
        }
    }

    //todo: rezaem: get this value from the business specific implementation
    public Timestamp getBusinessFromDateForBusinessDate(Timestamp businessDate)
    {
        if (businessDateAttribute.isToIsInclusive())
        {
            return addDays(businessDate, -1);
        }
        return businessDate;
    }

    protected boolean inactivateOnSameDayUpdate(Timestamp asOfDate)
    {
        return true;
    }

    protected Timestamp getCurrentBusinessDate()
    {
        return createProcessingTimestamp(MithraManagerProvider.getMithraManager().getCurrentTransaction());
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


    protected Timestamp addDays(Timestamp date, int daysToAdd)
    {
        Calendar c = getCalendar();
        c.setTimeInMillis(date.getTime());
        c.add(Calendar.DATE, daysToAdd);
        return new Timestamp(c.getTimeInMillis());
    }

    protected Calendar getCalendar()
    {
        Calendar c = (Calendar) threadLocalCalendar.get();
        if (c == null)
        {
            c = Calendar.getInstance();
            threadLocalCalendar.set(c);
        }
        return c;
    }
}