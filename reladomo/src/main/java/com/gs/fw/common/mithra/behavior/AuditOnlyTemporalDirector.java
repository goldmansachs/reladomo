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
import java.util.List;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.TimestampUpdateWrapper;
import com.gs.fw.common.mithra.behavior.state.DatedPersistedState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AuditOnlyTemporalDirector implements TemporalDirector
{
    private static Logger logger = LoggerFactory.getLogger(AuditOnlyTemporalDirector.class.getName());

    private AsOfAttribute processingDateAttribute;

    public AuditOnlyTemporalDirector(AsOfAttribute processingDateAttribute, DoubleAttribute[] doubleAttributes, BigDecimalAttribute[] bigDecimalAttributes)
    {
        this.processingDateAttribute = processingDateAttribute;
    }

    protected void checkInfinityProcessingDate(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp processingDate = processingDateAttribute.timestampValueOf(mithraObject);
        if (processingDate.getTime() != processingDateAttribute.getInfinityDate().getTime())
        {
            throw new MithraTransactionException("processing date must be infinity for an insert");
        }
    }

    protected void checkProcessingDateWithinRange(MithraDatedTransactionalObject mithraObject)
    {
        Timestamp processingDate = processingDateAttribute.timestampValueOf(mithraObject);
        Timestamp fromProcessingDate = processingDateAttribute.getFromAttribute().timestampValueOf(mithraObject);
        Timestamp toProcessingDate = processingDateAttribute.getToAttribute().timestampValueOf(mithraObject);

        if (processingDateAttribute.isToIsInclusive() &&
            (fromProcessingDate.compareTo(processingDate) >= 0 || toProcessingDate.compareTo(processingDate) < 0))
        {
             throw new MithraTransactionException("processing date must be valid for to and from processing dates");
        }

        if (!processingDateAttribute.isToIsInclusive() &&
            (fromProcessingDate.compareTo(processingDate) > 0 || toProcessingDate.compareTo(processingDate) <= 0))
        {
             throw new MithraTransactionException("processing date must be valid for to and from processing dates");
        }
    }

    public void insert(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkInfinityProcessingDate(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (container.getActiveDataFor(null) != null)
        {
            throw new MithraTransactionException("cannot insert data. data already exists " + PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }

        //todo: rezaem: check the database to ensure nothing matches this new object

        processingDateAttribute.getFromAttribute().setValue(data, createProcessingTimestamp(tx));
        processingDateAttribute.getToAttribute().setValue(data, processingDateAttribute.getInfinityDate());
        MithraDataObject containerData = container.getActiveDataFor(null);
        if (containerData != null)
        {
            throw new MithraTransactionException("can't insert a value on object "+mithraObject.getClass().getName());
        }
        InTransactionDatedTransactionalObject objectForInsert = container.makeUninsertedDataActiveAndCreateObject(data);
        container.getTransaction().insert(objectForInsert);

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
        throw new RuntimeException("audit only objects do not provide insert with increment functionality");
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("audit only objects do not provide insert with increment functionality");
    }

    public void insertUntil(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("audit only objects do not provide insert until functionality");
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        checkInfinityProcessingDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = container.getTransaction();
        InternalList inTxObjects = container.getObjectsForRange(mithraObject, null, null);
        InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(0); // guaranteed to have just one
        Cache cache = mithraObject.zGetCache();

        // update cache
        cache.removeDatedData(inTxObject.zGetTxDataForWrite());
        updateWrapper.updateData(inTxObject.zGetTxDataForWrite());
        cache.putDatedData(inTxObject.zGetTxDataForWrite());

        // inplace update
        tx.update(inTxObject, updateWrapper);
        mithraObject.zClearTxData();
    }

    public void update(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        container.enrollInWrite(mithraObject.zGetCurrentData());
        checkInfinityProcessingDate(mithraObject);
        MithraTransaction tx = container.getTransaction();
        Timestamp txStartTime = createProcessingTimestamp(tx);
        InternalList inTxObjects = container.getObjectsForRange(mithraObject, null, null);
        InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(0); // guaranteed to have just one
        Cache cache = mithraObject.zGetCache();

        if (inTxObject.isNewInThisTransaction())
        {
            cache.removeDatedData(inTxObject.zGetTxDataForWrite());
            updateWrapper.updateData(inTxObject.zGetTxDataForWrite());
            cache.putDatedData(inTxObject.zGetTxDataForWrite());
            if (inTxObject.isInserted())
            {
                tx.update(inTxObject, updateWrapper);
            }
        }
        else
        {
            InTransactionDatedTransactionalObject toInsert = inTxObject.copyForInsert();

            inactivate(inTxObject, cache, txStartTime, container, tx);

            MithraDataObject mithraDataObject = toInsert.zGetTxDataForWrite(); // this is a copy of the activeData
            updateWrapper.updateData(mithraDataObject);
            this.processingDateAttribute.getFromAttribute().setTimestampValue(mithraDataObject, txStartTime);
            cache.putDatedData(mithraDataObject);
            container.addObjectForTx(toInsert);

            tx.insert(toInsert);
        }
        mithraObject.zClearTxData();

    }

    protected void inactivate(InTransactionDatedTransactionalObject inTxObject, Cache cache, Timestamp txStartTime, TemporalContainer container, MithraTransaction tx)
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
            cache.removeDatedData(inactivatedData);
            TimestampUpdateWrapper updateProcessingToWrapper = new TimestampUpdateWrapper(this.processingDateAttribute.getToAttribute(),
                    inactivatedData, txStartTime);
            updateProcessingToWrapper.updateData();
            cache.putDatedData(inactivatedData);
            updateProcessingToWrapper.incrementUpdateCount();
            container.inactivateObject(inTxObject);
            tx.update(inTxObject, updateProcessingToWrapper);
            this.processingDateAttribute.getOwnerPortal().incrementClassUpdateCount();
        }
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

    public void increment(MithraDatedTransactionalObject mithraObject, TemporalContainer container, AttributeUpdateWrapper updateWrapper)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void terminate(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = container.getTransaction();
        this.inactivateForArchiving(mithraObject, container, createProcessingTimestamp(tx), null);
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
            for(int j=0; j<inTxObjects.size(); j++)
            {
                InTransactionDatedTransactionalObject txObjectToRemove = (InTransactionDatedTransactionalObject) inTxObjects.get(j);
                txObjectToRemove.zSetDeleted();
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

        tx.purge(currentObject);

        container.clearAllObjects();
        mithraObject.zClearTxData();
        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        this.processingDateAttribute.getOwnerPortal().incrementClassUpdateCount();
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
    }

    public void insertForRecovery(MithraDatedTransactionalObject mithraObject, TemporalContainer container)
    {
        checkDatesAreAllSet(mithraObject);
        checkProcessingDateWithinRange(mithraObject);
        MithraDataObject data = mithraObject.zGetTxDataForWrite();
        Timestamp processingDateTo = processingDateAttribute.getToAttribute().timestampValueOf(mithraObject);

        //Make sure that the recovered row is not colliding with an active row
        if (container.getActiveDataFor(null) != null &&
            processingDateTo.getTime() == processingDateAttribute.getInfinityDate().getTime())
        {
            throw new MithraTransactionException("cannot insert data. Active data already exists "+ PrintablePrimaryKeyMessageBuilder.createMessage(mithraObject, data));
        }

        InTransactionDatedTransactionalObject objectForInsert = container.makeUninsertedDataActiveAndCreateObject(data);
        container.getTransaction().insert(objectForInsert);

        mithraObject.zSetTxPersistenceState(DatedPersistedState.PERSISTED);
        mithraObject.zGetCache().put(mithraObject);
        mithraObject.zGetPortal().incrementClassUpdateCount();
    }

    private void checkDatesAreAllSet(MithraDatedTransactionalObject mithraObject)
    {
        MithraDataObject data = mithraObject.zGetTxDataForRead();

        TimestampAttribute processingToAttribute = processingDateAttribute.getToAttribute();
        TimestampAttribute processingFromAttribute = processingDateAttribute.getFromAttribute();

        if (processingFromAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify processingFrom attribute");
        }
        if (processingToAttribute.isAttributeNull(data))
        {
            throw new MithraBusinessException("need to specify processingTo attribute");
        }
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper, Timestamp until)
    {
        throw new RuntimeException("should never get here.");
    }

    public void updateUntil(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper, Timestamp until)
    {
        throw new RuntimeException("should never get here.");
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject mithraObject, TemporalContainer container, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        checkInfinityProcessingDate(mithraObject);
        container.enrollInWrite(mithraObject.zGetCurrentData());
        MithraTransaction tx = container.getTransaction();
        InternalList inTxObjects = container.getObjectsForRange(mithraObject, null, null);
        InTransactionDatedTransactionalObject inTxObject = (InTransactionDatedTransactionalObject) inTxObjects.get(0); // guaranteed to have just one
        Cache cache = mithraObject.zGetCache();

        if (inTxObject.isNewInThisTransaction())
        {
            cache.removeDatedData(inTxObject.zGetTxDataForWrite());
            tx.delete(inTxObject);
            container.inactivateObject(inTxObject);
        }
        else
        {
            this.inactivate(inTxObject, cache, processingDateTo, container, tx);
        }
        mithraObject.zClearTxData();
        mithraObject.zSetTxPersistenceState(DatedPersistedState.DELETED);
        this.processingDateAttribute.getOwnerPortal().incrementClassUpdateCount();
        tx.addLogicalDeleteForPortal(mithraObject.zGetPortal());
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp until)
    {
        throw new RuntimeException("should never get here.");
    }
}
