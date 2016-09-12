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

package com.gs.fw.common.mithra.superclassimpl;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.AbstractDatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.Time;


public abstract class MithraDatedTransactionalObjectImpl implements MithraDatedTransactionalObject, Serializable
{

//beginTemplate main
    protected volatile MithraDataObject currentData;
    protected volatile byte persistenceState = DatedPersistenceState.IN_MEMORY;
    protected volatile byte dataVersion;
    protected int classUpdateCount;
    protected volatile DatedTransactionalState transactionalState = null;
    private static final AtomicReferenceFieldUpdater txStateUpdater = AtomicReferenceFieldUpdater.newUpdater(MithraDatedTransactionalObjectImpl.class, DatedTransactionalState.class, "transactionalState");

    protected abstract void cascadeInsertImpl();

    protected abstract void zSerializeAsOfAttributes(java.io.ObjectOutputStream out) throws IOException;

    protected abstract void zDeserializeAsOfAttributes(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException;

    protected abstract boolean checkAsOfAttributesForRefresh(MithraDataObject current);

    protected abstract void copyNonPrimaryKeyAttributesFromImpl(MithraTransactionalObject from, MithraTransaction tx);

    protected abstract MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNewImpl(MithraTransaction tx);

    protected abstract boolean issueUpdates(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData);

    protected abstract void issuePrimitiveNullSetters(DatedTransactionalBehavior behavior, MithraDataObject data, boolean mustCheckCurrent);

    protected abstract DatedTransactionalState zCreateDatedTransactionalState(TemporalContainer container, MithraDataObject data, MithraTransaction threadTx);

    protected void zResetEmbeddedValueObjects(DatedTransactionalBehavior behavior)
    {

    }

    public boolean isInMemoryNonTransactional()
    {
        return this.persistenceState == DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
    }

    public void makeInMemoryNonTransactional()
    {
        if (this.persistenceState == DatedPersistenceState.IN_MEMORY &&
            (this.transactionalState == null || this.transactionalState.getPersistenceState() == DatedPersistenceState.IN_MEMORY))
        {
            if (this.transactionalState != null)
            {
                MithraDataObject transactionalData = this.transactionalState.getTxData();
                if (transactionalData != null)
                {
                    this.currentData = transactionalData;
                }
                this.transactionalState = null;
            }
            this.persistenceState = DatedPersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        }
        else if (!this.isInMemoryNonTransactional())
        {
            throw new MithraBusinessException("Only in memory objects not in transaction can be marked as in memory non transactional");
        }
    }

    protected boolean zCheckInfiniteBusinessDate(MithraDataObject data)
    {
        return true;
    }

    protected Object zGetLock()
    {
        return this;
    }

    public MithraDataObject zGetCurrentData()
    {
        return this.currentData;
    }

    public MithraDataObject zGetNonTxData()
    {
        return this.currentData;
    }

    public void zSetData(MithraDataObject data, Object optional)
    {
        throw new RuntimeException("should never be called");
    }

    public void zClearTxData()
    {
        if (this.currentData != null)
        {
            this.transactionalState.setTxData(null);
        }
    }

    public void zDeleteForRemote(int hierarchyDepth) throws MithraBusinessException
    {
        throw new RuntimeException("not implemented");
    }

    public void zInsertForRemote(int hierarchyDepth) throws MithraBusinessException
    {
        throw new RuntimeException("not implemented");
    }

    public void zMarkDirty()
    {
    }

    public boolean zIsSameObjectWithoutAsOfAttributes(MithraTransactionalObject other)
    {
        return this == other || this.zGetTxDataForRead().hasSamePrimaryKeyIgnoringAsOfAttributes(other.zGetTxDataForRead());
    }

    public boolean zIsDataChanged(MithraDataObject data)
    {
        DatedTransactionalBehavior behavior = zGetTransactionalBehavior();
        return behavior.getCurrentDataForRead(this).changed(data);
    }

    public void zSetInserted()
    {
        // nothing to do
    }

    public void zSetUpdated(List<AttributeUpdateWrapper> updates)
    {
        // nothing to do
    }

    public void zSetDeleted()
    {
        // nothing to do
    }

    public void zApplyUpdateWrappers(List updateWrappers)
    {
        throw new RuntimeException("not implemented");
    }

    public void zApplyUpdateWrappersForBatch(List updateWrappers)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isInMemoryAndNotInserted()
    {
        return this.zGetTxBehavior().isInMemory();
    }

    public boolean isDeletedOrMarkForDeletion()
    {
        DatedTransactionalBehavior behavior = this.zGetTxBehavior();
        return behavior.isDeleted(this);
    }

    public boolean isModifiedSinceDetachment()
    {
        DatedTransactionalBehavior behavior = zGetTransactionalBehavior();
        return behavior.isModifiedSinceDetachment(this);
    }

    public boolean isModifiedSinceDetachment(Extractor extractor)
    {
        MithraTransactionalObject object = this.zFindOriginal();
        return object == null || !extractor.valueEquals(this, object);
    }

    public boolean isModifiedSinceDetachment(RelatedFinder relatedFinder)
    {
        return ((DeepRelationshipAttribute) relatedFinder).isModifiedSinceDetachment(this);
    }

    public void zHandleRollback(MithraTransaction tx)
    {
        if (transactionalState != null && transactionalState.isEnrolledForWrite(tx))
        {
            if (this.currentData == null)
            {
                this.zGetCache().rollbackObject(this);
            }
            this.transactionalState = null;
        }
    }

    public boolean zIsDetached()
    {
        int maskedPersistenceState = this.persistenceState;
        return maskedPersistenceState == DatedPersistenceState.DETACHED || maskedPersistenceState == DatedPersistenceState.DETACHED_DELETED;
    }

    public void zSetNonTxPersistenceState(int state)
    {
        this.persistenceState = (byte) state;
    }

    public void zSetTxPersistenceState(int state)
    {
        this.transactionalState.setPersistenceState(state);
        if (state == DatedPersistenceState.PERSISTED)
        {
            this.transactionalState.setContainer(this.zGetCache().getOrCreateContainer(this.zGetTxDataForRead()));
        }
    }

    public boolean zIsParticipatingInTransaction(MithraTransaction tx)
    {
        DatedTransactionalState txState = this.transactionalState;
        return txState != null && txState.isParticipatingInReadOrWrite(tx);
    }

    public boolean zEnrollInTransactionForRead(DatedTransactionalState prev, MithraTransaction threadTx, int persistenceState)
    {
        if (txStateUpdater.compareAndSet(this, prev, threadTx.getReadLockedDatedTransactionalState(prev, persistenceState)))
        {
            threadTx.enrollReadLocked(this);
            return true;
        }
        else return false;
    }

    public void zLockForTransaction()
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null) return;
        do
        {
            DatedTransactionalState txState = this.transactionalState;
            if (txState == null || txState.hasNoTransactions())
            {
                if (zEnrollInTransactionForRead(txState, threadTx, DatedPersistenceState.PERSISTED))
                {
                    break;
                }
            }
            else
            {
                if (txState.isParticipatingInReadOrWrite(threadTx))
                {
                    break;
                }
                else if (txState.isEnrolledForWriteByOther(threadTx))
                {
                    throw new MithraTransactionException("must wait for other transaction", txState.getExculsiveWriteTransaction());
                }
                else if (txState.isSharedReaderByOthers(threadTx))
                {
                    if (zEnrollInTransactionForRead(txState, threadTx, DatedPersistenceState.PERSISTED))
                    {
                        break;
                    }
                }
            }
        }
        while (true);
    }

    public void purge() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.purge(this);
    }

    public MithraDataObject zGetTxDataForRead()
    {
        MithraDataObject data = null;
        DatedTransactionalState txState = this.transactionalState;
        if (txState != null)
        {
            data = txState.getTxData();
        }

        if (data == null)
        {
            data = this.currentData;
        }
        if (data == null)
        {
            data = this.zAllocateData();
            this.currentData = data;
        }
        return data;
    }

    public MithraDataObject zGetTxDataForWrite()
    {
        DatedTransactionalState txState = this.transactionalState;
        MithraDataObject data = txState.getTxData();
        if (data == null)
        {
            if (this.currentData == null)
            {
                data = this.zAllocateData();
            }
            else data = this.currentData.copy();
            txState.setTxData(data);
        }
        return data;
    }

    public MithraDataObject zUnsynchronizedGetData()
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetTxData(MithraDataObject newData)
    {
        this.transactionalState.setTxData(newData);
    }

    public void zSetNonTxData(MithraDataObject newData)
    {
        this.zSetCurrentData(newData);
    }

    public void zRefreshWithLock(TransactionalBehavior behavior)
    {
        throw new RuntimeException("not implemented");
    }

    public void zRefreshWithLockForRead(TransactionalBehavior persistedTxEnrollBehavior)
    {
        throw new RuntimeException("not implemented");
    }

    public void zRefreshWithLockForWrite(TransactionalBehavior persistedTxEnrollBehavior)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zGetCurrentOrTransactionalData()
    {
        MithraDataObject result = null;
        if (this.persistenceState != DatedPersistenceState.IN_MEMORY)
        {
            result = this.currentData;
        }
        if (result == null)
        {
            MithraTransaction threadTx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
            DatedTransactionalState txState = this.transactionalState;
            if (threadTx != null && txState != null && txState.isEnrolledForWrite(threadTx))
            {
                result = txState.getTxDataWithNoCheck();
            }
            if (result == null)
            {
                result = this.currentData;
            }
        }
        return result;
    }

    public void terminate() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.terminate(this);
    }

    public DatedTransactionalBehavior zGetTxBehavior()
    {
        return zGetTransactionalBehavior();
    }

    protected DatedTransactionalBehavior zGetTransactionalBehavior()
    {
        if (this.persistenceState == DatedPersistenceState.PERSISTED_NON_TRANSACTIONAL) return AbstractDatedTransactionalBehavior.getPersistedNonTransactionalBehavior();
        DatedTransactionalBehavior behavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getDatedTransactionalBehavior(this, this.transactionalState, this.persistenceState);
        if (behavior == null)
        {
            do
            {
                behavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getDatedTransactionalBehavior(this, this.transactionalState, this.persistenceState);
            }
            while (behavior == null);
        }
        return behavior;
    }

    protected DatedTransactionalBehavior zGetTransactionalBehaviorForWrite()
    {
        DatedTransactionalBehavior behavior = DatedPersistenceState.getTransactionalBehaviorForWrite(this, this.transactionalState, this.persistenceState);
        if (behavior == null)
        {
            do
            {
                behavior = DatedPersistenceState.getTransactionalBehaviorForWrite(this, this.transactionalState, this.persistenceState);
            }
            while (behavior == null);
        }
        return behavior;
    }

    protected DatedTransactionalBehavior zGetTransactionalBehaviorForDelete()
    {
        DatedTransactionalBehavior behavior = DatedPersistenceState.getTransactionalBehaviorForDelete(this, this.transactionalState, this.persistenceState);
        if (behavior == null)
        {
            do
            {
                behavior = DatedPersistenceState.getTransactionalBehaviorForDelete(this, this.transactionalState, this.persistenceState);
            }
            while (behavior == null);
        }
        return behavior;
    }

    public void cascadeInsert() throws MithraBusinessException
    {
        this.cascadeInsert(0);
    }

    protected void cascadeInsert(int _retryCount) throws MithraBusinessException
    {
        MithraTransaction _tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean _nested = _tx != null;
        for (_retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES - _retryCount; _retryCount > 0;)
        {
            try
            {
                if (!_nested)
                {
                    _tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    _tx.setTransactionName("cascadeInsert");
                }
                this.cascadeInsertImpl();
                if (!_nested) _tx.commit();
                break;
            }
            catch (Throwable _t)
            {
                if (_nested) MithraTransaction.handleNestedTransactionException(_tx, _t);
                _retryCount = MithraTransaction.handleTransactionException(_tx, _t, _retryCount);
            }
        }
    }

    public void zHandleCommit()
    {
        DatedTransactionalState txState = this.transactionalState;
        if (txState == null)
            return; // this can happen if the object constructor is badly coded and calls a setter method
        this.persistenceState = (byte) txState.getPersistenceState();
        this.zGetPortal().getPerClassUpdateCountHolder().commitUpdateCount();
        if (!txState.isDeleted())
        {
            MithraDataObject txData = txState.getTxData();
            if (txData != null)
            {
                if (transactionalState.isPersisted())
                {
                    txData.clearAllDirectRefs();
                    this.zClearAllDirectRefs();
                }
                this.currentData = txData;
                this.dataVersion = txData.zGetDataVersion();
            }
        }
        this.transactionalState = null;
    }

    protected void zClearAllDirectRefs()
    {
        // subclass to override
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException
    {
        DatedTransactionalBehavior behavior = zGetTransactionalBehavior();
        MithraDataObject dataToWrite = behavior.getCurrentDataForReadEvenIfDeleted(this);

        byte state = behavior.getPersistenceState();
        out.writeByte(state);
        if (state == DatedPersistenceState.IN_MEMORY || state == DatedPersistenceState.DETACHED)
        {
            dataToWrite.zSerializeFullData(out);
            dataToWrite.zSerializeRelationships(out);
        }
        else
        {
            dataToWrite.zSerializePrimaryKey(out);
        }
        zSerializeAsOfAttributes(out);
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        byte persistenceState = in.readByte();
        this.currentData = zAllocateData();
        boolean fullData = persistenceState == DatedPersistenceState.IN_MEMORY || persistenceState == DatedPersistenceState.DETACHED;
        if (fullData)
        {
            this.currentData.zDeserializeFullData(in);
            this.currentData.zDeserializeRelationships(in);
        }
        else
        {
            this.currentData.zDeserializePrimaryKey(in);
        }
        this.persistenceState = persistenceState;
        zDeserializeAsOfAttributes(in);
    }

    public void resetFromOriginalPersistentObject()
    {
        MithraDatedTransactionalObject original = (MithraDatedTransactionalObject) this.zFindOriginal();
        if (original == null) throw new MithraBusinessException("Original is deleted!");
        MithraDatedTransactionalObject copy = original.getDetachedCopy();
        DatedTransactionalBehavior behavior = zGetTransactionalBehaviorForWrite();
        behavior.resetDetachedData(this, copy.zGetCurrentData());
    }

    protected void zCheckDoubleValue(double newValue)
    {
        if (Double.isNaN(newValue))
        {
            throw new MithraBusinessException("NaN is not a valid value for a floating point attribute");
        }
        else if (Double.isInfinite(newValue))
        {
            throw new MithraBusinessException("Infinity is not a valid value for a floating point attribute");
        }
    }

    protected void zCheckFloatValue(float newValue)
    {
        if (Float.isNaN(newValue))
        {
            throw new MithraBusinessException("NaN is not a valid value for a floating point attribute");
        }
        else if (Float.isInfinite(newValue))
        {
            throw new MithraBusinessException("Infinity is not a valid value for a floating point attribute");
        }
    }

    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache)
    {
        throw new RuntimeException("should never be called");
    }

    public void delete()
    {
        throw new MithraBusinessException("Dated objects must not be deleted. The terminate method will chain out an existing object.");
    }

    public void cascadeDelete()
    {
        throw new MithraBusinessException("Dated objects must not be deleted. The terminate method will chain out an existing object.");
    }

    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other)
    {
        return nonPrimaryKeyAttributesChanged(other, 0.0);
    }

    protected void zNullify(Attribute attribute, boolean isPrimaryKey)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zCheckInfiniteBusinessDate(data) && attribute.isAttributeNull(data)) return;
        if (isPrimaryKey && !behavior.maySetPrimaryKey())
            throw new MithraBusinessException("cannot change the primary key");
        AttributeUpdateWrapper updateWrapper = attribute.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this));
        behavior.update(this, updateWrapper, isPrimaryKey && !behavior.maySetPrimaryKey(), true);
        this.zResetEmbeddedValueObjects(behavior);
    }

    protected void zNullifyUntil(Attribute attribute, boolean isPrimaryKey, Timestamp until)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zCheckInfiniteBusinessDate(data) && attribute.isAttributeNull(data)) return;
        AttributeUpdateWrapper updateWrapper = attribute.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this));
        behavior.updateUntil(this, updateWrapper, isPrimaryKey && !behavior.maySetPrimaryKey(), until, true);
        this.zResetEmbeddedValueObjects(behavior);
    }

    public MithraDataObject zGetCurrentDataWithCheck()
    {
        MithraDataObject current = this.currentData;
        boolean refresh = current.zGetDataVersion() < 0;
        if (!refresh && this.dataVersion != current.zGetDataVersion())
        {
            refresh = checkAsOfAttributesForRefresh(current);
        }
        if (refresh)
        {
            current = refreshData(current);
        }
        return current;
    }

    public boolean zIsDataDeleted()
    {
        MithraDataObject current = this.currentData;
        boolean refresh = current == null || current.zGetDataVersion() < 0;
        if (!refresh && this.dataVersion != current.zGetDataVersion())
        {
            refresh = checkAsOfAttributesForRefresh(current);
        }
        if (refresh)
        {
            current = refreshDataWithoutException(current);
        }
        return current == null;
    }

    public boolean zIsTxDataDeleted()
    {
        MithraDataObject txData = this.transactionalState.getTxDataWithNoCheck();
        if (txData == null)
        {
            return this.zIsDataDeleted();
        }
        return this.transactionalState.isTxDataDeleted();
    }

    private MithraDataObject refreshData(MithraDataObject current)
    {
        MithraDataObject newData = this.zGetCache().refreshOutsideTransaction(this, current);
        if (newData == null)
        {
            throw new MithraDeletedException("<%= wrapper.getClassName() %> has been deleted: " + current.zGetPrintablePrimaryKey());
        }
        if (this.currentData == current)
        {
            this.currentData = newData;
        }
        return newData;
    }

    private MithraDataObject refreshDataWithoutException(MithraDataObject current)
    {
        MithraDataObject newData = this.zGetCache().refreshOutsideTransaction(this, current);
        if (this.currentData == current && newData != null)
        {
            this.currentData = newData;
            this.dataVersion = newData.zGetDataVersion();
        }
        return newData;
    }

    protected MithraDataObject zSynchronizedGetData()
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehavior();
        return behavior.getCurrentDataForRead(this);
    }

    public void zSetCurrentData(MithraDataObject data)
    {
        this.currentData = data;
        this.dataVersion = data.zGetDataVersion();
    }

    public void insert() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.insert(this);
    }

    public void insertUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.insertUntil(this, exclusiveUntil);
    }

    public void insertForRecovery() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.insertForRecovery(this);
    }

    public void insertWithIncrement() throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.insertWithIncrement(this);
    }

    public void insertWithIncrementUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.insertWithIncrementUntil(this, exclusiveUntil);
    }

    public void terminateUntil(Timestamp exclusiveUntil) throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.terminateUntil(this, exclusiveUntil);
    }

    public void inactivateForArchiving(Timestamp processingDateTo, Timestamp businessDateTo) throws MithraBusinessException
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.inactivateForArchiving(this, processingDateTo, businessDateTo);
    }

    protected void zIncrement(double increment, DoubleAttribute attr, boolean readOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.increment(this, increment, readOnly, attr);
    }

    protected void zIncrement(BigDecimal increment, BigDecimalAttribute attr, boolean readOnly)
    {
        increment = com.gs.fw.common.mithra.util.BigDecimalUtil.validateBigDecimalValue(increment, attr.getPrecision(), attr.getScale());
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.increment(this, increment, readOnly, attr);
    }

    protected void zIncrementUntil(double increment, Timestamp exclusiveUntil, DoubleAttribute attr, boolean readOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.incrementUntil(this, increment, readOnly, attr, exclusiveUntil);
    }

    protected void zIncrementUntil(BigDecimal increment, Timestamp exclusiveUntil, BigDecimalAttribute attr, boolean readOnly)
    {
        increment = com.gs.fw.common.mithra.util.BigDecimalUtil.validateBigDecimalValue(increment, attr.getPrecision(), attr.getScale());
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        behavior.incrementUntil(this, increment, readOnly, attr, exclusiveUntil);
    }

    protected boolean zMustCheckCurrent(MithraDataObject data)
    {
        return true;
    }

    protected MithraDataObject zSetBigDecimal(BigDecimalAttribute attr, BigDecimal newValue, boolean isReadOnly)
    {
        newValue = com.gs.fw.common.mithra.util.BigDecimalUtil.validateBigDecimalValue(newValue, attr.getPrecision(), attr.getScale());
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        BigDecimal cur = attr.bigDecimalValueOf(data);
        if (zMustCheckCurrent(data))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetBoolean(BooleanAttribute attr, boolean newValue, boolean isReadOnly, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.booleanValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetByte(ByteAttribute attr, byte newValue, boolean isReadOnly, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.byteValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetShort(ShortAttribute attr, short newValue, boolean isReadOnly, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.shortValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetChar(CharAttribute attr, char newValue, boolean isReadOnly, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.charValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetInteger(IntegerAttribute attr, int newValue, boolean isReadOnly, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.intValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetLong(LongAttribute attr, long newValue, boolean isReadOnly, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.longValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetFloat(FloatAttribute attr, float newValue, boolean isReadOnly, boolean isNullable)
    {
        this.zCheckFloatValue(newValue);
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.floatValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetDouble(DoubleAttribute attr, double newValue, boolean isReadOnly, boolean isNullable)
    {
        this.zCheckDoubleValue(newValue);
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data) && !(isNullable && attr.isAttributeNull(data)) && attr.doubleValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetString(StringAttribute attr, String newValue, boolean isReadOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        String cur = attr.stringValueOf(data);
        if (zMustCheckCurrent(data))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetTimestamp(TimestampAttribute attr, Timestamp newValue, boolean isReadOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        Timestamp cur = attr.timestampValueOf(data);
        if (zMustCheckCurrent(data))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetDate(DateAttribute attr, Date newValue, boolean isReadOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        Date cur = attr.dateValueOf(data);
        if (zMustCheckCurrent(data))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetTime(TimeAttribute attr, Time newValue, boolean isReadOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        Time cur = attr.timeValueOf(data);
        if (zMustCheckCurrent(data))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected MithraDataObject zSetByteArray(ByteArrayAttribute attr, byte[] newValue, boolean isReadOnly)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data))
        {
            byte[] cur = attr.byteArrayValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (Arrays.equals(cur, newValue)) return null;
            }
        }
        return behavior.update(this, attr, newValue, isReadOnly, true);
    }

    protected boolean zMustCheckCurrent(MithraDataObject data, Timestamp exclusiveUntil)
    {
        return true;
    }

    protected MithraDataObject zSetBoolean(BooleanAttribute attr, boolean newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.booleanValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetByte(ByteAttribute attr, byte newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.byteValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetShort(ShortAttribute attr, short newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.shortValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetChar(CharAttribute attr, char newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.charValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetInteger(IntegerAttribute attr, int newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.intValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetLong(LongAttribute attr, long newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.longValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetFloat(FloatAttribute attr, float newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        this.zCheckFloatValue(newValue);
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.floatValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetDouble(DoubleAttribute attr, double newValue, boolean isReadOnly, Timestamp exclusiveUntil, boolean isNullable)
    {
        this.zCheckDoubleValue(newValue);
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil) && !(isNullable && attr.isAttributeNull(data)) && attr.doubleValueOf(data) == newValue)
            return null;
        zResetEmbeddedValueObjects(behavior);
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetBigDecimal(BigDecimalAttribute attr, BigDecimal newValue, boolean isReadOnly, Timestamp exclusiveUntil)
    {
        newValue = com.gs.fw.common.mithra.util.BigDecimalUtil.validateBigDecimalValue(newValue, attr.getPrecision(), attr.getScale());
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        BigDecimal cur = attr.bigDecimalValueOf(data);
        if (zMustCheckCurrent(data, exclusiveUntil))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetString(StringAttribute attr, String newValue, boolean isReadOnly, Timestamp exclusiveUntil)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        String cur = attr.stringValueOf(data);
        if (zMustCheckCurrent(data, exclusiveUntil))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetTimestamp(TimestampAttribute attr, Timestamp newValue, boolean isReadOnly, Timestamp exclusiveUntil)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        Timestamp cur = attr.timestampValueOf(data);
        if (zMustCheckCurrent(data, exclusiveUntil))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetDate(DateAttribute attr, Date newValue, boolean isReadOnly, Timestamp exclusiveUntil)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        Date cur = attr.dateValueOf(data);
        if (zMustCheckCurrent(data, exclusiveUntil))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetTime(TimeAttribute attr, Time newValue, boolean isReadOnly, Timestamp exclusiveUntil)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        Time cur = attr.timeValueOf(data);
        if (zMustCheckCurrent(data, exclusiveUntil))
        {
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
        }
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    protected MithraDataObject zSetByteArray(ByteArrayAttribute attr, byte[] newValue, boolean isReadOnly, Timestamp exclusiveUntil)
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (zMustCheckCurrent(data, exclusiveUntil))
        {
            byte[] cur = attr.byteArrayValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (Arrays.equals(cur, newValue)) return null;
            }
        }
        return behavior.updateUntil(this, attr, newValue, exclusiveUntil, isReadOnly, true);
    }

    private int issueUpdate(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, Attribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return 0;
        if (attr.isAttributeNull(newData))
        {
            behavior.update(this, attr.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this)), readOnly, false);
            return 2;
        }
        return 1;
    }

    protected boolean zUpdateBoolean(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, BooleanAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.booleanValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateByte(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ByteAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.byteValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateShort(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ShortAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.shortValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateChar(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, CharAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.charValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateInteger(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, IntegerAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.intValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateLong(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, LongAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.longValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateDouble(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, DoubleAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.doubleValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateBigDecimal(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, BigDecimalAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.bigDecimalValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateFloat(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, FloatAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.floatValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateString(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, StringAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.stringValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateDate(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, DateAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.dateValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateTime(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, TimeAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.timeValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateTimestamp(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, TimestampAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.timestampValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateByteArray(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ByteArrayAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.byteArrayValueOf(newData), readOnly, false);
        return true;
    }

    private int issueUpdate(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, Attribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        if (attr.valueEquals(data, newData)) return 0;
        if (attr.isAttributeNull(newData))
        {
            behavior.updateUntil(this, attr.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this)), readOnly, exclusiveUntil, false);
            return 2;
        }
        return 1;
    }

    protected boolean zUpdateBoolean(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, BooleanAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.booleanValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateByte(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ByteAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.byteValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateShort(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ShortAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.shortValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateChar(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, CharAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.charValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateInteger(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, IntegerAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.intValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateLong(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, LongAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.longValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateDouble(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, DoubleAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.doubleValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateBigDecimal(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, BigDecimalAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.bigDecimalValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateFloat(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, FloatAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly, exclusiveUntil);
        if (issue == 1)
        {
            behavior.updateUntil(this, attr, attr.floatValueOf(newData), exclusiveUntil, readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateString(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, StringAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.updateUntil(this, attr, attr.stringValueOf(newData), exclusiveUntil, readOnly, false);
        return true;
    }

    protected boolean zUpdateDate(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, DateAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.updateUntil(this, attr, attr.dateValueOf(newData), exclusiveUntil, readOnly, false);
        return true;
    }

    protected boolean zUpdateTime(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, TimeAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.updateUntil(this, attr, attr.timeValueOf(newData), exclusiveUntil, readOnly, false);
        return true;
    }

    protected boolean zUpdateTimestamp(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, TimestampAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.updateUntil(this, attr, attr.timestampValueOf(newData), exclusiveUntil, readOnly, false);
        return true;
    }

    protected boolean zUpdateByteArray(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ByteArrayAttribute attr, boolean readOnly, Timestamp exclusiveUntil)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.updateUntil(this, attr, attr.byteArrayValueOf(newData), exclusiveUntil, readOnly, false);
        return true;
    }

    public void copyNonPrimaryKeyAttributesFrom(MithraTransactionalObject from)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        for (int retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName("copyNonPrimaryKeyAttributesFrom");
                }

                this.copyNonPrimaryKeyAttributesFromImpl(from, tx);
                if (!nested) tx.commit();
                break;
            }
            catch (Throwable t)
            {
                if (nested) MithraTransaction.handleNestedTransactionException(tx, t);
                retryCount = MithraTransaction.handleTransactionException(tx, t, retryCount);
            }
        }
    }

    protected MithraDatedTransactionalObject zCopyDetachedValuesToOriginalOrInsertIfNew()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        MithraDatedTransactionalObject persisted = null;
        for (int retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName("copyDetachedValuesToOriginalOrInsertIfNew");
                }

                persisted = this.copyDetachedValuesToOriginalOrInsertIfNewImpl(tx);
                if (!nested) tx.commit();
                break;
            }
            catch (Throwable t)
            {
                if (nested) MithraTransaction.handleNestedTransactionException(tx, t);
                retryCount = MithraTransaction.handleTransactionException(tx, t, retryCount);
            }
        }
        return persisted;
    }

    public void zCopyAttributesFrom(MithraDataObject data)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        for (int retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName("zCopyAttributesFrom");
                }

                this.zCopyAttributesFromImpl(data, tx);
                if (!nested) tx.commit();
                break;
            }
            catch (Throwable t)
            {
                if (nested) MithraTransaction.handleNestedTransactionException(tx, t);
                retryCount = MithraTransaction.handleTransactionException(tx, t, retryCount);
            }
        }
    }

    public void zPersistDetachedChildDelete(MithraDataObject currentDataForRead)
    {
    }

    protected void zCopyAttributesFromImpl(MithraDataObject newData, MithraTransaction tx)
    {
        DatedTransactionalBehavior behavior = zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        this.zCheckOptimisticLocking(tx, data, newData);
        if (this.issueUpdates(behavior, data, newData))
        {
            this.triggerUpdateHookAfterCopy();
        }
        this.zResetEmbeddedValueObjects(behavior);
    }

    protected void zCheckOptimisticLocking(MithraTransaction tx, MithraDataObject data, MithraDataObject newData)
    {

    }

    public void copyNonPrimaryKeyAttributesUntilFrom(MithraDatedTransactionalObject from, Timestamp until)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        for (int retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName("copyNonPrimaryKeyAttributesUntilFrom");
                }

                this.copyNonPrimaryKeyAttributesUntilFromImpl(from, until, tx);
                if (!nested) tx.commit();
                break;
            }
            catch (Throwable t)
            {
                if (nested) MithraTransaction.handleNestedTransactionException(tx, t);
                retryCount = MithraTransaction.handleTransactionException(tx, t, retryCount);
            }
        }
    }

    protected void copyNonPrimaryKeyAttributesUntilFromImpl(MithraDatedTransactionalObject from, Timestamp until, MithraTransaction tx)
    {
        throw new MithraBusinessException("Until requires a business date as of attribute");
    }

    @Override
    public MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNewUntil(Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("copyDetachedValuesToOriginalOrInsertIfNewUntil is only supported for objects with a business date");
    }

    protected MithraDatedTransactionalObject zCopyDetachedValuesToOriginalOrInsertIfNewUntil(Timestamp until)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        MithraDatedTransactionalObject liveOriginal = null;
        for (int retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName("TinyBalance.copyDetachedValuesToOriginalOrInsertIfNewUntil");
                }

                liveOriginal = this.copyDetachedValuesToOriginalOrInsertIfNewUntilImpl(until, tx);
                if (!nested) tx.commit();
                break;
            }
            catch (Throwable t)
            {
                if (nested) MithraTransaction.handleNestedTransactionException(tx, t);
                retryCount = MithraTransaction.handleTransactionException(tx, t, retryCount);
            }
        }
        return liveOriginal;
    }

    protected MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNewUntilImpl(Timestamp until, MithraTransaction tx)
    {
        throw new MithraBusinessException("Until requires a business date as of attribute");
    }

    public void zCopyAttributesUntilFrom(MithraDataObject data, Timestamp until)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        for (int retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName("zCopyAttributesUntilFrom");
                }

                this.zCopyAttributesUntilFromImpl(data, until, tx);
                if (!nested) tx.commit();
                break;
            }
            catch (Throwable t)
            {
                if (nested) MithraTransaction.handleNestedTransactionException(tx, t);
                retryCount = MithraTransaction.handleTransactionException(tx, t, retryCount);
            }
        }
    }

    protected void zCopyAttributesUntilFromImpl(MithraDataObject newData, Timestamp until, MithraTransaction tx)
    {
        DatedTransactionalBehavior behavior = zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        this.zCheckOptimisticLocking(tx, data, newData);
        if (this.issueUpdatesUntil(behavior, data, newData, until))
        {
            this.triggerUpdateHookAfterCopy();
        }
        this.zResetEmbeddedValueObjects(behavior);
    }

    protected boolean issueUpdatesUntil(DatedTransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("Until requires a business date as of attribute");
    }

    public void zInsertRelationshipsUntil(MithraDataObject data, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("Until requires a business date as of attribute");
    }

    public void zPersistDetachedRelationshipsUntil(MithraDataObject data, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("Until requires a business date as of attribute");
    }

    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other, double toleranceForFloatingPointFields)
    {
        MithraDataObject otherData = ((MithraDatedTransactionalObjectImpl) other).zSynchronizedGetData();
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehavior();
        return behavior.getCurrentDataForRead(this).zNonPrimaryKeyAttributesChanged(otherData, toleranceForFloatingPointFields);
    }

    public void setNullablePrimitiveAttributesToNull()
    {
        DatedTransactionalBehavior behavior = this.zGetTransactionalBehaviorForWrite();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        boolean mustCheckCurrent = this.zMustCheckCurrent(data);
        issuePrimitiveNullSetters(behavior, data, mustCheckCurrent);
        zResetEmbeddedValueObjects(behavior);
    }

    protected void zNullify(DatedTransactionalBehavior behavior, MithraDataObject data, Attribute attr, boolean readOnly, boolean mustCheckCurrent)
    {
        if (!(mustCheckCurrent && attr.isAttributeNull(data)))
        {
            behavior.update(this, attr.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this)), readOnly, true);
        }
    }

    public void zClearUnusedTransactionalState(DatedTransactionalState prevState)
    {
        txStateUpdater.compareAndSet(this, prevState, null);
    }

    public void zEnrollInTransactionForWrite(TemporalContainer container, MithraDataObject data, MithraTransaction threadTx)
    {
        do
        {
            DatedTransactionalState txState = this.transactionalState;
            if (txState == null || txState.hasNoTransactions() || txState.isOnlyReader(threadTx))
            {
                DatedTransactionalState newState = zCreateDatedTransactionalState(container, data, threadTx);
                if (txStateUpdater.compareAndSet(this, txState, newState))
                {
                    threadTx.enrollObject(this, zGetCache());
                    break;
                }
            }
            else
            {
                if (txState.isEnrolledForWrite(threadTx)) return;
                if (txState.isEnrolledForWriteByOther(threadTx))
                {
                    throw new MithraTransactionException("must wait for other transaction", txState.getExculsiveWriteTransaction());
                }
                // the only choice left, is for this object to be in a read state by other threads
                txState.waitForTransactions(threadTx);
            }
        }
        while (true);
    }

    public boolean zEnrollInTransactionForWrite(DatedTransactionalState prevState, TemporalContainer container, MithraDataObject data, MithraTransaction threadTx)
    {
        DatedTransactionalState newState = zCreateDatedTransactionalState(container, data, threadTx);
        if (txStateUpdater.compareAndSet(this, prevState, newState))
        {
            threadTx.enrollObject(this, zGetCache());
            return true;
        }
        return false;
    }

    public void zEnrollInTransaction()
    {
        throw new RuntimeException("concon");
    }

    public boolean zEnrollInTransactionForRead(TransactionalState prev, MithraTransaction threadTx, int persistenceState)
    {
        throw new RuntimeException("not implemented");
    }

    public void zClearTempTransaction()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForDelete(TransactionalState prevState, TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForWrite(TransactionalState prev, TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public void zClearUnusedTransactionalState(TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public void zWaitForExclusiveWriteTx(MithraTransaction tx)
    {
        DatedTransactionalState txState = this.transactionalState;
        if (txState != null)
        {
            txState.waitForWriteTransaction(tx);
        }
    }

    public void zPrepareForDelete()
    {
        this.zGetTransactionalBehaviorForDelete();
    }

    public void zPrepareForRemoteInsert()
    {
        throw new RuntimeException("not implemented");
    }

    protected void zSetData(MithraDataObject data)
    {
        this.currentData = data;
        this.dataVersion = data.zGetDataVersion();
        if (this.transactionalState != null) this.transactionalState.setTxData(null);
    }

    public void triggerUpdateHook(UpdateInfo updateInfo)
    {
        //nothing to do by default. subclass may override
    }

    public void zCascadeUpdateInPlaceBeforeTerminate(MithraDataObject detachedData)
    {
        //nothing to do by default. subclass may override
    }

    public void zCascadeUpdateInPlaceBeforeTerminate()
    {
        //nothing to do by default. subclass may override
    }

    public void triggerUpdateHookAfterCopy()
    {
        //nothing to do by default. subclass may override
    }
//endTemplate
}
