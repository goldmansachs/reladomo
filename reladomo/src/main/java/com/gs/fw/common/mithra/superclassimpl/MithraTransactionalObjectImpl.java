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
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
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
import com.gs.fw.common.mithra.behavior.AbstractTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.behavior.state.PersistenceState;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.transaction.MithraTempTransaction;
import com.gs.fw.common.mithra.util.Time;


public abstract class MithraTransactionalObjectImpl implements MithraTransactionalObject, Serializable
{

//beginTemplate main
    protected volatile byte persistenceState = PersistenceState.IN_MEMORY;
    protected int classUpdateCount = 0;
    protected volatile MithraDataObject currentData;
    protected volatile TransactionalState transactionalState = null;
    private static final AtomicReferenceFieldUpdater txStateUpdater = AtomicReferenceFieldUpdater.newUpdater(MithraTransactionalObjectImpl.class, TransactionalState.class, "transactionalState");

    protected abstract void cascadeInsertImpl();

    protected abstract void cascadeDeleteImpl();

    protected abstract boolean issueUpdatesForNonPrimaryKeys(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData);

    protected abstract boolean issueUpdatesForPrimaryKeys(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData);

    protected abstract void issuePrimitiveNullSetters(TransactionalBehavior behavior, MithraDataObject data);

    protected void zResetEmbeddedValueObjects(TransactionalBehavior behavior)
    {

    }

    protected void zIncrementOptimiticAttribute(TransactionalBehavior behavior, MithraDataObject data)
    {

    }

    public MithraTransactionalObjectImpl getNonPersistentCopy() throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        MithraDataObject newData = data.copy(!behavior.isPersisted());
        MithraTransactionalObjectImpl result = (MithraTransactionalObjectImpl)
                this.zGetPortal().getMithraObjectFactory().createObject(newData);
        result.persistenceState = PersistenceState.IN_MEMORY;
        result.transactionalState = null;
        return result;
    }

    public boolean isInMemoryNonTransactional()
    {
        return this.persistenceState == PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
    }


    public void makeInMemoryNonTransactional()
    {
        if (this.persistenceState == PersistenceState.IN_MEMORY &&
            (this.transactionalState == null || this.transactionalState.getPersistenceState() == PersistenceState.IN_MEMORY))
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
            this.persistenceState = PersistenceState.IN_MEMORY_NON_TRANSACTIONAL;
        }
        else if (!this.isInMemoryNonTransactional())
        {
            throw new MithraBusinessException("Only in memory objects not in transaction can be marked as in memory non transactional");
        }
    }

    public MithraTransactionalObjectImpl getDetachedCopy() throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        MithraTransactionalObjectImpl result = (MithraTransactionalObjectImpl) behavior.getDetachedCopy(this);
        result.transactionalState = null;
        return result;
    }

    protected MithraDataObject zCheckOptimisticDirty(MithraDataObject data, TransactionalBehavior behavior)
    {
        return data;
    }

    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other, double toleranceForFloatingPointFields)
    {
        MithraDataObject otherData = ((MithraTransactionalObjectImpl) other).zSynchronizedGetData();
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        return behavior.getCurrentDataForRead(this).zNonPrimaryKeyAttributesChanged(otherData, toleranceForFloatingPointFields);
    }

    protected Object zGetLock()
    {
        return this;
    }

    protected TransactionalBehavior zGetTransactionalBehaviorForReadWithWaitIfNecessary()
    {
        if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL) return AbstractTransactionalBehavior.getPersistedNonTransactionalBehavior();
        TransactionalBehavior readBehavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getTransactionalBehaviorForReadWithWaitIfNecessary(this, this.transactionalState, this.persistenceState);
        while (readBehavior == null)
        {
            readBehavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getTransactionalBehaviorForReadWithWaitIfNecessary(this, this.transactionalState, this.persistenceState);
        }
        return readBehavior;
    }

    protected TransactionalBehavior zGetTransactionalBehaviorForWriteWithWaitIfNecessary()
    {
        if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL) return AbstractTransactionalBehavior.getPersistedNonTransactionalBehavior();
        TransactionalBehavior writeBehavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getTransactionalBehaviorForWriteWithWaitIfNecessary(this, this.transactionalState, this.persistenceState);
        while (writeBehavior == null)
        {
            writeBehavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getTransactionalBehaviorForWriteWithWaitIfNecessary(this, this.transactionalState, this.persistenceState);
        }
        return writeBehavior;
    }

    protected TransactionalBehavior zGetTransactionalBehaviorForDeleteWithWaitIfNecessary()
    {
        if (this.persistenceState == PersistenceState.PERSISTED_NON_TRANSACTIONAL) return AbstractTransactionalBehavior.getPersistedNonTransactionalBehavior();
        TransactionalBehavior writeBehavior =
                PersistenceState.getTransactionalBehaviorForTransactionForDeleteWithWaitIfNecessary(MithraManagerProvider.getMithraManager().getCurrentTransaction(),
                                                                                                    this, this.transactionalState, this.persistenceState);
        while (writeBehavior == null)
        {
            writeBehavior = MithraManagerProvider.getMithraManager().zGetTransactionalBehaviorChooser().getTransactionalBehaviorForWriteWithWaitIfNecessary(this, this.transactionalState, this.persistenceState);
        }
        return writeBehavior;
    }

    public void zClearUnusedTransactionalState(TransactionalState prev)
    {
        this.txStateUpdater.compareAndSet(this, prev, null);
    }

    public boolean zEnrollInTransactionForWrite(TransactionalState prev, TransactionalState transactionalState)
    {
        MithraDataObject data;
        if (this.currentData == null)
        {
            data = this.zAllocateData();
        }
        else data = this.currentData.copy();
        transactionalState.setTxData(data);
        if (txStateUpdater.compareAndSet(this, prev, transactionalState))
        {
            transactionalState.getExculsiveWriteTransaction().enrollObject(this, zGetCache());
            return true;
        }
        return false;
    }

    public boolean zEnrollInTransactionForDelete(TransactionalState prev, TransactionalState transactionalState)
    {
        transactionalState.setTxData(this.currentData);
        if (txStateUpdater.compareAndSet(this, prev, transactionalState))
        {
            transactionalState.getExculsiveWriteTransaction().enrollObject(this, zGetCache());
            return true;
        }
        return false;
    }

    public boolean zEnrollInTransactionForRead(TransactionalState prev, MithraTransaction threadTx, int persistenceState)
    {
        if (threadTx == null) return true;
        if (txStateUpdater.compareAndSet(this, prev, threadTx.getReadLockedTransactionalState(prev, persistenceState)))
        {
            threadTx.enrollReadLocked(this);
            return true;
        }
        else return false;
    }

    public void zPrepareForRemoteInsert()
    {
        this.persistenceState = PersistenceState.IN_MEMORY;
        this.transactionalState = null;
        TransactionalBehavior behavior = this.zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.clearTempTransaction(this);
    }

    public void zPrepareForDelete()
    {
        zGetTransactionalBehaviorForDeleteWithWaitIfNecessary();
    }

    public void zWaitForExclusiveWriteTx(MithraTransaction tx)
    {
        TransactionalState txState = this.transactionalState;
        if (txState != null) txState.waitForWriteTransaction(tx);
    }

    public void zSetData(MithraDataObject data, Object optionalBehavior)
    {
        TransactionalBehavior behavior = (TransactionalBehavior) optionalBehavior;
        behavior.setData(this, data);
    }

    public void zHandleCommit()
    {
        TransactionalState txState = transactionalState;
        if (txState == null)
            return; // this can happen if the object constructor is badly coded and calls setter methods
        int oldPersistenceState = this.persistenceState;
        this.persistenceState = (byte) this.transactionalState.getPersistenceState();
        if (txState.isDeleted())
        {
            if (this.currentData != null)
            {
                this.zGetCache().commitRemovedObject(this.currentData);
            }
        }
        else
        {
            MithraDataObject txData = this.transactionalState.getTxData();
            if (txData != null)
            {
                MithraDataObject oldData = this.currentData;
                if (this.transactionalState.isPersisted())
                {
                    this.zGetCache().commitObject(this, oldData);
                }
                else
                {
                    this.currentData = txData;
                }
            }
            else if (this.transactionalState.isInserted(oldPersistenceState))
            {
                this.zGetCache().commitObject(this, null);
            }
        }

        this.transactionalState = null;
    }

    public void zHandleRollback(MithraTransaction tx)
    {
        TransactionalState txState = this.transactionalState;
        if (txState != null && txState.isEnrolledForWrite(tx))
        {
            this.transactionalState = null;
        }
    }

    public void zClearTempTransaction()
    {
        TransactionalState txState = this.transactionalState;
        if (txState != null && txState.getExculsiveWriteTransaction() instanceof MithraTempTransaction)
        {
            this.transactionalState = null;
        }
    }

    public MithraDataObject zGetCurrentData()
    {
        return this.currentData;
    }

    //todo: concon: rename to zLockForTransaction when dated objects have the same behavior
    public void zLockForTransaction()
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null) return;
        do
        {
            TransactionalState txState = this.transactionalState;
            if (txState == null || txState.hasNoTransactions())
            {
                if (zEnrollInTransactionForRead(txState, threadTx, PersistenceState.PERSISTED))
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
                    if (zEnrollInTransactionForRead(txState, threadTx, PersistenceState.PERSISTED))
                    {
                        break;
                    }
                }
            }
        }
        while (true);
    }

    public void delete() throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.delete(this);
    }

    public void zDeleteForRemote(int hierarchyDepth) throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.deleteForRemote(this, hierarchyDepth);
    }

    public void zInsertForRemote(int hierarchyDepth) throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.insertForRemote(this, hierarchyDepth);
    }

    public void insertForRecovery() throws MithraBusinessException
    {
        this.insert();
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
                    _tx.setTransactionName("Order.cascadeInsert");
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

    public void cascadeDelete() throws MithraBusinessException
    {
        this.cascadeDelete(0);
    }

    protected void cascadeDelete(int _retryCount) throws MithraBusinessException
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
                    _tx.setTransactionName("Order.cascadeInsert");
                }

                this.cascadeDeleteImpl();
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

    public void zSetNonTxPersistenceState(int state)
    {
        this.persistenceState = (byte) state;
    }

    public void zSetTxPersistenceState(int state)
    {
        TransactionalState txState = this.transactionalState;
        if (txState != null) txState.setPersistenceState(state);
    }

    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other)
    {
        return nonPrimaryKeyAttributesChanged(other, 0.0);
    }

    public MithraDataObject zUnsynchronizedGetData()
    {
        TransactionalState txState = this.transactionalState;
        if (txState == null || txState.getTxData() == null)
        {
            return this.currentData;
        }
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null)
        {
            return this.currentData;
        }
        if (txState.isEnrolledForWrite(threadTx)) return txState.getTxData();
        return this.currentData;
    }

    public MithraDataObject zGetNonTxData()
    {
        return this.currentData;
    }

    public MithraDataObject zGetTxDataForRead()
    {
        MithraDataObject data = null;
        if (this.transactionalState != null)
        {
            data = this.transactionalState.getTxData();
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
        MithraDataObject data = this.transactionalState.getTxData();
        if (data == null)
        {
            if (this.currentData == null)
            {
                data = this.zAllocateData();
            }
            else data = this.currentData.copy();
            this.transactionalState.setTxData(data);
        }

        return data;
    }

    public void zSetTxData(MithraDataObject newData)
    {
        this.transactionalState.setTxData(newData);
    }

    public void zSetNonTxData(MithraDataObject newData)
    {
        this.currentData = newData;
    }

    public boolean zIsParticipatingInTransaction(MithraTransaction tx)
    {
        TransactionalState txState = this.transactionalState;
        return txState != null && txState.isParticipatingInReadOrWrite(tx);
    }

    //todo: concon: remove after dated objects have been converted
    public MithraTransaction zGetCurrentTransaction()
    {
        throw new RuntimeException("concon");
    }

    public boolean zIsDetached()
    {
        int maskedPersistenceState = this.persistenceState;
        return maskedPersistenceState == PersistenceState.DETACHED || maskedPersistenceState == PersistenceState.DETACHED_DELETED;
    }

    public Object readResolve() throws ObjectStreamException
    {
        if (this.persistenceState == PersistenceState.PERSISTED)
        {
            Cache cache = this.zGetCache();
            MithraTransactionalObject mithraObject = (MithraTransactionalObject) cache.getObjectByPrimaryKey(this.currentData, false);
            if (mithraObject == null)
            {
                MithraDataObject txData = this.zGetPortal().refresh(this.currentData, false);
                if (txData == null)
                {
                    throw new MithraDeletedException(this.getClass().getName() + " " + this.currentData.zGetPrintablePrimaryKey() + " has been deleted.");
                }
                mithraObject = (MithraTransactionalObject) cache.getObjectFromData(txData);
            }

            return mithraObject;
        }

        return this;
    }

    public boolean zIsSameObjectWithoutAsOfAttributes(MithraTransactionalObject other)
    {
        return this == other;
    }

    public void zSetDeleted()
    {
        // nothing to do
    }

    public boolean zIsDataChanged(MithraDataObject data)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        return behavior.getCurrentDataForRead(this).changed(data);
    }

    public boolean isModifiedSinceDetachment()
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
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

    public boolean isInMemoryAndNotInserted()
    {
        return this.zGetTransactionalBehaviorForReadWithWaitIfNecessary().isInMemory();
    }

    public boolean isDeletedOrMarkForDeletion()
    {
        return this.zGetTransactionalBehaviorForReadWithWaitIfNecessary().isDeleted();
    }

    public void zSerializeFullData(ObjectOutput out) throws IOException
    {
        currentData.zSerializeFullData(out);
    }

    public void zSerializeFullTxData(ObjectOutput out) throws IOException
    {
        this.zGetTxDataForRead().zSerializeFullData(out);
    }

    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache)
    {
        MithraTransaction threadTx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (threadTx == null)
        {
            TransactionalState txState = this.transactionalState;
            MithraDataObject cur = this.currentData;
            if (cur == null)
            {
                cur = txState.getTxData();
            }
            if (cur.changed(data))
            {
                synchronized (zGetLock())
                {
                    cache.reindex(this, data, AbstractTransactionalBehavior.getPersistedNoTxBehavior(), cur);
                }
            }
        }
        else
        {
            do
            {
                TransactionalState txState = this.transactionalState;
                if (txState == null || txState.hasNoTransactions())
                {
                    if (!this.zGetPortal().getTxParticipationMode(threadTx).mustParticipateInTxOnRead()
                        || zEnrollInTransactionForRead(txState, threadTx, PersistenceState.PERSISTED))
                    {
                        if (this.currentData.changed(data))
                        {
                            synchronized (zGetLock())
                            {
                                cache.reindex(this, data, AbstractTransactionalBehavior.getPersistedNoTxBehavior(), this.currentData);
                            }
                        }
                        break;
                    }
                }
                else
                {
                    if (txState.isEnrolledForWrite(threadTx))
                    {
                        if (txState.getTxData().changed(data))
                        {
                            synchronized (zGetLock())
                            {
                                cache.reindex(this, data, AbstractTransactionalBehavior.getPersistedSameTxBehavior(), txState.getTxData());
                            }
                        }
                        break;
                    }
                    else if (txState.isParticipatingInReadOrWrite(threadTx)) //must be read
                    {
                        if (this.currentData.changed(data))
                        {
                            if (zEnrollInTransactionForWrite(txState, new TransactionalState(threadTx, PersistenceState.PERSISTED)))
                            {
                                synchronized (zGetLock())
                                {
                                    cache.reindex(this, data, AbstractTransactionalBehavior.getPersistedSameTxBehavior(), txState.getTxData());
                                }
                                break;
                            }
                        }
                        else break;
                    }
                    else if (txState.isEnrolledForWriteByOther(threadTx))
                    {
                        throw new MithraTransactionException("must wait for other transaction", txState.getExculsiveWriteTransaction());
                    }
                    else if (txState.isSharedReaderByOthers(threadTx))
                    {
                        if (this.currentData.changed(data))
                        {
                            if (zEnrollInTransactionForWrite(txState, new TransactionalState(threadTx, PersistenceState.PERSISTED)))
                            {
                                synchronized (zGetLock())
                                {
                                    cache.reindex(this, data, AbstractTransactionalBehavior.getPersistedSameTxBehavior(), txState.getTxData());
                                }
                                break;
                            }
                        }
                        else
                        {
                            if (!this.zGetPortal().getTxParticipationMode(threadTx).mustParticipateInTxOnRead()
                                || zEnrollInTransactionForRead(txState, threadTx, PersistenceState.PERSISTED))
                            {
                                break;
                            }
                        }
                    }
                }
            }
            while (true);
        }
    }


    //todo: concon: remove after dated objects have been converted
    public void zEnrollInTransaction()
    {
        throw new RuntimeException("should not get here");
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        MithraDataObject dataToWrite = behavior.getCurrentDataForReadEvenIfDeleted(this);

        byte state = behavior.getPersistenceState();
        out.writeByte(state);
        if (state == PersistenceState.IN_MEMORY || state == PersistenceState.DETACHED)
        {
            dataToWrite.zSerializeFullData(out);
            dataToWrite.zSerializeRelationships(out);
        }
        else
        {
            dataToWrite.zSerializePrimaryKey(out);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        byte persistenceState = in.readByte();
        this.currentData = zAllocateData();
        boolean fullData = persistenceState == PersistenceState.IN_MEMORY || persistenceState == PersistenceState.DETACHED;
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

    protected void zNullify(Attribute attribute, boolean isReadOnly)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (attribute.isAttributeNull(data)) return;
            AttributeUpdateWrapper updateWrapper = attribute.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this));
            behavior.update(this, updateWrapper, isReadOnly, true);
            this.zResetEmbeddedValueObjects(behavior);
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSynchronizedGetDataForPrimaryKey()
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        return behavior.getDataForPrimaryKey(this);
    }

    protected MithraDataObject zSynchronizedGetData()
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        return zCheckOptimisticDirty(behavior.getCurrentDataForRead(this), behavior);
    }


    public void resetFromOriginalPersistentObject()
    {
        MithraTransactionalObject original = this.zFindOriginal();
        if (original == null) throw new MithraBusinessException("Original is deleted!");
        MithraTransactionalObject copy = original.getDetachedCopy();
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        if (!behavior.isDetached()) throw new MithraBusinessException("only detached objects can be reset");
        behavior.setData(this, copy.zGetCurrentData());
    }

    protected MithraDataObject zSetBoolean(BooleanAttribute attr, boolean newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.booleanValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetByte(ByteAttribute attr, byte newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.byteValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetShort(ShortAttribute attr, short newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.shortValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetChar(CharAttribute attr, char newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.charValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetInteger(IntegerAttribute attr, int newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForWrite(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.intValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetLong(LongAttribute attr, long newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.longValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetFloat(FloatAttribute attr, float newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        this.zCheckFloatValue(newValue);
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.floatValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetDouble(DoubleAttribute attr, double newValue, boolean isReadOnly, boolean hasOptimistic, boolean isNullable)
    {
        this.zCheckDoubleValue(newValue);
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            if (!(isNullable && attr.isAttributeNull(data)) && attr.doubleValueOf(data) == newValue) return null;
            zResetEmbeddedValueObjects(behavior);
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetString(StringAttribute attr, String newValue, boolean isReadOnly, boolean hasOptimistic)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForWrite(this);
            String cur = attr.stringValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetTimestamp(TimestampAttribute attr, Timestamp newValue, boolean isReadOnly, boolean hasOptimistic)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForWrite(this);
            Timestamp cur = attr.timestampValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetDate(DateAttribute attr, Date newValue, boolean isReadOnly, boolean hasOptimistic)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            Date cur = attr.dateValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetTime(TimeAttribute attr, Time newValue, boolean isReadOnly, boolean hasOptimistic)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            Time cur = attr.timeValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetByteArray(ByteArrayAttribute attr, byte[] newValue, boolean isReadOnly, boolean hasOptimistic)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            byte[] cur = attr.byteArrayValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (Arrays.equals(cur, newValue)) return null;
            }
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    protected MithraDataObject zSetBigDecimal(BigDecimalAttribute attr, BigDecimal newValue, boolean isReadOnly, boolean hasOptimistic)
    {
        newValue = com.gs.fw.common.mithra.util.BigDecimalUtil.validateBigDecimalValue(newValue, attr.getPrecision(), attr.getScale());
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForRead(this);
            BigDecimal cur = attr.bigDecimalValueOf(data);
            if (cur == null)
            {
                if (newValue == null) return null;
            }
            else
            {
                if (cur.equals(newValue)) return null;
            }
            data = behavior.update(this, attr, newValue, isReadOnly, true);
            if (hasOptimistic)
            {
                zIncrementOptimiticAttribute(behavior, data);
            }
            return data;
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    public void insert() throws MithraBusinessException
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.insert(this);
    }

    public MithraTransactionalObject copyDetachedValuesToOriginalOrInsertIfNewImpl(MithraTransaction tx)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForReadWithWaitIfNecessary();
        return behavior.updateOriginalOrInsert(this);
    }

    public void zSetInserted()
    {
    }

    public void zSetUpdated(List<AttributeUpdateWrapper> updates)
    {
    }

    public void zApplyUpdateWrappers(List updateWrappers)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.remoteUpdate(this, updateWrappers);
    }

    public void zApplyUpdateWrappersForBatch(List updateWrappers)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        behavior.remoteUpdateForBatch(this, updateWrappers);
    }

    public void zMarkDirty()
    {
    }

    public boolean zHasSameNullPrimaryKeyAttributes(MithraTransactionalObject other)
    {
        return true;
    }

    public void copyNonPrimaryKeyAttributesFrom(MithraTransactionalObject from)
    {
        this.copyNonPrimaryKeyAttributesFrom(from, 0);
    }

    protected void copyNonPrimaryKeyAttributesFrom(MithraTransactionalObject from, int _retryCount)
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
                    _tx.setTransactionName("copyNonPrimaryKeyAttributesFrom");
                }

                this.copyNonPrimaryKeyAttributesFromImpl(from, _tx);
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

    protected void copyNonPrimaryKeyAttributesFromImpl(MithraTransactionalObject mithraTransactionalObject, MithraTransaction tx)
    {
        MithraDataObject newData = ((MithraTransactionalObjectImpl) mithraTransactionalObject).zSynchronizedGetData();
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        if (this.issueUpdatesForNonPrimaryKeys(behavior, data, newData))
        {
            this.triggerUpdateHookAfterCopy();
        }
        this.zResetEmbeddedValueObjects(behavior);
    }

    private int issueUpdate(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, Attribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return 0;
        if (attr.isAttributeNull(newData))
        {
            behavior.update(this, attr.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this)), readOnly, false);
            return 2;
        }
        return 1;
    }

    protected boolean zUpdateBoolean(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, BooleanAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.booleanValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateByte(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ByteAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.byteValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateShort(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ShortAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.shortValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateChar(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, CharAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.charValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateInteger(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, IntegerAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.intValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateLong(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, LongAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.longValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateDouble(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, DoubleAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.doubleValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateFloat(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, FloatAttribute attr, boolean readOnly)
    {
        int issue = issueUpdate(behavior, data, newData, attr, readOnly);
        if (issue == 1)
        {
            behavior.update(this, attr, attr.floatValueOf(newData), readOnly, false);
        }
        return issue > 0;
    }

    protected boolean zUpdateString(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, StringAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.stringValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateDate(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, DateAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.dateValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateTime(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, TimeAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.timeValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateTimestamp(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, TimestampAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.timestampValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateByteArray(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, ByteArrayAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.byteArrayValueOf(newData), readOnly, false);
        return true;
    }

    protected boolean zUpdateBigDecimal(TransactionalBehavior behavior, MithraDataObject data, MithraDataObject newData, BigDecimalAttribute attr, boolean readOnly)
    {
        if (attr.valueEquals(data, newData)) return false;
        behavior.update(this, attr, attr.bigDecimalValueOf(newData), readOnly, false);
        return true;
    }

    protected void zNullify(TransactionalBehavior behavior, MithraDataObject data, Attribute attr, boolean readOnly)
    {
        if (!attr.isAttributeNull(data))
        {
            behavior.update(this, attr.zConstructNullUpdateWrapper(behavior.getCurrentDataForWrite(this)), readOnly, true);
        }
    }

    public void zCopyAttributesFrom(MithraDataObject data)
    {
        this.zCopyAttributesFrom(data, 0);
    }

    public void zPersistDetachedChildDelete(MithraDataObject currentDataForRead)
    {
    }

    protected void zCopyAttributesFrom(MithraDataObject data, int retryCount)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        for (retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES - retryCount; retryCount > 0;)
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

    protected void zCopyAttributesFromImpl(MithraDataObject newData, MithraTransaction tx)
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        MithraDataObject data = behavior.getCurrentDataForRead(this);
        MithraObjectPortal portal = zGetPortal();
        if (portal.getTxParticipationMode(tx).isOptimisticLocking() && !tx.retryOnOptimisticLockFailure())
        {
            VersionAttribute versionAttribute = portal.getFinder().getVersionAttribute();
            if (versionAttribute != null && !versionAttribute.hasSameVersion(data, newData))
            {
                throw new MithraOptimisticLockException("Optimistic lock failure. " + data.zGetPrintablePrimaryKey());
            }
        }
        boolean changed = this.issueUpdatesForNonPrimaryKeys(behavior, data, newData);
        changed |= this.issueUpdatesForPrimaryKeys(behavior, data, newData) || changed;
        if (changed) this.triggerUpdateHookAfterCopy();
        this.zResetEmbeddedValueObjects(behavior);
    }

    public void setNullablePrimitiveAttributesToNull()
    {
        TransactionalBehavior behavior = zGetTransactionalBehaviorForWriteWithWaitIfNecessary();
        try
        {
            MithraDataObject data = behavior.getCurrentDataForWrite(this);
            this.issuePrimitiveNullSetters(behavior, data);
            this.zResetEmbeddedValueObjects(behavior);
        }
        finally
        {
            behavior.clearTempTransaction(this);
        }
    }

    public void zRefreshWithLockForRead(TransactionalBehavior behavior)
    {
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        MithraDataObject oldData = behavior.getCurrentDataForRead(this);
        MithraDataObject data = zGetPortal().refresh(oldData, true);
        MithraTransaction threadTx = mithraManager.getCurrentTransaction();
        if (data == null)
        {
            if (!threadTx.isCautious())
            {
                this.persistenceState = PersistenceState.DELETED;
                zGetPortal().getCache().removeIgnoringTransaction(this);
            }
            throw new MithraDeletedException(this.getClass().getName() + " " + oldData.zGetPrintablePrimaryKey() + " has been deleted.");
        }
        else
        {
            do
            {
                TransactionalState txState = this.transactionalState;

                if (txState == null || txState.hasNoTransactions())
                {
                    if (zEnrollInTransactionForRead(txState, threadTx, PersistenceState.PERSISTED))
                    {
                        break;
                    }
                }
                else if (txState.isParticipatingInReadOrWrite(threadTx))
                {
                    break;
                }
                else
                {
                    MithraTransaction currentWriteTransaction = txState.getExculsiveWriteTransaction();
                    if (currentWriteTransaction != null)
                    {
                        throw new MithraTransactionException("retry read-locked after write intent", currentWriteTransaction);
                    }
                    if (zEnrollInTransactionForRead(txState, threadTx, PersistenceState.PERSISTED))
                    {
                        break;
                    }
                }
            }
            while (true);
            if (oldData.changed(data))
            {
                synchronized (zGetLock())
                {
                    zGetPortal().getCache().reindex(this, data, behavior, oldData);
                }
            }
        }
    }

    public void zRefreshWithLockForWrite(TransactionalBehavior behavior)
    {
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        MithraDataObject oldData = behavior.getCurrentDataForRead(this);
        MithraDataObject data = zGetPortal().refresh(oldData, true);
        MithraTransaction threadTx = mithraManager.getCurrentTransaction();
        if (data == null)
        {
            if (!threadTx.isCautious())
            {
                this.persistenceState = PersistenceState.DELETED;
                zGetPortal().getCache().removeIgnoringTransaction(this);
            }
            throw new MithraDeletedException(this.getClass().getName() + " " + oldData.zGetPrintablePrimaryKey() + " has been deleted.");
        }
        else
        {
            do
            {
                TransactionalState txState = this.transactionalState;

                if (txState == null || txState.hasNoTransactions())
                {
                    if (zEnrollInTransactionForWrite(txState, new TransactionalState(threadTx, PersistenceState.PERSISTED)))
                    {
                        break;
                    }
                }
                else
                {
                    MithraTransaction currentWriteTransaction = txState.getExculsiveWriteTransaction();
                    if (currentWriteTransaction != null)
                    {
                        if (currentWriteTransaction.equals(threadTx)) break;
                        throw new MithraTransactionException("retry write intent, read-locked after other write intent", currentWriteTransaction);
                    }
                    if (txState.isOnlyReader(threadTx))
                    {
                        if (zEnrollInTransactionForWrite(txState, new TransactionalState(threadTx, PersistenceState.PERSISTED)))
                        {
                            break;
                        }
                    }
                    txState.waitForTransactions(threadTx);
                }
            }
            while (true);
            if (oldData.changed(data))
            {
                synchronized (zGetLock())
                {
                    zGetPortal().getCache().reindex(this, data, behavior, oldData);
                }
            }
        }
    }

    protected MithraTransactionalObject zCopyDetachedValuesToOriginalOrInsertIfNew()
    {
        return this.copyDetachedValuesToOriginalOrInsertIfNew(0);
    }

    protected MithraTransactionalObject copyDetachedValuesToOriginalOrInsertIfNew(int retryCount)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        boolean nested = tx != null;
        MithraTransactionalObject persisted = null;
        for (retryCount = MithraTransaction.DEFAULT_TRANSACTION_RETRIES - retryCount; retryCount > 0;)
        {
            try
            {
                if (!nested)
                {
                    tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
                    tx.setTransactionName(this.getClass().getName()+" copyDetachedValuesToOriginalOrInsertIfNew");
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

    // called when the object is first constructed after reading the database for the first time
    // also called after constructing a new object for remote insert, non-persistent copy and detached copy
    protected void zSetData(MithraDataObject data)
    {
        this.currentData = data;
        this.persistenceState = PersistenceState.PERSISTED;

        MithraTransaction currentTransaction = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (currentTransaction != null && zGetPortal().getTxParticipationMode(currentTransaction).mustParticipateInTxOnRead())
        {
            this.transactionalState = currentTransaction.getReadLockedTransactionalState(null, PersistenceState.PERSISTED);
        }
    }

    public void triggerUpdateHook(UpdateInfo updateInfo)
    {
        //nothing to do by default. subclass may override
    }

    public void zCascadeUpdateInPlaceBeforeTerminate()
    {
        //nothing to do by default. todo: implement for mixed dated-non-dated dependencies
    }

    public void triggerUpdateHookAfterCopy()
    {
        //nothing to do by default. subclass may override
    }
//endTemplate
}
