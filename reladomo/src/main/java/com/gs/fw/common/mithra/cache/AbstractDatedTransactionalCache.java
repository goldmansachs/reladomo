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

package com.gs.fw.common.mithra.cache;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDataStorage;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.ObjectWithMapperStack;
import com.gs.fw.common.mithra.finder.asofop.AsOfOperation;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.transaction.MithraDatedObjectPersister;
import com.gs.fw.common.mithra.transaction.TransactionLocal;

import java.sql.Timestamp;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.util.ListFactory;

import java.util.Iterator;
import java.util.List;



public abstract class AbstractDatedTransactionalCache extends AbstractDatedCache
{

    private TransactionLocal containerIndexTxLocal = new TransactionLocal();
    private static final TransactionalDataContainerUnderlyingObjectGetter CONTAINER_UNDERLYING_OBJECT_GETTER = new TransactionalDataContainerUnderlyingObjectGetter();

    protected AbstractDatedTransactionalCache(Attribute[] nonDatedPkAttributes, AsOfAttribute[] asOfAttributes,
            MithraDatedObjectFactory factory, Attribute[] immutableAttributes, long timeToLive, long relationshipTimeToLive, OffHeapDataStorage dataStorage)
    {
        super(nonDatedPkAttributes, asOfAttributes, factory, immutableAttributes, timeToLive, relationshipTimeToLive, dataStorage);
        this.setPersistedState(DatedPersistenceState.PERSISTED);
    }

    @Override
    public Object put(MithraObject object)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            // put the data first:
            MithraTransactionalObject transactionalObject = (MithraTransactionalObject) object;
            MithraDataObject data = transactionalObject.zGetTxDataForRead();
            int nonPkHashCode = this.getNonDatedPkHashCode(data);
            this.addToIndiciesWithoutCopyInTransaction(data, nonPkHashCode);
            return this.getUniqueConcurrentDatedObjectIndex().put(object, nonPkHashCode, true);
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    /* unsynchronized */
    protected MithraDataObject addToIndiciesWithoutCopyInTransaction(MithraDataObject result, int nonDatedPkHashCode)
    {
        MithraDataObject removed = (MithraDataObject) this.getSemiUniqueDatedIndex().put(result, nonDatedPkHashCode);
        if (removed != null && removed != result)
        {
            for (int i = 2; i < indices.length; i++)
            {
                indices[i].remove(removed);
            }
            removed.zSetDataVersion(REMOVED_VERSION);
            releaseCacheData(removed);
        }
        for (int i = 2; i < this.indices.length; i++)
        {
            indices[i].put(result);
        }
        return result;
    }

    @Override
    public void remove(MithraObject object)
    {
        MithraTransactionalObject transactionalObject = (MithraTransactionalObject) object;
        MithraDataObject data = transactionalObject.zGetTxDataForRead();
        data.zSetDataVersion(REMOVED_VERSION);
        this.removeDatedData(data);
        // we purposely don't remove this from the dated object cache, because this method
        // is only called during in-place updates, which should not affect the dated object.
    }

    @Override
    public void removeUsingData(MithraDataObject object)
    {
        object.zSetDataVersion(REMOVED_VERSION);
        this.removeDatedData(object);
    }

    @Override
    public void getManyDatedObjectsFromData(Object[] dataArray, int length, ObjectWithMapperStack[] asOfOpWithStacks)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            super.getManyDatedObjectsFromData(dataArray, length, asOfOpWithStacks);
        }
        else
        {
            Timestamp[] asOfDates = this.getTempTimestamps();
            for(int i=0;i<length;i++)
            {
                MithraDataObject data = (MithraDataObject) dataArray[i];
                for(int a=0;a<asOfOpWithStacks.length;a++)
                {
                    AsOfOperation asOfOperation = (AsOfOperation) asOfOpWithStacks[a].getObject();
                    asOfDates[a] = asOfOperation.inflateAsOfDate(data);
                }
                dataArray[i] = getObjectFromDataForTx(data, asOfDates, tx, false);
            }
        }
    }

    @Override
    public Object getObjectFromData(MithraDataObject data, Timestamp[] asOfDates, boolean weak)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            return super.getObjectFromData(data, asOfDates, weak);
        }
        else
        {
            return getObjectFromDataForTx(data, asOfDates, tx, weak);
        }
    }

    private Object getObjectFromDataForTx(MithraDataObject data, Timestamp[] asOfDates, MithraTransaction tx, boolean weak)
    {
        MithraDatedTransactionalObject businessObject = null;
        int nonPkHashCode = this.getNonDatedPkHashCode(data);
        Boolean lock = null;
        try
        {
            boolean safeToReplaceCurrentData = false;
            TemporalContainer container = getContainerForTx(data, tx);
            MithraDataObject transactionalData = null;
            this.getCacheLock().acquireReadLock();
            lock = Boolean.TRUE;
            if (container != null)
            {
                transactionalData = container.getActiveDataFromData(data);
                data = checkContainerAndAddData(container, transactionalData, data, asOfDates);
                if (transactionalData == null) safeToReplaceCurrentData = true;
            }
            else
            {
                data = updateExistingDataIfAny(data, asOfDates, null);
                container = ((MithraDatedTransactionalObjectFactory) this.getFactory()).createContainer(tx);
                boolean added = false;
                if (this.getMithraObjectPortal().getTxParticipationMode(tx).isOptimisticLocking())
                {
                    SemiUniqueDatedIndex semiUniqueDatedIndex = this.getSemiUniqueDatedIndex();
                    added = semiUniqueDatedIndex.addSemiUniqueToContainer(data, container);
                }
                if (!added)
                {
                    container.addCommittedData(data);
                }
                putContainer(container, tx);
                tx.enrollCache(this);
                safeToReplaceCurrentData = true;
            }
            MithraDataObject committedData = container.getCommittedDataFromDates(asOfDates);
            if (committedData == null) committedData = transactionalData;
            if (committedData == null) committedData = data;
            businessObject = getOrCreateBusinessObject(committedData, asOfDates, nonPkHashCode, weak);
            this.getCacheLock().release();
            lock = null;
            container.lockForTransaction(businessObject, transactionalData, committedData, safeToReplaceCurrentData);
        }
        finally
        {
            if (lock != null) this.getCacheLock().release();
        }
        return businessObject;
    }

    private MithraDataObject checkContainerAndAddData(
            TemporalContainer container, MithraDataObject containerData,
            MithraDataObject data, Timestamp[] asOfDates)
    {
        if (containerData == null)
        {
            MithraDataObject committed = asOfDates == null ? container.getCommittedDataFromData(data) : container.getCommittedDataFromDates(asOfDates);
            if (committed != null)
            {
                boolean changed = committed.changed(data);
                if (changed)
                {
                    this.getCacheLock().upgradeToWriteLock();
                    this.reindexThenCopyOver(committed, data);
                }
                data = committed;
            }
            else
            {
                data = updateExistingDataIfAny(data, asOfDates, null);
                container.addCommittedData(data);
            }
        }
        else
        {
            data = containerData;
        }
        return data;
    }

    @Override
    protected MithraDataObject addToIndiciesIgnoringTransaction(MithraDataObject result)
    {
        result = copyDataForCacheIgnoringTransaction(result);
        TransactionalIndex index = (TransactionalIndex) this.getSemiUniqueDatedIndex();
        index.putIgnoringTransaction(result, result, false);
        Index[] indices = this.getIndices();
        for(int i=2;i<indices.length;i++)
        {
            index = (TransactionalIndex) indices[i];
            index.putIgnoringTransaction(result, result, false);
        }
        return result;
    }

    @Override
    protected void removeDatedDataIgnoringTransactionExcludingSemiUniqueIndex(MithraDataObject result)
    {
        Index[] indices = this.getIndices();
        for(int i=2;i<indices.length;i++)
        {
            TransactionalIndex index = (TransactionalIndex) indices[i];
            index.removeIgnoringTransaction(result);
        }
    }

    protected MithraDatedTransactionalObject getOrCreateBusinessObject(
            MithraDataObject data, Timestamp[] asOfDates, int nonPkHashCode, boolean weak)
    {
        return (MithraDatedTransactionalObject) this.getUniqueConcurrentDatedObjectIndex().getFromDataOrPutIfAbsent(data, asOfDates, nonPkHashCode, weak);
    }

    @Override
    protected List convertToBusinessObjectAndWrapInList(Object o, Extractor[] extractors, Timestamp[] asOfDates, boolean weak, boolean isLocked)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            return super.convertToBusinessObjectAndWrapInList(o, extractors, asOfDates, weak, isLocked);
        }
        List result = null;
        if (!(o instanceof List))
        {
            if (o == null) return ListFactory.EMPTY_LIST;
            result = new FastList(1);
            MithraDataObject data = (MithraDataObject) o;
            this.extractTimestampsFromData(data, extractors, asOfDates);
            result = getBusinessObjectFromExistingContainer(data, tx, asOfDates, result, this.getNonDatedPkHashCode(data), weak, isLocked);
        }
        else
        {
            List list = (List) o;
            result = new FastList(list.size());
            for(int i=0;i<list.size() && result != null;i++)
            {
                MithraDataObject data = (MithraDataObject)list.get(i);
                this.extractTimestampsFromData(data, extractors, asOfDates);
                if (this.matchesAsOfDates(data, asOfDates))
                {
                    result = getBusinessObjectFromExistingContainer(data, tx, asOfDates, result, this.getNonDatedPkHashCode(data), weak, isLocked);
                }
            }
        }
        if (result == null) result = ListFactory.EMPTY_LIST;
        return result;
    }

    @Override
    protected MithraDatedObject getBusinessObjectFromData(MithraDataObject oldData, Timestamp[] asOfDates, int nonPkHashCode,
            boolean weak, MithraTransaction tx, boolean isLocked)
    {
        if (tx == null)
        {
            return super.getBusinessObjectFromData(oldData, asOfDates, nonPkHashCode, weak, isLocked);
        }
        return getBusinessObjectFromExistingContainerAsOne(oldData, tx, asOfDates, nonPkHashCode, weak, isLocked);
    }

    @Override
    protected MithraDatedObject getBusinessObjectFromData(MithraDataObject data, Timestamp[] asOfDates, int nonPkHashCode,
            boolean weak, boolean isLocked)
    {
        if (data == null) return null;
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        return getBusinessObjectFromData(data, asOfDates, nonPkHashCode, weak, tx, isLocked);
    }

    private List getBusinessObjectFromExistingContainer(MithraDataObject data, MithraTransaction tx,
            Timestamp[] asOfDates, List result, int nonPkHashCode, boolean weak, boolean isLocked)
    {
        Object businessObject = getBusinessObjectFromExistingContainerAsOne(data, tx, asOfDates, nonPkHashCode, weak, isLocked);
        if (businessObject == null)
        {
            return null;
        }
        result.add(businessObject);
        return result;
    }

    private MithraDatedObject getBusinessObjectFromExistingContainerAsOne(MithraDataObject data, MithraTransaction tx,
            Timestamp[] asOfDates, int nonPkHashCode, boolean weak, boolean isLocked)
    {
        TemporalContainer container = getContainerForTx(data, tx);
        if (this.isInactiveData(data))
        {
            MithraDatedTransactionalObject businessObjectResult = null;

            if (container != null)
            {
                MithraDataObject transactionalData = container.getDataForTxByDates(data, asOfDates);
                if (transactionalData != null)
                {
                    businessObjectResult = getOrCreateBusinessObject(transactionalData, asOfDates, nonPkHashCode, weak);
                    if (isLocked)
                    {
                        getCacheLock().release();
                    }
                    container.possiblyEnroll(businessObjectResult, transactionalData, data, null);// no need to check the result as we're obtaining an exclusive object write lock and it can't fail without an exception
                    if (isLocked)
                    {
                        getCacheLock().acquireReadLock();
                    }
                }
            }

            if (businessObjectResult == null)
            {
                businessObjectResult = (MithraDatedTransactionalObject) super.getBusinessObjectFromData(data, asOfDates, nonPkHashCode, weak, isLocked);
            }
            return businessObjectResult;
        }
        boolean containerWasNull = false;
        boolean isPureHome = this.getMithraObjectPortal().isPureHome();
        if (data.zGetDataVersion() >= 0 && container == null && (this.getMithraObjectPortal().getTxParticipationMode(tx).isOptimisticLocking() || isPureHome))
        {
            containerWasNull = true;
            container = ((MithraDatedTransactionalObjectFactory) this.getFactory()).createContainer(tx);
            SemiUniqueDatedIndex semiUniqueDatedIndex = this.getSemiUniqueDatedIndex();
            boolean foundInCache = semiUniqueDatedIndex.addSemiUniqueToContainer(data, container);
            if (!foundInCache)
            {
                container.setAnyData(data);
            }
            putContainer(container, tx);
            if (isPureHome)
            {
                container.setInfiniteRange();
            }
            tx.enrollCache(this);
        }
        if (container != null)
        {
            MithraDataObject committedData = container.getCommittedDataFromDates(asOfDates);
            MithraDataObject transactionalData = container.getActiveDataFromData(data);
            boolean good = committedData == data  || transactionalData == data;
            if (!good && (this.getMithraObjectPortal().getTxParticipationMode(tx).isOptimisticLocking() || isPureHome))
            {
                good = true;
                container.addCommittedData(data);
                committedData = data;
            }
            if (good)
            {
                MithraDatedTransactionalObject businessObject = getOrCreateBusinessObject(committedData == null ? transactionalData : committedData, asOfDates, nonPkHashCode, weak);
                if (isLocked)
                {
                    getCacheLock().release();
                }
                container.lockForTransaction(businessObject, transactionalData, committedData, containerWasNull);
                if (isLocked)
                {
                    getCacheLock().acquireReadLock();
                }
                return businessObject;
            }
        }
        return null;
    }

    @Override
    public void commit(MithraTransaction tx)
    {
        // no lock is necessary for commit
        TransactionalIndex index = (TransactionalIndex) this.getSemiUniqueDatedIndex();
        index.commit(tx);
        Index[] indices = this.getIndices();
        for(int i=2;i<indices.length;i++)
        {
            index = (TransactionalIndex) indices[i];
            index.commit(tx);
        }
    }

    @Override
    public void rollback(MithraTransaction tx)
    {
        // no lock is necessary for rollback
        TransactionalIndex index = (TransactionalIndex) this.getSemiUniqueDatedIndex();
        index.rollback(tx);
        Index[] indices = this.getIndices();
        for(int i=2;i<indices.length;i++)
        {
            index = (TransactionalIndex) indices[i];
            index.rollback(tx);
        }
    }

    @Override
    public void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        Attribute attribute = updateWrapper.getAttribute();
        if (this.getMonitoredAttributes().contains(attribute))
        {
            List affectedIndicies = (List)this.getAttributeToIndexMap().get(attribute);
            if (affectedIndicies.size() > 0)
            {
                MithraManager mithraManager = MithraManagerProvider.getMithraManager();
                MithraTransaction tx = mithraManager.zGetCurrentTransactionWithNoCheck();
                try
                {
                    this.getCacheLock().acquireWriteLock();
                    for(Iterator it = affectedIndicies.iterator();it.hasNext();)
                    {
                        TransactionalIndex index = (TransactionalIndex)it.next();
                        index.prepareForReindexInTransaction(object, tx);
                    }
                    updateWrapper.updateData();
                    for(Iterator it = affectedIndicies.iterator();it.hasNext();)
                    {
                        TransactionalIndex index = (TransactionalIndex)it.next();
                        index.finishForReindex(object, tx);
                    }
                }
                finally
                {
                    this.getCacheLock().release();
                }
                return;
            }
        }
        updateWrapper.updateData();
    }

    @Override
    public void removeIgnoringTransaction(MithraObject object)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void prepareForCommit(MithraTransaction tx)
    {
        // nothing to do, because preparePut is never called on this index
    }

    @Override
    public void commitRemovedObject(MithraDataObject data)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            data.zSetDataVersion(REMOVED_VERSION);
            TransactionalIndex index = (TransactionalIndex) this.getSemiUniqueDatedIndex();
            MithraDataObject oldData = (MithraDataObject) index.removeUsingUnderlying(data);
            if (oldData != null)
            {
                Index[] indices = this.getIndices();
                for(int i=2;i<indices.length;i++)
                {
                    index = (TransactionalIndex) indices[i];
                    index.removeUsingUnderlying(oldData);
                }
                releaseCacheData(oldData);
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void commitObject(MithraTransactionalObject mithraObject, MithraDataObject oldData)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            MithraDataObject newData = copyDataForCacheIgnoringTransaction(mithraObject.zGetNonTxData());
            TransactionalIndex index = (TransactionalIndex) this.getSemiUniqueDatedIndex();
            if (oldData != null)
            {
                oldData.zSetDataVersion(REMOVED_VERSION);
                oldData = (MithraDataObject) index.removeUsingUnderlying(oldData);
            }
            MithraDataObject reallyOldData = (MithraDataObject) index.putIgnoringTransaction(newData, newData, true);
            Index[] indices = this.getIndices();
            if (reallyOldData != null && reallyOldData != newData)
            {
                for(int i=2;i<indices.length;i++)
                {
                    index = (TransactionalIndex) indices[i];
                    index.removeUsingUnderlying(reallyOldData);
                }
                reallyOldData.zSetDataVersion(REMOVED_VERSION);
                releaseCacheData(reallyOldData);
            }
            for(int i=2;i<indices.length;i++)
            {
                index = (TransactionalIndex) indices[i];
                if (oldData != null) index.removeUsingUnderlying(oldData);
                index.putIgnoringTransaction(newData, newData, true);
            }
            releaseCacheData(oldData);
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public TemporalContainer getOrCreateContainer(MithraDataObject data)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        FullUniqueIndex index = (FullUniqueIndex) this.containerIndexTxLocal.get(tx);
        TemporalContainer container = null;
        if (index == null)
        {
            index = new FullUniqueIndex("containerIndex", getNonDatedPkAttributes());
            index.setUnderlyingObjectGetter(CONTAINER_UNDERLYING_OBJECT_GETTER);
            this.containerIndexTxLocal.set(tx, index);
        }
        else
        {
            container = (TemporalContainer) index.getFromData(data);
        }
        if (container == null)
        {
            container = ((MithraDatedTransactionalObjectFactory) this.getFactory()).createContainer(tx);
            SemiUniqueDatedIndex semiUniqueDatedIndex = this.getSemiUniqueDatedIndex();
            boolean foundCachedValue = false;
            TxParticipationMode participationMode = data.zGetMithraObjectPortal().getTxParticipationMode(tx);
            if (participationMode.isOptimisticLocking())
            {
                foundCachedValue = semiUniqueDatedIndex.addSemiUniqueToContainer(data, container);
            }
            if (!foundCachedValue) container.setAnyData(data);
            index.put(container);
        }
        return container;
    }

    protected boolean dataMatches(MithraDatedTransactionalObject mithraObject, MithraDataObject data)
    {
        AsOfAttribute[] asOfAttributes = this.getAsOfAttributes();
        boolean matches = (mithraObject != null);
        for(int i=0;matches && i<asOfAttributes.length;i++)
        {
            matches = asOfAttributes[i].dataMatches(data, asOfAttributes[i].timestampValueOf(mithraObject));
        }
        return matches;
    }

    @Override
    public boolean enrollDatedObject(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        this.getCacheLock().acquireReadLock();
        Boolean lock = Boolean.TRUE;
        try
        {
            MithraDataObject data = mithraObject.zGetCurrentData();
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            TemporalContainer container = getContainerForTx(data, tx);
            MithraDataObject transactionalData = null;
            boolean setAsCurrentData = false;
            if (container == null)
            {
                container = ((MithraDatedTransactionalObjectFactory) this.getFactory()).createContainer(tx);
                setAsCurrentData = true;
                TxParticipationMode participationMode = mithraObject.zGetPortal().getTxParticipationMode(tx);
                if (data.zGetDataVersion() >=0 && participationMode.isOptimisticLocking())
                {
                    transactionalData = populateContainerAndGetTransactionalData(mithraObject, data, container);
                }
                else
                {
                    this.getCacheLock().release();
                    lock = null;
                    MithraDataObject committedData =
                            ((MithraDatedObjectPersister) mithraObject.zGetPortal().getMithraObjectPersister()).enrollDatedObject(mithraObject);
                    if (committedData == null)
                    {
                        throw new MithraDeletedException("The object "+mithraObject.getClass().getName()+" with primary key: "+
                            data.zGetPrintablePrimaryKey()+" has been terminated.");
                    }
                    this.getCacheLock().acquireReadLock();
                    lock = Boolean.TRUE;
                    transactionalData = updateExistingDataIfAny(committedData, null, mithraObject);
                    container.addCommittedData(transactionalData);
                }
                putContainer(container, tx);
                tx.enrollCache(this);
            }
            else
            {
                transactionalData = container.getTxDataFor(mithraObject);
                if (transactionalData == null)
                {
                    container.checkInactivated(mithraObject);
                    transactionalData = container.getCommitedDataFor(mithraObject);
                    setAsCurrentData = true;
                }
                if (transactionalData == null)
                {
                    setAsCurrentData = true;
                    this.getCacheLock().release();
                    lock = null;
                    tx.executeBufferedOperationsForEnroll(mithraObject.zGetPortal());
                    MithraDataObject committedData = ((MithraDatedObjectPersister) mithraObject.zGetPortal().getMithraObjectPersister()).enrollDatedObject(mithraObject);
                    if (committedData == null)
                    {
                        throw new MithraDeletedException("The object "+mithraObject.getClass().getName()+" with primary key: "+
                            data.zGetPrintablePrimaryKey()+" has been terminated.");
                    }
                    this.getCacheLock().acquireReadLock();
                    lock = Boolean.TRUE;
                    transactionalData = updateExistingDataIfAny(committedData, null, mithraObject);
                    container.addCommittedData(transactionalData);
                }
            }
            if (lock != null)
            {
                this.getCacheLock().release();
                lock = null;
            }
            if (setAsCurrentData)
            {
                if (forWrite)
                {
                    return container.enrollForWrite(mithraObject, transactionalData, prevState);
                }
                return container.enrollReadOnly(mithraObject, transactionalData, prevState);
            }
            else
            {
                return container.possiblyEnroll(mithraObject, transactionalData, null, prevState);
            }
        }
        finally
        {
            if (lock != null) this.getCacheLock().release();
        }
    }

    @Override
    public boolean enrollDatedObjectForDelete(MithraDatedTransactionalObject mithraObject, DatedTransactionalState prevState, boolean forWrite)
    {
        this.getCacheLock().acquireReadLock();
        Boolean lock = Boolean.TRUE;
        try
        {
            MithraDataObject data = mithraObject.zGetCurrentData();
            MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
            TemporalContainer container = getContainerForTx(data, tx);
            MithraDataObject transactionalData = null;
            boolean setAsCurrentData = false;
            if (container == null)
            {
                container = ((MithraDatedTransactionalObjectFactory) this.getFactory()).createContainer(tx);
                setAsCurrentData = true;
                TxParticipationMode participationMode = mithraObject.zGetPortal().getTxParticipationMode(tx);
                if (data.zGetDataVersion() >=0 && participationMode.isOptimisticLocking())
                {
                    transactionalData = populateContainerAndGetTransactionalData(mithraObject, data, container);
                }
                else
                {
                    transactionalData = data;
                    container.addCommittedData(transactionalData);
                }
                putContainer(container, tx);
                tx.enrollCache(this);
            }
            else
            {
                transactionalData = container.getTxDataFor(mithraObject);
                if (transactionalData == null)
                {
                    container.checkInactivatedForDelete(mithraObject);
                    transactionalData = container.getCommitedDataFor(mithraObject);
                    setAsCurrentData = true;
                }
                if (transactionalData == null)
                {
                    transactionalData = data;
                    container.addCommittedData(transactionalData);
                }
            }
            this.getCacheLock().release();
            lock = null;
            if (setAsCurrentData)
            {
                if (forWrite)
                {
                    return container.enrollForWrite(mithraObject, transactionalData, prevState);
                }
                return container.enrollReadOnly(mithraObject, transactionalData, prevState);
            }
            else
            {
                return container.possiblyEnroll(mithraObject, transactionalData, null, prevState);
            }
        }
        finally
        {
            if (lock != null) this.getCacheLock().release();
        }
    }

    private MithraDataObject populateContainerAndGetTransactionalData(MithraDatedTransactionalObject mithraObject, MithraDataObject data, TemporalContainer container)
    {
        MithraDataObject transactionalData;
        SemiUniqueDatedIndex semiUniqueDatedIndex = this.getSemiUniqueDatedIndex();
        boolean foundCachedValue = semiUniqueDatedIndex.addSemiUniqueToContainer(data, container);
        if (!foundCachedValue)
        {
            container.addCommittedData(data);
        }
        transactionalData = container.getCommitedDataFor(mithraObject);
        if (transactionalData == null)
        {
            if (foundCachedValue)
            {
                container.addCommittedData(data);
            }
            transactionalData = container.getCommitedDataFor(mithraObject);
            if (transactionalData == null)
            {
                transactionalData = data;
            }
        }
        return transactionalData;
    }

    @Override
    public MithraDataObject getTransactionalDataFromData(MithraDataObject data)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        this.getCacheLock().acquireReadLock();
        try
        {
            TemporalContainer container = getContainerForTx(data, tx);
            if (container != null)
            {
                MithraDataObject containerData = container.getActiveOrInactiveDataFromData(data);
                data = checkContainerAndAddData(container, containerData, data, null);
            }
            else
            {
                throw new RuntimeException("should never get here");
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
        return data;
    }

    private void putContainer(TemporalContainer container, MithraTransaction tx)
    {
        FullUniqueIndex index = (FullUniqueIndex) this.containerIndexTxLocal.get(tx);
        if (index == null)
        {
            index = new FullUniqueIndex("containerIndex", getNonDatedPkAttributes());
            index.setUnderlyingObjectGetter(CONTAINER_UNDERLYING_OBJECT_GETTER);
            this.containerIndexTxLocal.set(tx, index);
        }
        index.put(container);
    }

    private TemporalContainer getContainerForTx(MithraDataObject data, MithraTransaction tx)
    {
        FullUniqueIndex index = (FullUniqueIndex) this.containerIndexTxLocal.get(tx);
        if (index == null)
        {
            return null;
        }
        return (TemporalContainer) index.getFromData(data);
    }

    @Override
    public void rollbackObject(MithraObject mithraObject)
    {
        this.getUniqueConcurrentDatedObjectIndex().remove(mithraObject);
    }

    protected static class TransactionalDataContainerUnderlyingObjectGetter implements UnderlyingObjectGetter
    {
        public Object getUnderlyingObject(Object o)
        {
            TemporalContainer dataContainer = (TemporalContainer) o;
            return dataContainer.getAnyData();
        }
    }

    @Override
    protected void reindexAffectedIndices(MithraDataObject data, MithraDataObject oldData, UnifiedSet affectedIndicies)
    {
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        MithraTransaction tx = mithraManager.zGetCurrentTransactionWithNoCheck();
        if (tx == null)
        {
            super.reindexAffectedIndices(data, oldData, affectedIndicies);
            return;
        }
        List<TransactionalIndex> prepared = FastList.newList(affectedIndicies.size());
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            TransactionalIndex index = (TransactionalIndex) it.next();
            if (index.prepareForReindex(oldData, tx))
            {
                prepared.add(index);
                it.remove();
            }
            else
            {
                index.removeIgnoringTransaction(oldData);
            }
        }
        oldData.copyNonPkAttributes(data);
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            TransactionalIndex index = (TransactionalIndex) it.next();
            index.putIgnoringTransaction(oldData, oldData, false);
        }
        for(int i=0;i<prepared.size();i++)
        {
            prepared.get(i).finishForReindex(oldData, tx);
        }

    }
}
