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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalState;
import com.gs.fw.common.mithra.UpdateInfo;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.offheap.MithraOffHeapDataObject;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


public abstract class TupleImpl implements Tuple, Serializable, MithraTransactionalObject, MithraDataObject
{
    private static final long serialVersionUID = 498371650930536115L;

    public void cascadeDelete()
    {
        throw new RuntimeException("not implemented");
    }

    public void cascadeInsert()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MithraTransactionalObject copyDetachedValuesToOriginalOrInsertIfNew()
    {
        throw new RuntimeException("not implemented");
    }

    public void copyNonPrimaryKeyAttributesFrom(MithraTransactionalObject from)
    {
        throw new RuntimeException("not implemented");
    }

    public void delete()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraTransactionalObject getDetachedCopy()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isInMemoryNonTransactional()
    {
        return false;
    }

    public void makeInMemoryNonTransactional()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraTransactionalObject getNonPersistentCopy()
    {
        throw new RuntimeException("not implemented");
    }

    public void insert()
    {
        throw new RuntimeException("not implemented");
    }

    public void insertForRecovery()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isInMemoryAndNotInserted()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isModifiedSinceDetachment()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isModifiedSinceDetachment(Extractor extractor)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isModifiedSinceDetachment(RelatedFinder relatetionshipFinder)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isModifiedSinceDetachmentByDependentRelationships()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other, double toleranceForFloatingPointFields)
    {
        throw new RuntimeException("not implemented");
    }

    public void resetFromOriginalPersistentObject()
    {
        throw new RuntimeException("not implemented");
    }

    public void setNullablePrimitiveAttributesToNull()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zAllocateData()
    {
        throw new RuntimeException("not implemented");
    }

    public void zApplyUpdateWrappers(List updateWrappers)
    {
        throw new RuntimeException("not implemented");
    }

    public void zApplyUpdateWrappersForBatch(List updateWrappers)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraTransactionalObject zCascadeCopyThenInsert()
    {
        throw new RuntimeException("not implemented");
    }

    public void zClearTempTransaction()
    {
        throw new RuntimeException("not implemented");
    }

    public void zClearUnusedTransactionalState(TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public void zCopyAttributesFrom(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void zDeleteForRemote(int hierarchyDepth)
    {
        throw new RuntimeException("not implemented");
    }

    public void zEnrollInTransaction()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForDelete(TransactionalState prevState, TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForRead(TransactionalState prev, MithraTransaction threadTx, int persistenceState)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForWrite(TransactionalState prev, TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraTransactionalObject zFindOriginal()
    {
        throw new RuntimeException("not implemented");
    }

    public Cache zGetCache()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zGetNonTxData()
    {
        return this;
    }

    public MithraObjectPortal zGetPortal()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zGetTxDataForRead()
    {
        return this;
    }

    public MithraDataObject zGetTxDataForWrite()
    {
        return this;
    }

    public boolean zHasSameNullPrimaryKeyAttributes(MithraTransactionalObject other)
    {
        throw new RuntimeException("not implemented");
    }

    public void zInsertForRemote(int hierarchyDepth)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsDataChanged(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsDetached()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsParticipatingInTransaction(MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsSameObjectWithoutAsOfAttributes(MithraTransactionalObject other)
    {
        throw new RuntimeException("not implemented");
    }

    public void zLockForTransaction()
    {
        throw new RuntimeException("not implemented");
    }

    public void zPersistDetachedChildDelete(MithraDataObject currentDataForRead)
    {
        throw new RuntimeException("not implemented");
    }

    public void zPersistDetachedRelationships(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void zPrepareForDelete()
    {
        throw new RuntimeException("not implemented");
    }

    public void zPrepareForRemoteInsert()
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

    public void zSetDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetInserted()
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetNonTxData(MithraDataObject newData)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetTxData(MithraDataObject newData)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetTxPersistenceState(int state)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetUpdated(List<AttributeUpdateWrapper> updates)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zUnsynchronizedGetData()
    {
        throw new RuntimeException("not implemented");
    }

    public void zWaitForExclusiveWriteTx(MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isDeletedOrMarkForDeletion()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zGetCurrentData()
    {
        return this;
    }

    public void zMarkDirty()
    {
        throw new RuntimeException("not implemented");
    }

    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSerializeFullData(ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public void zSerializeFullTxData(ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }/* the optional object is the TransactionalBehavior in case of transactional objects */

    public void zSetData(MithraDataObject data, Object optional)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetNonTxPersistenceState(int state)
    {
        throw new RuntimeException("not implemented");
    }

    public void zWriteDataClassName(ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public void zHandleCommit()
    {
        throw new RuntimeException("not implemented");
    }

    public void zHandleRollback(MithraTransaction tx)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean changed(MithraDataObject newData)
    {
        throw new RuntimeException("not implemented");
    }

    public void clearAllDirectRefs()
    {
        throw new RuntimeException("not implemented");
    }

    public void clearRelationships()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject copy()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject copy(boolean copyRelationships)
    {
        throw new RuntimeException("not implemented");
    }

    public void copyNonPkAttributes(MithraDataObject newData)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean hasSamePrimaryKeyIgnoringAsOfAttributes(MithraDataObject mithraDataObject)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zAsOfAttributesChanged(MithraDataObject other)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zAsOfAttributesFromEquals(MithraDataObject other)
    {
        throw new RuntimeException("not implemented");
    }

    public void zDeserializeFullData(ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");
    }

    public void zDeserializePrimaryKey(ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");
    }

    public byte zGetDataVersion()
    {
        throw new RuntimeException("not implemented");
    }

    public Number zGetIdentityValue()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraObjectPortal zGetMithraObjectPortal()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraObjectPortal zGetMithraObjectPortal(int hierarchyDepth)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zHasIdentity()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zHasSameNullPrimaryKeyAttributes(MithraDataObject mithraDataObject)
    {
        throw new RuntimeException("not implemented");
    }

    public void zIncrementDataVersion()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zNonPrimaryKeyAttributesChanged(MithraDataObject newData)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zNonPrimaryKeyAttributesChanged(MithraDataObject newData, double tolerance)
    {
        throw new RuntimeException("not implemented");
    }

    public String zReadDataClassName(ObjectInput in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");
    }

    public void zSerializePrimaryKey(ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetDataVersion(byte version)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetIdentity(Number identityValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void zSerializeRelationships(ObjectOutputStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public void zDeserializeRelationships(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        throw new RuntimeException("not implemented");
    }

    public void triggerUpdateHook(UpdateInfo updateInfo)
    {
        //nothing to do
    }

    public void zSetNonTxDetachedDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetTxDetachedDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String zGetSerializationClassName()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MithraOffHeapDataObject zCopyOffHeap()
    {
        throw new RuntimeException("not implemented");
    }

    public void zCascadeUpdateInPlaceBeforeTerminate()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> zAddNavigatedRelationshipsStats(RelatedFinder finder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> zAddNavigatedRelationshipsStatsForUpdate(RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Map<RelatedFinder, StatisticCounter> zAddNavigatedRelationshipsStatsForDelete(RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MithraTransactionalObject getOriginalPersistentObject()
    {
        throw new RuntimeException("not implemented");
    }
}