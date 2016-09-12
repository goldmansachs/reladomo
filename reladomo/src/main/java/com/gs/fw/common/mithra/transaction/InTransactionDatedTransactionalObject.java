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

package com.gs.fw.common.mithra.transaction;

import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalDatabaseObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalState;
import com.gs.fw.common.mithra.UpdateInfo;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.TemporalDirector;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.StatisticCounter;

import java.io.IOException;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;



public class InTransactionDatedTransactionalObject implements MithraDatedTransactionalObject
{

    public static final byte COMMITTED_STATE = 1;
    public static final byte TO_BE_UPDATED_STATE = 2;
    public static final byte UPDATED_STATE = 3;
    public static final byte DEACTIVATED_STATE = 4;
    public static final byte DELETED_STATE = 5;
    public static final byte TO_BE_INSERTED_STATE = 6;
    public static final byte INSERTED_STATE = 7;

    private MithraObjectPortal mithraObjectPortal;
    private MithraDataObject committedData;
    private MithraDataObject txData;
    private byte txState;

    public InTransactionDatedTransactionalObject(MithraObjectPortal mithraObjectPortal,
                                                 MithraDataObject committedData, MithraDataObject txData, byte txState)
    {
        this.mithraObjectPortal = mithraObjectPortal;
        this.committedData = committedData;
        this.txData = txData;
        this.txState = txState;
    }

    public boolean isCommitted()
    {
        return this.txState == COMMITTED_STATE;
    }

    public void zEnrollInTransactionForWrite(TemporalContainer container, MithraDataObject data, MithraTransaction threadTx)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForWrite(DatedTransactionalState prevState, TemporalContainer container, MithraDataObject data, MithraTransaction threadTx)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zRefreshWithLock(boolean lock)
    {
        throw new RuntimeException("not implemented");
    }

    public TemporalDirector zGetTemporalDirector()
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetCurrentData(MithraDataObject data)
    {
        this.committedData = data;
    }

    public MithraDataObject zGetCurrentData()
    {
        return this.committedData;
    }

    public MithraDataObject zGetCurrentDataWithCheck()
    {
        return this.committedData;
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
    }

    /* the optional object is the TransactionalBehavior in case of transactional objects */
    public void zSetData(MithraDataObject data, Object optional)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zUnsynchronizedGetData()
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void makeInMemoryNonTransactional()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isInMemoryNonTransactional()
    {
        return false;
    }

    public MithraDataObject zSynchronizedGetData()
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zGetNonTxData()
    {
        return this.committedData;
    }

    public MithraDataObject zGetTxDataForRead()
    {
        if (this.txData == null)
        {
            return this.committedData;
        }
        return this.txData;
    }

    public boolean isNewInThisTransaction()
    {
        return this.txState == INSERTED_STATE || this.txState == TO_BE_INSERTED_STATE;
    }

    public MithraDataObject zGetTxDataForWrite()
    {
        if (txData == null)
        {
            txData = this.committedData.copy();
        }
        return this.txData;
    }

    public void zSetTxData(MithraDataObject newData)
    {
        this.txData = newData;
    }

    public void zSetNonTxData(MithraDataObject newData)
    {
        this.committedData = newData;
    }

    public void zEnrollInTransaction()
    {
        throw new RuntimeException("not implemented");
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

    public void zHandleCommit()
    {
        if (this.txState == DELETED_STATE)
        {
            this.zGetCache().commitRemovedObject(this.committedData);
        }
        else
        {
            MithraDataObject oldData = this.committedData;
            this.committedData = txData;
            if (this.txState == INSERTED_STATE || this.txState == UPDATED_STATE)
            {
                this.zGetCache().commitObject(this, oldData);
            }
        }
    }

    public void zHandleRollback(MithraTransaction tx)
    {
        // nothing to do
    }

    public MithraDataObject zAllocateData()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraTransactionalDatabaseObject zGetDatabaseObject()
    {
        return (MithraTransactionalDatabaseObject) this.mithraObjectPortal.getDatabaseObject();
    }

    public Cache zGetCache()
    {
        return this.mithraObjectPortal.getCache();
    }

    public MithraObjectPortal zGetPortal()
    {
        return this.mithraObjectPortal;
    }

    public boolean zIsParticipatingInTransaction(MithraTransaction tx)
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public MithraTransaction zGetCurrentTransaction()
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void zSetNonTxPersistenceState(int state)
    {
    }

    public void zSetTxPersistenceState(int state)
    {
        throw new RuntimeException("not implemented");
    }

    public void zLockForTransaction()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDatedTransactionalObject zFindOriginal()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MithraDatedTransactionalObject getOriginalPersistentObject()
    {
        throw new RuntimeException("not implemented");
    }

    public void zCopyAttributesFrom(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void zPersistDetachedChildDelete(MithraDataObject currentDataForRead)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNew()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsDataChanged(MithraDataObject data)
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

    public void insert()
    {
        throw new RuntimeException("not implemented");
    }

    public void delete()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsDetached()
    {
        return false;
    }

    public InTransactionDatedTransactionalObject copyForInsert()
    {
        MithraDataObject txData = null;
        if (this.txData != null) txData = this.txData.copy();
        InTransactionDatedTransactionalObject result = new InTransactionDatedTransactionalObject(this.mithraObjectPortal, this.committedData, txData, TO_BE_INSERTED_STATE);
        return result;
    }

    public boolean isInPlaceUpdated()
    {
        return this.txState == TO_BE_UPDATED_STATE || this.txState == UPDATED_STATE;
    }

    public void setToBeUpdated()
    {
        if (this.txState != TO_BE_INSERTED_STATE || this.txState != INSERTED_STATE)
        {
            this.txState = TO_BE_UPDATED_STATE;
        }
    }

    public void zClearTxData()
    {
        this.txData = null;
    }

    public void zSetInserted()
    {
        if (this.txState != DELETED_STATE)
        {
            this.txState = INSERTED_STATE;
        }
        if (committedData == null || committedData.zAsOfAttributesChanged(this.txData))
        {
            this.committedData = this.txData.copy();
        }
    }

    public void zSetUpdated(List<AttributeUpdateWrapper> updates)
    {
        if (this.txState != INSERTED_STATE && this.txState != DELETED_STATE)
        {
            this.txState = UPDATED_STATE;
        }
        if (committedData == null)
        {
            this.committedData = this.txData.copy();
        }
        else if (committedData.zAsOfAttributesChanged(this.txData))
        {
            MithraDataObject newCommitted = this.committedData.copy();
            for(int i=0;i<updates.size();i++)
            {
                updates.get(i).updateData(newCommitted);
            }
            this.committedData = newCommitted;
        }
    }

    public boolean isInserted()
    {
        return this.txState == INSERTED_STATE;
    }

    public void zSetDeleted()
    {
        this.txState = DELETED_STATE;
    }

    public void zApplyUpdateWrappers(List updateWrappers)
    {
        throw new RuntimeException("not implemented");
    }

    public void zApplyUpdateWrappersForBatch(List updateWrappers)
    {
        throw new RuntimeException("not implemented");
    }

    public InTransactionDatedTransactionalObject copyForDelete()
    {
        return new InTransactionDatedTransactionalObject(this.mithraObjectPortal, this.committedData, null, DELETED_STATE);
    }

    public void insertWithIncrement()
    {
        throw new RuntimeException("not implemented");
    }

    public void inactivateForArchiving(Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new RuntimeException("not implemented");
    }

    public void terminateUntil(Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void insertWithIncrementUntil(Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void terminate()
    {
        throw new RuntimeException("not implemented");
    }

    public void purge()
    {
        throw new RuntimeException("not implemented");
    }

    public void insertForRecovery()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean needsTransactionalUpdate()
    {
        return this.txState != TO_BE_INSERTED_STATE;
    }

    public boolean zIsSameObjectWithoutAsOfAttributes(MithraTransactionalObject other)
    {
        if (this == other) return true;
        return this.zGetTxDataForRead().hasSamePrimaryKeyIgnoringAsOfAttributes(other.zGetTxDataForRead());
    }

    public void setNullablePrimitiveAttributesToNull()
    {
        throw new RuntimeException("not implemented");
    }

    public void copyNonPrimaryKeyAttributesFrom(MithraTransactionalObject from)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zHasSameNullPrimaryKeyAttributes(MithraTransactionalObject other)
    {
        return this.zGetTxDataForRead().zHasSameNullPrimaryKeyAttributes(other.zGetTxDataForRead());
    }

    public boolean isInMemoryAndNotInserted()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isDeletedOrMarkForDeletion()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDataObject zGetCurrentOrTransactionalData()
    {
        MithraDataObject result = this.committedData;
        if (result == null) result = this.txData;
        return result;
    }

    public void zSerializePrimaryKey(ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public void cascadeInsert()
    {
        throw new RuntimeException("not implemented");
    }

    public void cascadeDelete()
    {
        throw new RuntimeException("not implemented");
    }

    public void zPersistDetachedRelationships(MithraDataObject data)
    {
        throw new RuntimeException("not implemented");
    }

    public void cascadeTerminate()
    {
        throw new RuntimeException("not implemented");
    }

    public void zCopyAttributesUntilFrom(MithraDataObject data, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void zPersistDetachedRelationshipsUntil(MithraDataObject data, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNewUntil(Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void copyNonPrimaryKeyAttributesUntilFrom(MithraDatedTransactionalObject from, Timestamp until)
    {
        throw new RuntimeException("not implemented");
    }

    public void resetFromOriginalPersistentObject()
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

    public boolean isModifiedSinceDetachment(RelatedFinder extractor)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isModifiedSinceDetachmentByDependentRelationships()
    {
        throw new RuntimeException("not implemented");
    }

    public void zInsertRelationshipsUntil(MithraDataObject data, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void zDeleteForRemote(int hierarchyDepth)
    {
        throw new RuntimeException("not implemented");
    }

    public void zInsertForRemote(int hierarchyDepth)
    {
        throw new RuntimeException("not implemented");
    }

    public void zWriteDataClassName(ObjectOutput out) throws IOException
    {
        if (this.mithraObjectPortal.getSuperClassPortals() != null || this.mithraObjectPortal.getJoinedSubClassPortals() != null)
        {
            out.writeObject(this.txData.getClass().getName());
        }
    }

    public void zMarkDirty()
    {
        // do nothing
    }

    public void insertUntil(Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void cascadeInsertUntil(Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void cascadeTerminateUntil(Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDatedTransactionalObject getDetachedCopy()
    {
        throw new RuntimeException("not implemented");
    }

    public MithraDatedTransactionalObject getNonPersistentCopy()
    {
        throw new RuntimeException("not implemented");
    }

    public void zClearUnusedTransactionalState(DatedTransactionalState prevState)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForRead(DatedTransactionalState prev, MithraTransaction threadTx, int persistenceState)
    {

        throw new RuntimeException("not implemented");
    }

    public void zClearUnusedTransactionalState(TransactionalState transactionalState)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zEnrollInTransactionForRead(TransactionalState prev, MithraTransaction threadTx, int persistenceState)
    {
        throw new RuntimeException("not implemented");
    }

    public void zClearTempTransaction()
    {
        throw new RuntimeException("not implemented");
    }

    public void zPrepareForRemoteInsert()
    {
        throw new RuntimeException("not implemented");
    }

    public void zPrepareForDelete()
    {
        throw new RuntimeException("not implemented");
    }

    public void zWaitForExclusiveWriteTx(MithraTransaction tx)
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

    public MithraDatedTransactionalObject zCascadeCopyThenInsert()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsDataDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    public boolean zIsTxDataDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    public void triggerUpdateHook(UpdateInfo updateInfo)
    {
        //nothing to do
    }

    public boolean zDataMatches(Object data, Timestamp[] asOfDates)
    {
        return false;
    }

    public void zSetTxDetachedDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    public void zSetNonTxDetachedDeleted()
    {
        throw new RuntimeException("not implemented");
    }

    public void zCascadeUpdateInPlaceBeforeTerminate(MithraDataObject detachedData)
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
}
