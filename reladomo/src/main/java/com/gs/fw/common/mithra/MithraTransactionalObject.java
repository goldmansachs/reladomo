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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.transaction.MithraTransactionalResource;
import com.gs.fw.common.mithra.util.StatisticCounter;

import java.util.List;
import java.util.Map;


/**
 * The methods in this interface that start with a lower case 'z' are private to mithra and must not be called
 * by user code.
 */
public interface MithraTransactionalObject extends MithraObject, MithraTransactionalResource
{

    public MithraDataObject zUnsynchronizedGetData();

    public MithraDataObject zGetNonTxData();

    public MithraDataObject zGetTxDataForRead();

    public MithraDataObject zGetTxDataForWrite();

    public void zSetTxData(MithraDataObject newData);

    public void zSetNonTxData(MithraDataObject newData);

    public void zEnrollInTransaction();

    public void zRefreshWithLockForRead(TransactionalBehavior persistedTxEnrollBehavior);

    public void zRefreshWithLockForWrite(TransactionalBehavior persistedTxEnrollBehavior);

    public MithraDataObject zAllocateData();

    public Cache zGetCache();

    public void zSetTxPersistenceState(int state);

    public boolean zIsParticipatingInTransaction(MithraTransaction tx);

    public void zLockForTransaction();

    public void makeInMemoryNonTransactional();

    public boolean isInMemoryNonTransactional();

    /**
     * inserts this object into the database. The object must not be persisted already.
     */
    public void insert();

    /**
     * inserts this object and any depndent relationships that have been set on it.
     */
    public void cascadeInsert();

    /**
     * Inserts the data as is for recovery of dated objects. All attributes, including to/from AsOfAttributes,
     * must be set for this to work.
     */
    public void insertForRecovery();

    /**
     * deletes this object from the database. A deleted object cannot be accessed any more.
     * An attempt to call methods on the deleted object will result in a MithraDeletedException.
     */
    public void delete();

    /**
     * delete this object and all of its dependent relationships.
     */
    public void cascadeDelete();

    public boolean zIsDetached();

    public MithraTransactionalObject zFindOriginal();

    public void zCopyAttributesFrom(MithraDataObject data);

    public void zPersistDetachedRelationships(MithraDataObject data);

    /**
     * This method is for detached objects only. You must not call this method for any
     * reason other than persisting the changes to a detached object.
     */
    public MithraTransactionalObject copyDetachedValuesToOriginalOrInsertIfNew();

    /**
     * This method is for detached objects only. It resets the values of the detached
     * object from the original persistent object.
     */
    public void resetFromOriginalPersistentObject();

    /**
     * This method is for detached objects only.
     *
     * @return true if the detached object is different than the persisted object it was detached from
     */
    public boolean isModifiedSinceDetachment();

    /**
     * This method is for detached objects only.
     *
     * @param extractor The attribute that should be checked. For example ProductFinder.description()
     * @return true if the attribute passed in is different from the persisted object.
     */
    public boolean isModifiedSinceDetachment(Extractor extractor);

    /**
     * This method is for detached objects only.
     *
     * @param relatetionshipFinder, e.g. OrderFinder.items(), is the dependent relationship to query
     * @return true if the relationship has changed on this detached object
     */
    public boolean isModifiedSinceDetachment(RelatedFinder relatetionshipFinder);

    /**
     * This method is for detached objects only.
     *
     * @return true if any dependent relationships have changed on this detached object
     */
    public boolean isModifiedSinceDetachmentByDependentRelationships();


    public boolean zIsDataChanged(MithraDataObject data);

    /**
     * This method is used to compare the non primary key attrributes of two Mithra transactional objects.
     *
     * @param other The other Mithra transactional object used for comparison.
     * @return true if the two Mithra transactional objects are of the same type
     *         and the values of their non primary key attributes are not the same.
     *         A ClassCastException will be thrown if the two Mithra transactional objects are not of the same type
     */
    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other);

    /**
     * This method is used to compare the non primary key attrributes of two Mithra transactional objects.
     *
     * @param other The other Mithra transactional object used for comparison.
     * @param toleranceForFloatingPointFields
     *              The total amount by which float/double attributes are allowed to vary
     *              from one another.
     * @return true if the two Mithra transactional objects are of the same type
     *         and the values of their non primary key attributes are not the same. Float and double attributes are
     *         allowed to vary an amount less than or equal to the specified tolerance and still considered equal.
     *         A ClassCastException will be thrown if the two Mithra transactional objects are not of the same type
     */
    public boolean nonPrimaryKeyAttributesChanged(MithraTransactionalObject other, double toleranceForFloatingPointFields);

    public void zSetInserted();

    public void zSetUpdated(List<AttributeUpdateWrapper> updates);

    public void zSetDeleted();

    public void zApplyUpdateWrappers(List updateWrappers);

    public void zApplyUpdateWrappersForBatch(List updateWrappers);

    public boolean zIsSameObjectWithoutAsOfAttributes(MithraTransactionalObject other);

    public void zDeleteForRemote(int hierarchyDepth);

    public void zInsertForRemote(int hierarchyDepth);

    /**
     * set all nullable primitive attributes to null. The default for primitive attributes
     * is the java default. This is a convenience method to set them to null (the database
     * default).
     */
    public void setNullablePrimitiveAttributesToNull();

    public void copyNonPrimaryKeyAttributesFrom(MithraTransactionalObject from);

    public boolean zHasSameNullPrimaryKeyAttributes(MithraTransactionalObject other);

    /**
     * This method is used to check if the Mithra transactional object is in memmory
     * and has not been persisted.
     *
     * @return true if the transactional object has not been persisted.
     */
    public boolean isInMemoryAndNotInserted();


    /**
     * Creates a copy of the persistent object and marks it as detached. Detached objects can be modified
     * without affecting the database. The detached object can then be copied back to the persistent object (and
     * there the database) via the copyDetachedValuesToOriginalOrInsertIfNew method. Also see the isModified*,
     * resetFromOriginalPersistentObject and isDeletedOrMarkForDeletion methods.
     * The primary keys of a detached object may not be modified (unless they've been marked as mutable).
     *
     * @return the detached copy.
     */
    public MithraTransactionalObject getDetachedCopy();

    /**
     * Finds the persistent version of this object, if it exists, else null.
     * It uses the primary key of this to try to find such object
     *
     * @return the persistent version of this object, or null if it does not exists.
     */
    public MithraTransactionalObject getOriginalPersistentObject();

    /**
     * creates a copy of the object. Modifying the copy has no relation to the original persistent object or
     * the underlying database.
     *
     * @return a copy of the original.
     */
    public MithraTransactionalObject getNonPersistentCopy();

    public void zPersistDetachedChildDelete(MithraDataObject currentDataForRead);

    public void zClearUnusedTransactionalState(TransactionalState transactionalState);

    public boolean zEnrollInTransactionForWrite(TransactionalState prev, TransactionalState transactionalState);

    public boolean zEnrollInTransactionForRead(TransactionalState prev, MithraTransaction threadTx, int persistenceState);

    public boolean zEnrollInTransactionForDelete(TransactionalState prevState, TransactionalState transactionalState);

    public void zPrepareForRemoteInsert();

    public void zWaitForExclusiveWriteTx(MithraTransaction tx);

    public void zClearTempTransaction();

    public void zPrepareForDelete();

    public MithraTransactionalObject zCascadeCopyThenInsert();

    public void triggerUpdateHook(UpdateInfo updateInfo);

    public void zSetTxDetachedDeleted();

    public void zSetNonTxDetachedDeleted();

    public void zCascadeUpdateInPlaceBeforeTerminate();

    public Map<RelatedFinder, StatisticCounter> zAddNavigatedRelationshipsStats(RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats);
    public Map<RelatedFinder, StatisticCounter> zAddNavigatedRelationshipsStatsForUpdate(RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats);
    public Map<RelatedFinder, StatisticCounter> zAddNavigatedRelationshipsStatsForDelete(RelatedFinder parentFinder, Map<RelatedFinder, StatisticCounter> navigationStats);
}
