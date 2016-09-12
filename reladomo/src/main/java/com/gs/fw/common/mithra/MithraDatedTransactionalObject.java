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

import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.behavior.TemporalDirector;

import java.sql.Timestamp;



public interface MithraDatedTransactionalObject extends MithraDatedObject, MithraTransactionalObject
{

    public void zEnrollInTransactionForWrite(TemporalContainer container, MithraDataObject data, MithraTransaction threadTx);

    public boolean zEnrollInTransactionForWrite(DatedTransactionalState prevState, TemporalContainer container, MithraDataObject data, MithraTransaction threadTx);

    public TemporalDirector zGetTemporalDirector();

    public MithraDatedTransactionalObject zFindOriginal();

    public void zClearTxData();

    /**
     * inserts this dated object into the database. The object must not be persisted already.
     * This method will end the valid period of the object from the date the object was constructed with,
     * up to, but not including the date specified as the parameter.
     *
     * @param exclusiveUntil the exclusive until date.
     */
    public void insertUntil(Timestamp exclusiveUntil);


    /**
     * inserts this dated object and any dependent relationships that have been set on it.
     * This method will end the valid period of the object (and dependents) from the date the object
     * was constructed with, up to, but not including the date specified as the parameter.
     *
     * @param exclusiveUntil the exclusive until date.
     */
    public void cascadeInsertUntil(Timestamp exclusiveUntil);

    /**
     * Insert this dated object and increment any double attributes from this point forward. Only meaningful if
     * there is a business date defined and the insert is in the past.
     */
    public void insertWithIncrement();

    /**
     * Dated objects cannot be deleted. They can be terminated instead. This method will end
     * the valid period of the object. As any dated operation, the date of termination is taken from
     * the retrieval time for the business date dimension. The processing date dimension is as always
     * the transaction (real) time.
     */
    public void terminate();

    /**
     * Dated objects cannot be deleted. They can be terminated instead.
     * As any dated operation, the date of termination is taken from
     * the retrieval time for the business date dimension. The processing date dimension is as always
     * the transaction (real) time.This method will end
     * the valid period of the object from the date the object was retrieved, up to, but not including
     * the date specified as the parameter.
     *
     * @param exclusiveUntil the exclusive until date.
     */
    public void terminateUntil(Timestamp exclusiveUntil);

    /**
     * Purges all the data associated with this dated object. This method will delete
     * the data rows for all dates associated with this object with no audit trail.
     */
    public void purge();

    /**
     * when copying data to an archive database, it may be necessary to set the processingDateTo on
     * an existing record to a value from the live (non-archive) database. This method allows setting the
     * processingDateTo without affecting anything else about the record. It must only be used in archiving
     * scenarios.
     *
     * @param processingDateTo The value to set the processingDateTo to.
     * @param businessDateTo   The value to set the businessDateTo to. If this is null, businessDateTo is not modified
     */
    public void inactivateForArchiving(Timestamp processingDateTo, Timestamp businessDateTo);

    /**
     * Inserts this dated object and increments any double attribute until the specified date.
     *
     * @param exclusiveUntil The date as of which no more increments are to be applied.
     */
    public void insertWithIncrementUntil(Timestamp exclusiveUntil);

    /**
     * terminate this object and related objects that are marked (in the xml) as dependents.
     */
    public void cascadeTerminate();

    /**
     * terminate this object and related objects that are marked (in the xml) as dependents.
     * This method will end the valid period of the object from the date the object was retrieved,
     * up to, but not including the date specified as the parameter.
     *
     * @param exclusiveUntil the exclusive until date.
     */
    public void cascadeTerminateUntil(Timestamp exclusiveUntil);

    public void zCopyAttributesUntilFrom(MithraDataObject data, Timestamp exclusiveUntil);

    public void zPersistDetachedRelationshipsUntil(MithraDataObject data, Timestamp exclusiveUntil);

    public void zInsertRelationshipsUntil(MithraDataObject data, Timestamp exclusiveUntil);

    /**
     * This method is used to synchronize changes on detached dated objects back to the original dated object. Changes
     * to any double attribute are updated until the specified date.
     * This method is for detached objects only. You must not call this method for any
     * reason other than persisting the changes to a detached object.
     *
     * @param exclusiveUntil The date as of which the updates are to be applied.
     */
    public MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNewUntil(Timestamp exclusiveUntil);

    /**
     * This method is used to copy the values from the specified object. The updates are applied until the
     * the specified date.
     *
     * @param from  The object used to copy the values
     * @param until The date as of which the updates are to be applied
     */
    public void copyNonPrimaryKeyAttributesUntilFrom(MithraDatedTransactionalObject from, Timestamp until);

    public MithraDatedTransactionalObject getDetachedCopy();

    public MithraDatedTransactionalObject getNonPersistentCopy();

    public MithraDatedTransactionalObject getOriginalPersistentObject();

    public MithraDatedTransactionalObject copyDetachedValuesToOriginalOrInsertIfNew();

    public void zClearUnusedTransactionalState(DatedTransactionalState prevState);

    public boolean zEnrollInTransactionForRead(DatedTransactionalState prev, MithraTransaction threadTx, int persistenceState);

    public boolean zIsDataDeleted();

    public boolean zIsTxDataDeleted();

    public void zCascadeUpdateInPlaceBeforeTerminate(MithraDataObject detachedData);
}
