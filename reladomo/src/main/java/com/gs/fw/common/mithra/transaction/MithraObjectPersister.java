
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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.behavior.txparticipation.TxParticipationMode;
import com.gs.fw.common.mithra.portal.MithraObjectReader;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.MutableInteger;
import com.gs.fw.common.mithra.util.PersisterId;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.List;


public interface MithraObjectPersister extends MithraObjectReader
{

    void update(MithraTransactionalObject mithraObject, AttributeUpdateWrapper wrapper) throws MithraDatabaseException;

    void update(MithraTransactionalObject mithraObject, List updateWrappers) throws MithraDatabaseException;

    void insert(MithraDataObject mithraDataObject) throws MithraDatabaseException;

    void delete(MithraDataObject mithraDataObject) throws MithraDatabaseException;

    void purge(MithraDataObject mithraDataObject) throws MithraDatabaseException;

    void batchInsert(List mithraObjects, int bulkInsertThreshold) throws MithraDatabaseException;

    void batchDelete(List mithraObjects) throws MithraDatabaseException;

    /**
     * Batch deletes objects but does not check that the number of rows deleted matches the expected number. This
     * should only be used for cleaning up bad data (duplicates and invalid milestoning)
     */
    void batchDeleteQuietly(List mithraObjects) throws MithraDatabaseException;

    void batchPurge(List mithraObjects) throws MithraDatabaseException;

    List findForMassDelete(Operation op, boolean forceImplicitJoin);

    void deleteUsingOperation(Operation op);

    int deleteBatchUsingOperation(Operation op, int batchSize);

    void batchUpdate(BatchUpdateOperation batchUpdateOperation);

    void multiUpdate(MultiUpdateOperation multiUpdateOperation);

    public void prepareForMassDelete(Operation op, boolean forceImplicitJoin);

    public void prepareForMassPurge(Operation op, boolean forceImplicitJoin);

    public void prepareForMassPurge(List mithraObjects);

    public void setTxParticipationMode(TxParticipationMode mode, MithraTransaction tx);
}
