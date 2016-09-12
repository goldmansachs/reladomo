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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.transaction.InTransactionDatedTransactionalObject;
import com.gs.fw.common.mithra.util.InternalList;

import java.sql.Timestamp;
import java.util.List;



public interface TemporalContainer
{

    public InTransactionDatedTransactionalObject makeUninsertedDataActiveAndCreateObject(MithraDataObject data);

    public InternalList getObjectsForRange(MithraDatedTransactionalObject mithraObject, Timestamp startRange, Timestamp endRange);

    public void inactivateObject(InTransactionDatedTransactionalObject obj);

    public void clearAllObjects();

    public void addObjectForTx(InTransactionDatedTransactionalObject toInsert);

    public MithraDataObject getActiveDataFor(Timestamp businessDate);

    public MithraDataObject getDataForTxByDates(MithraDataObject mithraDataObject, Timestamp[] asOfDates);

    public MithraDataObject getCommitedDataFor(MithraDatedTransactionalObject mithraObject);

    public boolean isInactivatedOrSplit(Timestamp businessDate);

    public void deleteInTxObject(InTransactionDatedTransactionalObject inTxObject);

    public void voidData(MithraDataObject activeData);

    public void updateInPlaceData(MithraDataObject activeData, MithraDataObject updatedData);

    public void setAnyData(MithraDataObject uninsertedData);

    public void addActiveData(MithraDataObject data);

    // used by cache
    public MithraDataObject getCommittedDataFromData(MithraDataObject data);

    public MithraDataObject getCommittedDataFromDates(Timestamp[] asOfDates);

    public MithraDataObject getActiveDataFromData(MithraDataObject data);

    public MithraDataObject getActiveOrInactiveDataFromData(MithraDataObject data);

    public void addCommittedData(MithraDataObject data);

    public MithraTransaction getTransaction();

    public MithraDataObject getTxDataFor(MithraDatedTransactionalObject mithraObject);

    public MithraDataObject getAnyData();

    public List getForDateRange(MithraDataObject mithraDataObject, Timestamp start, Timestamp end);

    public InTransactionDatedTransactionalObject getObjectForTx(MithraDataObject mithraDataObject);

    public InternalList getInTxObjects();

    public void setInfiniteRange();

    public void lockForTransaction(MithraDatedTransactionalObject businessObject, MithraDataObject transactionalData,
            MithraDataObject committedData, boolean setAsCurrent);
    
    public boolean enrollReadOnly(MithraDatedTransactionalObject businessObjectResult,
            MithraDataObject committedData, DatedTransactionalState prevState);

    public boolean enrollForWrite(MithraDatedTransactionalObject businessObject,
            MithraDataObject committedData, DatedTransactionalState prevState);

    public boolean possiblyEnroll(MithraDatedTransactionalObject businessObject, MithraDataObject transactionalData,
            MithraDataObject committedData, DatedTransactionalState prevState);

    public void checkInactivated(MithraDatedTransactionalObject mithraObject);

    public void enrollInWrite(MithraDataObject committedData);

    public void checkInactivatedForDelete(MithraDatedTransactionalObject mithraObject);
}
