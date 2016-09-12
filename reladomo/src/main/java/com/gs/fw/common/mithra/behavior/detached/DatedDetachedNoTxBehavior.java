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

package com.gs.fw.common.mithra.behavior.detached;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.behavior.inmemory.DatedInMemoryNoTxBehavior;
import com.gs.fw.common.mithra.behavior.state.DatedPersistedState;
import com.gs.fw.common.mithra.behavior.state.DatedPersistenceState;

import java.sql.Timestamp;
import java.math.BigDecimal;


public class DatedDetachedNoTxBehavior extends DatedInMemoryNoTxBehavior
{
    public DatedDetachedNoTxBehavior()
    {
        super((byte) DatedPersistenceState.DETACHED, true, false);
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        throw new RuntimeException("detached increment not implemented");
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr)
    {
        throw new RuntimeException("detached increment not implemented");
    }

    public void insertWithIncrement(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("you may not insert a detached object!");
    }

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("you may not insert a detached object!");
    }

    public void insertUntil(MithraDatedTransactionalObject obj, Timestamp exclusiveUntil)
    {
        throw new MithraBusinessException("you may not insert a detached object!");
    }

    public void insert(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("you may not insert a detached object!");
    }

    public MithraDatedTransactionalObject updateOriginalOrInsert(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Please start a transaction before performing dated updates.");
    }

    public void cascadeUpdateInPlaceBeforeTerminate(MithraDatedTransactionalObject obj)
    {
        throw new MithraBusinessException("Please start a transaction before performing dated updates.");
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        obj.zSetNonTxDetachedDeleted();
    }

    public boolean isModifiedSinceDetachment(MithraDatedTransactionalObject obj)
    {
        MithraDatedTransactionalObject mithraTransactionalObject = (MithraDatedTransactionalObject) obj.zFindOriginal();
        if (mithraTransactionalObject == null) return true;
        return mithraTransactionalObject.zIsDataChanged(this.getCurrentDataForRead(obj));
    }
}
