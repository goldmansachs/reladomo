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

package com.gs.fw.common.mithra.behavior.persisted;

import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransaction;import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.util.Time;



public class DatedPersistedDifferentTxBehavior extends DatedPersistedBehavior
{
    public DatedPersistedDifferentTxBehavior()
    {
        super(DATA_UNEXPECTED, DATA_UNEXPECTED, false);
    }

    public DatedTransactionalBehavior enrollInTransaction(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should never get here");
    }

    public DatedTransactionalBehavior enrollInTransactionForWrite(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should never get here");
    }

    public DatedTransactionalBehavior enrollInTransactionForDelete(MithraDatedTransactionalObject mithraObject, MithraTransaction tx, DatedTransactionalState prevState, int persistenceState)
    {
        throw new RuntimeException("should never get here");
    }

    public void resetDetachedData(MithraDatedTransactionalObject mithraObject, MithraDataObject newData)
    {
        throw new RuntimeException("should never get here");
    }

    public void terminate(MithraDatedTransactionalObject obj)
    {
        throw new RuntimeException("should never get here");
    }

    public void purge(MithraDatedTransactionalObject obj)
    {
        throw new RuntimeException("should never get here");
    }

    public void update(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject update(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public void inPlaceUpdate(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly)
    {
        throw new RuntimeException("should never get here");
    }

    public void increment(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr)
    {
        throw new RuntimeException("should never get here");
    }

    public void increment(MithraDatedTransactionalObject obj, BigDecimal increment, boolean readOnly, BigDecimalAttribute attr)
    {
        throw new RuntimeException("should never get here");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, double increment, boolean isReadonly, DoubleAttribute attr, Timestamp until)
    {
        throw new RuntimeException("should never get here");
    }

    public void incrementUntil(MithraDatedTransactionalObject obj, BigDecimal increment, boolean isReadonly, BigDecimalAttribute attr, Timestamp until)
    {
        throw new RuntimeException("should never get here");
    }

    public void updateUntil(MithraDatedTransactionalObject obj, AttributeUpdateWrapper updateWrapper, boolean isReadonly, Timestamp until, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BooleanAttribute attr, boolean newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteArrayAttribute attr, byte[] newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ByteAttribute attr, byte newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, CharAttribute attr, char newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DateAttribute attr, Date newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimeAttribute attr, Time newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, DoubleAttribute attr, double newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, BigDecimalAttribute attr, BigDecimal newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, FloatAttribute attr, float newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, IntegerAttribute attr, int newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, LongAttribute attr, long newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, ShortAttribute attr, short newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, StringAttribute attr, String newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public MithraDataObject updateUntil(MithraDatedTransactionalObject obj, TimestampAttribute attr, Timestamp newValue, Timestamp exclusiveUntil, boolean readOnly, boolean triggerHook)
    {
        throw new RuntimeException("should never get here");
    }

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, Timestamp processingDateTo, Timestamp businessDateTo)
    {
        throw new RuntimeException("should never get here");
    }

    public void terminateUntil(MithraDatedTransactionalObject obj, Timestamp until)
    {
        throw new RuntimeException("should never get here");
    }
}
