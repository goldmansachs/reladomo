
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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.extractor.EmbeddedValueExtractor;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;
import java.util.Date;
import java.math.BigDecimal;


public interface MithraDelegatedTransactionalList<E> extends MithraDelegatedList<E>
{

    void insertAll(DelegatingList<E> delegatingList);
    void bulkInsertAll(DelegatingList<E> delegatingList);
    void deleteAll(DelegatingList<E> delegatingList);
    void deleteAllInBatches(DelegatingList<E> delegatingList, int batchSize);
    void terminateAll(DelegatingList<E> delegatingList);
    void purgeAll(DelegatingList<E> delegatingList);
    void purgeAllInBatches(DelegatingList<E> delegatingList, int batchSize);

    /**
     * This method must only be used with a detached list. A detached list keeps track of which objects have been
     * added and removed and can insert added objects and delete removed ones.
     *
     * This method is not appropriate for dated objects. use copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved
     * instead.
     * @param delegatingList
     */
    void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved(DelegatingList<E> delegatingList);

    /**
     * This method must only be used with a detached list. A detached list keeps track of which objects have been
     * added and removed and can insert added objects and terminate removed ones.
     *
     * This method is for dated objects only.
     * @param delegatingList
     */
    void copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved(DelegatingList<E> delegatingList);

    void copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(DelegatingList<E> delegatingList, Timestamp exclusiveUntil);

    MithraDelegatedTransactionalList<E> zSetRemoveHandler(DelegatingList<E> delegatingList, DependentRelationshipRemoveHandler removeHandler);

    void cascadeInsertAll(DelegatingList<E> delegatingList);

    void cascadeInsertAllUntil(DelegatingList<E> delegatingList, Timestamp exclusiveUntil);

    void cascadeDeleteAll(DelegatingList<E> delegatingList);

    void cascadeTerminateAll(DelegatingList<E> delegatingList);

    void cascadeTerminateAllUntil(DelegatingList<E> delegatingList, Timestamp exclusiveUntil);

    MithraDelegatedTransactionalList<E> zSetAddHandler(DelegatingList<E> delegatingList, DependentRelationshipAddHandler addHandler);

    public void zCopyDetachedValuesDeleteIfRemovedOnly(DelegatingList<E> delegatingList);

    public void cascadeUpdateInPlaceBeforeTerminate(DelegatingList<E> delegatingList);

    public void setBoolean(DelegatingList<E> delegatingList, BooleanAttribute attr, boolean newValue);

    public void setByte(DelegatingList<E> delegatingList, ByteAttribute attr, byte newValue);

    public void setShort(DelegatingList<E> delegatingList, ShortAttribute attr, short newValue);

    public void setChar(DelegatingList<E> delegatingList, CharAttribute attr, char newValue);

    public void setInteger(DelegatingList<E> delegatingList, IntegerAttribute attr, int newValue);

    public void setLong(DelegatingList<E> delegatingList, LongAttribute attr, long newValue);

    public void setFloat(DelegatingList<E> delegatingList, FloatAttribute attr, float newValue);

    public void setDouble(DelegatingList<E> delegatingList, DoubleAttribute attr, double newValue);

    public void setString(DelegatingList<E> delegatingList, StringAttribute attr, String newValue);

    public void setTimestamp(DelegatingList<E> delegatingList, TimestampAttribute attr, Timestamp newValue);

    public void setDate(DelegatingList<E> delegatingList, DateAttribute attr, Date newValue);

    public void setTime(DelegatingList<E> delegatingList, TimeAttribute attr, Time newValue);

    public void setByteArray(DelegatingList<E> delegatingList, ByteArrayAttribute attr, byte[] newValue);

    public void setBigDecimal(DelegatingList<E> delegatingList, BigDecimalAttribute attr, BigDecimal newValue);

    public void setAttributeNull(DelegatingList<E> delegatingList, Attribute attr);

    public void setEvoValue(DelegatingList<E> delegatingList, EmbeddedValueExtractor attribute, Object evo);
}
