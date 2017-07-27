
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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.extractor.EmbeddedValueExtractor;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.list.merge.MergeBuffer;
import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.math.BigDecimal;


public class AbstractTransactionalNonOperationBasedList<E> extends AbstractNonOperationBasedList<E> implements MithraDelegatedTransactionalList<E>
{
    public static final AbstractTransactionalNonOperationBasedList DEFAULT = new AbstractTransactionalNonOperationBasedList();

    public AbstractTransactionalNonOperationBasedList()
    {
        //for Externalizable
    }

    protected AbstractTransactionalNonOperationBasedList newCopy()
    {
        return new AbstractTransactionalNonOperationBasedList();
    }

    @Override
    public void init(DelegatingList delegatingList)
    {
        delegatingList.zSetFastListOrCachedQuery(new TransactionalAdhocFastList(delegatingList));
    }

    @Override
    public void init(DelegatingList delegatingList, Collection c)
    {
        delegatingList.zSetFastListOrCachedQuery(new TransactionalAdhocFastList(delegatingList, c));
    }

    @Override
    public MithraDelegatedList getNonPersistentDelegate()
    {
        return AbstractTransactionalNonOperationBasedList.DEFAULT;
    }

    @Override
    public void init(DelegatingList delegatingList, int initialSize)
    {
        delegatingList.zSetFastListOrCachedQuery(new TransactionalAdhocFastList(delegatingList, initialSize));
    }

    @Override
    protected TransactionalAdhocFastList getFastList(DelegatingList delegatingList)
    {
        return (TransactionalAdhocFastList) super.getFastList(delegatingList);
    }

    public void insertAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).insertAll();
    }

    public void bulkInsertAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).bulkInsertAll();
    }

    public void cascadeInsertAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).cascadeInsertAll();
    }

    public void cascadeInsertAllUntil(DelegatingList delegatingList, Timestamp exclusiveUntil)
    {
        this.getFastList(delegatingList).cascadeInsertAllUntil(exclusiveUntil);
    }

    public void deleteAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).deleteAll();
    }

    public void deleteAllInBatches(DelegatingList delegatingList, int batchSize)
    {
        this.getFastList(delegatingList).deleteAllInBatches(batchSize);
    }

    public void cascadeDeleteAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).cascadeDeleteAll();
    }

    public void terminateAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).terminateAll();
    }

    public void cascadeTerminateAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).cascadeTerminateAll();
    }

    public void cascadeTerminateAllUntil(DelegatingList delegatingList, Timestamp exclusiveUntil)
    {
        this.getFastList(delegatingList).cascadeTerminateAllUntil(exclusiveUntil);
    }

    public void purgeAll(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).purgeAll();
    }


    public void purgeAllInBatches(DelegatingList delegatingList, int batchSize)
    {
        this.getFastList(delegatingList).purgeAllInBatches(batchSize);
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved();
    }

    public void zCopyDetachedValuesDeleteIfRemovedOnly(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).zCopyDetachedValuesDeleteIfRemovedOnly();
    }

    public void cascadeUpdateInPlaceBeforeTerminate(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).cascadeUpdateInPlaceBeforeTerminate();
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved();
    }

    public void copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(DelegatingList delegatingList, Timestamp exclusiveUntil)
    {
        this.getFastList(delegatingList).copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(exclusiveUntil);
    }

    public MithraDelegatedTransactionalList zSetAddHandler(DelegatingList delegatingList, DependentRelationshipAddHandler addHandler)
    {
        this.getFastList(delegatingList).zSetAddHandler(addHandler);
        return this;
    }

    public MithraDelegatedTransactionalList zSetRemoveHandler(DelegatingList delegatingList, DependentRelationshipRemoveHandler removeHandler)
    {
        this.getFastList(delegatingList).zSetRemoveHandler(removeHandler);
        return this;
    }

    public void setBoolean(DelegatingList delegatingList, BooleanAttribute attr, boolean newValue)
    {
        this.getFastList(delegatingList).setBoolean(attr, newValue);
    }

    public void setByte(DelegatingList delegatingList, ByteAttribute attr, byte newValue)
    {
        this.getFastList(delegatingList).setByte(attr, newValue);
    }

    public void setShort(DelegatingList delegatingList, ShortAttribute attr, short newValue)
    {
        this.getFastList(delegatingList).setShort(attr, newValue);
    }

    public void setChar(DelegatingList delegatingList, CharAttribute attr, char newValue)
    {
        this.getFastList(delegatingList).setChar(attr, newValue);
    }

    public void setInteger(DelegatingList delegatingList, IntegerAttribute attr, int newValue)
    {
        this.getFastList(delegatingList).setInteger(attr, newValue);
    }

    public void setLong(DelegatingList delegatingList, LongAttribute attr, long newValue)
    {
        this.getFastList(delegatingList).setLong(attr, newValue);
    }

    public void setFloat(DelegatingList delegatingList, FloatAttribute attr, float newValue)
    {
        this.getFastList(delegatingList).setFloat(attr, newValue);
    }

    public void setDouble(DelegatingList delegatingList, DoubleAttribute attr, double newValue)
    {
        this.getFastList(delegatingList).setDouble(attr, newValue);
    }

    public void setString(DelegatingList delegatingList, StringAttribute attr, String newValue)
    {
        this.getFastList(delegatingList).setString(attr, newValue);
    }

    public void setTimestamp(DelegatingList delegatingList, TimestampAttribute attr, Timestamp newValue)
    {
        this.getFastList(delegatingList).setTimestamp(attr, newValue);
    }

    public void setDate(DelegatingList delegatingList, DateAttribute attr, Date newValue)
    {
        this.getFastList(delegatingList).setDate(attr, newValue);
    }

    public void setTime(DelegatingList delegatingList, TimeAttribute attr, Time newValue)
    {
        this.getFastList(delegatingList).setTime(attr, newValue);
    }

    public void setByteArray(DelegatingList delegatingList, ByteArrayAttribute attr, byte[] newValue)
    {
        this.getFastList(delegatingList).setByteArray(attr, newValue);
    }

    public void setBigDecimal(DelegatingList delegatingList, BigDecimalAttribute attr, BigDecimal newValue)
    {
        this.getFastList(delegatingList).setBigDecimal(attr, newValue);
    }

    public void setAttributeNull(DelegatingList delegatingList, Attribute attr)
    {
        this.getFastList(delegatingList).setAttributeNull(attr);
    }

    public void setEvoValue(DelegatingList delegatingList, EmbeddedValueExtractor attr, Object newValue)
    {
        this.getFastList(delegatingList).setEvoValue(attr, newValue);
    }

    @Override
    public void merge(final DelegatingList<E> dbList, final MithraList<E> incoming, final TopLevelMergeOptions<E> mergeOptions)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {

                MergeBuffer mergeBuffer = new MergeBuffer(mergeOptions);
                mergeBuffer.mergeLists(dbList, incoming);
                mergeBuffer.executeBufferForPersistence();
                return null;
            }
        });
    }
}
