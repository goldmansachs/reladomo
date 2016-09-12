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
import com.gs.fw.common.mithra.util.Time;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.Date;
import java.math.BigDecimal;


public class TransactionalAdhocFastList extends AdhocFastList
{
    private transient DependentRelationshipAddHandler addHandler = null;

    public TransactionalAdhocFastList()
    {
        //for Externalizable
    }

    public TransactionalAdhocFastList(DelegatingList originalList)
    {
        super(originalList);
    }

    public TransactionalAdhocFastList(DelegatingList originalList, Collection c)
    {
        super(originalList, c);
    }

    public TransactionalAdhocFastList(DelegatingList originalList, int initialCapacity)
    {
        super(originalList, initialCapacity);
    }

    @Override
    protected void addHook(Object obj)
    {
        super.addHook(obj);
        if (this.addHandler != null)
        {
            this.addHandler.addRelatedObject((MithraTransactionalObject) obj);
        }
    }

    public boolean addAll(int index, Collection c)
    {
        boolean result = super.addAll(index, c);
        if (this.addHandler !=  null)
        {
            for(Iterator it = c.iterator(); it.hasNext(); )
            {
                this.addHandler.addRelatedObject((MithraTransactionalObject) it.next());
            }
        }
        return result;
    }

    public boolean addAll(Collection c)
    {
        boolean result = super.addAll(c);
        if (this.addHandler !=  null)
        {
            for(Iterator it = c.iterator(); it.hasNext(); )
            {
                this.addHandler.addRelatedObject((MithraTransactionalObject) it.next());
            }
        }
        return result;
    }

    public void insertAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new InsertAllTransactionalCommand(this));
    }

    public void bulkInsertAll()
    {
        MithraManager manager = MithraManagerProvider.getMithraManager();
        manager.executeTransactionalCommand(new InsertAllTransactionalCommand(this, 100), 0);
    }

    public void cascadeInsertAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeInsertAllTransactionalCommand(this));
    }

    public void cascadeInsertAllUntil(Timestamp exclusiveUntil)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeInsertAllUntilTransactionalCommand(this, exclusiveUntil));
    }

    public void deleteAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new DeleteAllOneByOneCommand(this));
    }

    public void deleteAllInBatches(int batchSize)
    {
        verifyNotInTransaction();
        final int[] bSize = new int[1];
        bSize[0] = batchSize;
        TransactionalCommand command = new DeleteAllInBatchesForNonOperationListCommand(this, bSize);
        executeCommandInBatches(bSize, command);
    }

    private void executeCommandInBatches(int[] batchSize, TransactionalCommand command)
    {
        int affectedRows;
        long sleepTime = 2000;

        int retryCount = 5;
        do
        {
            try
            {
                do
                {
                   affectedRows = (Integer) MithraManagerProvider.getMithraManager().executeTransactionalCommand(command, 0);
                }
                while(affectedRows >= batchSize[0]);

                retryCount = 0;
            }
            catch (Throwable throwable)
            {
                retryCount = handleRetryException(throwable, retryCount);
                try
                {
                    Thread.sleep(sleepTime);
                    sleepTime *= 2;
                }
                catch(InterruptedException e)
                {
                    //log something here
                }
            }
        }
        while(retryCount > 0);
    }

    private void verifyNotInTransaction()
    {
        if (MithraManagerProvider.getMithraManager().isInTransaction())
        {
            throw new MithraTransactionException("deleteAllInBatches cannot be called from another transaction!");
        }
    }

    private int handleRetryException(Throwable t, int retryCount)
    {
       --retryCount;
       if(retryCount == 0)
       {
           throw new MithraBusinessException("Retried the transaction but still failing. This could be happening due to issues with the transaction log");
       }
       return retryCount;
    }


    public void cascadeDeleteAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeDeleteAllOneByOneCommand(this));
    }

    public void terminateAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TerminateAllTransactionalCommand(this));
    }

    public void cascadeTerminateAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeTerminateAllTransactionalCommand(this));
    }

    public void cascadeTerminateAllUntil(Timestamp exclusiveUntil)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new CascadeTerminateAllUntilTransactionalCommand(this, exclusiveUntil));
    }

    public void purgeAll()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new PurgeAllForNonOperationListCommand(this) );

    }


    public void purgeAllInBatches(int batchSize)
    {
        throw new RuntimeException("Not implemented");
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new UpdateOriginalObjectsFromDetachedList(this, null, null));
    }

    public void zCopyDetachedValuesDeleteIfRemovedOnly()
    {
        // nothing to do
    }

    public void cascadeUpdateInPlaceBeforeTerminate()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new InPlaceUpdateOriginalObjectsBeforeTerminate(this, null, null));
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new UpdateOriginalObjectsFromDetachedList(this, null, null));
    }

    public void copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(Timestamp exclusiveUntil)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new UpdateOriginalObjectsUntilFromDetachedList(this, null, null, exclusiveUntil));
    }

    public void zSetAddHandler(DependentRelationshipAddHandler addHandler)
    {
        this.addHandler = addHandler;
    }

    public void zSetRemoveHandler(DependentRelationshipRemoveHandler removeHandler)
    {
        throw new RuntimeException("Can't set remove handler for non-operation based list");
    }

    public void setBoolean(BooleanAttribute attr, boolean newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setBooleanValue(this.get(i), newValue);
        }
    }

    public void setByte(ByteAttribute attr, byte newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setByteValue(this.get(i), newValue);
        }
    }

    public void setShort(ShortAttribute attr, short newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setShortValue(this.get(i), newValue);
        }
    }

    public void setChar(CharAttribute attr, char newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setCharValue(this.get(i), newValue);
        }
    }

    public void setInteger(IntegerAttribute attr, int newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setIntValue(this.get(i), newValue);
        }
    }

    public void setLong(LongAttribute attr, long newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setLongValue(this.get(i), newValue);
        }
    }

    public void setFloat(FloatAttribute attr, float newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setFloatValue(this.get(i), newValue);
        }
    }

    public void setDouble(DoubleAttribute attr, double newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setDoubleValue(this.get(i), newValue);
        }
    }

    public void setString(StringAttribute attr, String newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setStringValue(this.get(i), newValue);
        }
    }

    public void setTimestamp(TimestampAttribute attr, Timestamp newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setTimestampValue(this.get(i), newValue);
        }
    }

    public void setDate(DateAttribute attr, Date newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setDateValue(this.get(i), newValue);

        }
    }

    public void setTime(TimeAttribute attr, Time newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setTimeValue(this.get(i), newValue);
        }
    }

    public void setByteArray(ByteArrayAttribute attr, byte[] newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setByteArrayValue(this.get(i), newValue);
        }
    }

    public void setBigDecimal(BigDecimalAttribute attr, BigDecimal newValue)
    {
        for(int i=0;i<size();i++)
        {
            attr.setBigDecimalValue(this.get(i), newValue);
        }
    }

    public void setAttributeNull(Attribute attr)
    {
        for(int i=0;i<size();i++)
        {
            attr.setValueNull(this.get(i));
        }
    }

    public void setEvoValue(EmbeddedValueExtractor attr, Object evo)
    {
        for(int i=0;i<size();i++)
        {
            attr.setValue(this.get(i), evo);
        }
    }
}
