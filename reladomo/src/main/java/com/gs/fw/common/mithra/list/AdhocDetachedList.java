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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.querycache.CompactUpdateCountOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public class AdhocDetachedList extends TransactionalAdhocFastList
{
    private FastList removedItems;
    private Operation operation;
    private boolean separateDelete = false;

    public AdhocDetachedList()
    {
        // for externalizable
    }

    public AdhocDetachedList(DelegatingList originalList, int initialCapacity)
    {
        super(originalList, initialCapacity);
    }

    public void setOperation(Operation op)
    {
        this.operation = op;
        if (op instanceof CompactUpdateCountOperation)
        {
            this.operation = ((CompactUpdateCountOperation)op).forceGetCachableOperation();
        }
    }

     public void copyDetachedValuesToOriginalOrInsertIfNewOrDeleteIfRemoved()
     {
         MithraManagerProvider.getMithraManager().executeTransactionalCommand(
            new UpdateOriginalObjectsFromDetachedList(this, separateDelete ? null : removedItems,
                    cloneOperationBasedList()));
     }

    public void zCopyDetachedValuesDeleteIfRemovedOnly()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
           new UpdateOriginalObjectsFromDetachedList(null, removedItems, null));
        separateDelete = true;
    }

    private MithraList cloneOperationBasedList()
    {
        return this.operation != null ? this.operation.getResultObjectPortal().getFinder().findMany(this.operation) : null;
    }

    public void copyDetachedValuesToOriginalOrInsertIfNewOrTerminateIfRemoved()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
           new UpdateOriginalObjectsFromDetachedList(this, removedItems, cloneOperationBasedList()));
    }

    @Override
    public void cascadeUpdateInPlaceBeforeTerminate()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
           new InPlaceUpdateOriginalObjectsBeforeTerminate(this, removedItems, cloneOperationBasedList()));
    }

    public void copyDetachedValuesToOriginalUntilOrInsertIfNewUntilOrTerminateIfRemoved(Timestamp exclusiveUntil)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
           new UpdateOriginalObjectsUntilFromDetachedList(this, removedItems, cloneOperationBasedList(), exclusiveUntil));
    }

    @Override
    public boolean removeAll(Collection collection)
    {
        int currentSize = this.size();
        UnifiedSet set = new UnifiedSet(collection);
        int currentFilledIndex = 0;
        for (int i = 0; i < this.size; i++)
        {
            if (set.remove(this.items[i]))
            {
                removeHook(this.items[i]);
                this.items[i] = null;
            }
            else
            {
                // keep it
                if (currentFilledIndex != i)
                {
                    this.items[currentFilledIndex] = this.items[i];
                }
                currentFilledIndex++;
            }
        }
        this.size = currentFilledIndex;
        return currentSize != this.size();
    }

    public void cascadeDeleteAll()
    {
        this.clear();
    }

    public void terminateAll()
    {
        this.clear();
    }

    public void cascadeTerminateAll()
    {
        this.clear();
    }

    public void cascadeTerminateAllUntil(Timestamp exclusiveUntil)
    {
        this.clear();
    }

    protected void addRemovedItem(MithraTransactionalObject result)
    {
        if (result.zIsDetached())
        {
            if (this.removedItems == null)
            {
                this.removedItems = new FastList();
            }
            this.removedItems.add(result);
            if (result instanceof MithraDatedTransactionalObject)
            {
                ((MithraDatedTransactionalObject)result).terminate(); // will really do a cascadeTerminate when reattached
            }
            else
            {
                result.delete(); // will really do a cascadeDelete when reattached
            }
        }
    }

    @Override
    protected void removeHook(Object obj)
    {
        super.removeHook(obj);
        this.addRemovedItem((MithraTransactionalObject) obj);
    }

// remove(Object) is not overriden because FastList implementation calls remove(int)
//    public boolean remove(Object object)

    public void clear()
    {
        if (this.removedItems == null)
        {
            this.removedItems = new FastList(this.size());
        }
        for(int i=0;i < this.size(); i++)
        {
            MithraTransactionalObject result = (MithraTransactionalObject) this.get(i);
            this.addRemovedItem(result);
        }
        super.clear();
    }

    public void markAllAsRemovedExcept(List dontRemove)
    {
        for (int i = 0; i < this.size(); i++)
        {
            MithraTransactionalObject result = (MithraTransactionalObject) this.get(i);
            if (dontRemove == null || !dontRemove.contains(result))
            {
                this.addRemovedItem(result);
            }
        }
        super.clear();
    }

    public void forceRefresh()
    {
        // nothing to do, this is a detached list
    }

    public boolean isModifiedSinceDetachment()
    {
       if(this.removedItems != null && removedItems.size() > 0)
       {
           return true;
       }
       for(int i = 0; i < this.size();i++)
       {
           MithraTransactionalObject result = (MithraTransactionalObject) this.get(i);
           if(result.isModifiedSinceDetachmentByDependentRelationships())
           {
               return true;
           }
       }
       return false;
    }

    public void zMarkMoved(Object item)
    {
        int index = this.indexOf(item);
        if (index >= 0)
        {
            this.removeWithoutHook(index);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(removedItems);
        out.writeObject(operation);
        out.writeBoolean(separateDelete);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        this.removedItems = (FastList) in.readObject();
        this.operation = (Operation) in.readObject();
        this.separateDelete = in.readBoolean();
    }
}
