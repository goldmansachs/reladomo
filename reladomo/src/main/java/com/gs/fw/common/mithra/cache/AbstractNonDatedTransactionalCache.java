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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectFactory;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.behavior.AbstractTransactionalBehavior;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;



public abstract class AbstractNonDatedTransactionalCache extends AbstractNonDatedCache
{
    private static final NonNullMutableBoolean IRRELEVANT_DIRTY = new NonNullMutableBoolean();

    protected AbstractNonDatedTransactionalCache(Attribute[] pkAttributes, MithraObjectFactory factory, long timeToLive, long relationshipTimeToLive)
    {
        super(pkAttributes, factory, timeToLive, relationshipTimeToLive);
    }

    protected AbstractNonDatedTransactionalCache(Attribute[] pkAttributes, MithraObjectFactory factory, Attribute[] immutableAttributes, long timeToLive, long relationshipTimeToLive)
    {
        super(pkAttributes, factory, immutableAttributes, timeToLive, relationshipTimeToLive);
    }

    @Override
    public IndexReference getIndexRef(Attribute attribute)
    {
        IndexReference indexRef = this.getIndexRefForSingleAttribute(attribute);
        if (indexRef.isValid())
        {
            Index index = this.getIndices()[indexRef.indexReference-1];
            if (MithraManagerProvider.getMithraManager().isInTransaction() && !index.isUnique())
            {
                return this.noIndexReference;
            }
            return indexRef;
        }
        return this.noIndexReference;
    }

    @Override
    public IndexReference getBestIndexReference(List attributes)
    {
        IndexReference bestIndex = this.getBestIndex(attributes);
        if (bestIndex.isValid())
        {
            Index index = this.getIndices()[bestIndex.indexReference-1];
            if (MithraManagerProvider.getMithraManager().isInTransaction() && !index.isUnique())
            {
                return this.noIndexReference;
            }
            return bestIndex;
        }
        return this.noIndexReference;
    }

    @Override
    protected void reindexAffectedIndicesAndSetData(MithraObject object, MithraDataObject newData,
            UnifiedSet affectedIndicies, Object optionalBehavior)
    {
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        MithraTransaction tx = mithraManager.zGetCurrentTransactionWithNoCheck();
        if (tx == null)
        {
            super.reindexAffectedIndicesAndSetData(object, newData, affectedIndicies, optionalBehavior);
            return;
        }
        List<TransactionalIndex> prepared = FastList.newList(affectedIndicies.size());
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            TransactionalIndex index = (TransactionalIndex) it.next();
            if (index.prepareForReindex(object, tx))
            {
                prepared.add(index);
                it.remove();
            }
            else
            {
                index.removeIgnoringTransaction(object);
            }
        }
        object.zSetData(newData, optionalBehavior);
        for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
        {
            TransactionalIndex index = (TransactionalIndex) it.next();
            index.putIgnoringTransaction(object, newData, false);
        }
        for(int i=0;i<prepared.size();i++)
        {
            prepared.get(i).finishForReindex(object, tx);
        }
    }

    public void reindex(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        Boolean lock = null;
        try
        {
            Attribute attribute = updateWrapper.getAttribute();
            List affectedIndicies = Collections.EMPTY_LIST;
            if (monitoredAttributes.contains(attribute))
            {
                affectedIndicies = (List) this.attributeToIndexMap.get(attribute);
                if (affectedIndicies.size() > 0)
                {
                    this.readWriteLock.acquireWriteLock();
                    lock = Boolean.TRUE;
                    for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
                    {
                        Index index = (Index) it.next();
                        index.remove(object);
                    }
                }
            }
/*
             the indicies have now been updated with the new value, but the object still holds onto the old
             value. If the index is queried in this state, it can return the wrong results
             because of equality checking inside the index.
             Therefore, we must perform the object update under the cache's write lock
*/
            updateWrapper.updateData();
            if (affectedIndicies.size() > 0)
            {
                for (Iterator it = affectedIndicies.iterator(); it.hasNext();)
                {
                    Index index = (Index) it.next();
                    index.put(object);
                }
            }
        }
        finally
        {
            if (lock != null)
            {
                this.readWriteLock.release();
            }
        }
    }

    @Override
    public void reindexForTransaction(MithraObject object, AttributeUpdateWrapper updateWrapper)
    {
        Attribute attribute = updateWrapper.getAttribute();
        if (this.getMonitoredAttributes().contains(attribute))
        {
            List affectedIndicies = (List)this.getAttributeToIndexMap().get(attribute);
            if (affectedIndicies.size() > 0)
            {
                MithraManager mithraManager = MithraManagerProvider.getMithraManager();
                MithraTransaction tx = mithraManager.zGetCurrentTransactionWithNoCheck();
                try
                {
                    this.getCacheLock().acquireWriteLock();
                    for(Iterator it = affectedIndicies.iterator();it.hasNext();)
                    {
                        TransactionalIndex index = (TransactionalIndex)it.next();
                        index.prepareForReindexInTransaction(object, tx);
                    }
                    updateWrapper.updateData();
                    for(Iterator it = affectedIndicies.iterator();it.hasNext();)
                    {
                        TransactionalIndex index = (TransactionalIndex)it.next();
                        index.finishForReindex(object, tx);
                    }
                }
                finally
                {
                    this.getCacheLock().release();
                }
                return;
            }
        }
        updateWrapper.updateData();
    }

    @Override
    public void removeIgnoringTransaction(MithraObject object)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            Index[] indices = this.getIndices();
            for(int i=0;i<indices.length;i++)
            {
                TransactionalIndex index = (TransactionalIndex) indices[i];
                index.removeIgnoringTransaction(object);
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public Object preparePut(MithraObject obj)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            return ((TransactionalIndex)this.getPrimaryKeyIndex()).preparePut(obj);
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void commitPreparedForIndex(Object index)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            ((TransactionalIndex)this.getPrimaryKeyIndex()).commitPreparedForIndex(index);
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void commitObject(MithraTransactionalObject obj, MithraDataObject oldData)
    {
        MithraDataObject newData = obj.zGetTxDataForRead();
        try
        {
            this.getCacheLock().acquireWriteLock();
            Index[] indices = this.getIndices();
            for(int i=0;i<indices.length;i++)
            {
                TransactionalIndex index = (TransactionalIndex) indices[i];
                if (oldData != null) index.removeUsingUnderlying(oldData);
                index.putIgnoringTransaction(obj, newData, true);
            }
            // has to done under the cache lock
            obj.zSetData(newData, AbstractTransactionalBehavior.getPersistedNoTxBehavior());
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void commitRemovedObject(MithraDataObject data)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            Index[] indices = this.getIndices();
            for(int i=0;i<indices.length;i++)
            {
                TransactionalIndex index = (TransactionalIndex) indices[i];
                index.removeUsingUnderlying(data);
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void commit(MithraTransaction tx)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            Index[] indices = this.getIndices();
            for(int i=0;i<indices.length;i++)
            {
                TransactionalIndex index = (TransactionalIndex) indices[i];
                index.commit(tx);
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void prepareForCommit(MithraTransaction tx)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            ((TransactionalIndex)this.getPrimaryKeyIndex()).prepareForCommit(tx);
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    @Override
    public void rollback(MithraTransaction tx)
    {
        try
        {
            this.getCacheLock().acquireWriteLock();
            Index[] indices = this.getIndices();
            for(int i=0;i<indices.length;i++)
            {
                TransactionalIndex index = (TransactionalIndex) indices[i];
                index.rollback(tx);
            }
        }
        finally
        {
            this.getCacheLock().release();
        }
    }

    /* unsynchronized */
    protected Object addToIndiciesIgnoringTransaction(MithraObject result, boolean weak)
    {
        TransactionalIndex primaryKeyIndex = ((TransactionalIndex) this.getPrimaryKeyIndex());
        Object old = primaryKeyIndex.putIgnoringTransaction(result, result.zGetCurrentData(), weak);
        Index[] indices = this.getIndices();
        for(int i=1;i<indices.length;i++)
        {
            TransactionalIndex index = (TransactionalIndex) indices[i];
            index.putIgnoringTransaction(result, result.zGetCurrentData(), weak);
        }
        return old;
    }

    @Override
    protected void addToIndicesUnderLockAfterCreate(MithraObject result, boolean weak)
    {
        this.addToIndiciesIgnoringTransaction(result, weak);
    }

    @Override
    protected MithraObject getFromDataByPrimaryKeyForCreation(MithraDataObject data, NonNullMutableBoolean isDirty)
    {
        MithraObject result = (MithraObject) this.getPrimaryKeyIndex().getFromDataEvenIfDirty(data, isDirty);
        if (result == null)
        {
            result = (MithraObject) ((TransactionalIndex) this.getPrimaryKeyIndex()).getFromPreparedUsingData(data);
        }
        return result;
    }

    @Override
    public Object getObjectByPrimaryKey(MithraDataObject data, boolean evenIfDirty)
    {
        Boolean lock = null;
        MithraTransactionalObject result = null;
        try
        {
            this.getCacheLock().acquireReadLock();
            lock = Boolean.TRUE;
            if (evenIfDirty)
            {
                result = (MithraTransactionalObject) this.getPrimaryKeyIndex().getFromDataEvenIfDirty(data, IRRELEVANT_DIRTY);
            }
            else
            {
                result = (MithraTransactionalObject) this.getPrimaryKeyIndex().getFromData(data);
            }
            if (result == null)
            {
                result = (MithraTransactionalObject) ((TransactionalIndex) this.getPrimaryKeyIndex()).getFromPreparedUsingData(data);
            }
            this.getCacheLock().release();
            lock = null;
            if (result != null)
            {
                result.zLockForTransaction();
            }
        }
        finally
        {
            if (lock != null) this.getCacheLock().release();
        }
        return result;
    }

    @Override
    protected void updateCacheForNewObjects(List newDataList, List updatedDataList, FastList checkToReindexList)
    {
        NonNullMutableBoolean isDirty = new NonNullMutableBoolean();
        for (int i = 0; i < newDataList.size(); i++)
        {
            MithraDataObject data = (MithraDataObject) newDataList.get(i);
            MithraObject newObject = this.getFromDataByPrimaryKeyForCreation(data, isDirty);
            if (newObject == null)
            {
                newObject = this.getFactory().createObject(data);
                addToIndicesUnderLockAfterCreate(newObject, false);
            }
            else
            {
                if (isDirty.value)
                {
                    addToIndicesUnderLockAfterCreate(newObject, false);
                }
                updatedDataList.add(data);
                checkToReindexList.add(newObject);
            }
        }
    }

}
