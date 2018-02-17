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

package com.gs.fw.common.mithra.transaction;

import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;



public class BatchInsertOperation extends TransactionOperation
{

    private final FastList objects;
    private transient FullUniqueIndex index;

    public BatchInsertOperation(MithraTransactionalObject first, MithraTransactionalObject second, MithraObjectPortal portal)
    {
        super(first, portal);
        objects = new FastList();
        addObject(first);
        addObject(second);
    }

    public BatchInsertOperation(MithraTransactionalObject mithraObject, List allObjects, MithraObjectPortal portal)
    {
        super(mithraObject, portal);
        objects = new FastList(allObjects);
        addObject(mithraObject);
    }

    private void addAll(TransactionOperation op)
    {
        List allObjects = op.getAllObjects();
        this.objects.addAll(allObjects);
        if (index != null) index.addAll(allObjects);
    }

    private void addObject(MithraTransactionalObject first)
    {
        objects.add(first);
        if (index != null) index.put(first);
    }

    @Override
    protected FullUniqueIndex getIndexedObjects()
    {
        if (index == null)
        {
            index = createFullUniqueIndex(objects);
        }
        return index;
    }

    @Override
    public int getTotalOperationsSize()
    {
        return objects.size();
    }

    @Override
    public void execute() throws MithraDatabaseException
    {
        if (objects.size() > 0)
        {
            int bulkInsertThreshold = MithraManagerProvider.getMithraManager().getCurrentTransaction().getBulkInsertThreshold();
            this.getPortal().getMithraObjectPersister().batchInsert(objects, bulkInsertThreshold);
            for(int i=0;i<objects.size();i++)
            {
                MithraTransactionalObject obj = (MithraTransactionalObject) objects.get(i);
                obj.zSetInserted();
            }
        }
    }

    @Override
    public TransactionOperation combineInsert(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        if (incomingPortal == this.getPortal())
        {
            addObject(obj);
            return this;
        }
        return null;
    }

    @Override
    public TransactionOperation combineDelete(MithraTransactionalObject incoming, MithraObjectPortal incomingPortal)
    {
        for(int i=0;i<objects.size();i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) objects.get(i);
            if (obj == incoming)
            {
                removeObject(i);
                return this;
            }
        }
        return null;
    }

    private void removeObject(int i)
    {
        Object removed = objects.remove(i);
        if (index != null) index.remove(removed);
    }

    @Override
    public TransactionOperation combinePurge(MithraTransactionalObject incoming, MithraObjectPortal incomingPortal)
    {
        MithraObjectPortal portal = this.getPortal();
        for(int i=0;i<objects.size();i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) objects.get(i);
            if (incomingPortal == portal && obj.zIsSameObjectWithoutAsOfAttributes(incoming))
            {
                obj.zSetInserted();
                removeObject(i);
                return null;
            }
        }
        return null;
    }

    @Override
    public List getAllObjects()
    {
        return objects;
    }

    @Override
    protected int getCombineDirectionForParent()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    protected int getCombineDirectionForChild()
    {
        return COMBINE_DIRECTION_BACKWARD;
    }

    @Override
    public boolean isInsert()
    {
        return true;
    }

    @Override
    public TransactionOperation combineInsertOperation(TransactionOperation op)
    {
        if (op.getPortal() == this.getPortal())
        {
            if (op instanceof InsertOperation)
            {
                addObject(op.getMithraObject());
            }
            else
            {
                this.addAll(op);
            }
            return this;
        }
        return null;
    }
}
