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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;



public class BatchDeleteOperation extends TransactionOperation
{
    private final FastList objects;
    private final boolean deleteQuietly;
    private transient FullUniqueIndex index;

    public BatchDeleteOperation(MithraTransactionalObject first, MithraTransactionalObject second, MithraObjectPortal portal)
    {
        super(first, portal);
        objects = new FastList();
        addObject(first);
        addObject(second);
        this.deleteQuietly = false;
    }

    public BatchDeleteOperation(MithraTransactionalObject first, List more, MithraObjectPortal portal)
    {
        this(first, more, portal, false);
    }

    public BatchDeleteOperation(MithraTransactionalObject first, List more, MithraObjectPortal portal, boolean deleteQuietly)
    {
        super(first, portal);
        objects = new FastList();
        addObject(first);
        objects.addAll(more);
        this.deleteQuietly = deleteQuietly;
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
        if (this.deleteQuietly)
        {
            this.getPortal().getMithraObjectPersister().batchDeleteQuietly(objects);
        }
        else
        {
            this.getPortal().getMithraObjectPersister().batchDelete(objects);
        }
        for(int i=0;i<objects.size();i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) objects.get(i);
            obj.zSetDeleted();
        }
    }

    @Override
    public List getAllObjects()
    {
        return this.objects;
    }


    @Override
    protected int getCombineDirectionForParent()
    {
        return COMBINE_DIRECTION_BACKWARD;
    }

    @Override
    protected int getCombineDirectionForChild()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    public TransactionOperation combineDelete(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        if (incomingPortal == this.getPortal() && (this.getMithraObject().zHasSameNullPrimaryKeyAttributes(obj)))
        {
            addObject(obj);
            return this;
        }
        return null;
    }

    @Override
    public boolean isDelete()
    {
        return true;
    }

    @Override
    public TransactionOperation combineDeleteOperation(TransactionOperation op)
    {
        if (this.getPortal() == op.getPortal() && (this.getMithraObject().zHasSameNullPrimaryKeyAttributes(op.getMithraObject())))
        {
            addAll(op);
            return this;
        }
        return null;
    }
}
