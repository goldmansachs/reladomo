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


public class BatchPurgeOperation extends TransactionOperation
{

    private final FastList objects;
    private transient FullUniqueIndex index;

    public BatchPurgeOperation(MithraTransactionalObject first, MithraTransactionalObject second, MithraObjectPortal portal)
    {
        super(first, portal);
        objects = new FastList();
        addObject(first);
        addObject(second);
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
        this.getPortal().getMithraObjectPersister().batchPurge(objects);
        for(int i=0;i<objects.size();i++)
        {
            MithraTransactionalObject obj = (MithraTransactionalObject) objects.get(i);
            obj.zSetDeleted();
        }
    }

    @Override
    public TransactionOperation combinePurge(MithraTransactionalObject incoming, MithraObjectPortal incomingPortal)
    {
        if (incomingPortal == this.getPortal())
        {
            addObject(incoming);
            return this;
        }
        return null;
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

}
