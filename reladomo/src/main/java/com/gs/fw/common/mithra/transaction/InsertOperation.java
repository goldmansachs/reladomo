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

package com.gs.fw.common.mithra.transaction;

import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;



public class InsertOperation extends TransactionOperation
{

    public InsertOperation(MithraTransactionalObject mithraObject, MithraObjectPortal portal)
    {
        super(mithraObject, portal);
    }

    @Override
    public void execute() throws MithraDatabaseException
    {
        this.getPortal().getMithraObjectPersister().insert(this.getMithraObject().zGetTxDataForRead());
        this.getMithraObject().zSetInserted();
    }

    @Override
    public TransactionOperation combineDelete(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        if (obj == this.getMithraObject())
        {
            return DoNothingTransactionOperation.getInstance();
        }
        return null;
    }

    @Override
    public TransactionOperation combineInsert(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        if (incomingPortal == this.getPortal())
        {
            return new BatchInsertOperation(this.getMithraObject(), obj, incomingPortal);
        }
        return null;
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
            return new BatchInsertOperation(this.getMithraObject(), op.getAllObjects(), this.getPortal());
        }
        return null;
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
    public TransactionOperation combinePurge(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        if (incomingPortal == this.getPortal() &&
            obj.zIsSameObjectWithoutAsOfAttributes(this.getMithraObject()))
        {
            this.getMithraObject().zSetInserted();
            return new PurgeOperation(obj, incomingPortal);
        }
        return null;
    }
}
