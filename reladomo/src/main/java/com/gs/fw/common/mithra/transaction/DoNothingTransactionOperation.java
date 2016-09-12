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



public class DoNothingTransactionOperation extends TransactionOperation
{

    private static DoNothingTransactionOperation instance = new DoNothingTransactionOperation();

    public static DoNothingTransactionOperation getInstance()
    {
        return instance;
    }

    private DoNothingTransactionOperation()
    {
        super(null, null);
    }

    @Override
    public void execute() throws MithraDatabaseException
    {
        // nothing to do
    }

    @Override
    public TransactionOperation combineInsert(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        return new InsertOperation(obj, incomingPortal);
    }

    @Override
    public TransactionOperation combineDelete(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        return new DeleteOperation(obj, incomingPortal);
    }

    @Override
    public TransactionOperation combineUpdate(TransactionOperation op)
    {
        return op;
    }

    @Override
    public int getPassThroughDirection(TransactionOperation next)
    {
        return COMBINE_DIRECTION_BACKWARD | COMBINE_DIRECTION_FORWARD;
    }

    @Override
    protected int getCombineDirectionForParent()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    protected int getCombineDirectionForChild()
    {
        return COMBINE_DIRECTION_FORWARD;
    }

    @Override
    public TransactionOperation combinePurge(MithraTransactionalObject obj, MithraObjectPortal incomingPortal)
    {
        return new PurgeOperation(obj, incomingPortal);
    }
}
