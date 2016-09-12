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

package com.gs.fw.common.mithra.tempobject;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionLifeCycleListener;


public class MithraTxTempContext extends AbstractMithraTemporaryContext implements TransactionLifeCycleListener
{

    private MithraTransaction tx;

    public MithraTxTempContext(TempContextContainer container, MithraTransaction tx)
    {
        super(container);
        this.tx = tx;
        this.tx.registerLifeCycleListener(this);
    }

    public void destroy()
    {
        try
        {
            tx.executeBufferedOperations();
        }
        finally
        {
            this.getDbObject().dropTempTable(null);
            this.getContainer().clearTxContext(tx);
            this.getMithraObjectPortal().incrementClassUpdateCount();
            this.setDestroyed(true);
        }
    }

    public void associateToCurrentThread()
    {
        throw new MithraBusinessException("Temporary objects in a transactional context are bound to the transaction, not the thread");
    }

    protected void createTable()
    {
        this.getDbObject().createNonSharedTempTable(null);
    }

    public void beforeCommit()
    {
        if (!this.isDestroyed()) this.destroy();
    }

    public void beforeRollback()
    {
        if (!this.isDestroyed()) this.destroy();
    }
}
