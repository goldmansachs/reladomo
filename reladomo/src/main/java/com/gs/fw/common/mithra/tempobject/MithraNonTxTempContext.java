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

import com.gs.fw.common.mithra.*;


public class MithraNonTxTempContext extends AbstractMithraTemporaryContext implements TransactionLifeCycleListener
{

    public MithraNonTxTempContext(TempContextContainer container)
    {
        super(container);
    }

    public void destroy()
    {
        MithraObjectPortal portal = this.getMithraObjectPortal();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().getCurrentTransaction();
        if (tx == null)
        {
            this.getDbObject().dropTempTable(null);
        }
        else
        {
            tx.registerLifeCycleListener(this);
        }
        this.getContainer().clearCurrentContext();
        portal.incrementClassUpdateCount();
        this.setDestroyed(true);
    }

    public void associateToCurrentThread()
    {
        if (this.isSingleThreaded())
        {
            throw new RuntimeException("The database type is configured for un-shared table usage and cannot be used from multiple threads");
        }
        this.getContainer().associateToCurrentThread(this);
    }

    protected void createTable()
    {
        this.getDbObject().createSharedTempTable(null, this);
    }

    public void beforeCommit()
    {
        this.getDbObject().dropTempTable(null);
    }

    public void beforeRollback()
    {
        this.getDbObject().dropTempTable(null);
    }
}
