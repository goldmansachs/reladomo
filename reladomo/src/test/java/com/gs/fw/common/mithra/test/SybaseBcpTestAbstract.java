
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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;

import java.util.concurrent.Callable;

public class SybaseBcpTestAbstract extends MithraTestAbstract
{
    private static final String testDataFileName = "sybase/sybaseBcpTestData.txt";

    private MithraTestResource mithraTestResource;

    protected void setUp()
            throws Exception
    {
        // todo -- gonzra -- super.setUp();
        mithraTestResource = new MithraTestResource("SybaseBcpTestConfig.xml", getSybaseDatabaseType());
        mithraTestResource.setRestrictedClassList(this.getRestrictedClassList());

        SybaseTestConnectionManager connectionManager = SybaseTestConnectionManager.getInstance();
        connectionManager.setDefaultSource("mithra_qa");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());

        mithraTestResource.createSingleDatabase(connectionManager, "mithra_qa", testDataFileName);
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    public SybaseDatabaseType getSybaseDatabaseType()
    {
        return SybaseDatabaseType.getInstance();
    }

    protected void tearDown()
            throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
        super.tearDown();
    }

    protected Object runAsBcpTransaction(final Callable transactionBlock)
    {
        return MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx)
                    throws Throwable
            {
                tx.setBulkInsertThreshold(1);
                return transactionBlock.call();
            }
        });
    }

}
