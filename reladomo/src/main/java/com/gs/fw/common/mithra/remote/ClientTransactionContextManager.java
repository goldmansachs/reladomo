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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.transaction.TransactionLocal;

import javax.transaction.SystemException;
import javax.transaction.RollbackException;
import java.util.IdentityHashMap;



public class ClientTransactionContextManager
{

    private static final ClientTransactionContextManager INSTANCE = new ClientTransactionContextManager();

    private TransactionLocal clientContext = new TransactionLocal();

    private ClientTransactionContextManager()
    {
        // singleton
    }

    public static ClientTransactionContextManager getInstance()
    {
        return INSTANCE;
    }

    public ClientTransactionContext getClientTransactionContext(RemoteMithraService remoteService, MithraTransaction tx)
    {
        IdentityHashMap map = (IdentityHashMap) clientContext.get(tx);
        if (map == null)
        {
            return null;
        }
        return (ClientTransactionContext) map.get(remoteService);
    }

    public void setClientTransactionContext(RemoteMithraService remoteService, ClientTransactionContext context, MithraTransaction tx)
    {
        IdentityHashMap map = (IdentityHashMap) clientContext.get(tx);
        if (map == null)
        {
            map = new IdentityHashMap(2);
            this.clientContext.set(tx, map);
        }
        map.put(remoteService, context);
        try
        {
            MithraManagerProvider.getMithraManager().getCurrentTransaction().enlistResource(context);
        }
        catch (SystemException e)
        {
            throw new MithraTransactionException("could not enroll remote context", e);
        }
        catch (RollbackException e)
        {
            throw new MithraTransactionException("could not enroll remote context", e);
        }
    }

    public void clearClientTransactionContext(MithraTransaction tx)
    {
        this.clientContext.set(tx, null);
    }
}
