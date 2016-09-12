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

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraTransaction;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;



public class ClientTransactionContext implements XAResource
{

    private RemoteMithraService remoteMithraService;
    private RemoteTransactionId remoteTransactionId;
    private Xid xid;
    private boolean establishedContext = false;
    private MithraTransaction owner;
    //todo: rezaem: need to add the list of remote XA resources in the transaction

    public ClientTransactionContext(RemoteMithraService remoteMithraService, MithraTransaction tx)
    {
        this.remoteMithraService = remoteMithraService;
        this.owner = tx;
    }

    public void setRemoteTransactionId(RemoteTransactionId remoteTransactionId)
    {
        if (this.owner != null && this.remoteTransactionId == null)
        {
            this.remoteTransactionId = remoteTransactionId;
            establishedContext = true;
            ClientTransactionContextManager.getInstance().setClientTransactionContext(remoteMithraService, this, owner);
        }
    }

    public Xid getXid()
    {
        return xid;
    }

    public RemoteTransactionId getRemoteTransactionId()
    {
        return remoteTransactionId;
    }

    public void commit(Xid xid, boolean onePhase) throws XAException
    {
        if (!establishedContext)
        {
            throw new XAException("could not commit remote transaction. remote transaction id was never set");
        }
        long requestorVmId = MithraManager.getInstance().getNotificationEventManager().getMithraVmId();
        remoteTransactionId.setRequestorVmId(requestorVmId);
        remoteMithraService.commit(remoteTransactionId, onePhase);
        ClientTransactionContextManager.getInstance().clearClientTransactionContext(this.owner);
    }

    public void end(Xid xid, int i) throws XAException
    {
        // nothing to do
    }

    public void forget(Xid xid) throws XAException
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public int getTransactionTimeout() throws XAException
    {
        throw new RuntimeException("not implemented");
    }

    public int getTransactionTimeoutWithoutException()
    {
        if (owner == null)
        {
            return RemoteMithraService.NO_TRANSACTION;
        }
        return (int)(owner.getTimeoutInMilliseconds()/1000);
    }

    public boolean isSameRM(XAResource xaResource) throws XAException
    {
        if (xaResource instanceof ClientTransactionContext)
        {
            final ClientTransactionContext other = (ClientTransactionContext) xaResource;
            return this.remoteTransactionId.equals(other.remoteTransactionId);
        }
        return false;
    }

    public int prepare(Xid xid) throws XAException
    {
        // todo: rezaem: we should relay this to the other side
        return XAResource.XA_OK;
    }

    public Xid[] recover(int i) throws XAException
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void rollback(Xid xid) throws XAException
    {
        if (establishedContext)
        {
            remoteMithraService.rollback(remoteTransactionId);
        }
        ClientTransactionContextManager.getInstance().clearClientTransactionContext(this.owner);
    }

    public boolean setTransactionTimeout(int i) throws XAException
    {
        // todo: rezaem: implement not implemented method
        throw new RuntimeException("not implemented");
    }

    public void start(Xid xid, int i) throws XAException
    {
        this.xid = xid;
    }
}
