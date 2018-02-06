/*
  Copyright 2018 Goldman Sachs.
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

package com.gs.reladomo.jms;

import java.util.Arrays;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.impl.set.mutable.UnifiedSet;

public class InMemoryXaResource implements XAResource
{
    private InMemoryXaSession inMemoryXaSession;

    private Xid currentXid;

    private UnifiedSet<InMemoryTopicState> callbacks = new UnifiedSet<InMemoryTopicState>();

    public InMemoryXaResource(InMemoryXaSession inMemoryXaSession)
    {
        this.inMemoryXaSession = inMemoryXaSession;
    }

    public Xid getCurrentXid()
    {
        return currentXid;
    }

    @Override
    public void commit(final Xid xid, boolean b) throws XAException
    {
        if (this.currentXid == null || !Arrays.equals(xid.getGlobalTransactionId(), this.currentXid.getGlobalTransactionId()))
        {
            throw new RuntimeException("not implemented");
        }
        callbacks.forEach(new Procedure<InMemoryTopicState>() {
            @Override
            public void value(InMemoryTopicState each)
            {
                each.commit(xid);
            }
        });
        this.currentXid = null;
        callbacks.clear();
    }

    @Override
    public void end(Xid xid, int i) throws XAException
    {
    }

    @Override
    public void forget(Xid xid) throws XAException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getTransactionTimeout() throws XAException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException
    {
        return false;
    }

    @Override
    public int prepare(Xid xid) throws XAException
    {
        return XAResource.XA_OK;
    }

    @Override
    public Xid[] recover(int i) throws XAException
    {
        return null; // todo:
    }

    @Override
    public synchronized void rollback(final Xid xid) throws XAException
    {
        if (this.currentXid == null || !Arrays.equals(xid.getGlobalTransactionId(), this.currentXid.getGlobalTransactionId()))
        {
            throw new RuntimeException("not implemented");
        }
        callbacks.forEach(new Procedure<InMemoryTopicState>() {
            @Override
            public void value(InMemoryTopicState each)
            {
                each.rollback(xid);
            }
        });
        this.currentXid = null;
        callbacks.clear();
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public synchronized void start(Xid xid, int i) throws XAException
    {
        if (currentXid != null)
        {
            throw new RuntimeException("not implemented");
        }
        this.currentXid = xid;
    }

    public synchronized void registerCallback(InMemoryTopicState inMemoryTopicState)
    {
        this.callbacks.add(inMemoryTopicState);
    }
}
