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

package com.gs.fw.common.mithra.test.localtx;

import junit.framework.Assert;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


public class ExceptionThrowingXaResource implements XAResource
{


    private boolean throwExceptionOnCommit = false;
    private int expectedCommit = 1;
    private int expectedRollack = 0;
    private ExceptionThrowingXaResource sameRmResource;
    private int startMode;
    private int endMode;
    private boolean commitMode;
    private Xid xid;
    private Xid[] recoverXids = null;

    public ExceptionThrowingXaResource(boolean throwExceptionOnCommit)
    {
        this.throwExceptionOnCommit = throwExceptionOnCommit;
    }

    public void setSameRmResource(ExceptionThrowingXaResource sameRmResource)
    {
        this.sameRmResource = sameRmResource;
    }

    public void setExpectedCommit(int expectedCommit)
    {
        this.expectedCommit = expectedCommit;
    }

    public void setExpectedRollack(int expectedRollack)
    {
        this.expectedRollack = expectedRollack;
    }

    public int getTransactionTimeout() throws XAException
    {
        return 0;
    }

    public boolean setTransactionTimeout(int i) throws XAException
    {
        ExecutionOrder.addMethod(this, "setTransactionTimeout");
        return true;
    }

    public boolean isSameRM(XAResource xaResource) throws XAException
    {
        ExecutionOrder.addMethod(this, "isSameRM");
        return xaResource == this || xaResource == sameRmResource;
    }

    public Xid[] recover(int i) throws XAException
    {
        ExecutionOrder.addMethod(this, "recover");
        Xid[] result = this.recoverXids;
        this.recoverXids = null;
        return result;
    }

    public int prepare(Xid xid) throws XAException
    {
        ExecutionOrder.addMethod(this, "prepare");
        return 0;
    }

    public void forget(Xid xid) throws XAException
    {
        ExecutionOrder.addMethod(this, "forget");
    }

    public void rollback(Xid xid) throws XAException
    {
        ExecutionOrder.addMethod(this, "rollback");
        this.expectedRollack--;
        if (this.xid != null)
        {
            // this.xid can be null during recovery
            Assert.assertEquals(this.xid, xid);
        }
    }

    public void end(Xid xid, int i) throws XAException
    {
        ExecutionOrder.addMethod(this, "end");
        this.endMode = i;
        Assert.assertEquals(this.xid, xid);
    }

    public void start(Xid xid, int i) throws XAException
    {
        ExecutionOrder.addMethod(this, "start");
        this.startMode = i;
        this.xid = xid;
    }

    public void commit(Xid xid, boolean b) throws XAException
    {
        ExecutionOrder.addMethod(this, "commit");
        this.commitMode = b;
        this.expectedCommit--;
        if (this.xid != null)
        {
            // this.xid can be null during recovery
            Assert.assertEquals(this.xid, xid);
        }
        if (this.throwExceptionOnCommit)
        {
            throw new XAException("for testing.");
        }
    }

    public Xid getXid()
    {
        return xid;
    }

    public void setRecoverXids(Xid[] recoverXids)
    {
        this.recoverXids = recoverXids;
    }

    public void verify()
    {
        Assert.assertEquals(0, expectedCommit);
        Assert.assertEquals(0, expectedRollack);
    }

    public void verifyStartEndCommit(int startMode, int endMode, boolean commitMode)
    {
        Assert.assertEquals(startMode, this.startMode);
        Assert.assertEquals(endMode, this.endMode);
        Assert.assertEquals(commitMode, this.commitMode);
    }
}
