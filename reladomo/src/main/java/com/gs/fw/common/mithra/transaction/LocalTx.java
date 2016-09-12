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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.util.InternalList;

import javax.transaction.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Arrays;


public class LocalTx implements Transaction
{
    private static final Logger logger = LoggerFactory.getLogger(LocalTx.class.getName());

    private static final TxStatus ACTIVE = new TxStatusActive();
    private static final TxStatus MARKED_ROLLBACK = new TxStatusMarkedRollback();
    private static final TxStatus ROLLING_BACK = new TxStatusRollingBack();
    private static final TxStatus ROLLED_BACK = new TxStatusRolledBack();
    private static final TxStatus PREPARING = new TxStatusPreparing();
    private static final TxStatus COMMITTING = new TxStatusCommitting();
    private static final TxStatus COMMITTED = new TxStatusCommitted();

    private boolean timedout = false;
    private boolean asyncCommitOrRollback = false;
    private short currentBranch = 0;
    private final InternalList resourceManagers = new InternalList(2);
    private final InternalList activeXaResources = new InternalList(3);
    private final InternalList synchronizations = new InternalList(2);
    private LocalXid xid;
    private TxStatus status = ACTIVE;
    private Throwable rollbackCause;
    private LocalTm localTm;
    private long timeToDie;

    public LocalTx(int timeout, LocalTm tm)
    {
        this.timeToDie = System.currentTimeMillis() + timeout * 1000;
        this.xid = new LocalXid(this.currentBranch);
        this.localTm = tm;
    }

    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException
    {
        this.status.preEnlistCheck(this);
        return this.status.enlistResource(this, xaRes);
    }

    public boolean isAsyncCommitOrRollback()
    {
        return asyncCommitOrRollback;
    }

    protected void setAsyncCommitOrRollback(boolean asyncCommitOrRollback)
    {
        this.asyncCommitOrRollback = asyncCommitOrRollback;
    }

    private TxGroup removeByCommitter(XAResource res)
    {
        for(int i=0;i<activeXaResources.size();i++)
        {
            TxGroup transactionBranch = (TxGroup) activeXaResources.get(i);
            XAResource other = transactionBranch.getResource();
            if (res == other || res.equals(other))
            {
                activeXaResources.remove(i);
                return transactionBranch;
            }
        }
        return null;
    }

    public synchronized boolean delistResource(XAResource xaResource, int flag) throws IllegalStateException, SystemException
    {
        if (flag == XAResource.TMSUSPEND)
        {
            throw new SystemException("suspend not supported");
        }
        flag = this.status.preDelistCheck(this, flag);

        TxGroup manager = this.removeByCommitter(xaResource);
        if (manager == null)
        {
            throw new IllegalStateException("Cannot delist a resource that's not enlisted " + xaResource);
        }

        try
        {
            xaResource.end(manager.getBranchXid(), flag);
            return true;
        }
        catch (XAException e)
        {
            logger.warn("Unable to delist XAResource " + xaResource + ", error code: " + e.errorCode, e);
            this.status = MARKED_ROLLBACK;
            return false;
        }
    }

    private void beforeCompletion()
    {
        try
        {
            for(int i=0;i<this.synchronizations.size();i++)
            {
                Synchronization s = (Synchronization) this.synchronizations.get(i);
                s.beforeCompletion();
            }
        }
        catch(Throwable t)
        {
            logger.error("The synchronization before completion failed. marked for rollback ", t);
            this.status = MARKED_ROLLBACK;
        }
    }

    private void bestEffortEndResources()
    {
        for(int i=0;i<activeXaResources.size();i++)
        {
            TxGroup branch = (TxGroup) activeXaResources.get(i);
            try
            {
                branch.getResource().end(branch.getBranchXid(), this.status.getEndFlag());
            }
            catch (Throwable e)
            {
                logger.error("could not call end on XaResource "+branch.getResource(), e);
                this.status = MARKED_ROLLBACK;
            }
        }
    }

    private void rollbackResources() throws SystemException
    {
        this.status = ROLLING_BACK;
        SystemException cause = null;

        for (int i=0; i < resourceManagers.size(); i++)
        {
            TxGroup manager = (TxGroup) resourceManagers.get(i);
            try
            {
                manager.getResource().rollback(manager.getBranchXid());
            }
            catch (Throwable e)
            {
                logger.error("Unexpected exception rolling back " + manager.getResource() + "; continuing with rollback", e);
                if (cause == null)
                {
                    cause = new SystemException("Unexpected exception rolling back " + manager.getResource());
                    cause.initCause(e);
                }
            }
        }
        this.status = ROLLED_BACK;
        if (cause != null)
        {
            throw cause;
        }
    }

    private void commitResources() throws SystemException
    {
        SystemException cause = null;
        boolean rollbackInstead = false;
        for (int i = 0; i < resourceManagers.size(); i++)
        {
            TxGroup branch = (TxGroup) resourceManagers.get(i);
            try
            {
                branch.getResource().commit(branch.getBranchXid(), i == 0);
            }
            catch (Throwable t)
            {
                logger.error("Unexpected exception committing " + branch.getResource() + "; continuing to commit", t);
                if (cause == null)
                {
                    cause = new SystemException("Unexpected exception committing " + branch.getResource());
                    cause.initCause(t);
                }
                if (i == 0)
                {
                    // not too late yet, we can still rollback
                    rollbackInstead = true;
                    this.rollbackCause = cause;
                    break;
                }
            }
        }
        if (rollbackInstead)
        {
            this.status = ROLLING_BACK;
            this.status.commitOrPossiblyRollback(this);
        }
        else
        {
            status = COMMITTED;
            if (cause != null)
            {
                throw cause;
            }
        }
    }

    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException
    {
        this.status.prePrepareCheck(this);
        bestEffortEndResources();
        this.status.beforeCompletion(this);
        // at this point, our status is either ACTIVE or MARKED_ROLLBACK
        this.status.chooseStateForPrepare(this);
        // possible statuses are now PREPARING, COMMITTED, COMMITTING, ROLLING_BACK
        this.status.prepare(this);
        // possible statuses are now COMMITTED, COMMITTING, ROLLING_BACK
        try
        {
            this.status.commitOrPossiblyRollback(this);
        }
        finally
        {
            // possible statuses are now COMMITTED, ROLLED_BACK
            this.status.afterCompletion(this);
            localTm.removeTransactionFromThread(this);
        }
        this.status.postCommitCheck(this);
    }

    private void afterCompletion(int status)
    {
        for(int i=0;i<this.synchronizations.size();i++)
        {
            Synchronization synch = (Synchronization) synchronizations.get(i);
            try
            {
                synch.afterCompletion(status);
            }
            catch(Throwable t)
            {
                this.logger.error("error calling afterCompletion on synch "+synch, t);
            }
        }
    }

    private Xid makeNewBranch()
    {
        this.currentBranch++;
        return this.xid.makeNewBranch(this.currentBranch);
    }

    public int getStatus() throws SystemException
    {
        return this.status.getStatus(this);
    }

    public void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException
    {
        this.status.registerSynchronization(this, synchronization);
    }

    public void rollback() throws IllegalStateException, SystemException
    {
        this.status.preRollbackCheck(this);
        bestEffortEndResources();
        // at this point, our status is MARKED_ROLLBACK
        this.status.chooseRollbackState(this);
        try
        {
            this.status.rollback(this);
        }
        finally
        {
            this.status.afterCompletion(this);
            localTm.removeTransactionFromThread(this);
        }
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        this.status.setRollbackOnly(this);
    }

    private static class TxGroup
    {
        private final XAResource resource;
        private final Xid branchXid;

        public TxGroup(XAResource res, Xid branchId)
        {
            resource = res;
            this.branchXid = branchId;
        }

        public XAResource getResource()
        {
            return resource;
        }

        public Xid getBranchXid()
        {
            return branchXid;
        }
    }


    private static class LocalXid implements Xid, Externalizable
    {
        private static volatile int currentGlobalId = 100;
        private static final long serialVersionUID = 936789320783279383L;


        private byte[] branchId = new byte[2];
        private byte[] globalId;
        private static long UNIQUE_ID;

        static
        {
            long time = System.currentTimeMillis();

            try
            {
                InetAddress inetadr = InetAddress.getLocalHost();
                byte[] tmp = inetadr.getAddress();
                UNIQUE_ID = ((tmp[3]*255+tmp[2])*255+tmp[1])*255+tmp[0];
            }
            catch (Exception e)
            {
                // let's just fill it with something random:
                UNIQUE_ID  = (int)(Math.random()*Integer.MAX_VALUE);
            }
            UNIQUE_ID ^= time;
            UNIQUE_ID ^= ((long)System.identityHashCode(new Object())) << 32;
        }

        public LocalXid()
        {
            // for externalizable
        }

        public LocalXid(short currentBranch)
        {
            int global;
            synchronized(LocalXid.class)
            {
                global = currentGlobalId++;
            }
            this.globalId = new byte[] {(byte) 0x70, (byte) 0x6E, (byte) 0x65, (byte) 0x79, (byte) 0x20,
                    (byte) 0x76, (byte) 0x66, (byte) 0x20, (byte) 0x6E, (byte) 0x20, (byte) 0x6F, (byte) 0x62,
                    (byte) 0x61, (byte) 0x72, (byte) 0x75, (byte) 0x72, (byte) 0x6E, (byte) 0x71,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0};
            putInt(globalId, globalId.length - 12, (int) (UNIQUE_ID >> 32));
            putInt(globalId, globalId.length - 8, (int) UNIQUE_ID);
            putInt(globalId, globalId.length - 4, global);
            putShort(branchId, 0, currentBranch);
        }

        public LocalXid(byte[] globalId, short currentBranch)
        {
            this.globalId = globalId;
            putShort(branchId, 0, currentBranch);
        }

        static void putInt(byte[] b, int off, int val)
        {
            b[off + 3] = (byte) (val >>> 0);
            b[off + 2] = (byte) (val >>> 8);
            b[off + 1] = (byte) (val >>> 16);
            b[off + 0] = (byte) (val >>> 24);
        }

        static void putShort(byte[] b, int off, short val)
        {
            b[off + 1] = (byte) (val >>> 0);
            b[off + 0] = (byte) (val >>> 8);
        }


        public int getFormatId()
        {
            return 108;
        }

        public byte[] getBranchQualifier()
        {
            return this.branchId;
        }

        public byte[] getGlobalTransactionId()
        {
            return this.globalId;
        }

        public Xid makeNewBranch(short currentBranch)
        {
            return new LocalXid(this.globalId, currentBranch);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
        {
            int len = in.readByte();
            this.globalId = new byte[len];
            in.readFully(this.globalId);
            len = in.readByte();
            this.branchId = new byte[len];
            in.readFully(this.branchId);
        }

        public void writeExternal(ObjectOutput out) throws IOException
        {
            out.writeByte(this.globalId.length);
            out.write(this.globalId);
            out.writeByte(this.branchId.length);
            out.write(this.branchId);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final LocalXid localXid = (LocalXid) o;

            if (!Arrays.equals(branchId, localXid.branchId)) return false;
            if (!Arrays.equals(globalId, localXid.globalId)) return false;

            return true;
        }

        public int hashCode()
        {
            return  (((int)globalId[globalId.length - 4]) << 24) |
                    (((int)globalId[globalId.length - 3]) << 16) |
                    (((int)globalId[globalId.length - 2]) << 8) |
                    (((int)globalId[globalId.length - 1]));
        }
    }

    private abstract static class TxStatus
    {
        public abstract int getStatus(LocalTx tx);

        public void preEnlistCheck(LocalTx localTx) throws RollbackException
        {
            throw new IllegalStateException("cannot enlist with status "+this.getClass().getName());
        }

        public boolean enlistResource(LocalTx tx, XAResource xaResource) throws SystemException
        {
            throw new IllegalStateException("cannot enlist with status "+this.getClass().getName());
        }

        public int preDelistCheck(LocalTx tx, int incomingFlag)
        {
            throw new IllegalStateException("cannot delist with status "+this.getClass().getName());
        }

        public void prePrepareCheck(LocalTx tx)
        {
            throw new IllegalStateException("cannot commit with status "+this.getClass().getName());
        }

        public abstract int getEndFlag();

        public void beforeCompletion(LocalTx localTx)
        {
            // nothing to do
        }

        public void chooseStateForPrepare(LocalTx localTx) throws SystemException
        {
            throw new SystemException("unexpected state "+this.getClass().getName());
        }

        public void prepare(LocalTx localTx) throws SystemException
        {
            throw new SystemException("unexpected state "+this.getClass().getName());
        }

        public void commitOrPossiblyRollback(LocalTx tx) throws SystemException
        {
            throw new SystemException("unexpected commit "+this.getClass().getName());
        }

        public void afterCompletion(LocalTx tx) throws SystemException
        {
            throw new SystemException("unexpected afterCompletion "+this.getClass().getName());
        }

        public void registerSynchronization(LocalTx tx, Synchronization synchronization)
        {
            throw new IllegalStateException("can't register synchronization due to transaction state "+this.getClass().getName());
        }

        public void postCommitCheck(LocalTx localTx) throws RollbackException
        {
            throw new IllegalStateException("can't postCommitCheck due to transaction state "+this.getClass().getName());
        }

        public void setRollbackOnly(LocalTx tx)
        {
            throw new IllegalStateException("can't setRollbackOnly due to transaction state "+this.getClass().getName());
        }

        public void preRollbackCheck(LocalTx tx)
        {
            throw new IllegalStateException("can't preRollbackCheck due to transaction state "+this.getClass().getName());
        }

        public void chooseRollbackState(LocalTx tx)
        {
            throw new IllegalStateException("can't chooseRollbackState due to transaction state "+this.getClass().getName());
        }

        public void rollback(LocalTx tx) throws SystemException
        {
            throw new IllegalStateException("can't rollback due to transaction state "+this.getClass().getName());
        }
    }

    private static class TxStatusActive extends TxStatus
    {
        @Override
        public void registerSynchronization(LocalTx tx, Synchronization synchronization)
        {
            if (checkTimeout(tx))
            {
                tx.status.registerSynchronization(tx, synchronization);
            }
            else
            {
                tx.synchronizations.add(synchronization);
            }
        }

        private boolean checkTimeout(LocalTx tx)
        {
            if (System.currentTimeMillis() > tx.timeToDie)
            {
                tx.status = MARKED_ROLLBACK;
                tx.timedout = true;
                return true;
            }
            return false;

        }

        @Override
        public int getStatus(LocalTx tx)
        {
            if (checkTimeout(tx))
            {
                return Status.STATUS_MARKED_ROLLBACK;
            }
            else
            {
                return Status.STATUS_ACTIVE;
            }
        }

        @Override
        public void preEnlistCheck(LocalTx tx) throws RollbackException
        {
            if (checkTimeout(tx))
            {
                tx.status.preEnlistCheck(tx);
            }
        }

        private boolean containsByCommitter(InternalList all, XAResource res)
        {
            for(int i=0;i<all.size();i++)
            {
                XAResource other = ((TxGroup) all.get(i)).getResource();
                if (res == other || res.equals(other)) return true;
            }
            return false;
        }

        @Override
        public boolean enlistResource(LocalTx tx, XAResource xaResource) throws SystemException
        {
            if (checkTimeout(tx))
            {
                tx.status.enlistResource(tx, xaResource);
            }
            if (containsByCommitter(tx.activeXaResources, xaResource))
            {
                throw new IllegalStateException("XAResource: " + xaResource + " is already enlisted!");
            }

            try
            {
                for (int i = 0; i < tx.resourceManagers.size(); i++)
                {
                    TxGroup branch = (TxGroup) tx.resourceManagers.get(i);
                    boolean sameRM = false;
                    //if the resource is already known, can't enlist it again.
                    if (xaResource == branch.getResource() || xaResource.equals(branch.getResource()))
                    {
                        throw new IllegalStateException("XAResource " + xaResource + " was already delisted. Can't re-enlist!");
                    }
                    sameRM = xaResource.isSameRM(branch.getResource());
                    if (sameRM)
                    {
                        xaResource.start(branch.getBranchXid(), XAResource.TMJOIN);
                        tx.activeXaResources.add(new TxGroup(xaResource, branch.getBranchXid()));
                        return true;
                    }
                }
                //we know nothing about this XAResource or resource manager
                Xid branchId = tx.makeNewBranch();
                xaResource.start(branchId, XAResource.TMNOFLAGS);
                TxGroup branch = new TxGroup(xaResource, branchId);
                tx.activeXaResources.add(branch);
                if (xaResource instanceof LocalTm.SinglePhaseResource)
                {
                    tx.resourceManagers.add(0, branch);
                }
                else
                {
                    tx.resourceManagers.add(branch);
                }
                return true;
            }
            catch (XAException e)
            {
                logger.warn("Unable to enlist XAResource " + xaResource + ", errorCode: " + e.errorCode, e);
                return false;
            }
            catch(Throwable t)
            {
                SystemException systemException = new SystemException("Unable to enlist XAResource " + xaResource);
                systemException.initCause(t);
                throw systemException;
            }
        }

        @Override
        public int preDelistCheck(LocalTx tx, int incomingFlag)
        {
            if (incomingFlag == XAResource.TMFAIL)
            {
                tx.status = MARKED_ROLLBACK;
            }
            return incomingFlag;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMSUCCESS;
        }

        @Override
        public void prePrepareCheck(LocalTx tx)
        {
            if (checkTimeout(tx))
            {
                tx.status.prePrepareCheck(tx);
            }
        }

        @Override
        public void beforeCompletion(LocalTx localTx)
        {
            localTx.beforeCompletion();
        }

        @Override
        public void chooseStateForPrepare(LocalTx tx) throws SystemException
        {
            if (tx.resourceManagers.size() == 0)
            {
                tx.status = COMMITTED;
            }
            else if (tx.resourceManagers.size() == 1)
            {
                // one-phase commit decision
                tx.status = COMMITTING;
            }
            else
            {
                // start prepare part of two-phase
                tx.status = PREPARING;
            }
        }

        @Override
        public void setRollbackOnly(LocalTx tx)
        {
            tx.status = MARKED_ROLLBACK;
        }

        @Override
        public void preRollbackCheck(LocalTx tx)
        {
            tx.status = MARKED_ROLLBACK;
        }
    }

    private static class TxStatusMarkedRollback extends TxStatus
    {
        @Override
        public int getStatus(LocalTx tx)
        {
            return Status.STATUS_MARKED_ROLLBACK;
        }

        @Override
        public void preEnlistCheck(LocalTx localTx) throws RollbackException
        {
            String msg = "transaction is marked rollback only";
            if (localTx.timedout)
            {
                msg += " due to timeout";
            }
            throw new RollbackException(msg);
        }

        @Override
        public int preDelistCheck(LocalTx tx, int incomingFlag)
        {
            return XAResource.TMFAIL;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prePrepareCheck(LocalTx tx)
        {
            // ok, we'll just rollback instead
        }

        @Override
        public void chooseStateForPrepare(LocalTx localTx) throws SystemException
        {
            localTx.status = ROLLING_BACK;
        }

        @Override
        public void setRollbackOnly(LocalTx tx)
        {
            // nothing to do
        }

        @Override
        public void preRollbackCheck(LocalTx tx)
        {
            // nothing to do
        }

        @Override
        public void chooseRollbackState(LocalTx tx)
        {
            tx.status = ROLLING_BACK;
        }
    }

    private static class TxStatusRollingBack extends TxStatus
    {
        @Override
        public int getStatus(LocalTx tx)
        {
            return Status.STATUS_ROLLING_BACK;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(LocalTx localTx) throws SystemException
        {
            // nothing to do, we'll rollback soon
        }

        @Override
        public void commitOrPossiblyRollback(LocalTx tx) throws SystemException
        {
            tx.rollbackResources();
        }

        @Override
        public void rollback(LocalTx tx) throws SystemException
        {
            tx.rollbackResources();
        }
    }

    private static class TxStatusRolledBack extends TxStatus
    {
        @Override
        public int getStatus(LocalTx tx)
        {
            return Status.STATUS_ROLLEDBACK;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void afterCompletion(LocalTx tx) throws SystemException
        {
            tx.afterCompletion(Status.STATUS_ROLLEDBACK);
        }

        @Override
        public void postCommitCheck(LocalTx tx) throws RollbackException
        {
            String msg = "commit did not succeed. Rolled back instead. ";
            if (tx.timedout)
            {
                msg += " rollback was due to timeout";
            }
            RollbackException ex = new RollbackException(msg);
            if (tx.rollbackCause != null)
            {
                ex.initCause(tx.rollbackCause);
            }
            throw ex;
        }

        @Override
        public void preRollbackCheck(LocalTx tx)
        {
            // nothing to do
        }

        @Override
        public void chooseRollbackState(LocalTx tx)
        {
            // nothing to do
        }

        @Override
        public void rollback(LocalTx tx) throws SystemException
        {
            // nothing to do
        }
    }

    private static class TxStatusPreparing extends TxStatus
    {
        @Override
        public int getStatus(LocalTx tx)
        {
            return Status.STATUS_PREPARING;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(LocalTx tx) throws SystemException
        {
            int i = -1;
            try
            {
                for(i = 1; i < tx.resourceManagers.size();)
                {
                    TxGroup branch = (TxGroup) tx.resourceManagers.get(i);
                    int prepareState = branch.getResource().prepare(branch.getBranchXid());
                    if (prepareState == XAResource.XA_RDONLY)
                    {
                        tx.resourceManagers.remove(i);
                    }
                    else i++;
                }
                tx.status = COMMITTING;
            }
            catch (Throwable e)
            {
                tx.resourceManagers.remove(i); // resource threw an exception and is presumed rolled back
                tx.rollbackCause = e;
                tx.status = ROLLING_BACK;
            }
        }
    }

    private static class TxStatusCommitted extends TxStatus
    {
        @Override
        public int getStatus(LocalTx tx)
        {
            return Status.STATUS_COMMITTED;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(LocalTx localTx) throws SystemException
        {
            // all done, nothing to do
        }

        @Override
        public void commitOrPossiblyRollback(LocalTx tx) throws SystemException
        {
            // all done, nothing to do
        }

        @Override
        public void afterCompletion(LocalTx tx) throws SystemException
        {
            tx.afterCompletion(Status.STATUS_COMMITTED);
        }

        @Override
        public void postCommitCheck(LocalTx localTx)
        {
            // all good
        }
    }

    private static class TxStatusCommitting extends TxStatus
    {
        @Override
        public int getStatus(LocalTx tx)
        {
            return Status.STATUS_COMMITTING;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(LocalTx localTx) throws SystemException
        {
            // no need to prepare, we'll commit soon
        }

        @Override
        public void commitOrPossiblyRollback(LocalTx tx) throws SystemException
        {
            tx.commitResources();
        }
    }
}
