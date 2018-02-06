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
package com.gs.fw.common.mithra.transaction;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.factory.Lists;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.util.MithraProcessInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MultiThreadedTx implements Transaction
{
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadedTx.class.getName());

    private static final Future<Boolean> TRUE_FUTURE = new ImmediateFuture<Boolean>(true);
    private static final Future<Boolean> FALSE_FUTURE = new ImmediateFuture<Boolean>(false);

    private static final TxStatus ACTIVE = new TxStatusActive();
    private static final TxStatus MARKED_ROLLBACK = new TxStatusMarkedRollback();
    private static final TxStatus ROLLING_BACK = new TxStatusRollingBack();
    private static final TxStatus ROLLED_BACK = new TxStatusRolledBack();
    private static final TxStatus PREPARING = new TxStatusPreparing();
    private static final TxStatus COMMITTING = new TxStatusCommitting();
    private static final TxStatus COMMITTED = new TxStatusCommitted();

    private volatile boolean timedout = false;
    private boolean asyncCommitOrRollback = false;
    private short currentBranch = 0;
    private final FastList<TxGroup> resourceManagers = FastList.newList();
    private final FastList<TxGroup> activeXaResources = FastList.newList();
    private final FastList<Synchronization> synchronizations = FastList.newList();
    private LocalXid xid;
    private AtomicReference<TxStatus> status = new AtomicReference<TxStatus>(ACTIVE);
    private Throwable rollbackCause;
    private MultiThreadedTm multiThreadedTm;
    private final long timeToDie;

    public MultiThreadedTx(int timeout, MultiThreadedTm tm)
    {
        this.timeToDie = System.currentTimeMillis() + timeout * 1000;
        this.xid = new LocalXid(this.currentBranch);
        this.multiThreadedTm = tm;
    }

    public byte[] getGlobalTransactionId()
    {
        return xid.getGlobalTransactionId();
    }

    public synchronized boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException
    {
        this.status.get().preEnlistCheck(this);
        return this.status.get().enlistResource(this, xaRes);
    }

    public synchronized Future<Boolean> enlistResource(FutureXaResource xaRes) throws IllegalStateException, RollbackException, SystemException
    {
        this.status.get().preEnlistCheck(this);
        return this.status.get().enlistFutureResource(this, xaRes);
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
            TxGroup transactionBranch = activeXaResources.get(i);
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
        flag = this.status.get().preDelistCheck(this, flag);

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
            this.status.set(MARKED_ROLLBACK);
            return false;
        }
    }

    public synchronized Future<Boolean> delistResource(FutureXaResource futureXaResource, int flag) throws IllegalStateException, SystemException
    {
        if (flag == XAResource.TMSUSPEND)
        {
            throw new SystemException("suspend not supported");
        }
        flag = this.status.get().preDelistCheck(this, flag);

        TxGroup manager = this.removeByCommitter(futureXaResource.getDelegated());
        if (manager == null)
        {
            throw new IllegalStateException("Cannot delist a resource that's not enlisted " + futureXaResource.getDelegated());
        }

        try
        {
            return futureXaResource.end(manager.getBranchXid(), flag);
        }
        catch (XAException e)
        {
            logger.warn("Unable to delist XAResource " + futureXaResource.getDelegated() + ", error code: " + e.errorCode, e);
            this.status.set(MARKED_ROLLBACK);
            return FALSE_FUTURE;
        }
    }

    private void beforeCompletion()
    {
        try
        {
            for(int i=0;i<this.synchronizations.size();i++)
            {
                Synchronization s = this.synchronizations.get(i);
                s.beforeCompletion();
            }
        }
        catch(Throwable t)
        {
            logger.error("The synchronization before completion failed. marked for rollback ", t);
            this.status.set(MARKED_ROLLBACK);
        }
    }

    private void bestEffortEndResources()
    {
        for(int i=0;i<activeXaResources.size();i++)
        {
            TxGroup branch = activeXaResources.get(i);
            XAResource resource = null;
            try
            {
                resource = branch.getResource();
                resource.end(branch.getBranchXid(), this.status.get().getEndFlag());
            }
            catch (Throwable e)
            {
                logger.error("could not call end on XaResource "+ (resource == null ? "unknown" : resource), e);
                this.status.set(MARKED_ROLLBACK);
            }
        }
    }

    private void rollbackResources() throws SystemException
    {
        this.status.set(ROLLING_BACK);
        SystemException cause = null;

        for (int i=0; i < resourceManagers.size(); i++)
        {
            TxGroup manager = resourceManagers.get(i);
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
        this.status.set(ROLLED_BACK);
        if (cause != null)
        {
            throw cause;
        }
    }

    private void commitResources() throws SystemException
    {
        SystemException cause = null;
        boolean rollbackInstead = false;
        MutableList<Future<Void>> futures = Lists.fixedSize.of();
        for (int i = 0; i < resourceManagers.size(); i++)
        {
            TxGroup branch = resourceManagers.get(i);
            try
            {
                futures = branch.commit(i == 0, futures);
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
        for(int j=0;j<futures.size();j++)
        {
            try
            {
                futures.get(j).get();
            }
            catch (Throwable t)
            {
                logger.error("Unexpected exception committing... this will likely require manual intervention to correct", t);
                if (cause == null)
                {
                    cause = new SystemException("Unexpected exception committing ");
                    cause.initCause(t);
                }
            }
        }
        if (rollbackInstead)
        {
            this.status.set(ROLLING_BACK);
            this.status.get().commitOrPossiblyRollback(this);
        }
        else
        {
            this.status.set(COMMITTED);
            if (cause != null)
            {
                throw cause;
            }
        }
    }

    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException
    {
        boolean performAfterCompletion = false;
        try
        {
            synchronized (this)
            {
                this.status.get().prePrepareCheck(this);
                bestEffortEndResources();
                this.status.get().beforeCompletion(this);
                // at this point, our status is either ACTIVE or MARKED_ROLLBACK
                this.status.get().chooseStateForPrepare(this);
                // possible statuses are now PREPARING, COMMITTED, COMMITTING, ROLLING_BACK
                this.status.get().prepare(this);
                // possible statuses are now COMMITTED, COMMITTING, ROLLING_BACK
                performAfterCompletion = true;
                this.status.get().commitOrPossiblyRollback(this);
            }
        }
        finally
        {
            if (performAfterCompletion)
            {
                // possible statuses are now COMMITTED, ROLLED_BACK
                this.status.get().afterCompletion(this);
                multiThreadedTm.removeTransactionFromThread(this);
            }
        }
        this.status.get().postCommitCheck(this);
    }

    private void afterCompletion(int status)
    {
        for(int i=0;i<this.synchronizations.size();i++)
        {
            Synchronization synch = synchronizations.get(i);
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
        return this.status.get().getStatus(this);
    }

    public synchronized void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException
    {
        this.status.get().registerSynchronization(this, synchronization);
    }

    public void rollback() throws IllegalStateException, SystemException
    {
        boolean performAfterCompletion = false;
        try
        {
            synchronized (this)
            {
                this.status.get().preRollbackCheck(this);
                bestEffortEndResources();
                // at this point, our status is MARKED_ROLLBACK
                this.status.get().chooseRollbackState(this);
                performAfterCompletion = true;
                this.status.get().rollback(this);
            }
        }
        finally
        {
            if (performAfterCompletion)
            {
                this.status.get().afterCompletion(this);
                multiThreadedTm.removeTransactionFromThread(this);
            }
        }
    }

    public synchronized void setRollbackOnly() throws IllegalStateException, SystemException
    {
        this.status.get().setRollbackOnly(this);
    }

    private static class TxGroup
    {
        private final FutureXaResource futureXaResource;
        private final XAResource resource;
        private final Xid branchXid;
        private final Future<Boolean> isStarted;

        private TxGroup(FutureXaResource futureXaResource, Xid branchXid, Future<Boolean> isStarted)
        {
            this.futureXaResource = futureXaResource;
            this.branchXid = branchXid;
            this.resource = this.futureXaResource.getDelegated();
            this.isStarted = isStarted;
        }

        private TxGroup(XAResource res, Xid branchId)
        {
            resource = res;
            this.branchXid = branchId;
            futureXaResource = null;
            isStarted = TRUE_FUTURE;
        }

        public XAResource getResource()
        {
            try
            {
                this.isStarted.get();
            }
            catch (Exception e)
            {
                throw new RuntimeException("couldn't wait for start", e);
            }
            return resource;
        }

        public Xid getBranchXid()
        {
            return branchXid;
        }

        public synchronized void waitForStart() throws XAException
        {
            try
            {
                isStarted.get();
            }
            catch (InterruptedException e)
            {
                XAException xaException = new XAException("how did this get interrupted?");
                xaException.initCause(e);
                throw xaException;
            }
            catch (ExecutionException e)
            {
                Throwable cause = e.getCause();
                if (cause instanceof XAException)
                {
                    throw (XAException) cause;
                }
                XAException xaException = new XAException("XAResource start failed");
                xaException.initCause(e);
                throw xaException;
            }
        }

        public Future<Boolean> getIsStartedFuture()
        {
            return isStarted;
        }

        public MutableList<Future<Void>> commit(final boolean b, MutableList<Future<Void>> futures) throws XAException
        {
            if (this.futureXaResource != null)
            {
                futures = futures.with(this.futureXaResource.commit(this.branchXid, b));
            }
            else
            {
                this.resource.commit(this.branchXid, b);
            }
            return futures;
        }

        public Future<Integer> prepare() throws XAException
        {
            if (this.futureXaResource != null)
            {
                return this.futureXaResource.prepare(this.getBranchXid());
            }
            else
            {
                int prepareState = this.getResource().prepare(this.getBranchXid());
                return new ImmediateFuture<Integer>(prepareState);
            }

        }
    }


    private static class LocalXid implements Xid, Externalizable
    {
        private static AtomicInteger currentGlobalId = new AtomicInteger(new SecureRandom().nextInt());
        private static final long serialVersionUID = 936789320783279383L;


        private byte[] branchId = new byte[2];
        private byte[] globalId;
        private static long UNIQUE_ID_1;
        private static int UNIQUE_ID_2;

        static
        {
            long time = System.currentTimeMillis();

            try
            {
                InetAddress inetadr = InetAddress.getLocalHost();
                byte[] tmp = inetadr.getAddress();
                UNIQUE_ID_1 = ((tmp[3]*255+tmp[2])*255+tmp[1])*255+tmp[0];
            }
            catch (Exception e)
            {
                // let's just fill it with something random:
                UNIQUE_ID_1  = (int)(Math.random()*Integer.MAX_VALUE);
            }
            UNIQUE_ID_1 = (((long)MithraProcessInfo.getPidAsShort()) << 32) | UNIQUE_ID_1;
            UNIQUE_ID_1 = ((time >>> 10) << 48) | UNIQUE_ID_1;

            UNIQUE_ID_2 = (int) (time >>> 26);

            logger.info("Global Xid "+Long.toHexString(UNIQUE_ID_1)+Integer.toHexString(UNIQUE_ID_2));
        }

        public LocalXid()
        {
            // for externalizable
        }

        private LocalXid(short currentBranch)
        {
            int global = currentGlobalId.incrementAndGet();
            this.globalId = new byte[] {
                    (byte) 0x70, (byte) 0x6E, (byte) 0x65, (byte) 0x79,
                    (byte) 0x20, (byte) 0x76, (byte) 0x66, (byte) 0x20,
                    (byte) 0x6E, (byte) 0x20, (byte) 0x6F, (byte) 0x62,
                    (byte) 0x61, (byte) 0x72,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0,
                    (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x0};
            putInt(globalId, globalId.length - 16, (int) (UNIQUE_ID_1 >> 32));
            putInt(globalId, globalId.length - 12, (int) UNIQUE_ID_1);
            putInt(globalId, globalId.length - 8, (int) UNIQUE_ID_2);
            putInt(globalId, globalId.length - 4, global);
            putShort(branchId, 0, currentBranch);
        }

        private LocalXid(byte[] globalId, short currentBranch)
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
            return Arrays.equals(globalId, localXid.globalId);
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
        public abstract int getStatus(MultiThreadedTx tx);

        public void preEnlistCheck(MultiThreadedTx localTx) throws RollbackException
        {
            throw new IllegalStateException("cannot enlist with status "+this.getClass().getName());
        }

        public boolean enlistResource(MultiThreadedTx tx, XAResource xaResource) throws SystemException
        {
            throw new IllegalStateException("cannot enlist with status "+this.getClass().getName());
        }

        public Future<Boolean> enlistFutureResource(MultiThreadedTx tx, FutureXaResource xaResource) throws SystemException
        {
            throw new IllegalStateException("cannot enlist with status "+this.getClass().getName());
        }

        public int preDelistCheck(MultiThreadedTx tx, int incomingFlag)
        {
            throw new IllegalStateException("cannot delist with status "+this.getClass().getName());
        }

        public void prePrepareCheck(MultiThreadedTx tx)
        {
            throw new IllegalStateException("cannot commit with status "+this.getClass().getName());
        }

        public abstract int getEndFlag();

        public void beforeCompletion(MultiThreadedTx localTx)
        {
            // nothing to do
        }

        public void chooseStateForPrepare(MultiThreadedTx localTx) throws SystemException
        {
            throw new SystemException("unexpected state "+this.getClass().getName());
        }

        public void prepare(MultiThreadedTx localTx) throws SystemException
        {
            throw new SystemException("unexpected state "+this.getClass().getName());
        }

        public void commitOrPossiblyRollback(MultiThreadedTx tx) throws SystemException
        {
            throw new SystemException("unexpected commit "+this.getClass().getName());
        }

        public void afterCompletion(MultiThreadedTx tx) throws SystemException
        {
            throw new SystemException("unexpected afterCompletion "+this.getClass().getName());
        }

        public void registerSynchronization(MultiThreadedTx tx, Synchronization synchronization)
        {
            throw new IllegalStateException("can't register synchronization due to transaction state "+this.getClass().getName());
        }

        public void postCommitCheck(MultiThreadedTx localTx) throws RollbackException
        {
            throw new IllegalStateException("can't postCommitCheck due to transaction state "+this.getClass().getName());
        }

        public void setRollbackOnly(MultiThreadedTx tx)
        {
            throw new IllegalStateException("can't setRollbackOnly due to transaction state "+this.getClass().getName());
        }

        public void preRollbackCheck(MultiThreadedTx tx)
        {
            throw new IllegalStateException("can't preRollbackCheck due to transaction state "+this.getClass().getName());
        }

        public void chooseRollbackState(MultiThreadedTx tx)
        {
            throw new IllegalStateException("can't chooseRollbackState due to transaction state "+this.getClass().getName());
        }

        public void rollback(MultiThreadedTx tx) throws SystemException
        {
            throw new IllegalStateException("can't rollback due to transaction state "+this.getClass().getName());
        }
    }

    private static class TxStatusActive extends TxStatus
    {
        @Override
        public void registerSynchronization(MultiThreadedTx tx, Synchronization synchronization)
        {
            if (checkTimeout(tx))
            {
                tx.status.get().registerSynchronization(tx, synchronization);
            }
            else
            {
                tx.synchronizations.add(synchronization);
            }
        }

        private boolean checkTimeout(MultiThreadedTx tx)
        {
            if (System.currentTimeMillis() > tx.timeToDie)
            {
                if (tx.status.compareAndSet(this, MARKED_ROLLBACK))
                {
                    logger.warn("Transaction marked for rollback due to timeout");
                    tx.timedout = true;
                    return true;
                }
                return tx.timedout;
            }
            return false;

        }

        @Override
        public int getStatus(MultiThreadedTx tx)
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
        public void preEnlistCheck(MultiThreadedTx tx) throws RollbackException
        {
            if (checkTimeout(tx))
            {
                tx.status.get().preEnlistCheck(tx);
            }
        }

        private boolean containsByCommitter(FastList all, XAResource res)
        {
            for(int i=0;i<all.size();i++)
            {
                XAResource other = ((TxGroup) all.get(i)).getResource();
                if (res == other || res.equals(other)) return true;
            }
            return false;
        }

        @Override
        public boolean enlistResource(MultiThreadedTx tx, XAResource xaResource) throws SystemException
        {
            if (checkTimeout(tx))
            {
                return tx.status.get().enlistResource(tx, xaResource); // always throws
            }
            if (containsByCommitter(tx.activeXaResources, xaResource))
            {
                throw new IllegalStateException("XAResource: " + xaResource + " is already enlisted!");
            }

            try
            {
                for (int i = 0; i < tx.resourceManagers.size(); i++)
                {
                    TxGroup branch = tx.resourceManagers.get(i);
                    boolean sameRM = false;
                    //if the resource is already known, can't enlist it again.
                    if (xaResource == branch.getResource() || xaResource.equals(branch.getResource()))
                    {
                        throw new IllegalStateException("XAResource " + xaResource + " was already delisted. Can't re-enlist!");
                    }
                    sameRM = xaResource.isSameRM(branch.getResource());
                    if (sameRM)
                    {
                        branch.waitForStart();
                        xaResource.start(branch.getBranchXid(), XAResource.TMJOIN);
                        TxGroup txGroup = new TxGroup(xaResource, branch.getBranchXid());
                        tx.activeXaResources.add(txGroup);
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
        public Future<Boolean> enlistFutureResource(MultiThreadedTx tx, FutureXaResource futureXaResource) throws SystemException
        {
            if (checkTimeout(tx))
            {
                return tx.status.get().enlistFutureResource(tx, futureXaResource); // always throws
            }
            if (containsByCommitter(tx.activeXaResources, futureXaResource.getDelegated()))
            {
                throw new IllegalStateException("XAResource: " + futureXaResource.getDelegated() + " is already enlisted!");
            }

            try
            {
                for (int i = 0; i < tx.resourceManagers.size(); i++)
                {
                    TxGroup branch = tx.resourceManagers.get(i);
                    boolean sameRM = false;
                    //if the resource is already known, can't enlist it again.
                    if (futureXaResource.getDelegated() == branch.getResource() || futureXaResource.getDelegated().equals(branch.getResource()))
                    {
                        throw new IllegalStateException("XAResource " + futureXaResource.getDelegated() + " was already delisted. Can't re-enlist!");
                    }
                    sameRM = futureXaResource.getDelegated().isSameRM(branch.getResource());
                    if (sameRM)
                    {
                        Future<Boolean> result = futureXaResource.start(branch.getBranchXid(), XAResource.TMJOIN, branch.getIsStartedFuture());
                        TxGroup txGroup = new TxGroup(futureXaResource, branch.getBranchXid(), result);
                        tx.activeXaResources.add(txGroup);
                        return result;
                    }
                }
                //we know nothing about this XAResource or resource manager
                Xid branchId = tx.makeNewBranch();
                Future<Boolean> result = futureXaResource.start(branchId, XAResource.TMNOFLAGS, TRUE_FUTURE);
                TxGroup branch = new TxGroup(futureXaResource, branchId, result);
                tx.activeXaResources.add(branch);
                tx.resourceManagers.add(branch);
                return result;
            }
            catch (XAException e)
            {
                logger.warn("Unable to enlist XAResource " + futureXaResource.getDelegated() + ", errorCode: " + e.errorCode, e);
                return FALSE_FUTURE;
            }
            catch(Throwable t)
            {
                SystemException systemException = new SystemException("Unable to enlist XAResource " + futureXaResource.getDelegated());
                systemException.initCause(t);
                throw systemException;
            }
        }

        @Override
        public int preDelistCheck(MultiThreadedTx tx, int incomingFlag)
        {
            if (incomingFlag == XAResource.TMFAIL)
            {
                logger.warn("transaction marked for rollback due to TMFAIL");
                tx.status.set(MARKED_ROLLBACK);
            }
            return incomingFlag;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMSUCCESS;
        }

        @Override
        public void prePrepareCheck(MultiThreadedTx tx)
        {
            if (checkTimeout(tx))
            {
                tx.status.get().prePrepareCheck(tx);
            }
        }

        @Override
        public void beforeCompletion(MultiThreadedTx localTx)
        {
            localTx.beforeCompletion();
        }

        @Override
        public void chooseStateForPrepare(MultiThreadedTx tx) throws SystemException
        {
            if (tx.resourceManagers.isEmpty())
            {
                tx.status.set(COMMITTED);
            }
            else if (tx.resourceManagers.size() == 1)
            {
                // one-phase commit decision
                tx.status.set(COMMITTING);
            }
            else
            {
                // start prepare part of two-phase
                tx.status.set(PREPARING);
            }
        }

        @Override
        public void setRollbackOnly(MultiThreadedTx tx)
        {
            logger.warn("Transaction marked for rollback by request");
            tx.status.set(MARKED_ROLLBACK);
        }

        @Override
        public void preRollbackCheck(MultiThreadedTx tx)
        {
            tx.status.set(MARKED_ROLLBACK);
        }
    }

    private static class TxStatusMarkedRollback extends TxStatus
    {
        @Override
        public int getStatus(MultiThreadedTx tx)
        {
            return Status.STATUS_MARKED_ROLLBACK;
        }

        @Override
        public void preEnlistCheck(MultiThreadedTx localTx) throws RollbackException
        {
            String msg = "transaction is marked rollback only";
            if (localTx.timedout)
            {
                msg += " due to timeout";
            }
            throw new RollbackException(msg);
        }

        @Override
        public int preDelistCheck(MultiThreadedTx tx, int incomingFlag)
        {
            return XAResource.TMFAIL;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prePrepareCheck(MultiThreadedTx tx)
        {
            // ok, we'll just rollback instead
        }

        @Override
        public void chooseStateForPrepare(MultiThreadedTx localTx) throws SystemException
        {
            localTx.status.set(ROLLING_BACK);
        }

        @Override
        public void setRollbackOnly(MultiThreadedTx tx)
        {
            // nothing to do
        }

        @Override
        public void preRollbackCheck(MultiThreadedTx tx)
        {
            // nothing to do
        }

        @Override
        public void chooseRollbackState(MultiThreadedTx tx)
        {
            tx.status.set(ROLLING_BACK);
        }
    }

    private static class TxStatusRollingBack extends TxStatus
    {
        @Override
        public int getStatus(MultiThreadedTx tx)
        {
            return Status.STATUS_ROLLING_BACK;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(MultiThreadedTx localTx) throws SystemException
        {
            // nothing to do, we'll rollback soon
        }

        @Override
        public void commitOrPossiblyRollback(MultiThreadedTx tx) throws SystemException
        {
            tx.rollbackResources();
        }

        @Override
        public void rollback(MultiThreadedTx tx) throws SystemException
        {
            tx.rollbackResources();
        }
    }

    private static class TxStatusRolledBack extends TxStatus
    {
        @Override
        public int getStatus(MultiThreadedTx tx)
        {
            return Status.STATUS_ROLLEDBACK;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void afterCompletion(MultiThreadedTx tx) throws SystemException
        {
            tx.afterCompletion(Status.STATUS_ROLLEDBACK);
        }

        @Override
        public void postCommitCheck(MultiThreadedTx tx) throws RollbackException
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
        public void preRollbackCheck(MultiThreadedTx tx)
        {
            // nothing to do
        }

        @Override
        public void chooseRollbackState(MultiThreadedTx tx)
        {
            // nothing to do
        }

        @Override
        public void rollback(MultiThreadedTx tx) throws SystemException
        {
            // nothing to do
        }
    }

    private static class TxStatusPreparing extends TxStatus
    {
        @Override
        public int getStatus(MultiThreadedTx tx)
        {
            return Status.STATUS_PREPARING;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(MultiThreadedTx tx) throws SystemException
        {
            MutableList<Future<Integer>> futures = FastList.newList(tx.resourceManagers.size());
            int i = -1;
            try
            {
                for(i = 1; i < tx.resourceManagers.size();i++)
                {
                    TxGroup branch = tx.resourceManagers.get(i);
                    futures.add(branch.prepare());
                }
                int offset = 1;
                for(i = 0; i < futures.size();i++)
                {
                    Future<Integer> result = futures.get(i);
                    int prepareState = result.get();
                    if (prepareState == XAResource.XA_RDONLY)
                    {
                        tx.resourceManagers.remove(i + offset);
                        offset--;
                    }
                }
                tx.status.set(COMMITTING);
            }
            catch (Throwable e)
            {
                logger.error("Error preparing. Rolling back instead", e);
                tx.resourceManagers.remove(i); // resource threw an exception and is presumed rolled back
                tx.rollbackCause = e;
                tx.status.set(ROLLING_BACK);
            }
        }
    }

    private static class TxStatusCommitted extends TxStatus
    {
        @Override
        public int getStatus(MultiThreadedTx tx)
        {
            return Status.STATUS_COMMITTED;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(MultiThreadedTx localTx) throws SystemException
        {
            // all done, nothing to do
        }

        @Override
        public void commitOrPossiblyRollback(MultiThreadedTx tx) throws SystemException
        {
            // all done, nothing to do
        }

        @Override
        public void afterCompletion(MultiThreadedTx tx) throws SystemException
        {
            tx.afterCompletion(Status.STATUS_COMMITTED);
        }

        @Override
        public void postCommitCheck(MultiThreadedTx localTx)
        {
            // all good
        }
    }

    private static class TxStatusCommitting extends TxStatus
    {
        @Override
        public int getStatus(MultiThreadedTx tx)
        {
            return Status.STATUS_COMMITTING;
        }

        @Override
        public int getEndFlag()
        {
            return XAResource.TMFAIL;
        }

        @Override
        public void prepare(MultiThreadedTx localTx) throws SystemException
        {
            // no need to prepare, we'll commit soon
        }

        @Override
        public void commitOrPossiblyRollback(MultiThreadedTx tx) throws SystemException
        {
            tx.commitResources();
        }
    }

    private static class ImmediateFuture<V> implements Future<V>
    {
        private final V result;

        private ImmediateFuture(V result)
        {
            this.result = result;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException
        {
            return result;
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return result;
        }
    }
}
