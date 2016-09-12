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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.util.ExceptionHandlingTask;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;



public class ServerTransactionWorkerTask extends ExceptionHandlingTask implements ServerContext, ServerCursorExecutor
{
    private static Logger logger = LoggerFactory.getLogger(ServerTransactionWorkerTask.class.getName());
    private static final AtomicReferenceFieldUpdater cursorUpdater = AtomicReferenceFieldUpdater.newUpdater(ServerTransactionWorkerTask.class,
            RemoteCursorResult.class, "currentCursor");

    private RemoteTransactionId remoteTransactionId;
    private RemoteMithraServiceImpl remoteMithraService;
    private volatile MithraRemoteResult runnable;
    private volatile boolean done = false;
    private int timeout = 30000;
    private boolean timedOut = false;
    private MithraRemoteTransactionProxy tx;
    private long startTime;
    private boolean establishedContext = false;
    private volatile MithraTransaction transactionToWaitFor;
    private static final long REMOTE_WAIT_TIMEOUT = 30000;
    private boolean rolledBack;
    private volatile RemoteCursorResult currentCursor;

    public ServerTransactionWorkerTask(RemoteTransactionId remoteTransactionId, int remoteTransactionTimeout, RemoteMithraServiceImpl remoteMithraService)
    {
        this.remoteTransactionId = remoteTransactionId;
        this.timeout = remoteTransactionTimeout * 1000;
        this.remoteMithraService = remoteMithraService;
    }

    public RemoteTransactionId getRemoteTransactionId()
    {
        return remoteTransactionId;
    }

    public void startTransaction(final Xid xid)
    {
        MithraRemoteResult runnable = new MithraRemoteResult() {
            public void run()
            {
                tx = MithraManagerProvider.getMithraManager().startRemoteTransactionProxy(xid, timeout);
            }

            public void writeExternal(ObjectOutput out) throws IOException
            {
                throw new RuntimeException("not implemented");
            }

            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
            {
                throw new RuntimeException("not implemented");
            }
        };
        synchronized(this)
        {
            this.runnable = runnable;
            this.waitForTaskToFinish();
        }
        checkForThrown(runnable);
    }

    public void execute(MithraRemoteResult runnable)
    {
        synchronized(this)
        {
            this.runnable = runnable;
            waitForTaskToFinish();
        }
        runnable.setRemoteTransactionId(remoteTransactionId);
        checkForThrown(runnable);
        this.establishedContext = true;
    }

    public void serializeFullData(MithraObject object, ObjectOutput out) throws IOException
    {
        object.zWriteDataClassName(out);
        if (object instanceof MithraTransactionalObject)
        {
            object.zSerializeFullTxData(out);
        }
        else
        {
            object.zSerializeFullData(out);
        }
        this.startTime = System.currentTimeMillis();
    }

    public ServerCursorExecutor getServerCursorExecutor()
    {
        return this;
    }

    public void continueCursor(RemoteCursorResult remoteCursorResult)
    {
        cursorUpdater.set(this, remoteCursorResult);
    }

    public void setCursorDone(RemoteCursorResult remoteCursorResult)
    {
        cursorUpdater.compareAndSet(this, remoteCursorResult, null);
    }

    public synchronized void keepReading()
    {
        this.notify();  // this will abort the wait for runnable
    }

    public void executeAndWaitUntilDone(final Runnable runnable)
    {
        this.execute(new MithraRemoteResult()
        {
            public void run()
            {
                runnable.run();
            }

            public void writeExternal(ObjectOutput out) throws IOException
            {
                throw new RuntimeException("not implemented");
            }

            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
            {
                throw new RuntimeException("not implemented");
            }
        });
    }

    public void commit(final boolean onePhase)
    {
        MithraRemoteResult runnable = new MithraRemoteResult() {
            public void run()
            {
                tx.setRequestorVmId(remoteTransactionId.getRequestorVmId());
                tx.commit(onePhase);
            }
            
            public void writeExternal(ObjectOutput out) throws IOException
            {
                throw new RuntimeException("not implemented");
            }

            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
            {
                throw new RuntimeException("not implemented");
            }
        };
        synchronized(this)
        {
            this.runnable = runnable;
            this.waitForTaskToFinish();
        }
        this.exit();
        checkForThrown(runnable);
    }

    public void rollback()
    {
        MithraRemoteResult runnable = new MithraRemoteResult() {
            public void run()
            {
                tx.rollback();
                rolledBack = true;
            }

            public void writeExternal(ObjectOutput out) throws IOException
            {
                throw new RuntimeException("not implemented");
            }

            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
            {
                throw new RuntimeException("not implemented");
            }
        };
        synchronized(this)
        {
            if (!rolledBack)
            {
                this.runnable = runnable;
                this.waitForTaskToFinish();
            }
        }
        if (this.transactionToWaitFor == null)
        {
            this.exit();
        }
        checkForThrown(runnable);
    }

    private void checkForThrown(MithraRemoteResult runnable)
    {
        if (runnable.getThrown() != null)
        {
            if (!establishedContext)
            {
                this.rollback();
            }
            Throwable t = runnable.getThrown();
            if (t instanceof RuntimeException)
            {
                throw (RuntimeException) t;
            }
            throw new MithraTransactionException("error running on server thread", t);
        }
    }

    private void waitForTaskToFinish()
    {
        this.notify();
        try
        {
            this.wait();
        }
        catch (InterruptedException e)
        {
            logger.warn("unexpected interrupt", e);
        }
    }

    public synchronized boolean isTimedOut()
    {
        return timedOut;
    }

    public synchronized void exit()
    {
        this.done = true;
        waitForTaskToFinish();
    }

    public void execute()
    {
        startTime = System.currentTimeMillis();
        logger.debug("server side worker thread started");
        try
        {
            while(!done)
            {
                boolean waitForRunnable = true;
                RemoteCursorResult localCursor = this.currentCursor;
                if (localCursor != null && this.runnable == null)
                {
                    waitForRunnable = localCursor.readMore();
                }
                MithraRemoteResult localRunnable = this.runnable;
                try
                {
                    if (waitForRunnable || localRunnable != null)
                    {
                        synchronized(this)
                        {
                            if (this.runnable == null && !done)
                            {
                                // add 10ms buffer to avoid thrashing around the loop if we hit the boundary condition
                                long millisRemainingUntilTimeout = timeout - (System.currentTimeMillis() - startTime) + 10;

                                if (millisRemainingUntilTimeout > 0)
                                {
                                    this.wait(millisRemainingUntilTimeout);
                                }
                            }
                            localRunnable = this.runnable;
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    logger.warn("unexpected interrupt", e);
                }
                if (!done && localRunnable != null)
                {
                    try
                    {
                        localRunnable.run();
                    }
                    catch(MithraTransactionException txException)
                    {
                        this.transactionToWaitFor = txException.getTransactionToWaitFor();
                        txException.setRemoteTransactionId(this.remoteTransactionId);
                        localRunnable.setThrown(txException);
                    }
                    catch(Throwable t)
                    {
                        logger.error("server method "+runnable.getClass().getName()+" threw an exception ", t);
                        localRunnable.setThrown(t);
                    }
                    synchronized(this)
                    {
                        runnable = null;
                        this.notify();
                    }
                }
                else if (System.currentTimeMillis() - startTime > timeout)
                {
                    logger.error("server side worker thread timedout");
                    if (currentCursor != null)
                    {
                        currentCursor.setErrorAndClose(new MithraTransactionException("Transaction timed out. The timeout is set to "+timeout));
                    }
                    done = true;
                    timedOut = true;
                    if (!rolledBack)
                    {
                        tx.rollback();
                        rolledBack = true;
                    }
                }
            }
            synchronized(this)
            {
                this.notify();
            }
        }
        finally
        {
            remoteMithraService.removeServerSideTask(this);
            this.logger.debug("server side thread exiting");
        }
    }

    public void waitForOtherTransactionToFinish()
    {
        if (this.transactionToWaitFor != null)
        {
            this.transactionToWaitFor.waitUntilFinished(REMOTE_WAIT_TIMEOUT);
        }
        this.exit();
    }
}
