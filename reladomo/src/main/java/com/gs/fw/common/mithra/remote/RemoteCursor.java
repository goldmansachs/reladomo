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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.util.AutoShutdownThreadExecutor;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;


public class RemoteCursor implements Cursor
{
    private RemoteMithraObjectPersister remoteMithraObjectPersister;
    private List currentBatch;
    private boolean isRemoteSideFinished;
    private int currentPos;
    private int remoteQueueSize;
    private RemoteTransactionId remoteCursorId;
    private int maxParallelDegree;
    private boolean isTransactional;
    private boolean isRemoteSideFinishedWaitingInQueue;
    private int currentOrder;
    private int tasksQueued;
    private IntObjectHashMap<RemoteContinuedCursorResult> orderedResults;
    private AutoShutdownThreadExecutor executor;
    private volatile Throwable error;

    public RemoteCursor(RemoteMithraObjectPersister remoteMithraObjectPersister, List initialList, boolean isRemoteSideFinished,
            int remoteQueueSize, RemoteTransactionId remoteCursorId)
    {
        this.remoteMithraObjectPersister = remoteMithraObjectPersister;
        this.currentBatch = initialList;
        this.isRemoteSideFinished = isRemoteSideFinished;
        this.remoteQueueSize = remoteQueueSize;
        this.remoteCursorId = remoteCursorId;
    }

    public RemoteTransactionId getRemoteCursorId()
    {
        return remoteCursorId;
    }

    public void close()
    {
        if (remoteMithraObjectPersister.getLogger().isDebugEnabled())
        {
            remoteMithraObjectPersister.getLogger().debug("remote cursor finished");
        }
        if (maxParallelDegree > 1)
        {
            synchronized (this)
            {
                if (executor != null)
                {
                    executor.shutdown();
                }
                while(error == null && tasksQueued > 0)
                {
                    try
                    {
                        this.wait();
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
            }
        }
        if (isRemoteSideFinished || isRemoteSideFinishedWaitingInQueue) return;
        this.remoteMithraObjectPersister.closeCursor(this.remoteCursorId);
        checkError();
    }

    public boolean hasNext()
    {
        checkError();
        if (currentPos >= currentBatch.size())
        {
            if (isRemoteSideFinished) return false;
            if (maxParallelDegree > 1 && !this.isTransactional)
            {
                synchronized (this)
                {
                    boolean queueAndWait = executor != null;
                    if (executor == null)
                    {
                        if (this.remoteQueueSize > maxParallelDegree)
                        {
                            executor = new AutoShutdownThreadExecutor(this.maxParallelDegree, "Remote Cursor "+this.remoteCursorId.getTransactionId());
                            executor.setExceptionHandler(new AutoShutdownThreadExecutor.ExceptionHandler()
                            {
                                public void handleException(AutoShutdownThreadExecutor executor, Runnable target, Throwable exception)
                                {
                                    error = exception;
                                    remoteMithraObjectPersister.getLogger().error("Remote cursor error ", error);
                                }
                            });
                            orderedResults = new IntObjectHashMap<RemoteContinuedCursorResult>(maxParallelDegree * 3);
                            queueAndWait = true;
                        }
                    }
                    if (queueAndWait)
                    {
                        queueMoreTasks();
                        RemoteContinuedCursorResult continuedCursorResult = orderedResults.removeKey(this.currentOrder + 1);
                        while (continuedCursorResult == null)
                        {
                            try
                            {
                                this.wait();
                            }
                            catch (InterruptedException e)
                            {
                                //ignore
                            }
                            checkError();
                            continuedCursorResult = orderedResults.removeKey(this.currentOrder + 1);
                        }
                        this.setMoreObjects(continuedCursorResult);
                        queueMoreTasks();
                    }
                    else
                    {
                        readMoreSingleThreaded();
                    }
                }
            }
            else
            {
                readMoreSingleThreaded();
            }
        }
        return currentPos < currentBatch.size();
    }

    private void checkError()
    {
        if (error != null)
        {
            if (error instanceof RuntimeException)
            {
                throw (RuntimeException) error;
            }
            throw new MithraBusinessException("Remote cursor error", error);
        }
    }

    private void queueMoreTasks()
    {
        if (!isRemoteSideFinishedWaitingInQueue)
        {
            int toQueue = maxParallelDegree - tasksQueued;
            for(int i=0;i<toQueue;i++)
            {
                tasksQueued++;
                executor.submit(new AsyncContinue());
            }
        }
    }

    private void readMoreSingleThreaded()
    {
        RemoteContinuedCursorResult continuedCursorResult = remoteMithraObjectPersister.continueCursor(this);
        if (continuedCursorResult == null)
        {
            this.currentPos = 0;
            this.currentBatch = Collections.EMPTY_LIST;
            this.remoteQueueSize = 0;
            this.isRemoteSideFinished = true;
        }
        else
        {
            setMoreObjects(continuedCursorResult);
        }
    }

    private void setMoreObjects(RemoteContinuedCursorResult continuedCursorResult)
    {
        if (continuedCursorResult != null)
        {
            synchronized (this)
            {
                this.currentPos = 0;
                this.currentBatch = continuedCursorResult.getResult();
                this.remoteQueueSize = continuedCursorResult.getRemoteQueueSize();
                this.isRemoteSideFinished = continuedCursorResult.isFinished();
                this.currentOrder = continuedCursorResult.getOrder();
            }
        }
    }

    public Object next()
    {
        if (hasNext())
        {
            return currentBatch.get(currentPos++);
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException("remove not supported on cursor");
    }

    public void setMaxParallelDegree(int maxParallelDegree)
    {
        this.maxParallelDegree = maxParallelDegree;
    }

    public void setTransactional(boolean transactional)
    {
        this.isTransactional = transactional;
    }

    private class AsyncContinue implements Runnable
    {
        public void run()
        {
            RemoteContinuedCursorResult continuedCursorResult = remoteMithraObjectPersister.continueCursor(RemoteCursor.this);
            if (continuedCursorResult != null)
            {
                synchronized (RemoteCursor.this)
                {
                    orderedResults.put(continuedCursorResult.getOrder(), continuedCursorResult);
                    if (continuedCursorResult.isFinished())
                    {
                        isRemoteSideFinishedWaitingInQueue = true;
                        executor.shutdown();
                    }
                }
            }
            synchronized (RemoteCursor.this)
            {
                tasksQueued--;
                RemoteCursor.this.notify();
            }
        }
    }
}
