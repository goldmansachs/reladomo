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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.util.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class RemoteCursorResult extends MithraRemoteResult
{
    private static final int BATCH_SIZE = 1000;
    private static final AtomicInteger ID_FACTORY = new AtomicInteger(0);
    private static final UnifiedMap<RemoteTransactionId, RemoteCursorResult> CURSOR_MAP = new UnifiedMap<RemoteTransactionId, RemoteCursorResult>();
    private static final Logger logger = LoggerFactory.getLogger(RemoteCursorResult.class.getName());

    private Operation op;
    private Filter postLoadFilter;
    private transient OrderBy orderBy;
    private transient boolean bypassCache;
    private transient int maxObjectsToRetrieve;
    private transient int maxParallelDegree;
    private transient ServerContext serverContext;
    private transient ServerCursorExecutor serverCursorExecutor;
    private transient Worker worker;
    private transient LinkedBlockingQueue<ListWithOrder> queue;
    private transient boolean forceImplicitJoin;
    private Map databaseIdentifierMap;
    private RemoteTransactionId remoteCursorId;
    private List deserializedResult;
    private boolean noMoreResults;
    private int remoteQueueSize;
    private volatile Throwable remoteError;
    private volatile int batchNumber;

    public RemoteCursorResult()
    {
        // for Externalizable
    }

    public RemoteCursorResult(Operation op, Filter postLoadOperation, OrderBy orderBy, boolean bypassCache, int maxObjectsToRetrieve,
                              int maxParallelDegree, ServerContext serverContext, int vmId, boolean forceImplicitJoin)
    {
        this.op = op;
        this.postLoadFilter = postLoadOperation;
        this.orderBy = orderBy;
        this.bypassCache = bypassCache;
        this.maxObjectsToRetrieve = maxObjectsToRetrieve;
        this.maxParallelDegree = maxParallelDegree;
        this.serverContext = serverContext;
        this.remoteCursorId = new RemoteTransactionId(vmId, ID_FACTORY.getAndIncrement());
        synchronized (CURSOR_MAP)
        {
            CURSOR_MAP.put(this.remoteCursorId, this);
        }
        this.forceImplicitJoin = forceImplicitJoin;
    }

    public void run()
    {
        MithraObjectPortal portal = op.getResultObjectPortal();
        if (!bypassCache)
        {
            if (!portal.isCacheDisabled())
            {
                CachedQuery resolved = portal.zFindInMemory(op, orderBy);
                if (resolved != null)
                {
                    this.queue = new LinkedBlockingQueue<ListWithOrder>();
                    this.worker = new InMemoryWorker(resolved);
                }
            }
        }
        if (this.worker == null)
        {
            this.queue = new LinkedBlockingQueue<ListWithOrder>(getMaxQueueLength() + 2);
            Cursor cursor = op.getResultObjectPortal().findCursorFromServer(
                    this.op, this.postLoadFilter, this.orderBy,
                    this.maxObjectsToRetrieve, this.bypassCache, this.maxParallelDegree, this.forceImplicitJoin);
            this.worker = new WithCursorWorker(cursor);
        }
        this.worker.doWork();
        databaseIdentifierMap = op.getResultObjectPortal().extractDatabaseIdentifiers(op);
        if (!worker.isDone())
        {
            this.serverCursorExecutor = serverContext.getServerCursorExecutor();
            this.serverCursorExecutor.continueCursor(this);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        out.writeObject(this.remoteCursorId);
        out.writeObject(op);
        //todo: we seem to ignore reachedMaxRetrieveCount in forEachWithCursor
        //out.writeBoolean(this.reachedMaxRetrieveCount);
        out.writeInt(op.getResultObjectPortal().getFinder().getSerialVersionId());
        ListWithOrder serverSideList = this.queue.poll();
        if (serverSideList == null)
        {
            out.writeBoolean(true);
            out.writeObject(this.worker.getError());
        }
        else
        {
            out.writeBoolean(false);
            out.writeInt(serverSideList.list.size());
            for (int i = 0; i < serverSideList.list.size(); i++)
            {
                MithraObject mithraObject = (MithraObject) serverSideList.list.get(i);
                serverContext.serializeFullData(mithraObject, out);
            }
            out.writeBoolean(checkFinished());
            out.writeInt(queue.size());
        }
        writeDatabaseIdentifierMap(out, databaseIdentifierMap);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        this.remoteCursorId = (RemoteTransactionId) in.readObject();
        this.op = (Operation) in.readObject();
        //this.reachedMaxRetrieveCount = in.readBoolean();
        MithraObjectPortal mithraObjectPortal = op.getResultObjectPortal();
        int serverVersion = in.readInt();
        int localVersion = mithraObjectPortal.getFinder().getSerialVersionId();
        if (serverVersion != localVersion)
        {
            throw new IOException("version of the object " + mithraObjectPortal.getFinder().getClass().getName() +
                    " does not match this version. Server version " + serverVersion + " local version " + localVersion);
        }
        boolean hadError = in.readBoolean();
        if (hadError)
        {
            this.remoteError = (Throwable) in.readObject();
        }
        else
        {
            this.deserializedResult = mithraObjectPortal.getMithraObjectDeserializer().deserializeList(op, in, true);
            this.noMoreResults = in.readBoolean();
            this.remoteQueueSize = in.readInt();
        }
        this.databaseIdentifierMap = readDatabaseIdentifierMap(in);
    }

    public void registerForNotification()
    {
        this.registerForNotification(this.databaseIdentifierMap);
    }

    public RemoteCursor getCursor(RemoteMithraObjectPersister remoteMithraObjectPersister)
    {
        if (this.remoteError != null)
        {
            if (remoteError instanceof RuntimeException)
            {
                throw (RuntimeException) remoteError;
            }
            else throw new MithraBusinessException("Remote cursor error", remoteError);
        }
        return new RemoteCursor(remoteMithraObjectPersister, this.deserializedResult, this.noMoreResults,
                this.remoteQueueSize, this.remoteCursorId);
    }

    protected void queueResult(List resultBatch)
    {
        try
        {
            this.queue.put(new ListWithOrder(batchNumber++, resultBatch));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    // return true if invoking thread should wait for an incoming (remote) runnable
    public boolean readMore()
    {
        if (this.worker.isDone())
        {
            this.serverCursorExecutor.setCursorDone(this);
        }
        if (queue.size() > getMaxQueueLength() || this.worker.isDone())
        {
            return true;
        }
        this.worker.doWork();
        return false;
    }

    private int getMaxQueueLength()
    {
        return maxParallelDegree * 3;
    }

    public static RemoteCursorResult getExisting(RemoteTransactionId remoteCursorId)
    {
        synchronized (CURSOR_MAP)
        {
            return CURSOR_MAP.get(remoteCursorId);
        }
    }

    public RemoteContinuedCursorResult getContinuedResult()
    {
        if (remoteError != null)
        {
            synchronized (CURSOR_MAP)
            {
                CURSOR_MAP.remove(this.remoteCursorId);
            }
            throw (RuntimeException) remoteError;
        }
        ListWithOrder moreResults = queue.poll();
        while (moreResults == null)
        {
            if (checkFinished()) return null;
            Throwable error = worker.getError();
            if (error != null)
            {
                if (error instanceof RuntimeException)
                {
                    throw (RuntimeException) error;
                }
                else throw new MithraBusinessException("Remote cursor error ", error);
            }
            try
            {
                moreResults = queue.poll(100, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                //ignore
            }
        }
        if (!worker.isDone()) this.serverCursorExecutor.keepReading();
        boolean finished = checkFinished();
        return new RemoteContinuedCursorResult(moreResults.list, moreResults.order, finished, queue.size(), this.op, serverContext);
    }

    private boolean checkFinished()
    {
        boolean finished = this.worker.isDone() && this.queue.isEmpty();
        if (finished)
        {
            synchronized (CURSOR_MAP)
            {
                CURSOR_MAP.remove(this.remoteCursorId);
            }
        }
        return finished;
    }

    public void closeCursor()
    {
        this.worker.markForClosure();
        if (this.serverCursorExecutor != null)
        {
            this.serverCursorExecutor.setCursorDone(this);
            this.serverCursorExecutor.executeAndWaitUntilDone(new Runnable()
            {
                public void run()
                {
                    worker.doWork();
                }
            });
        }
        synchronized (CURSOR_MAP)
        {
            CURSOR_MAP.remove(this.remoteCursorId);
        }
    }

    public void setErrorAndClose(MithraBusinessException e)
    {
        this.remoteError = e;
        this.worker.forceClose();
    }

    private interface Worker
    {
        public boolean isDone();

        public void doWork();

        public Throwable getError();

        public void markForClosure();

        public void forceClose();
    }

    private class InMemoryWorker implements Worker
    {
        private List result;
        private volatile boolean done;

        public InMemoryWorker(CachedQuery resolved)
        {
            this.result = resolved.getResult();
        }

        public void doWork()
        {
            if (done) return;
            if (result.isEmpty()) queueResult(Collections.EMPTY_LIST);
            for (int i = 0; i < result.size(); i += BATCH_SIZE)
            {
                queueResult(result.subList(i, Math.min(result.size(), i + BATCH_SIZE)));
            }
            done = true;
        }

        public boolean isDone()
        {
            return true;
        }

        public Throwable getError()
        {
            return null;
        }

        public void markForClosure()
        {
            //nothing to do
        }

        public void forceClose()
        {
            //nothing to do
        }
    }

    private class WithCursorWorker implements Worker
    {
        private Cursor cursor;
        private volatile boolean closed = false;
        private volatile boolean markedForClosure = false;
        private volatile Throwable error;

        private WithCursorWorker(Cursor cursor)
        {
            this.cursor = cursor;
        }

        public void doWork()
        {
            if (closed) return;
            try
            {
                if (markedForClosure)
                {
                    forceClose();
                    return;
                }
                FastList list = new FastList();
                for (int i = 0; i < BATCH_SIZE && cursor.hasNext() && !markedForClosure; i++)
                {
                    list.add(cursor.next());
                }
                queueResult(list);
                if (!cursor.hasNext() || markedForClosure)
                {
                    forceClose();
                }
            }
            catch (Throwable e)
            {
                this.error = e;
                forceClose();
            }
        }

        public Throwable getError()
        {
            return error;
        }

        public void markForClosure()
        {
            this.markedForClosure = true;
        }

        public void forceClose()
        {
            if (!closed)
            {
                closed = true;
                try
                {
                    cursor.close();
                }
                catch (Exception e)
                {
                    logger.warn("Could not close cursor", e);
                }
            }
        }

        public boolean isDone()
        {
            return closed;
        }
    }

    private static class ListWithOrder
    {
        private int order;
        private List list;

        private ListWithOrder(int order, List list)
        {
            this.order = order;
            this.list = list;
        }
    }
}