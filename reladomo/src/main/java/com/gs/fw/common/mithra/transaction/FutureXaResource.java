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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.*;

public class FutureXaResource
{
    private static Logger logger = LoggerFactory.getLogger(FutureXaResource.class.getName());

    private final MultiThreadedTm multiThreadedTm;
    private final XAResource xaResource;
    private final ResourceThread resourceThread;

    public FutureXaResource(MultiThreadedTm multiThreadedTm, XAResource xaResource, String name)
    {
        this.multiThreadedTm = multiThreadedTm;
        this.xaResource = xaResource;
        this.resourceThread = new ResourceThread(name);
        this.resourceThread.start();
    }

    public XAResource getDelegated()
    {
        return this.xaResource;
    }


    public <V> Future<V> executeOnResourceThread(Callable<V> callable)
    {
        checkLiveness();
        return this.resourceThread.scheduleTask(callable);
    }

    private void checkLiveness()
    {
        if (!this.resourceThread.isAlive())
        {
            throw new RuntimeException("Resource thread is no longer running");
        }
    }

    public Future<Void> commit(final Xid xid, final boolean b) throws XAException
    {
        Callable<Void> callable = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try
                {
                    xaResource.commit(xid, b);
                }
                finally
                {
                    multiThreadedTm.disassociateTransactionWithThread();
                }
                return null;
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Boolean> setTransactionTimeout(final int i) throws XAException
    {
        Callable<Boolean> callable = new Callable<Boolean>()
        {
            @Override
            public Boolean call() throws Exception
            {
                return xaResource.setTransactionTimeout(i);
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Void> forget(final Xid xid) throws XAException
    {
        Callable<Void> callable = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try
                {
                    xaResource.forget(xid);
                }
                finally
                {
                    multiThreadedTm.disassociateTransactionWithThread();
                }
                return null;
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Integer> prepare(final Xid xid) throws XAException
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                int prepare = xaResource.prepare(xid);
                return prepare;
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Boolean> start(final Xid xid, final int i, final Future<Boolean> parentToWaitFor) throws XAException
    {
        final MultiThreadedTx localTransaction = multiThreadedTm.getLocalTransaction();
        Callable<Boolean> callable = new Callable<Boolean>()
        {
            @Override
            public Boolean call() throws Exception
            {
                parentToWaitFor.get();
                multiThreadedTm.associateTransactionWithThread(localTransaction);
                xaResource.start(xid, i);
                return true;
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Boolean> end(final Xid xid, final int i) throws XAException
    {
        Callable<Boolean> callable = new Callable<Boolean>()
        {
            @Override
            public Boolean call() throws Exception
            {
                xaResource.end(xid, i);
                return true;
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Void> rollback(final Xid xid) throws XAException
    {
        Callable<Void> callable = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try
                {
                    xaResource.rollback(xid);
                }
                finally
                {
                    multiThreadedTm.disassociateTransactionWithThread();
                }
                return null;
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Integer> getTransactionTimeout() throws XAException
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return xaResource.getTransactionTimeout();
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public Future<Xid[]> recover(final int i) throws XAException
    {
        Callable<Xid[]> callable = new Callable<Xid[]>()
        {
            @Override
            public Xid[] call() throws Exception
            {
                return xaResource.recover(i);
            }
        };
        return this.resourceThread.scheduleTask(callable);
    }

    public void shutdown()
    {
        if (!resourceThread.isAlive())
        {
            return;
        }
        Callable<Void> callable = new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                resourceThread.shutdown();
                return null;
            }
        };
        try
        {
            this.resourceThread.scheduleTask(callable).get();
        }
        catch (InterruptedException e)
        {
            //ignore
        }
        catch (ExecutionException e)
        {
            logger.error("Could not shutdown resource thread", e);
        }
        try
        {
            this.resourceThread.join();
        }
        catch (InterruptedException e)
        {
            //ignore
        }
    }

    private static class ResourceThread extends Thread
    {
        private LinkedBlockingQueue<FutureTask> todo = new LinkedBlockingQueue<FutureTask>();
        private boolean shutdown = false;

        private ResourceThread(String name)
        {
            super(name);
        }

        @Override
        public void run()
        {
            while(!shutdown)
            {
                FutureTask futureTask = null;
                try
                {
                    futureTask = todo.take();
                    futureTask.run();
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
            }
        }

        public <V> Future scheduleTask(Callable<V> callable)
        {
            FutureTask task = new FutureTaskOnThread(callable, this);
            try
            {
                this.todo.put(task);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("must not happen!", e);
            }
            return task;
        }

        public void shutdown()
        {
            shutdown = true;
        }
    }

    private static class FutureTaskOnThread<V> extends FutureTask<V>
    {
        private final Thread executionThread;

        private FutureTaskOnThread(Callable callable, Thread executionThread)
        {
            super(callable);
            this.executionThread = executionThread;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException
        {
            if (Thread.currentThread() == this.executionThread)
            {
                this.run();
            }
            return super.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            if (Thread.currentThread() == this.executionThread)
            {
                this.run();
            }
            return super.get(timeout, unit);
        }
    }
}
