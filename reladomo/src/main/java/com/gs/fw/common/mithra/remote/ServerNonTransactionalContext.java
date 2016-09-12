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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.util.ExceptionCatchingThread;
import com.gs.fw.common.mithra.util.ExceptionHandlingTask;

import java.io.IOException;
import java.io.ObjectOutput;



public class ServerNonTransactionalContext implements ServerContext
{

    private static final int CURSOR_TIMEOUT = 5*60*1000; // 5 minutes

    private static final ServerNonTransactionalContext instance = new ServerNonTransactionalContext();

    public static ServerNonTransactionalContext getInstance()
    {
        return instance;
    }

    private ServerNonTransactionalContext()
    {
        // singleton
    }

    public void execute(MithraRemoteResult runnable)
    {
        runnable.run();
    }

    public void serializeFullData(MithraObject object, ObjectOutput out)
            throws IOException
    {
        object.zWriteDataClassName(out);
        object.zSerializeFullData(out);
    }

    public ServerCursorExecutor getServerCursorExecutor()
    {
        return new NonTransactionalServerCursorExecutor();
    }

    private static class NonTransactionalServerCursorExecutor extends ExceptionHandlingTask implements ServerCursorExecutor
    {
        private volatile boolean done = false;
        private RemoteCursorResult remoteCursorResult;
        private Runnable runnable;
        private boolean finished;
        private Object endLock = new Object();

        @Override
        public void execute()
        {
            long lastReadTime = System.currentTimeMillis();
            while(!done)
            {
                if (remoteCursorResult.readMore() && !done && this.runnable == null)
                {
                    if (lastReadTime < System.currentTimeMillis() - CURSOR_TIMEOUT)
                    {
                        done = true;
                        remoteCursorResult.setErrorAndClose(new MithraBusinessException("remote cursor timed out after "+CURSOR_TIMEOUT/1000.0+" seconds"));
                    }
                    try
                    {
                        synchronized (this)
                        {
                            this.wait(100);
                        }
                    }
                    catch (InterruptedException e)
                    {
                        // ignore
                    }
                }
                else
                {
                    lastReadTime = System.currentTimeMillis();
                }
            }
            synchronized (endLock)
            {
                if (this.runnable != null)
                {
                    this.runnable.run();
                    this.runnable = null;
                }
                finished = true;
                endLock.notify();
            }
        }

        public void continueCursor(RemoteCursorResult remoteCursorResult)
        {
            this.remoteCursorResult = remoteCursorResult;
            ExceptionCatchingThread.submitTask(this);
        }

        public void setCursorDone(RemoteCursorResult remoteCursorResult)
        {
            this.done = true;
        }

        public synchronized void keepReading()
        {
            this.notify();
        }

        public void executeAndWaitUntilDone(Runnable runnable)
        {
            synchronized (this)
            {
                this.notify();
            }
            synchronized (endLock)
            {
                this.runnable = runnable;
                if (!finished)
                {
                    try
                    {
                        endLock.wait();
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
                if (this.runnable != null)
                {
                    runnable.run();
                }
            }
        }
    }
}
