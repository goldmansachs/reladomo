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

package com.gs.fw.common.mithra.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;


public abstract class CpuBoundTask implements Runnable
{
    static private Logger logger = LoggerFactory.getLogger(CpuBoundTask.class.getName());
    private static final AtomicIntegerFieldUpdater stateUpdater = AtomicIntegerFieldUpdater.newUpdater(CpuBoundTask.class, "state");

    private static final int NOT_CLAIMED = 0;
    private static final int CLAIMED = 1;
    private static final int FINISHED = 2;

    private volatile int state = 0;
    private Throwable thrown;

    public abstract void execute();

    public final void run()
    {
        if (state == NOT_CLAIMED && stateUpdater.compareAndSet(this, NOT_CLAIMED, CLAIMED))
        {
            try
            {
                this.execute();
            }
            catch (Throwable e)
            {
                this.thrown = e;
                logger.error("runnable threw exception", e);
            }
            synchronized (this)
            {
                state = FINISHED;
                this.notify();
            }
        }
    }

    public void waitUntilDoneWithExceptionHandling()
    {
        waitUntilDoneIgnoringExceptions();
        final Throwable exception = this.thrown;
        if (exception != null)
        {
            if (this.thrown instanceof RuntimeException)
            {
                throw (RuntimeException) this.thrown;
            }
            else
            {
                throw new RuntimeException(exception);
            }
        }
    }

    public void waitUntilDoneIgnoringExceptions()
    {
        synchronized (this)
        {
            while(state != FINISHED)
            {
                try
                {
                    this.wait();
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
            }
        }
    }
}
