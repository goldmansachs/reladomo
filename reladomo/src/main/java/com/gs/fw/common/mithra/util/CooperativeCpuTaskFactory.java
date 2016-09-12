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


import sun.misc.Unsafe;

import java.util.concurrent.CountDownLatch;

public abstract class CooperativeCpuTaskFactory implements TaskFactory
{
    private static final Unsafe UNSAFE = MithraUnsafe.getUnsafe();
    private CountDownLatch finishLatch = new CountDownLatch(1);
    private volatile int state; // lower 16 bits: uncreated tasks; 2nd 16 bits: busy tasks;
    private final MithraCpuBoundThreadPool pool;
    private volatile Throwable killedThrowable;
    private volatile boolean factoryDone = false;

    private static long STATE_OFFSET;

    static
    {
        try
        {
            STATE_OFFSET = UNSAFE.objectFieldOffset(CooperativeCpuTaskFactory.class.getDeclaredField("state"));
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException("could not get head offset", e);
        }
    }

    public CooperativeCpuTaskFactory(MithraCpuBoundThreadPool pool, int threads)
    {
        this.pool = pool;
        compareAndUpdateState(0, threads, 0);
    }

    private boolean casState(int expected, int newValue)
    {
        return UNSAFE.compareAndSwapInt(this, STATE_OFFSET, expected, newValue);
    }

    public void kill(Throwable t)
    {
        this.killedThrowable = t;
        while(true)
        {
            int cur = this.state;
            int busy = this.getBusy(cur);
            if (busy == 0) return;
            if (compareAndUpdateState(cur, 0, 0))
            {
                return;
            }
        }
    }

    public boolean isDead()
    {
        return killedThrowable != null;
    }

    private boolean compareAndUpdateState(int original, int uncreated, int busy)
    {

        int newState = busy << 16 | uncreated;
        if (casState(original, newState))
        {
            if (uncreated == 0)
            {
                setFactoryDone();
                if (busy == 0)
                {
                    finishLatch.countDown();
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public CpuTask createTask()
    {
        if (this.isDead()) return null;
        while(true)
        {
            int cur = this.state;
            int uncreated = this.getUncreated(cur);
            int busy = this.getBusy(cur);
            if (uncreated > 0)
            {
                if (compareAndUpdateState(cur, uncreated - 1, busy + 1))
                {
                    CpuTask task = this.createCpuTask();
                    task.setTaskFactory(this);
                    return task;
                }
            }
            else
            {
                return null;
            }
        }
    }

    protected abstract CpuTask createCpuTask();

    public void setTaskDone()
    {
        while(true)
        {
            int cur = this.state;
            int busy = this.getBusy(cur);
            if (busy == 0) return;
            if (compareAndUpdateState(cur, 0, busy - 1))
            {
                return;
            }
        }
    }

    private void setFactoryDone()
    {
        if (!factoryDone)
        {
            factoryDone = true;
            this.pool.setFactoryDone(this);
        }
    }

    private int getUncreated(int cur)
    {
        return (int) (cur & 0xFFFFL);
    }

    private int getBusy(int cur)
    {
        return (cur >>> 16);
    }

    @Override
    public void startAndWorkUntilFinished()
    {
        CpuTask mainTask = createTask();

        if(mainTask == null)
        {
            return;
        }

        if (getUncreated(this.state) > 0)
        {
            this.pool.submitTaskFactory(this);
        }
        mainTask.run();
        try
        {
            this.finishLatch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("unexpected interrupt", e);
        }
        final Throwable exception = this.killedThrowable;
        if (exception != null)
        {
            if (exception instanceof RuntimeException)
            {
                throw (RuntimeException) exception;
            }
            else
            {
                throw new RuntimeException(exception);
            }
        }
    }

}
