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

package com.gs.fw.common.mithra.cache.offheap;


import com.gs.fw.common.mithra.util.MithraUnsafe;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OffHeapFreeThread extends Thread
{
    private static Logger logger = LoggerFactory.getLogger(OffHeapFreeThread.class.getName());
    private static final int POLL_PERIOD = 60000;

    private static Unsafe UNSAFE = MithraUnsafe.getUnsafe();
//    private static MithraUnsafe.AuditedMemory UNSAFE = MithraUnsafe.getAuditedMemory();

    private List<OffHeapThreadSnapShot> toFree = FastList.newList();
    private List<OffHeapThreadSnapShot> newToFree = FastList.newList();

    public OffHeapFreeThread()
    {
        super("Off Heap Free Thread");
        this.setDaemon(true);
    }

    public void queue(long base)
    {
        Map<Thread, StackTraceElement[]> allStackTraces = UnifiedMap.newMap(Thread.getAllStackTraces());
        allStackTraces.remove(Thread.currentThread());
        for (Iterator<Thread> it = allStackTraces.keySet().iterator(); it.hasNext(); )
        {
            Thread thread = it.next();
            if (!isBusy(thread, allStackTraces.get(thread)))
            {
                it.remove();
            }
        }
        logger.debug("Queuing "+base+" to free later");
        synchronized (newToFree)
        {
            newToFree.add(new OffHeapThreadSnapShot(base, allStackTraces));
        }
    }

    @Override
    public void run()
    {
        while (true)
        {
            try
            {
                sleep(POLL_PERIOD);
                synchronized (newToFree)
                {
                    if (!newToFree.isEmpty())
                    {
                        toFree.addAll(newToFree);
                        newToFree.clear();
                    }
                }
                processToFree();
            }
            catch (Throwable t)
            {
                logger.error("Unexpected exception", t);
            }
        }
    }

    private void processToFree()
    {
        if (!toFree.isEmpty())
        {
            long time = System.currentTimeMillis();
            freeEligible(time);
            Map<Thread, StackTraceElement[]> allStackTraces = getStackTracesForRelevantThreads();
            for (Thread thread : allStackTraces.keySet())
            {
                StackTraceElement[] newStackTraceElement = allStackTraces.get(thread);
                if (!isBusy(thread, newStackTraceElement))
                {
                    removeUnbusyThread(time, thread);
                }
                else
                {
                    removeBusyThreadIfMoved(time, thread, newStackTraceElement);
                }
            }
        }
    }

    private void removeBusyThreadIfMoved(long time, Thread thread, StackTraceElement[] newStackTraceElement)
    {
        int newBase = getBasePosition(newStackTraceElement);
        int newLength = newStackTraceElement.length - newBase;
        for (int i = 0; i < toFree.size(); i++)
        {
            OffHeapThreadSnapShot snapShot = toFree.get(i);
            StackTraceElement[] old = snapShot.activeThreads.get(thread);
            if (old != null)
            {
                int oldBase = getBasePosition(old);
                int oldLength = old.length - oldBase;
                if (hasMoved(old, oldBase, oldLength, newStackTraceElement, newBase, newLength))
                {
                    snapShot.activeThreads.remove(thread);
                    if (snapShot.isReadyForFree(time))
                    {
                        toFree.remove(i);
                        i--;
                        logger.debug("Freeing "+snapShot.base);
                        UNSAFE.freeMemory(snapShot.base);
                    }
                }
            }
        }
    }

    private void removeUnbusyThread(long time, Thread thread)
    {
        for (int i = 0; i < toFree.size(); i++)
        {
            OffHeapThreadSnapShot snapShot = toFree.get(i);
            snapShot.activeThreads.remove(thread);
            if (snapShot.isReadyForFree(time))
            {
                toFree.remove(i);
                i--;
                logger.info("Freeing "+snapShot.base);
                UNSAFE.freeMemory(snapShot.base);
            }
        }
    }

    private Map<Thread, StackTraceElement[]> getStackTracesForRelevantThreads()
    {
        UnifiedMap<Thread, StackTraceElement[]> result = UnifiedMap.newMap();
        for (int i = 0; i < toFree.size(); i++)
        {
            OffHeapThreadSnapShot snapShot = toFree.get(i);
            for (Iterator<Thread> it = snapShot.activeThreads.keySet().iterator(); it.hasNext(); )
            {
                Thread thread = it.next();
                if (!thread.getState().equals(State.RUNNABLE))
                {
                    it.remove();
                }
                else if (!result.containsKey(thread))
                {
                    result.put(thread, thread.getStackTrace());
                }
            }
        }
        return result;
    }

    private void freeEligible(long time)
    {
        for (int i = 0; i < toFree.size(); i++)
        {
            OffHeapThreadSnapShot snapShot = toFree.get(i);
            if (snapShot.isReadyForFree(time))
            {
                toFree.remove(i);
                i--;
                logger.info("Freeing "+snapShot.base);
                UNSAFE.freeMemory(snapShot.base);
            }
        }
    }

    private boolean hasMoved(StackTraceElement[] old, int oldBase, int oldLength, StackTraceElement[] newStackTraceElement, int newBase, int newLength)
    {
        if (oldLength != newLength)
        {
            return true;
        }
        for(int i=oldBase;i<oldLength;i++)
        {
            if (!old[i].equals(newStackTraceElement[i]))
            {
                return true;
            }
        }
        return false;
    }

    private int getBasePosition(StackTraceElement[] stackTraceElement)
    {
        int oldBase = 0;
        for(; oldBase < 3; oldBase++)
        {
            if (stackTraceElement[oldBase].getClassName().equals(FastUnsafeOffHeapDataStorage.class.getName()))
            {
                break;
            }
        }
        return oldBase + 1;
    }

    private boolean isBusy(Thread thread, StackTraceElement[] newStackTraceElement)
    {
        boolean busy = thread.getState().equals(State.RUNNABLE);
        if (busy)
        {
            busy = false;
            for (int i = 0; i < 3 && i < newStackTraceElement.length; i++)
            {
                if (newStackTraceElement[i].getClassName().equals(FastUnsafeOffHeapDataStorage.class.getName()))
                {
                    busy = true;
                    break;
                }
            }
        }
        return busy;
    }

    private static final class OffHeapThreadSnapShot
    {
        private long base;
        private Map<Thread, StackTraceElement[]> activeThreads;
        private long startTime;

        private OffHeapThreadSnapShot(long base, Map<Thread, StackTraceElement[]> activeThreads)
        {
            this.base = base;
            this.activeThreads = activeThreads;
            startTime = System.currentTimeMillis();
        }

        public boolean isReadyForFree(long time)
        {
            return activeThreads.isEmpty() && time > startTime + POLL_PERIOD/2;
        }
    }
}
