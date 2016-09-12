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

package com.gs.fw.common.mithra.test;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraManager;

import java.lang.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;


public class MultiThreadingUtil
{
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadingUtil.class.getName());

    public static Logger getLogger()
    {
        return logger;
    }

    public static boolean runMultithreadedTest(int concurrentCount, int maxTimeToWait, FailureHandler handler, Runnable... runnables)
    {
        long start = System.currentTimeMillis();
        boolean allSuccess = true;
        Thread[] threads = new Thread[concurrentCount];
        TestRunnable[] testRunnables = new TestRunnable[concurrentCount];

        for(int i = 0; i < runnables.length && haveTimeLeft(start, maxTimeToWait);)
        {
            boolean allThreadsAlive = true;
            for(int threadIndex = 0; threadIndex < concurrentCount && i < runnables.length; threadIndex++)
            {
                if(threads[threadIndex] == null || !threads[threadIndex].isAlive())
                {
                    if(threads[threadIndex] != null && testRunnables[threadIndex].getCauseOfFailure() != null)
                    {
                        allSuccess = false;
                        handler.handleFailure(threads, testRunnables[threadIndex].getCauseOfFailure());
                    }
                    testRunnables[threadIndex] = new TestRunnable(runnables[i++]);
                    //ideally we would re-use threads (and thus use a Runnable producer/thread consumer model),
                    //but that would require concurrency controls beyond the scope of this test...
                    threads[threadIndex] = new Thread(testRunnables[threadIndex]);
                    threads[threadIndex].start();
                    allThreadsAlive = false;
                }
            }
            waitIfAllowed(allThreadsAlive, start, maxTimeToWait);
        }

        boolean stillWorking = haveTimeLeft(start, maxTimeToWait);
        while(stillWorking)
        {
            boolean haveSurvivor = false;
            for(int i = 0; i < threads.length; i++)
            {
                if(threads[i] != null)
                {
                    if(!threads[i].isAlive())
                    {
                        if(testRunnables[i].getCauseOfFailure() != null)
                        {
                            allSuccess = false;
                            handler.handleFailure(threads, testRunnables[i].getCauseOfFailure());
                        }
                        threads[i] = null;
                    }
                    else
                        haveSurvivor = true;
                }
            }
            stillWorking = haveSurvivor && haveTimeLeft(start, maxTimeToWait);
            waitIfAllowed(stillWorking, start, maxTimeToWait);
        }

        if(!haveTimeLeft(start, maxTimeToWait))
        {
            String msg = "";
            for(int i = 0; i < threads.length; i++)
            {
                if (threads[i] != null)
                {
                    long curThread = threads[i].getId();
                    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                    ThreadInfo threadInfo = threadMXBean.getThreadInfo(curThread, Integer.MAX_VALUE);
                    StackTraceElement[] stackTrace = threadInfo.getStackTrace();
                    msg += "Thread: "+threads[i].getName()+" is "+threads[i].getState()+" at:"+"\n";
                    for(StackTraceElement elm: stackTrace)
                    {
                        msg += "    "+elm.toString()+"\n";
                    }
                }
            }

            for(int i = 0; i < threads.length; i++)
            {
                if(threads[i] != null && threads[i].isAlive())
                    threads[i].interrupt();
            }
            allSuccess = false;
            Assert.fail("Failed to finish in allowable maximum of " + maxTimeToWait + " ms. Stack traces:\n"+msg);
        }
        return allSuccess;
    }

    private static void waitIfAllowed(boolean stillAlive, long start, int maxTimeToWait)
    {
        try
        {
            if(stillAlive && haveTimeLeft(start, maxTimeToWait)) Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
            Assert.fail("Test thread interrupted???");
        }
    }

    private static boolean haveTimeLeft(long start, int maxTimeToWait)
    {
        return (System.currentTimeMillis() - start) < maxTimeToWait;
    }

    public static boolean runMultithreadedTest(Runnable listInserterThread1, Runnable listInserterThread2)
    {
        return runMultithreadedTest(2, 1000000, new FailureHandler(), listInserterThread1, listInserterThread2);
    }

    public static class FailureHandler
    {
        public final void handleFailure(Thread[] threads, Throwable t)
        {
            String errorMsg = this.handleFailure(t);
            if (errorMsg != null)
            {
                for (int i = 0; i < threads.length; i++)
                {
                    Thread thread = threads[i];
                    if(thread != null && thread.isAlive()) thread.interrupt();
                }
                Assert.fail(errorMsg);
            }
        }

        public String handleFailure(Throwable t)
        {
            return null;
        }
    }

    protected static class TestRunnable implements Runnable
    {
        private Runnable wrapped;
        private Throwable causeOfFailure;

        public TestRunnable(Runnable wrapped)
        {
            this.wrapped = wrapped;
        }

        public void run()
        {
            try
            {
                this.wrapped.run();
            }
            catch (Throwable e)
            {
                getLogger().error("error in test runnable ", e);
                causeOfFailure = e;
                MithraTransaction tx = MithraManager.getInstance().getCurrentTransaction();
                if(tx != null)
                {
                    tx.rollback();
                }
            }
        }

        public Throwable getCauseOfFailure()
        {
            return causeOfFailure;
        }
    }
}
