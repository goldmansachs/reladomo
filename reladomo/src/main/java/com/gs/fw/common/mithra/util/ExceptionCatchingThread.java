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
import com.gs.fw.common.mithra.MithraManagerProvider;


public class ExceptionCatchingThread extends Thread
{
    static private Logger logger = LoggerFactory.getLogger(ExceptionCatchingThread.class.getName());
    private Throwable thrown;

    private static AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(5000, "Mithra Reusable")
    {
        protected void cleanUpAfterTask()
        {
            MithraManagerProvider.getMithraManager().zDealWithHungTx();
        }
    };

    static
    {
        executor.setTimeoutInMilliseconds(5*60*1000); // 5 minutes
    }

    public ExceptionCatchingThread(Runnable target)
    {
        super(target);
    }

    public void run()
    {
        try
        {
            super.run();
        }
        catch (Throwable e)
        {
            this.thrown = e;
            logger.error("runnable threw exception", e);
        }
    }

    public void joinWithExceptionHandling()
    {
        try
        {
            this.join();
        }
        catch (InterruptedException e)
        {
            logger.error("unexpected interruption", e);
        }
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

    public static void runToCompletion(Runnable runnable)
    {
        ExceptionCatchingThread thread = new ExceptionCatchingThread(runnable);
        thread.start();
        thread.joinWithExceptionHandling();
    }

    public static void submitTask(ExceptionHandlingTask runnable)
    {
        executor.submit(runnable);
    }

    public static void executeTask(ExceptionHandlingTask runnable)
    {
        executor.submit(runnable);
        runnable.waitUntilDoneWithExceptionHandling();
    }

    public static void executeTaskIgnoringExceptions(ExceptionHandlingTask runnable)
    {
        executor.submit(runnable);
        runnable.waitUntilDoneIgnoringExceptions();
    }
}
