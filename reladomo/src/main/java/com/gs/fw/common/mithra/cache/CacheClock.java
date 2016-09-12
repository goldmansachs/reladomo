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

package com.gs.fw.common.mithra.cache;

import java.util.concurrent.atomic.AtomicLong;


public class CacheClock
{

    private static AtomicLong now = null;
    private static MithraCacheClockRunnable running = null;
    private static boolean mustRun = true;

    public static synchronized void register(long interval)
    {
        if (interval > 0 && mustRun)
        {
            long realInterval = interval / 10;
            if ((realInterval % 10) > 0) realInterval -= (realInterval % 10);
            if (realInterval > 10)
            {
                if (running == null)
                {
                    now = new AtomicLong(System.currentTimeMillis());
                    running = new MithraCacheClockRunnable(realInterval);
                    running.startRunning();
                }
                else
                {
                    running.modifyInterval(realInterval);
                }
            }
            else
            {
                mustRun = false;
                if (running != null)
                {
                    running.stopRunning();
                }
            }
        }
    }

    public static long getTime()
    {
        if (now == null) return System.currentTimeMillis();
        return now.get();
    }

    public static void forceTick()
    {
        if (now != null) now.set(System.currentTimeMillis());
    }

    private static class MithraCacheClockRunnable implements Runnable
    {
        private long interval;
        private volatile boolean running = true;
        private Thread runner;

        private MithraCacheClockRunnable(long interval)
        {
            this.interval = interval;
        }

        public void run()
        {
            while(running)
            {
                try
                {
                    Thread.sleep(interval);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }
                now.set(System.currentTimeMillis());
            }
        }

        public synchronized void startRunning()
        {
            runner = new Thread(this);
            runner.setDaemon(true);
            runner.setName("Mithra Cache Clock");
            runner.start();
        }

        public synchronized void stopRunning()
        {
            this.running = false;
            runner.interrupt();
            try
            {
                runner.join(interval);
            }
            catch (InterruptedException e)
            {
                // ignore
            }
            now = null;
        }

        public void modifyInterval(long interval)
        {
            if (interval < this.interval)
            {
                this.interval = interval;
                runner.interrupt();
            }
        }
    }
}
