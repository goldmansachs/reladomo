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

package com.gs.reladomo.util;

import java.sql.Timestamp;

import org.slf4j.Logger;

public class InterruptableBackoff
{
    private final long minBackoffTime;
    private long maxBackoffTime;
    private final Logger logger;

    private long currentBackoff;
    private long interruptedTime = 0;
    private long lastLogTime = 0;
    private long wakeUpSoonAfter = 0;

    public InterruptableBackoff(long minBackoffTime, long maxBackoffTime, Logger logger)
    {
        this.minBackoffTime = minBackoffTime;
        this.maxBackoffTime = maxBackoffTime;
        this.logger = logger;
        this.currentBackoff = minBackoffTime;
    }

    public void setMaxBackoffTime(long maxBackoffTime)
    {
        this.maxBackoffTime = maxBackoffTime;
    }

    public synchronized void reset()
    {
        currentBackoff = minBackoffTime;
    }

    public synchronized boolean sleep()
    {
        boolean result = false;
        long start = System.currentTimeMillis();
        try
        {
            long waitTarget = this.currentBackoff;
            if (this.wakeUpSoonAfter != 0)
            {
                long now = System.currentTimeMillis();
                if (now - this.currentBackoff / 2 < this.wakeUpSoonAfter && now + this.currentBackoff > this.wakeUpSoonAfter)
                {
                    waitTarget = Math.min(waitTarget, Math.abs(this.wakeUpSoonAfter - now) + 100);
                }
            }
            if (waitTarget != this.currentBackoff)
            {
                logger.info("Reduced sleeping near cut off to {} millis", waitTarget);
            }
            else
            {
                logger.info("Sleeping for {} millis before starting the next batch", waitTarget);
            }
            this.wait(waitTarget);
        }
        catch (InterruptedException e)
        {
            //ignore
        }
        if (interruptedTime > start)
        {
            result = true;
            if (lastLogTime < System.currentTimeMillis() - 5 * 60 * 1000)
            {
                lastLogTime = System.currentTimeMillis();
                logger.info("Async interrupt for immediate processing");
            }
        }
        else
        {
            currentBackoff *= 2;
            if (currentBackoff > maxBackoffTime)
            {
                currentBackoff = maxBackoffTime;
            }
        }
        return result;
    }

    public synchronized void asyncInterrupt()
    {
        this.notify();
        interruptedTime = System.currentTimeMillis();
        reset();
    }

    public void setWakeUpSoonAfter(Timestamp wakeUpSoonAfter)
    {
        this.wakeUpSoonAfter = wakeUpSoonAfter.getTime();
    }
}
