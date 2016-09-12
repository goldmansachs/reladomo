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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ContextCache implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ContextCache.class.getName());

    private static final ContextCache INSTANCE;
    private static final int SLEEP_TIME = 30000; // every 30 seconds

    private final ConcurrentHashMap<RequestId, Context> cache = new ConcurrentHashMap<RequestId, Context>();

    static
    {
        INSTANCE = new ContextCache();
        Thread clearCacheThread = new Thread(INSTANCE);
        clearCacheThread.setName("PSP clear context cache thread");
        clearCacheThread.setDaemon(true);
        clearCacheThread.start();
    }

    // singleton
    private ContextCache()
    {
    }

    public static ContextCache getInstance()
    {
        return INSTANCE;
    }

    public static Logger getLogger()
    {
        return LOGGER;
    }

    public void removeContext(RequestId requestId)
    {
        this.cache.remove(requestId);
    }

    public Context getContext(RequestId requestId)
    {
        return this.cache.get(requestId);
    }

    public Context getOrCreateContext(RequestId requestId)
    {
        Context result = new Context();
        Context existing = this.cache.putIfAbsent(requestId, result);
        if (existing == null)
        {
            existing = result;
        }
        return existing;
    }

    private void removeExpiredContexts()
    {
        if (this.cache.isEmpty())
        {
            return;
        }
        Iterator it = this.cache.values().iterator();
        while (it.hasNext())
        {
            Context context = (Context) it.next();
            if (context.isExpired())
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("removing stale context");
                }
                it.remove();
            }
        }
    }

    public void run()
    {
        while (true)
        {
            try
            {
                Thread.sleep(SLEEP_TIME);
                this.removeExpiredContexts();
            }
            catch (InterruptedException e)
            {
                // ok, nothing to do
            }
            catch (Throwable t)
            {
                // this is impossible, but let's not take any chances.
                LOGGER.error("Unexpected exception", t);
            }
        }
    }
}
