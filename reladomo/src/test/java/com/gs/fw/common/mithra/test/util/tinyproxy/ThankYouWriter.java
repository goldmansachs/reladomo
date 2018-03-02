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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ThankYouWriter implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ThankYouWriter.class);
    private static final ThankYouWriter INSTANCE;
    private static final int SLEEP_TIME = 500; // so multiple requests get coalesced

    private boolean done = true;

    private final Map<URL, List> requestMap = UnifiedMap.newMap();

    // singelton
    private ThankYouWriter()
    {
    }

    public static ThankYouWriter getInstance()
    {
        return INSTANCE;
    }

    static
    {
        INSTANCE = new ThankYouWriter();
    }

    private synchronized void startThankYouThread()
    {
        if (this.done)
        {
            this.done = false;
            Thread thankYouThread = new Thread(INSTANCE);
            thankYouThread.setName("PSP Thank You Thread");
            thankYouThread.setDaemon(true);
            thankYouThread.start();
        }
    }

    public synchronized void stopThankYouThread()
    {
        this.done = true;
        this.requestMap.clear();
    }

    public static Logger getLogger()
    {
        return LOGGER;
    }

    public synchronized void addRequest(URL url, RequestId requestId)
    {
        this.startThankYouThread();
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("added request for {}", url);
        }
        requestId.setFinishedTime(System.currentTimeMillis());
        ArrayList list = (ArrayList) this.requestMap.get(url);
        if (list == null)
        {
            list = new ArrayList(5);
            this.requestMap.put(url, list);
        }
        list.add(requestId);
        this.notifyAll();
    }

    public synchronized ArrayList removeRequestList(URL url)
    {
        return (ArrayList) this.requestMap.remove(url);
    }

    public void run()
    {
        List urlsToSend = new ArrayList(5);
        while (!this.done)
        {
            try
            {
                synchronized (this)
                {
                    if (this.requestMap.isEmpty())
                    {
                        this.wait();
                    }
                }
                Thread.sleep(SLEEP_TIME);
                this.getUrlsToSend(urlsToSend);
                for (int i = 0; i < urlsToSend.size(); i++)
                {
                    URL url = (URL) urlsToSend.get(i);
                    if (!this.done)
                    {
                        this.sendThankYouRequest(url);
                    }
                }
                urlsToSend.clear();
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

    void sendThankYouRequest(URL url)
    {
        boolean success = false;
        ArrayList requestList = this.removeRequestList(url);
        try
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Sending thank you for {}", requestList.size());
            }
            OutputStreamWriter writer = new ThankYouStreamWriter(requestList);
            HttpURLConnection urlc = OutputStreamWriter.post(new URL(url.toString() + "?thanks"), writer);

            int code = OutputStreamWriter.flushInputStream(urlc);
            success = code == 200;
        }
        catch (Exception e)
        {
            LOGGER.warn("Exception in PSP thank you note for URL: {} Retrying.", url.toString(), e);
        }
        if (!success)
        {
            this.readdList(url, requestList);
        }
    }

    void readdList(URL url, ArrayList requestList)
    {
        // borisv per Moh's direction: technically requestList cannot be null, but in rear conditions, where JVM runs
        // out of memory, it can be null. Here is example:
        if (!this.done && requestList != null)
        {
            for (int i = 0; i < requestList.size(); )
            {
                RequestId requestId = (RequestId) requestList.get(i);
                if (requestId.isExpired())
                {
                    requestList.remove(i);
                }
                else
                {
                    i++;
                }
            }
            if (requestList.isEmpty())
            {
                return;
            }
            synchronized (this)
            {
                ArrayList list = (ArrayList) this.requestMap.get(url);
                if (list == null)
                {
                    this.requestMap.put(url, requestList);
                    this.notifyAll();
                }
                else
                {
                    list.addAll(requestList);
                }
            }
        }
    }

    synchronized void getUrlsToSend(List listToSend)
    {
        listToSend.addAll(this.requestMap.keySet());
    }

    protected static class ThankYouStreamWriter extends OutputStreamWriter
    {
        private final ArrayList requestList;

        protected ThankYouStreamWriter(ArrayList requestList)
        {
            this.requestList = requestList;
        }

        @Override
        public byte getRequestType()
        {
            return StreamBasedInvocator.THANK_YOU_REQUEST;
        }

        @Override
        public void writeParameters(ObjectOutputStream objectOutputStream) throws IOException
        {
            objectOutputStream.writeInt(this.requestList.size());
            for (int i = 0; i < this.requestList.size(); i++)
            {
                //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
                objectOutputStream.writeObject(this.requestList.get(i));
            }
        }
    }
}
