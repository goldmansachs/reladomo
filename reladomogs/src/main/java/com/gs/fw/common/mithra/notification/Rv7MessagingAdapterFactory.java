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

package com.gs.fw.common.mithra.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraException;
import com.tibco.tibrv.*;



public class Rv7MessagingAdapterFactory implements MithraMessagingAdapterFactory, TibrvErrorCallback
{

    private final Logger logger = LoggerFactory.getLogger(Rv7MessagingAdapterFactory.class.getName());
    private boolean             rvOpen = false;
    private boolean rvInitialized = false;
    private TibrvQueue          queue = null;
    private TibrvDispatcher     dispatcher = null;
    private TibrvTransport   transport = null;
    private String daemon;
    private String service = "27111";
    private String network = null;
    private String rvTopicPrefix = "";

    public Rv7MessagingAdapterFactory()
    {
    }

    public String getNetwork()
    {
        return network;
    }

    public void setNetwork(String network)
    {
        this.network = network;
    }

    public Logger getLogger()
    {
        return logger;
    }

    public String getService()
    {
        return service;
    }

    public void setService(String service)
    {
        this.service = service;
    }

    public String getDaemon()
    {
        return daemon;
    }

    public void setDaemon(String daemon)
    {
        this.daemon = daemon;
    }

    public String getRvTopicPrefix()
    {
        return rvTopicPrefix;
    }

    public void setRvTopicPrefix(String rvTopicPrefix)
    {
        this.rvTopicPrefix = rvTopicPrefix;
    }

    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        try
        {
            this.initialiseRvObjects();
            return new Rv7NotificationMessagingAdapter(rvTopicPrefix, subject, queue, this.transport);
        }
        catch (TibrvException e)
        {
            String msg = "RV Session initialization failed "+e.getClass().getSimpleName()+": "+e.getMessage()
                    +"- daemon:  " + this.getDaemon() + ", service: " + this.getService();
            logger.error(msg);
            if (logger.isDebugEnabled())
            {
                logger.debug(msg, e);
            }
            throw new RuntimeException(e);
        }
    }

    private TibrvTransport initializeTransport() throws TibrvException
    {
        if (this.transport == null)
        {
            if (this.getDaemon() == null)
            {
                throw new RuntimeException("Daemon must be set before transport initialization!");
            }
            this.transport = new TibrvRvdTransport(this.getService(), network, this.getDaemon());
        }
        return this.transport;
    }

    protected synchronized void ensureRvOpen() throws TibrvException
    {
        if (!this.rvOpen)
        {
            loadWindowsLibs();
            Tibrv.open(Tibrv.IMPL_NATIVE);
            Tibrv.setErrorCallback(this);
            this.rvOpen = true;
        }
    }

    private void loadWindowsLibs()
    {
        if (System.getProperty("os.name").startsWith("Windows"))
        {
            System.loadLibrary("tibrv");
            System.loadLibrary("tibrvcm");
            System.loadLibrary("tibrvft");
            System.loadLibrary("tibrvcmq");
        }
    }

    private void initialiseRvObjects()
    throws TibrvException
    {
        if(!this.rvInitialized)
        {
            this.ensureRvOpen();
            this.queue = new TibrvQueue();
            this.dispatcher = new TibrvDispatcher(this.queue);
            this.initializeTransport();
            this.rvInitialized = true;

        }
    }

    public void shutdown()
    {
        if(this.dispatcher != null)
            this.dispatcher.destroy();
        if(this.queue != null)
            this.queue.destroy();
        if(this.transport != null)
            this.transport.destroy();
        this.rvInitialized = false;

        if(this.rvOpen)
        {
            try
            {
                Tibrv.close();
                this.rvOpen = false;
            }
            catch(TibrvException e)
            {
                throw new MithraException("Could not destroy RV service");
            }
        }
    }

    public void onError(Object tibrvObject, int errorCode, String errorMessage, Throwable throwable)
    {
        logger.error("Processing asynchronous error: ");
        if(tibrvObject instanceof TibrvListener)
        {
            TibrvListener listener = (TibrvListener)tibrvObject;
            logger.error("Problems with TibRvListener");
            logger.error("Topic: "+listener.getSubject());
        }
        else
        {
            logger.error("Problems with TibRvTransport");
        }
        logger.error("Error code: "+errorCode);
        logger.error("Error message: "+errorMessage);
        if(throwable != null)
        {
            logger.error("Additional information: "+throwable.getMessage());
        }
    }
}
