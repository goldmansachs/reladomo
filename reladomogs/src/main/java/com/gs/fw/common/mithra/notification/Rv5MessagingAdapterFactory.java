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

import COM.TIBCO.rv.RvException;
import COM.TIBCO.rv.RvSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraException;



public class Rv5MessagingAdapterFactory implements MithraMessagingAdapterFactory
{

    private final Logger logger = LoggerFactory.getLogger(Rv5MessagingAdapterFactory.class.getName());
    private RvSession  rvSession = null;
    private String daemon = null;
    private String service = "27111";
    private boolean rvInitialized = false;

    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        this.initialiseRvObjects();
        return new Rv5NotificationMessagingAdapter(subject, rvSession);
    }
                                  
    public Logger getLogger()
    {
        return logger;
    }

    public Rv5MessagingAdapterFactory()
    {
    }

    public Rv5MessagingAdapterFactory(String daemon)
    {
        this.daemon = daemon;
    }

    public void setDaemon(String daemon)
    {
        this.daemon = daemon;
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

    private void initialiseRvObjects()
    {
        if(!this.rvInitialized)
        {
            try
            {
                if (this.rvSession == null)
                {
                    this.rvSession = new RvSession(this.service,"",this.daemon);
                }
                rvInitialized = true;
            }
            catch (RvException e)
            {                
                logger.error("RV Session initialization failed - daemon:  "+this.getDaemon()+", service: "+this.getService());
                throw new RuntimeException("Could not initialize RV5 messaging service. See the cause below", e);
            }
        }
    }
    public void shutdown()
    {
        if(rvSession != null && rvSession.isValid())
            try
            {
                rvSession.term();
            }
            catch (RvException e)
            {
                throw new MithraException("Could not terminate RV5 session", e);
            }
    }
}
