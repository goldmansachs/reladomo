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

import COM.TIBCO.rv.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Rv5NotificationMessagingAdapter  implements MithraNotificationMessagingAdapter, RvDataCallback
{
    static private Logger logger = LoggerFactory.getLogger(Rv5NotificationMessagingAdapter.class.getName());
    private String topicName;
    private RvSession  rvSession = null;
    private RvSender   rvSender  = null;
    private RvListener rvListener  = null;
    private MithraNotificationMessageHandler messageHandler;
    private static final String FIELD_NAME = "MITHRA_NOTIFICATION_MESSAGE";

    public Rv5NotificationMessagingAdapter(String topicName, RvSession rvSession)
    {
        this.topicName = topicName;
        this.rvSession = rvSession;
        this.initialize();
    }

    private void initialize()
    {
       this.createRvListener();
    }

    private RvListener createRvListener()
    {
        if (this.rvListener == null)
        {
            try
            {
                this.rvListener = this.rvSession.newListener(topicName, this);
            }

            catch (RvException e)
            {
                throw new RuntimeException("could not create RV listener", e);
            }
        }
        return this.rvListener;
    }

    public void broadcastMessage(byte[] message)
    {
         RvMsg msg = new RvMsg();
        try
        {
            msg.append(FIELD_NAME, message);
            this.getRvSender().send(msg);
        }
        catch (RvException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
    {
        this.messageHandler = messageHandler;
    }

    public void shutdown()
    {
        if(this.rvListener != null)
            rvListener.remove();
    }

    public void onData(String s, RvSender rvSender, Object o, RvListener rvListener)
    {
        if (o instanceof RvMsg)
        {
            byte[] mithraNotificationMessage = null;
            RvMsg rvMsg = (RvMsg)o;
            RvOpaque message = (RvOpaque)rvMsg.get(FIELD_NAME);
            mithraNotificationMessage = message.contents;
            this.messageHandler.processNotificationMessage(topicName, mithraNotificationMessage);
        }
    }

    protected RvSender getRvSender() throws RvException
    {
        if (this.rvSender == null)
        {
            this.rvSender = this.rvSession.newSender(topicName);
        }
        return this.rvSender;
    }
}
