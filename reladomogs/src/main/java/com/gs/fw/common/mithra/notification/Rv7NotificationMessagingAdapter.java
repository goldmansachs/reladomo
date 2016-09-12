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

import com.tibco.tibrv.*;



public class Rv7NotificationMessagingAdapter implements MithraNotificationMessagingAdapter, TibrvMsgCallback
{

    private TibrvListener       rvListener = null;
    private MithraNotificationMessageHandler messageHandler;
    private String rvTopicName;
    private String internalTopicName;
    private TibrvQueue queue;
    private TibrvTransport transport;
    private static final String FIELD_NAME = "MITHRA_NOTIFICATION_MESSAGE";

    public Rv7NotificationMessagingAdapter(String internalTopicName,TibrvQueue queue, TibrvTransport transport)
    {
       this( "", internalTopicName, queue, transport);
    }

    public Rv7NotificationMessagingAdapter(String rvTopicPrefix, String internalTopicName, TibrvQueue queue, TibrvTransport transport)
    {
        this.internalTopicName = internalTopicName;
        this.rvTopicName = rvTopicPrefix + internalTopicName;
        this.queue = queue;
        this.transport = transport;
    }

    private void initialize()
    {
       this.createRvListener();
    }

    public void broadcastMessage(byte[] message)
    {
        try
        {
            TibrvMsg msg = new TibrvMsg();
            msg.setSendSubject(rvTopicName);
            msg.add(FIELD_NAME, message);
            this.transport.send(msg);
        }
        catch (TibrvException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
    {
        this.messageHandler = messageHandler;
        this.initialize();
    }

    public void shutdown()
    {
        if(rvListener != null)
            this.rvListener.destroy();
    }

    public void onMsg(TibrvListener tibrvListener, TibrvMsg tibrvMsg)
    {
        try
        {
            byte[] mithraNotificationMessage = (byte[])tibrvMsg.get(FIELD_NAME);
            this.messageHandler.processNotificationMessage(internalTopicName, mithraNotificationMessage);
        }
        catch(TibrvException e)
        {
            throw new RuntimeException(e);
        }
    }

    private TibrvListener createRvListener()
    {
        if (this.rvListener == null)
        {
            try
            {
                this.rvListener = new TibrvListener(queue, this, transport, rvTopicName, null);
            }
            catch (TibrvException e)
            {
                throw new RuntimeException(e);
            }
        }
        return this.rvListener;
    }
}
