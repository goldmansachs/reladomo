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

import javax.jms.*;



public class JmsMessagingAdapter implements MithraNotificationMessagingAdapter, MessageListener
{
    static private Logger logger = LoggerFactory.getLogger(JmsMessagingAdapter.class.getName());
    private String topicName;
    private TopicSession topicSession;
    private TopicPublisher topicPublisher;
    private TopicSubscriber topicSubscriber;
    private MithraNotificationMessageHandler messageHandler;

    public JmsMessagingAdapter(String topicName, TopicSession topicSession)
    {
        this.topicName = topicName;
        this.topicSession = topicSession;
    }

    private void initialize()
    throws JMSException
    {
        Topic topic = topicSession.createTopic(topicName);
        topicSubscriber = topicSession.createSubscriber(topic);
        topicPublisher = topicSession.createPublisher(topic);
        topicSubscriber.setMessageListener(this);
    }

    public void broadcastMessage(byte[] message)
    {
        try
        {
            BytesMessage  bytesMessage = topicSession.createBytesMessage();
            bytesMessage.writeBytes(message);
            topicPublisher.publish(bytesMessage);
            if(logger.isDebugEnabled())
            {
                logger.debug("Message with topic "+topicName+" sent by topicPublisher: "+topicPublisher);
            }
        }
        catch(JMSException jmse)
        {
            logger.error("Unable to send notification message", jmse);
        }
    }

    public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
    {
        this.messageHandler = messageHandler;
        try
        {
            this.initialize();
        }
        catch (JMSException e)
        {
            throw new RuntimeException("Unable to initialize ActiveMq messaging adapter", e);
        }
    }

    public void shutdown()
    {
        try
        {
           if(topicSubscriber != null)
               topicSubscriber.close();
           if(topicPublisher != null)
               topicPublisher.close();
        }
        catch(JMSException e)
        {
            throw new MithraException("Could not shutdown Active MQ messaging adapter", e);
        }

    }

    public void onMessage(Message message)
    {
        try
        {
            if(logger.isDebugEnabled())
            {
                logger.debug("Message with topic "+topicName+" received by Subscriber: "+topicSubscriber);
            }
            BytesMessage bytesMessage = (BytesMessage) message;
            byte[] messageBuffer = new byte[(int)bytesMessage.getBodyLength()];
            bytesMessage.readBytes(messageBuffer);
            this.messageHandler.processNotificationMessage(topicName, messageBuffer);
        }
        catch (Exception e)
        {
           logger.error("Unable to extract message object");
           throw new RuntimeException("Unable to extract message object",e);
        }
    }
}
