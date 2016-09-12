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

package com.gs.fw.common.mithra.test.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.notification.MithraMessagingAdapterFactory;
import com.gs.fw.common.mithra.notification.MithraNotificationMessagingAdapter;
import com.gs.fw.common.mithra.notification.JmsMessagingAdapter;
import org.apache.activemq.ActiveMQTopicSession;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;



public class ActiveMqMessagingAdapterFactory implements MithraMessagingAdapterFactory
{
    private static Logger logger = LoggerFactory.getLogger(ActiveMqMessagingAdapterFactory.class);
    private boolean transacted = false;
    private int port;
    private TopicSession topicSession;
    private TopicConnection topicConnection;
    private ActiveMQTopicSession amqTopicSession;

    public ActiveMqMessagingAdapterFactory(int port)
    {
        this.port = port;
        try
        {
            this.initialize();
        }
        catch(JMSException e)
        {
            throw new RuntimeException("Unable to initialize JMS Connection", e);
        }
    }

    public void shutdown()
    {
        if(amqTopicSession != null)
        {
            try
            {
                amqTopicSession.close();
                amqTopicSession = null;
            }
            catch(JMSException jmse)
            {
                throw new RuntimeException("Unable to close Active MQ topicSession", jmse);
            }
        }
        if(topicConnection != null)
        {
            try
            {
                topicConnection.stop();
                topicConnection.close();
                topicConnection = null;
            }
            catch(JMSException jmse)
            {
                throw new RuntimeException("Unable to close Active MQ topicConnection", jmse);
            }
        }
    }

    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        return new JmsMessagingAdapter(subject, amqTopicSession);
    }

    private void initialize()
    throws JMSException
    {
        if(logger.isDebugEnabled())
        {
            logger.debug("Initializing ActiveMQConnectionFactory");
        }
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:"+this.port);
        topicConnection = connectionFactory.createTopicConnection();
        topicConnection.start();
        topicSession = topicConnection.createTopicSession(transacted, Session.AUTO_ACKNOWLEDGE);
        amqTopicSession = new ActiveMQTopicSession(topicSession);
    }
}
