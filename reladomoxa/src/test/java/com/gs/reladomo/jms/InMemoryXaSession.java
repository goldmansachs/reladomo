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

package com.gs.reladomo.jms;

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;

import javax.jms.*;
import javax.transaction.xa.XAResource;
import java.io.Serializable;

public class InMemoryXaSession implements XASession
{
    private InMemoryXaConnection inMemoryXaConnection;
    private InMemoryXaResource xaResource = new InMemoryXaResource(this);

    public InMemoryXaSession(InMemoryXaConnection inMemoryXaConnection)
    {
        this.inMemoryXaConnection = inMemoryXaConnection;
    }

    public InMemoryXaConnection getInMemoryXaConnection()
    {
        return inMemoryXaConnection;
    }

    @Override
    public Session getSession() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public InMemoryXaResource getXAResource()
    {
        return xaResource;
    }

    @Override
    public boolean getTransacted() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void commit() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void rollback() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException
    {
        return new InMemoryBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Message createMessage() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public TextMessage createTextMessage() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public TextMessage createTextMessage(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getAcknowledgeMode() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws JMSException
    {
        //nothing to do
    }

    @Override
    public void recover() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MessageListener getMessageListener() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void run()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException
    {
        return new InMemoryProducer(destination, this);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String s, boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Queue createQueue(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Topic createTopic(String s) throws JMSException
    {
        return new InMemoryTopic(this);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String consumerName) throws JMSException
    {
        return getOrCreateTopicState((InMemoryTopic)topic).addConsumer(consumerName);
    }

    private InMemoryTopicState getOrCreateTopicState(InMemoryTopic topic)
    {
        return InMemoryBroker.getInstance().getOrCreateTopicState(topic);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String s, String s2, boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void unsubscribe(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }
}
