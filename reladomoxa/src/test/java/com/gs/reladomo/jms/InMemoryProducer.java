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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

public class InMemoryProducer implements MessageProducer
{
    private Destination destination;
    private InMemoryXaSession inMemoryXaSession;

    public InMemoryProducer(Destination destination, InMemoryXaSession inMemoryXaSession)
    {
        this.destination = destination;
        this.inMemoryXaSession = inMemoryXaSession;
    }

    @Override
    public void setDisableMessageID(boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean getDisableMessageID() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setDisableMessageTimestamp(boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setDeliveryMode(int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getDeliveryMode() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setPriority(int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getPriority() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setTimeToLive(long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long getTimeToLive() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Destination getDestination() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws JMSException
    {
    }

    @Override
    public void send(Message message) throws JMSException
    {
        InMemoryTopic topic = (InMemoryTopic) this.destination;
        topic.send(message);
    }

    @Override
    public void send(Message message, int i, int i2, long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void send(Destination destination, Message message, int i, int i2, long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }
}
