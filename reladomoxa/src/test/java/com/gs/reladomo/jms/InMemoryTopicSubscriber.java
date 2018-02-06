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

import javax.jms.*;

public class InMemoryTopicSubscriber implements TopicSubscriber
{
    private InMemoryConsumerState inMemoryConsumerState;
    private InMemoryTopic topic;

    public InMemoryTopicSubscriber(InMemoryConsumerState inMemoryConsumerState, InMemoryTopic topic)
    {
        this.inMemoryConsumerState = inMemoryConsumerState;
        this.topic = topic;
    }

    @Override
    public Topic getTopic() throws JMSException
    {
        return topic;
    }

    @Override
    public boolean getNoLocal() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getMessageSelector() throws JMSException
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
    public Message receive() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Message receive(long timeout) throws JMSException
    {
        return this.inMemoryConsumerState.receive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws JMSException
    {
        //nothing to do
    }
}
