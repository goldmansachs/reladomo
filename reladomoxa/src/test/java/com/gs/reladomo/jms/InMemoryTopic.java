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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;

public class InMemoryTopic implements Topic
{
    private InMemoryXaSession inMemoryXaSession;

    public InMemoryTopic(InMemoryXaSession inMemoryXaSession)
    {
        this.inMemoryXaSession = inMemoryXaSession;
    }

    @Override
    public String getTopicName() throws JMSException
    {
        return inMemoryXaSession.getInMemoryXaConnection().getInMemoryTopicConfig().getTopicName();
    }

    @Override
    public int hashCode()
    {
        return inMemoryXaSession.getInMemoryXaConnection().getInMemoryTopicConfig().getTopicName().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        InMemoryTopic other = (InMemoryTopic) obj;
        return inMemoryXaSession.getInMemoryXaConnection().getInMemoryTopicConfig().getTopicName().equals(other.inMemoryXaSession.getInMemoryXaConnection().getInMemoryTopicConfig().getTopicName());
    }

    public void send(Message message)
    {
        InMemoryBroker.getInstance().getTopicState(this).send(getXaResource(), message);
    }

    public InMemoryXaResource getXaResource()
    {
        return inMemoryXaSession.getXAResource();
    }
}
