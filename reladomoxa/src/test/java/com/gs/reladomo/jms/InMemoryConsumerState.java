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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import javax.jms.Message;
import javax.jms.TopicSubscriber;

import com.gs.collections.impl.list.mutable.FastList;

public class InMemoryConsumerState
{
    private final String consumerName;
    private final InMemoryTopicState topicState;
    private final Deque<Message> messages = new ArrayDeque<Message>();
    private final List<Message> readButNotCommitted = FastList.newList();

    public InMemoryConsumerState(String consumerName, InMemoryTopicState inMemoryTopicState)
    {
        this.consumerName = consumerName;
        this.topicState = inMemoryTopicState;
    }

    public TopicSubscriber subscribe(InMemoryTopic topic)
    {
        return new InMemoryTopicSubscriber(this, topic);
    }

    public synchronized Message receive(long timeout)
    {
        this.topicState.registerCallback();
        try
        {
            if (messages.isEmpty())
            {
                this.wait(timeout);
            }
        }
        catch (InterruptedException e)
        {
            // ignore
        }
        if (messages.isEmpty())
        {
            return null;
        }
        Message message = messages.removeFirst();
        readButNotCommitted.add(message);
        return message;
    }

    public synchronized void commit(List<Message> messages)
    {
        if (messages != null)
        {
            this.messages.addAll(messages);
            this.notifyAll();
        }
        this.readButNotCommitted.clear();
    }

    public synchronized void rollback()
    {
        for(int i=readButNotCommitted.size() - 1; i>=0; i--)
        {
            this.messages.addFirst(readButNotCommitted.get(i));
        }
        readButNotCommitted.clear();
    }
}
