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

import java.util.List;

import com.gs.collections.api.block.function.Function0;
import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;

import javax.jms.Message;
import javax.jms.TopicSubscriber;
import javax.transaction.xa.Xid;

public class InMemoryTopicState
{
    private InMemoryTopic topic;
    private ConcurrentHashMap<String, InMemoryConsumerState> consumerStates = ConcurrentHashMap.newMap();
    private ConcurrentHashMap<Xid, List<Message>> uncommittedMessages = ConcurrentHashMap.newMap();

    public InMemoryTopicState(InMemoryTopic topic)
    {
        this.topic = topic;
    }

    public TopicSubscriber addConsumer(String consumerName)
    {
        InMemoryConsumerState state = consumerStates.getIfAbsentPut(consumerName, new InMemoryConsumerState(consumerName, this));
        return state.subscribe(topic);
    }

    public void send(InMemoryXaResource xaResource, Message message)
    {
        xaResource.registerCallback(this);
        List<Message> list = uncommittedMessages.getIfAbsentPut(xaResource.getCurrentXid(), new Function0<List<Message>>()
        {
            @Override
            public List<Message> value()
            {
                return FastList.newList();
            }
        });
        synchronized (list)
        {
            list.add(message);
        }
    }

    public void rollback(Xid xid)
    {
        uncommittedMessages.remove(xid);
        consumerStates.forEach(new Procedure<InMemoryConsumerState>()
        {
            @Override
            public void value(InMemoryConsumerState each)
            {
                each.rollback();
            }
        });
    }

    public void commit(Xid xid)
    {
        final List<Message> messages = uncommittedMessages.remove(xid);
        if (messages != null)
        {
            synchronized (messages)
            {
                consumerStates.forEach(new Procedure<InMemoryConsumerState>()
                {
                    @Override
                    public void value(InMemoryConsumerState each)
                    {
                        each.commit(messages);
                    }
                });
            }
        }
        else
        {
            consumerStates.forEach(new Procedure<InMemoryConsumerState>()
                            {
                                @Override
                                public void value(InMemoryConsumerState each)
                                {
                                    each.commit(null);
                                }
                            });
        }
    }

    public void registerCallback()
    {
        this.topic.getXaResource().registerCallback(this);
    }
}
