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

public class InMemoryBroker
{
    private static InMemoryBroker instance = new InMemoryBroker();

    public static InMemoryBroker getInstance()
    {
        return instance;
    }

    private ConcurrentHashMap<InMemoryTopic, InMemoryTopicState> topicStates = ConcurrentHashMap.newMap();

    private InMemoryBroker()
    {

    }

    public InMemoryTopicState getOrCreateTopicState(InMemoryTopic topic)
    {
        return this.topicStates.getIfAbsentPut(topic, new com.gs.collections.api.block.function.Function<InMemoryTopic, InMemoryTopicState>()
                {
                    @Override
                    public InMemoryTopicState valueOf(InMemoryTopic object)
                    {
                        return new InMemoryTopicState(object);
                    }
                });
    }

    public InMemoryTopicState getTopicState(InMemoryTopic topic)
    {
        return this.topicStates.get(topic);
    }

    public void clear()
    {
        this.topicStates.clear();
    }
}
