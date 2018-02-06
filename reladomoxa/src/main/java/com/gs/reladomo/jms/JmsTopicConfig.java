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
import javax.naming.NamingException;

public abstract class JmsTopicConfig
{
    private String userName;
    private String password;
    private String durableConsumerName;
    private String topicName;
    private boolean async = false;
    private boolean enableLowLatency = false;

    public JmsTopicConfig(String userName, String password, String durableConsumerName, String topicName)
    {
        this.userName = userName;
        this.password = password;
        this.durableConsumerName = durableConsumerName;
        this.topicName = topicName;
    }

    public JmsTopicConfig(String durableConsumerName, String topicName) // for tests
    {
        this.durableConsumerName = durableConsumerName;
        this.topicName = topicName;
    }

    public abstract XAConnectionFactory createXaConnectionFactory(JmsTopicConfig config) throws NamingException;

    public abstract Topic createTopic(JmsTopicConfig config, Session session) throws NamingException, JMSException;

    public String getPassword()
    {
        return password;
    }

    public String getUserName()
    {
        return userName;
    }

    public String getDurableConsumerName()
    {
        return durableConsumerName;
    }

    public String getTopicName()
    {
        return topicName;
    }

    public XAConnection createConnection() throws JMSException, NamingException
    {
        XAConnectionFactory xaConnectionFactory = createXaConnectionFactory(this);
        if (getUserName() == null)
        {
            return xaConnectionFactory.createXAConnection();
        }
        else
        {
            return xaConnectionFactory.createXAConnection(this.getUserName(), this.getPassword());
        }
    }

    public boolean isOutGoingAsync()
    {
        return async;
    }

    public void setAsync(boolean async)
    {
        this.async = async;
    }

    public void setEnableLowLatency(boolean enableLowLatency)
    {
        this.enableLowLatency = enableLowLatency;
    }

    public boolean isEnableLowLatency()
    {
        return enableLowLatency;
    }

    public void markConnectionDirty()
    {
        //only tibjms does something here
    }
}
