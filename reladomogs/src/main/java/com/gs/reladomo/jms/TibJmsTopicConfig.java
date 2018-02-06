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

import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.NamingException;
import java.util.concurrent.atomic.AtomicInteger;
import com.tibco.tibjms.TibjmsTopic;
import com.tibco.tibjms.TibjmsXAConnectionFactory;

public class TibJmsTopicConfig extends JmsTopicConfig
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TibJmsTopicConfig.class);

    private static final AtomicInteger publisherId = new AtomicInteger(1);
    private static final AtomicInteger forcedConnectionCounter = new AtomicInteger(1);
    private static final UnifiedMap<XaConnnectionKey, RefCountedJmsXaConnection> pool = UnifiedMap.newMap();
    private String clientIdPrefix;
    private int assignedPostfix = -1;
    private boolean usePooling = true;

    private String brokerUrl;

    public TibJmsTopicConfig(String durableConsumerName, String clientIdPrefix, String topicName, String brokerUrl, String userName, String password)
    {
        super(userName, password, durableConsumerName, topicName);
        this.brokerUrl = brokerUrl;
        this.clientIdPrefix  = clientIdPrefix;
    }

    public static TibJmsTopicConfig newPublishTopicConfig(String clientIdPrefix, String topicName, String brokerUrl, String userName, String password)
    {
        if (clientIdPrefix == null || clientIdPrefix.trim().isEmpty())
        {
            throw new IllegalArgumentException("Client id prefix cannot be null or empty for publish topic " + topicName);
        }
        return new TibJmsTopicConfig(null, clientIdPrefix, topicName, brokerUrl, userName, password);
    }

    public static TibJmsTopicConfig newConsumeTopicConfig(String durableConsumerName, String topicName, String brokerUrl, String userName, String password)
    {
        if (durableConsumerName == null || durableConsumerName.trim().isEmpty())
        {
            throw new IllegalArgumentException("Durable consumer name cannot be null or empty for consume topic " + topicName);
        }
        return new TibJmsTopicConfig(durableConsumerName, null, topicName, brokerUrl, userName, password);
    }

    public boolean isUsePooling()
    {
        return usePooling;
    }

    public void setUsePooling(boolean usePooling)
    {
        this.usePooling = usePooling;
    }

    @Override
    public XAConnectionFactory createXaConnectionFactory(JmsTopicConfig config) throws NamingException
    {
        return new TibjmsXAConnectionFactory(this.brokerUrl);
    }

    @Override
    public Topic createTopic(JmsTopicConfig config, Session session) throws NamingException, JMSException
    {
        return new TibjmsTopic(this.getTopicName());
    }

    @Override
    public XAConnection createConnection() throws JMSException, NamingException
    {
        String durableConsumerName = this.getDurableConsumerName();
        if (durableConsumerName != null)
        {
            return createConnectionForDurable(durableConsumerName);
        }
        if (usePooling)
        {
            return getOrCreatePooledConnection();
        }
        return createUnpooledConnection();
    }

    protected XAConnection createUnpooledConnection() throws JMSException, NamingException
    {
        XAConnection connection = connect();
        String clientId = this.clientIdPrefix + "_" + publisherId.incrementAndGet();
        LOGGER.debug("Setting client id to {} for topic {} on broker {}", clientId, this.getTopicName(), this.brokerUrl);
        connection.setClientID(clientId);
        return connection;
    }

    protected XAConnection createConnectionForDurable(String durableConsumerName) throws JMSException, NamingException
    {
        XAConnection connection = connect();
        LOGGER.debug("Setting client id to {} for topic {} on broker {}", durableConsumerName, this.getTopicName(), this.brokerUrl);
        connection.setClientID(durableConsumerName);
        return connection;
    }

    protected XAConnection getOrCreatePooledConnection() throws JMSException, NamingException
    {
        if (this.assignedPostfix < 0)
        {
            this.assignedPostfix = publisherId.get();
        }
        XaConnnectionKey key = makeConnectionKey();
        synchronized (pool)
        {
            RefCountedJmsXaConnection connection = pool.get(key);
            if (connection == null)
            {
                XAConnection underlying = connect();
                String clientId = this.clientIdPrefix + "_" + this.assignedPostfix + "_" + forcedConnectionCounter.incrementAndGet();
                LOGGER.info("Pooled publisher connection to broker {} for user {} with client id {}", this.brokerUrl, this.getUserName(), clientId);
                underlying.setClientID(clientId);

                connection = new RefCountedJmsXaConnection(underlying);
                pool.put(key, connection);
            }
            else
            {
                connection.incrementCount();
            }
            return connection;
        }
    }

    private XaConnnectionKey makeConnectionKey()
    {
        return new XaConnnectionKey(this.assignedPostfix, this.brokerUrl, this.clientIdPrefix, this.getUserName(), this.getPassword());
    }

    private XAConnection connect() throws JMSException, NamingException
    {
        return createXaConnectionFactory(this).createXAConnection(this.getUserName(), this.getPassword());
    }

    @Override
    public void markConnectionDirty()
    {
        while (this.assignedPostfix == publisherId.get())
        {
            publisherId.compareAndSet(this.assignedPostfix, this.assignedPostfix + 1);
        }
        synchronized (pool)
        {
            pool.remove(makeConnectionKey());
        }
        this.assignedPostfix = -1;
    }

    @Override
    public String toString()
    {
        return "Topic name: " + this.getTopicName() + " On broker " + this.brokerUrl + " for user " + this.getUserName();
    }

    private static class XaConnnectionKey
    {
        private String brokerUrl;
        private String userName;
        private String password;
        private String clientIdPrefix;
        private int assignedPostFix;

        private XaConnnectionKey(int assignedPostFix, String brokerUrl, String clientIdPrefix, String userName, String password)
        {
            this.assignedPostFix = assignedPostFix;
            this.brokerUrl = brokerUrl;
            this.clientIdPrefix = clientIdPrefix;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            XaConnnectionKey that = (XaConnnectionKey) o;

            if (assignedPostFix != that.assignedPostFix)
            {
                return false;
            }
            if (!brokerUrl.equals(that.brokerUrl))
            {
                return false;
            }
            if (!userName.equals(that.userName))
            {
                return false;
            }
            if (!password.equals(that.password))
            {
                return false;
            }
            return clientIdPrefix.equals(that.clientIdPrefix);
        }

        @Override
        public int hashCode()
        {
            int result = brokerUrl.hashCode();
            result = 31 * result + userName.hashCode();
            result = 31 * result + password.hashCode();
            result = 31 * result + clientIdPrefix.hashCode();
            result = 31 * result + assignedPostFix;
            return result;
        }
    }
}
