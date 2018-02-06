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
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.naming.NamingException;
import javax.transaction.xa.XAResource;

import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.reladomo.util.InterruptableBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsTopic
{
    private static final Logger logger = LoggerFactory.getLogger(JmsTopic.class);

    private final MultiThreadedTm multiThreadedTm;
    private final JmsTopicConfig config;
    private XAConnection xaConnection;
    private XASession xaSession;
    private Session nonXaSession;
    private Topic nonXaTopic;
    private Topic xaTopic;
    private InterruptableBackoff interruptableBackoff;

    public JmsTopic(JmsTopicConfig config, MultiThreadedTm multiThreadedTm, InterruptableBackoff interruptableBackoff) throws JMSException, NamingException
    {
        this.multiThreadedTm = multiThreadedTm;
        this.config = config;
        this.interruptableBackoff = interruptableBackoff;

        start();
    }

    protected void start() throws JMSException, NamingException
    {
        this.xaConnection = this.config.createConnection();
        xaConnection.start();

        if (this.config.isEnableLowLatency() && interruptableBackoff != null)
        {
            this.nonXaSession = xaConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.nonXaTopic = this.config.createTopic(this.config, nonXaSession);
        }

        this.xaSession = xaConnection.createXASession();

        this.xaTopic = this.config.createTopic(this.config, this.getXaSession());
    }

    protected InterruptableBackoff getInterruptableBackoff()
    {
        return interruptableBackoff;
    }

    public void close()
    {
        try
        {
            if (this.xaSession != null)
            {
                this.xaSession.close();
                this.xaSession = null;
            }
        }
        catch (JMSException e)
        {
            logger.warn("could not close XA session", e);
        }
        try
        {
            if (this.nonXaSession != null)
            {
                this.nonXaSession.close();
                this.nonXaSession = null;
            }
        }
        catch (JMSException e)
        {
            logger.warn("could not close non-XA session", e);
        }
        try
        {
            if (this.xaConnection != null)
            {
                this.xaConnection.stop();
                this.xaConnection.close();
                this.xaConnection = null;
            }
        }
        catch (JMSException e)
        {
            logger.warn("could not close connection", e);
        }
    }

    public void restart() throws JMSException, NamingException
    {
        close();
        this.config.markConnectionDirty();
        start();
    }

    protected MultiThreadedTm getMultiThreadedTm()
    {
        return multiThreadedTm;
    }

    public JmsTopicConfig getConfig()
    {
        return config;
    }

    protected XAConnection getXaConnection()
    {
        return xaConnection;
    }

    protected XASession getXaSession()
    {
        return xaSession;
    }

    public XAResource getXaResource()
    {
        return this.xaSession.getXAResource();
    }

    protected Topic getXaTopic()
    {
        return xaTopic;
    }

    protected Session getNonXaSession()
    {
        return nonXaSession;
    }

    protected Topic getNonXaTopic()
    {
        return nonXaTopic;
    }
}
