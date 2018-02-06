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

public class InMemoryXaConnection implements XAConnection
{
    private static final int NOT_STARTED = 10;
    private static final int STARTED = 20;
    private static final int CLOSED = 30;

    private InMemoryTopicConfig inMemoryTopicConfig;

    private int state = NOT_STARTED;

    public InMemoryXaConnection(InMemoryTopicConfig inMemoryTopicConfig)
    {
        this.inMemoryTopicConfig = inMemoryTopicConfig;
    }

    public InMemoryTopicConfig getInMemoryTopicConfig()
    {
        return inMemoryTopicConfig;
    }

    @Override
    public XASession createXASession() throws JMSException
    {
        return new InMemoryXaSession(this);
    }

    @Override
    public Session createSession(boolean b, int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getClientID() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setClientID(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void start() throws JMSException
    {
        assertNotClosed();
        this.state = STARTED;
    }

    @Override
    public void stop() throws JMSException
    {
        assertNotClosed();
        this.state = NOT_STARTED;
    }

    private void assertNotClosed() throws JMSException
    {
        if (this.state == CLOSED)
        {
            throw new JMSException("Cannot start a closed connection!");
        }
    }

    @Override
    public void close() throws JMSException
    {
        this.state = CLOSED;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s2, ServerSessionPool serverSessionPool, int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }
}
