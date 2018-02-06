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

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;

public class RefCountedJmsXaConnection implements XAConnection
{
    private final XAConnection underlying;
    private final AtomicInteger count = new AtomicInteger(1);
    private final AtomicInteger startCount = new AtomicInteger(0);
    private boolean isDirty;

    public RefCountedJmsXaConnection(XAConnection underlying)
    {
        this.underlying = underlying;
    }

    public void incrementCount()
    {
        count.incrementAndGet();
    }

    @Override
    public Session createSession(boolean b, int i) throws JMSException
    {
        return underlying.createSession(b, i);
    }

    @Override
    public XASession createXASession() throws JMSException
    {
        return underlying.createXASession();
    }

    @Override
    public void close() throws JMSException
    {
        int remaining = count.decrementAndGet();
        if (remaining == 0)
        {
            underlying.close();
        }
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i) throws JMSException
    {
        return underlying.createConnectionConsumer(destination, s, serverSessionPool, i);
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException
    {
        return underlying.createDurableConnectionConsumer(topic, s, s1, serverSessionPool, i);
    }

    @Override
    public String getClientID() throws JMSException
    {
        return underlying.getClientID();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException
    {
        return underlying.getExceptionListener();
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException
    {
        return underlying.getMetaData();
    }

    @Override
    public void setClientID(String s) throws JMSException
    {
        underlying.setClientID(s);
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException
    {
        underlying.setExceptionListener(exceptionListener);
    }

    @Override
    public synchronized void start() throws JMSException
    {
        int count = startCount.incrementAndGet();
        if (count == 1)
        {
            underlying.start();
        }
    }

    @Override
    public synchronized void stop() throws JMSException
    {
        int count = startCount.decrementAndGet();
        if (count == 0)
        {
            underlying.stop();
        }
    }
}
