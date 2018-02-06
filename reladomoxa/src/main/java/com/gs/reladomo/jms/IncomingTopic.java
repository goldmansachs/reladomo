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
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.naming.NamingException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.reladomo.util.InterruptableBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingTopic extends JmsTopic
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingTopic.class);
    private MessageConsumer xaConsumer;
    private MessageConsumer nonXaConsumer;

    public IncomingTopic(JmsTopicConfig config, MultiThreadedTm multiThreadedTm) throws JMSException, NamingException
    {
        super(config, multiThreadedTm, null);
    }

    public IncomingTopic(JmsTopicConfig config, MultiThreadedTm multiThreadedTm, InterruptableBackoff interruptableBackoff) throws JMSException, NamingException
    {
        super(config, multiThreadedTm, interruptableBackoff);
    }

    public void enlistIntoTransaction() throws RollbackException, RestartTopicException
    {
        try
        {
            getMultiThreadedTm().getTransaction().enlistResource(this.getXaSession().getXAResource());
        }
        catch(RollbackException e)
        {
            throw e;
        }
        catch(Throwable e)
        {
            throw new RestartTopicException("could not enlist into transaction for topic "+this.getConfig().getTopicName(), e, this);
        }
    }

    public void delistFromTransaction(boolean success) throws RestartTopicException
    {
        try
        {
            getMultiThreadedTm().getTransaction().delistResource(this.getXaSession().getXAResource(), success ? XAResource.TMSUCCESS : XAResource.TMFAIL);
        }
        catch (SystemException e)
        {
            throw new RestartTopicException("could not delist from transaction for topic "+this.getConfig().getTopicName(), e, this);
        }
    }

    public Message receive(long timeoutInMillis) throws JMSException
    {
        return xaConsumer.receive(timeoutInMillis);
    }

    public Message receiveNoWait() throws JMSException
    {
        return xaConsumer.receiveNoWait();
    }

    @Override
    protected void start() throws JMSException, NamingException
    {
        super.start();
        this.xaConsumer = this.getXaSession().createDurableSubscriber(this.getXaTopic(), this.getConfig().getDurableConsumerName());
        if (this.getInterruptableBackoff() != null && this.getNonXaSession() != null)
        {
            this.nonXaConsumer = this.getNonXaSession().createConsumer(this.getNonXaTopic());
            this.nonXaConsumer.setMessageListener(new MessageListener()
            {
                @Override
                public void onMessage(Message message)
                {
                    IncomingTopic.this.getInterruptableBackoff().asyncInterrupt();
                }
            });
        }
    }

    @Override
    public void close()
    {
        try
        {
            if (this.nonXaConsumer != null)
            {
                this.nonXaConsumer.setMessageListener(null);
                this.nonXaConsumer.close();
            }
        }
        catch (JMSException e)
        {
            logger.warn("could not close nonXaConsumer", e);
        }
        try
        {
            if (this.xaConsumer != null)
            {
                this.xaConsumer.close();
            }
        }
        catch (JMSException e)
        {
            logger.warn("could not close xaConsumer", e);
        }
        super.close();
    }
}
