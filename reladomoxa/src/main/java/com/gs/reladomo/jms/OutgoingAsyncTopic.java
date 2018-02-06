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

import com.gs.fw.common.mithra.transaction.FutureXaResource;
import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.fw.common.mithra.transaction.MultiThreadedTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.naming.NamingException;
import javax.transaction.RollbackException;
import javax.transaction.xa.XAResource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class OutgoingAsyncTopic extends JmsTopic implements OutgoingTopic
{
    private static final Logger logger = LoggerFactory.getLogger(OutgoingAsyncTopic.class);
    private MessageProducer producer;
    protected FutureXaResource futureXaResource;
    private final String threadNamePostfix;
    private final boolean async;
    private OutgoingTopicListener listener;

    public OutgoingAsyncTopic(JmsTopicConfig config, MultiThreadedTm multiThreadedTm, String threadNamePostfix, OutgoingTopicListener listener) throws JMSException, NamingException
    {
        super(config, multiThreadedTm, null);
        this.threadNamePostfix = threadNamePostfix;
        this.async = config.isOutGoingAsync();
        this.listener = listener;
        localStart();
    }

    public OutgoingAsyncTopic(OutgoingAsyncTopic topic, OutgoingTopicListener listener) throws JMSException, NamingException
    {
        this(topic.getConfig(), topic.getMultiThreadedTm(), topic.threadNamePostfix, listener);
    }

    public String getTopicName()
    {
        return this.getConfig().getTopicName();
    }

    public Future<Void> asyncSendMessages(final List<byte[]> messages, final Map<String, Object> msgProperties)
    {
        final MultiThreadedTx localTransaction = this.getMultiThreadedTm().getLocalTransaction();
        if (async)
        {
            return futureXaResource.executeOnResourceThread(new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        Future<Boolean> enlistFuture = localTransaction.enlistResource(futureXaResource);
                        enlistFuture.get();

                        if (listener != null)
                        {
                            listener.startBatchSend(true);
                        }
                        for (int i = 0; i < messages.size(); i++)
                        {
                            BytesMessage message = getXaSession().createBytesMessage();
                            message.writeBytes(messages.get(i));
                            setMessageProperties(message, msgProperties);
                            producer.send(message);
                            if (listener != null)
                            {
                                listener.logByteMessage(getTopicName(), message, messages.get(i));
                            }
                        }
                        Future<Boolean> delistFuture = localTransaction.delistResource(futureXaResource, XAResource.TMSUCCESS);
                        delistFuture.get();
                        if (listener != null)
                        {
                            listener.endBatchSend();
                        }
                        return null;
                    }
                    catch (RollbackException e)
                    {
                        logger.error("Could not send messages, because transaction is rolled back", e);
                        throw e;
                    }
                    catch (Exception e)
                    {
                        logger.error("unexpected exception", e);
                        //todo: do we need to be more subtle here, instead of asking for a restart?
                        throw new RestartTopicException("unexpected exception", e, OutgoingAsyncTopic.this);
                    }
                }
            });
        }
        else
        {
            try
            {
                localTransaction.enlistResource(futureXaResource.getDelegated());

                if (listener != null)
                {
                    listener.startBatchSend(false);
                }
                for (int i = 0; i < messages.size(); i++)
                {
                    BytesMessage message = getXaSession().createBytesMessage();
                    message.writeBytes(messages.get(i));
                    setMessageProperties(message, msgProperties);
                    producer.send(message);
                    if (listener != null)
                    {
                        listener.logByteMessage(getTopicName(), message, messages.get(i));
                    }
                }
                localTransaction.delistResource(futureXaResource.getDelegated(), XAResource.TMSUCCESS);
                if (listener != null)
                {
                    listener.endBatchSend();
                }
                return new ImmediateFuture<Void>(null, null);
            }
            catch (RollbackException e)
            {
                logger.error("Could not send messages, because transaction is rolled back", e);
                return new ImmediateFuture<Void>("Could not send messages, because transaction is rolled back", e);
            }
            catch (Exception e)
            {
                logger.error("unexpected exception", e);
                //todo: do we need to be more subtle here, instead of asking for a restart?
                return new ImmediateFuture<Void>("unexpected exception", e);
            }
        }
    }

    private void setMessageProperties(Message message, Map<String, Object> msgProperties) throws JMSException
    {
        if (msgProperties != null && !msgProperties.isEmpty())
        {
            Set<Map.Entry<String, Object>> entries = msgProperties.entrySet();
            for(Map.Entry<String, Object> it: entries)
            {
                message.setObjectProperty(it.getKey(), it.getValue());
            }
        }
    }

    public Future<Void> sendSyncMessageClones(List<Message> originals)
    {
        final MultiThreadedTx localTransaction = this.getMultiThreadedTm().getLocalTransaction();
        try
        {
            localTransaction.enlistResource(futureXaResource.getDelegated());

            if (listener != null)
            {
                listener.startBatchSend(false);
            }
            for (int i = 0; i < originals.size(); i++)
            {
                JmsUtil.MessageHolder messageHolder = JmsUtil.cloneMessage(getXaSession(), originals.get(i));
                producer.send(messageHolder.message);
                if (listener != null)
                {
                    listener.logClonedMessageSend(getTopicName(), messageHolder.message, messageHolder.body, messageHolder.original);
                }
            }
            localTransaction.delistResource(futureXaResource.getDelegated(), XAResource.TMSUCCESS);
            if (listener != null)
            {
                listener.endBatchSend();
            }
            return new ImmediateFuture<Void>(null, null);
        }
        catch (RollbackException e)
        {
            logger.error("Could not send messages, because transaction is rolled back", e);
            return new ImmediateFuture<Void>("Could not send messages, because transaction is rolled back", e);
        }
        catch (Exception e)
        {
            logger.error("unexpected exception", e);
            //todo: do we need to be more subtle here, instead of asking for a restart?
            return new ImmediateFuture<Void>("unexpected exception", e);
        }
    }

    public void syncSendMessages(List<String> messages, final Map<String, Object> msgProperties)
    {
        final MultiThreadedTx localTransaction = this.getMultiThreadedTm().getLocalTransaction();
        try
        {
            localTransaction.enlistResource(futureXaResource.getDelegated());

            if (listener != null)
            {
                listener.startBatchSend(false);
            }
            for (int i = 0; i < messages.size(); i++)
            {
                BytesMessage message = getXaSession().createBytesMessage();
                byte[] bytes = messages.get(i).getBytes();
                message.writeBytes(bytes);
                setMessageProperties(message, msgProperties);
                producer.send(message);
                if (listener != null)
                {
                    listener.logStringMessage(getTopicName(), message, messages.get(i));
                }
            }
            localTransaction.delistResource(futureXaResource.getDelegated(), XAResource.TMSUCCESS);
            if (listener != null)
            {
                listener.endBatchSend();
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (RollbackException e)
        {
            logger.error("Could not send messages, because transaction is rolled back", e);
            throw new RuntimeException("rolledback?", e);
        }
        catch (Exception e)
        {
            logger.error("unexpected exception", e);
            //todo: do we need to be more subtle here, instead of asking for a restart?
            throw new RuntimeException("unexpected exception", e);
        }
    }

    protected void localStart() throws JMSException, NamingException
    {
        this.producer = this.getXaSession().createProducer(this.getXaTopic());
        this.futureXaResource = new FutureXaResource(this.getMultiThreadedTm(), this.getXaSession().getXAResource(), "Outgoing: " + this.getXaTopic().getTopicName() + threadNamePostfix);
    }

    @Override
    public void close()
    {
        this.futureXaResource.shutdown();
        try
        {
            if (this.producer != null)
            {
                this.producer.close();
                this.producer = null;
            }
        }
        catch (JMSException e)
        {
            logger.warn("Could not close producer", e);
        }
        super.close();
    }

    private static class ImmediateFuture<V> implements Future<Void>
    {
        private final ExecutionException executionException;

        private ImmediateFuture(String message, Exception result)
        {
            ExecutionException exp = result == null ? null : new ExecutionException( message, result);
            this.executionException = exp;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException
        {
            if (this.executionException != null)
            {
                throw this.executionException;
            }
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            if (this.executionException != null)
            {
                throw this.executionException;
            }
            return null;
        }
    }
}
