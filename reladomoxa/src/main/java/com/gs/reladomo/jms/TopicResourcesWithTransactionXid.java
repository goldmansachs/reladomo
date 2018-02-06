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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraTransactionalList;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.transaction.MultiThreadedTm;
import com.gs.fw.common.mithra.util.serializer.DeserializationClassMetaData;
import com.gs.fw.common.mithra.util.serializer.DeserializationException;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import com.gs.reladomo.txid.ReladomoTxIdInterface;
import com.gs.reladomo.txid.ReladomoTxIdInterfaceFinder;
import com.gs.reladomo.util.Base64;
import com.gs.reladomo.util.InterruptableBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicResourcesWithTransactionXid
{
    public static final int DEFAULT_WAIT_TIME_TO_CONNECT = 9 * 60 * 1000;  // default 9 minutes. we use retry with exponential back-off:  2^10 > 9*60 > 2^9

    private final Logger logger;
    private final long maxWaitTimeToConnect;
    private final MultiThreadedTm multiThreadedTm;
    private final List<JmsTopic> jmsTopics = FastList.newList();
    private int transactionXidId;
    private final String flowId;
    private Attribute txIdSourceAttribute;
    private Object sourceAttributeValue;
    private ReladomoTxIdInterfaceFinder txIdFinder;

    public TopicResourcesWithTransactionXid(ReladomoTxIdInterfaceFinder txIdFinder, long maxWaitTimeToConnect, MultiThreadedTm multiThreadedTm, String flowId, Object sourceAttributeValue)
    {
        this.multiThreadedTm = multiThreadedTm;
        this.flowId = flowId;
        this.maxWaitTimeToConnect = maxWaitTimeToConnect;
        logger = LoggerFactory.getLogger("TopicResourcesWithTransactionXid."+flowId);
        this.txIdSourceAttribute = txIdFinder.getSourceAttribute();
        this.sourceAttributeValue = sourceAttributeValue;
        this.txIdFinder = txIdFinder;
        if (txIdSourceAttribute != null)
        {
            if (sourceAttributeValue == null)
            {
                throw new IllegalArgumentException("sourceAttributeValue must be specified if the txIdFinder has a source attribute");
            }
        }
        else
        {
            if (sourceAttributeValue != null)
            {
                throw new IllegalArgumentException("sourceAttributeValue must be null if the txIdFinder has no source attribute");
            }
        }
    }

    public void addJmsTopic(JmsTopic jmsTopic)
    {
        this.jmsTopics.add(jmsTopic);
    }

    public OutgoingAsyncTopic connectOutgoingTopicWithRetry(JmsTopicConfig topicConfig, String outgoingThreadNamePostFix, OutgoingTopicListener listener)
    {
        long start = System.currentTimeMillis();
        long retryWait = 1000;
        while(true)
        {
            try
            {
                OutgoingAsyncTopic outgoingAsyncTopic = new OutgoingAsyncTopic(topicConfig, this.multiThreadedTm, outgoingThreadNamePostFix, listener);
                this.addJmsTopic(outgoingAsyncTopic);
                return outgoingAsyncTopic;
            }
            catch (Exception e)
            {
                retryWait = this.handleTopicRetry(topicConfig, start, retryWait, e);
            }
        }
    }

    public IncomingTopic connectIncomingTopicWithRetry(JmsTopicConfig incomingTopicConfig, InterruptableBackoff interruptableBackoff)
    {
        long start = System.currentTimeMillis();
        long retryWait = 1000;
        while(true)
        {
            try
            {
                IncomingTopic result = new IncomingTopic(incomingTopicConfig, this.multiThreadedTm, interruptableBackoff);
                this.addJmsTopic(result);
                return result;
            }
            catch (Exception e)
            {
                retryWait = this.handleTopicRetry(incomingTopicConfig, start, retryWait, e);
            }
        }
    }

    public MithraTransaction startTransaction()
    {
        MithraTransaction mithraTransaction = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        updateXidRecord();
        return mithraTransaction;
    }

    public void updateXidRecord()
    {
        final byte[] globalTransactionId = this.multiThreadedTm.getLocalTransaction().getGlobalTransactionId();
        MithraManagerProvider.getMithraManager().executeTransactionalCommandInSeparateThread(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                ReladomoTxIdInterface transactionXid = TopicResourcesWithTransactionXid.this.findTransactionXid();
                setBinaryXid(transactionXid, globalTransactionId);
                transactionXid.setCommitted(false);
                return null;
            }
        });
    }

    private ReladomoTxIdInterface findTransactionXid()
    {
        Operation op = txIdFinder.id().eq(this.transactionXidId);
        if (this.txIdSourceAttribute != null)
        {
            op = op.and(this.txIdSourceAttribute.nonPrimitiveEq(this.sourceAttributeValue));
        }
        return (ReladomoTxIdInterface) txIdFinder.findOne(op);
    }

    public void commit(MithraTransaction mithraTransaction, List<Future> outgoingMessageFutures, List<JmsTopic> topicsToRestartAndRecover) throws RestartTopicException
    {
        commitXid();
        mithraTransaction.executeBufferedOperations();
        waitForAllFutures(outgoingMessageFutures, topicsToRestartAndRecover);
        mithraTransaction.commit();
    }

    public void commitXid()
    {
        ReladomoTxIdInterface transactionXid = findTransactionXid();
        transactionXid.setCommitted(true);
    }

    private void waitForAllFutures(List<Future> outgoingMessageFutures, List<JmsTopic> topicsToRestartAndRecover) throws RestartTopicException
    {
        Throwable unrecoverable = null;
        RestartTopicException restartTopicException = null;
        for(int i=0;i<outgoingMessageFutures.size();i++)
        {
            try
            {
                outgoingMessageFutures.get(i).get();
            }
            catch (InterruptedException e)
            {
                this.logger.error("got interrupted ?!??!", e);
                unrecoverable = e;
            }
            catch(CancellationException e)
            {
                this.logger.error("got cancelled ?!??!", e);
                unrecoverable = e;
            }
            catch (ExecutionException e)
            {
                Throwable cause = e.getCause();
                if (cause instanceof RestartTopicException)
                {
                    restartTopicException = ((RestartTopicException)cause);
                    topicsToRestartAndRecover.add(restartTopicException.getTopic());
                }
                else
                {
                    this.logger.error("got unexpected exception", e);
                    unrecoverable = e;
                }
            }
        }
        if (restartTopicException != null)
        {
            throw restartTopicException;
        }
        if (unrecoverable != null)
        {
            if (unrecoverable instanceof RuntimeException)
            {
                throw (RuntimeException) unrecoverable;
            }
            throw new RuntimeException("unexpected exception while waiting for outgoing topics", unrecoverable);
        }
    }

    public void restartTopics(List<JmsTopic> topicsToRestartAndRecover)
    {
        ReladomoTxIdInterface transactionXid = findTransactionXid();
        byte[] globalTransactionId = this.getGlobalTransactionId(transactionXid);
        for(int i=0;i<topicsToRestartAndRecover.size();i++)
        {
            long start = System.currentTimeMillis();
            long retryWait = 1000;
            while(true)
            {
                try
                {
                    topicsToRestartAndRecover.get(i).restart();
                    recoverTopic(topicsToRestartAndRecover.get(i), globalTransactionId, transactionXid.isCommitted());
                    break;
                }
                catch (Exception e)
                {
                    retryWait = handleTopicRetry(topicsToRestartAndRecover.get(i).getConfig(), start, retryWait, e);
                }
            }
        }
    }

    protected void recoverTopic(JmsTopic topic, byte[] globalXid, boolean committed) throws XAException
    {
        int leftOver = recoverResource(globalXid, committed, topic.getXaResource());
        if (leftOver > 50)
        {
            logger.warn("Too many indoubts on topic " + topic.getConfig().getTopicName() + " with config " + topic.getConfig().toString());
        }
    }

    protected int recoverResource(byte[] globalXid, boolean committed, XAResource xaResource) throws XAException
    {
        Xid[] recovered = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
        int recoverCount = 0;
        int originalCount = 0;
        if (recovered != null)
        {
            originalCount = recovered.length;
            for (Xid xid : recovered)
            {
                if (Arrays.equals(xid.getGlobalTransactionId(), globalXid))
                {
                    recoverCount++;
                    if (committed)
                    {
                        xaResource.commit(xid, false);
                    }
                    else
                    {
                        xaResource.rollback(xid);
                    }
                    logger.info("recovered indoubt transaction");
                }
            }
        }
        return originalCount - recoverCount;
    }

    protected long handleTopicRetry(JmsTopicConfig topicConfig, long start, long retryWait, Exception e)
    {
        retryWait *= 2;
        String msg = "Could not connect to topic " + topicConfig.getTopicName();
        if (System.currentTimeMillis() + retryWait < start + maxWaitTimeToConnect)
        {
            msg += " will retry in "+retryWait/1000+" seconds";
            logger.warn(msg, e);
            try
            {
                Thread.sleep(retryWait);
            }
            catch (InterruptedException e1)
            {
                //ignore
            }
        }
        else
        {
            logger.error(msg, e);
            throw new RuntimeException("Could not connect to topic "+topicConfig.getTopicName()+" retries failed. Aborting.", e);
        }
        return retryWait;
    }

    public ReladomoTxIdInterface recover() throws XAException
    {
        ReladomoTxIdInterface transactionXid = findOrCreateTransactionXid();

        byte[] globalXid = this.getGlobalTransactionId(transactionXid);
        if (globalXid != null)
        {
            boolean committed = transactionXid.isCommitted();
            recoverTopics(globalXid, committed);
        }
        return transactionXid;
    }

    private ReladomoTxIdInterface findOrCreateTransactionXid()
    {
        ReladomoTxIdInterface transactionXid = this.setupTransactionXidForFlowId(this.flowId);
        this.transactionXidId = transactionXid.getId();
        return transactionXid;
    }

    private void recoverTopics(byte[] globalXid, boolean committed) throws XAException
    {
        for(JmsTopic topic: this.jmsTopics)
        {
            recoverTopic(topic, globalXid, committed);
        }
    }

    public void closeTopics()
    {
        for(JmsTopic topic: this.jmsTopics)
        {
            topic.close();
        }
        this.jmsTopics.clear();
    }

    public void setBinaryXid(ReladomoTxIdInterface txId, byte[] gtid)
    {
        String encodedGtid = encodeToBase64(gtid);
        txId.setXid(encodedGtid);
    }

    public static String encodeToBase64(byte[] gtid)
    {
        return Base64.encodeBytes(gtid);
    }

    public byte[] getGlobalTransactionId(ReladomoTxIdInterface txId)
    {
        String xid = txId.getXid();
        if (xid != null)
        {
            try
            {
                return Base64.decode(xid);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Invalid base64 encoding for "+xid, e);
            }
        }
        return null;
    }

    protected ReladomoTxIdInterface allocateTransactionXid(String topicName)
    {
        int xidId = stableHash(topicName);
        ReladomoTxIdInterface transactionXid = null;
        while (transactionXid == null)
        {
            transactionXid = (ReladomoTxIdInterface) this.txIdFinder.findOne(this.txIdFinder.id().eq(xidId));
            if (transactionXid == null)
            {
                transactionXid = initializeTransactionXidRange(topicName, xidId);
            }
            else
            {
                transactionXid = null; // we don't want to use the existing one, as it doesn't match our topic name
                xidId = mixOne(xidId);
            }
        }
        return transactionXid;
    }

    private static int stableHash(String topicName)
    {
        int hash = mixTwo(topicName.charAt(0));
        for (int i = 1; i < topicName.length(); i++)
        {
            hash = mixOne(hash) ^ mixTwo(topicName.charAt(i));
        }
        return hash < 0 ? -hash : hash;
    }

    //We allocate a range of these values to make sure bad implementations of row level locking do not lock any
    //real records.
    private ReladomoTxIdInterface initializeTransactionXidRange(String flowId, int xidId)
    {
        ReladomoTxIdInterface transactionXid = null;
        MithraTransactionalList list = null;
        try
        {
            DeserializationClassMetaData deserializationClassMetaData = new DeserializationClassMetaData(this.txIdFinder);
            transactionXid = (ReladomoTxIdInterface) deserializationClassMetaData.constructObject(null, null);
            transactionXid.setId(xidId);
            transactionXid.setFlowId(flowId);
            list = (MithraTransactionalList) this.txIdFinder.constructEmptyList();
            list.add(transactionXid);
            for (int i = xidId - 80; i < xidId; i++)
            {
                ReladomoTxIdInterface filler = (ReladomoTxIdInterface) deserializationClassMetaData.constructObject(null, null);
                filler.setId(i);
                filler.setFlowId(i+" Padding");
                list.add(filler);
            }
            for (int i = xidId + 1; i < xidId + 80; i++)
            {
                ReladomoTxIdInterface filler = (ReladomoTxIdInterface) deserializationClassMetaData.constructObject(null, null);
                filler.setId(i);
                filler.setFlowId(i+ " Padding");
                list.add(filler);
            }
        }
        catch (DeserializationException e)
        {
            throw new RuntimeException("Could not construct transactionId class", e);
        }
        list.insertAll();
        return transactionXid;
    }

    private static int mixOne(int hash)
    {
        hash ^= hash >>> 15;
        hash *= 0xACAB2A4D;
        hash ^= hash >>> 15;
        hash *= 0x5CC7DF53;
        hash ^= hash >>> 12;

        return hash;

    }

    private static int mixTwo(int hash)
    {
        hash ^= hash >>> 14;
        hash *= 0xBA1CCD33;
        hash ^= hash >>> 13;
        hash *= 0x9B6296CB;
        hash ^= hash >>> 12;

        return hash;
    }

    public ReladomoTxIdInterface setupTransactionXidForFlowId(String flowId)
    {
        ReladomoTxIdInterface transactionXid = (ReladomoTxIdInterface) this.txIdFinder.findOne(this.txIdFinder.flowId().eq(flowId));
        if (transactionXid == null)
        {
            transactionXid = allocateTransactionXid(flowId);
        }
        return transactionXid;
    }

}
