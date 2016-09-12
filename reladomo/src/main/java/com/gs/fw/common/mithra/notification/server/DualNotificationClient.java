/*
 Copyright 2016 Goldman Sachs.
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

package com.gs.fw.common.mithra.notification.server;


import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraProcessInfo;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DualNotificationClient implements ClientNotificationHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DualNotificationClient.class);

    private final GenerationalExpiringSet<MessageKey> messagesWeHaveSeenBefore = new GenerationalExpiringSet(300);

    private static final int WRAPPED_HEADER_SIZE = 12;

    private final AtomicInteger nextWrapperMessageId = new AtomicInteger((int) (1000 * Math.random()));
    private final ClientNotificationHandler handler;
    private final NotificationClient client1;
    private final NotificationClient client2;

    public DualNotificationClient(String host1, int port1, String host2, int port2, ClientNotificationHandler handler)
    {
        this.handler = handler;
        this.client1 = new NotificationClient(host1, port1, this);
        this.client2 = new NotificationClient(host2, port2, this);
    }

    @Override
    public void handleMessage(String subject, byte[] wrappedMessage)
    {
        long wrapperVmId = this.readLong(wrappedMessage, 0);
        int wrappedSenderMessageId = this.readInt(wrappedMessage, 8);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received wrapped from " + wrapperVmId + ", messageId " + wrappedSenderMessageId);
        }
        if (this.isNewUnseenMessage(subject, wrapperVmId, wrappedSenderMessageId))
        {
            int size = wrappedMessage.length - DualNotificationClient.WRAPPED_HEADER_SIZE;
            byte[] message = new byte[size];
            System.arraycopy(wrappedMessage, DualNotificationClient.WRAPPED_HEADER_SIZE, message, 0, size);
            handler.handleMessage(subject, message);
        }
    }

    private boolean isNewUnseenMessage(String subject, long vmId, int messageId)
    {
        MessageKey key = new MessageKey(subject, vmId, messageId);
        synchronized (this.messagesWeHaveSeenBefore)
        {
            if (this.messagesWeHaveSeenBefore.contains(key))
            {
                LOGGER.debug("Duplicate/redundant message received: " + key);
                return false;
            }
            else
            {
                LOGGER.debug("New/unseen message received: " + key);
                this.messagesWeHaveSeenBefore.add(key);
                return true;
            }
        }
    }

    private long readLong(byte[] bytes, int offset)
    {
        return    (((long)bytes[offset    ]) & 0xFF) << 56
                | (((long)bytes[offset + 1]) & 0xFF) << 48
                | (((long)bytes[offset + 2]) & 0xFF) << 40
                | (((long)bytes[offset + 3]) & 0xFF) << 32
                | (((long)bytes[offset + 4]) & 0xFF) << 24
                | (((long)bytes[offset + 5]) & 0xFF) << 16
                | (((long)bytes[offset + 6]) & 0xFF) << 8
                | (((long)bytes[offset + 7]) & 0xFF);
    }

    private int readInt(byte[] bytes, int offset)
    {
        return    (((int)bytes[offset  ]) & 0xFF) << 24
                | (((int)bytes[offset+1]) & 0xFF) << 16
                | (((int)bytes[offset+2]) & 0xFF) << 8
                | (((int)bytes[offset+3]) & 0xFF);
    }


    public void start()
    {
        this.client1.start();
        this.client2.start();
    }

    public void shutdown()
    {
        this.client1.shutdown();
        this.client2.shutdown();
    }

    public void subscribe(String subject)
    {
        this.client1.subscribe(subject);
        this.client2.subscribe(subject);
    }

    public void broadcastNotification(String subject, byte[] originalMessage)
    {
        long wrapperVmId = this.getMithraVmId();
        int wrapperMessageId = this.nextWrapperMessageId.getAndIncrement();
        byte[] wrappedMessage = new byte[WRAPPED_HEADER_SIZE + originalMessage.length];
        writeLong(wrapperVmId, wrappedMessage, 0);
        writeInt(wrapperMessageId, wrappedMessage, 8);
        System.arraycopy(originalMessage, 0, wrappedMessage, WRAPPED_HEADER_SIZE, originalMessage.length);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Sending wrapped from " + wrapperVmId + ", messageId " + wrapperMessageId);
        }
        this.client1.broadcastNotification(subject, wrappedMessage);
        this.client2.broadcastNotification(subject, wrappedMessage);
    }

    private long getMithraVmId()
    {
        return MithraProcessInfo.getVmId();
    }

    private void writeLong(long value, byte[] bytes, int offset)
    {
        bytes[offset    ] = (byte) ((value >> 56) & 0xFF);
        bytes[offset + 1] = (byte) ((value >> 48) & 0xFF);
        bytes[offset + 2] = (byte) ((value >> 40) & 0xFF);
        bytes[offset + 3] = (byte) ((value >> 32) & 0xFF);
        bytes[offset + 4] = (byte) ((value >> 24) & 0xFF);
        bytes[offset + 5] = (byte) ((value >> 16) & 0xFF);
        bytes[offset + 6] = (byte) ((value >> 8) & 0xFF);
        bytes[offset + 7] = (byte) (value & 0xFF);
    }

    private void writeInt(int i, byte[] bytes, int offset)
    {
        bytes[offset    ] = (byte) ((i >> 24) & 0xFF);
        bytes[offset + 1] = (byte) ((i >> 16) & 0xFF);
        bytes[offset + 2] = (byte) ((i >> 8) & 0xFF);
        bytes[offset + 3] = (byte) (i & 0xFF);
    }

    public void waitForAllAcks()
    {
        this.client1.waitForAllAcks();
        this.client2.waitForAllAcks();
    }

    public void waitForClientOneAcks()
    {
        this.client1.waitForAllAcks();
    }


    private static class MessageKey
    {
        private final String subject;
        private final long vmId;
        private final int messageId;

        private MessageKey(String subject, long vmId, int messageId)
        {
            this.subject = subject;
            this.vmId = vmId;
            this.messageId = messageId;
        }

        @Override
        public int hashCode()
        {

            return HashUtil.combineHashes(HashUtil.hash(subject), HashUtil.combineHashes(HashUtil.hash(vmId), messageId));
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            MessageKey that = (MessageKey) other;
            return this.messageId == that.messageId && this.vmId == that.vmId && this.subject.equals(that.subject);
        }

        @Override
        public String toString()
        {
            return "MessageKey[subject=" + this.subject + "; vmId=" + this.vmId + "; msgId=" + this.messageId + ']';
        }
    }
}
