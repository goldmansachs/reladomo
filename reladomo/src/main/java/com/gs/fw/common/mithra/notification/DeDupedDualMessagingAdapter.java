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

package com.gs.fw.common.mithra.notification;

import com.gs.fw.common.mithra.notification.server.GenerationalExpiringSet;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MithraProcessInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/*
    Adapter that
    1) is composed of two underlying adapters
    2) each event is sent to both the underlying adapters
    3) each event received from the underlying adapters is de-duped before being delivered to the message handler
 */

public class DeDupedDualMessagingAdapter implements MithraNotificationMessagingAdapter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DeDupedDualMessagingAdapter.class);
    public static final int WRAPPED_HEADER_SIZE = 12;
    private final AtomicInteger nextWrapperMessageId = new AtomicInteger((int) (1000 * Math.random()));

    private MithraNotificationMessagingAdapter adapter1;
    private MithraNotificationMessagingAdapter adapter2;
    private final MithraMessagingAdapterFactory factory1;
    private final MithraMessagingAdapterFactory factory2;
    private final String subject;

    public DeDupedDualMessagingAdapter(String subject, MithraMessagingAdapterFactory factory1, MithraMessagingAdapterFactory factory2)
    {
        this.subject = subject;
        this.factory1 = factory1;
        this.factory2 = factory2;
    }

    private void initialize()
    {
        adapter1 = factory1.createMessagingAdapter(subject);
        adapter2 = factory2.createMessagingAdapter(subject);
    }

    public void broadcastMessage(byte[] originalMessage)
    {
        /*
            Wrap the message so as to add a unique message id to each message.
            The Mithra event represented by the input byte[] array does not contain a message id.
         */
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
        adapter1.broadcastMessage(wrappedMessage);
        adapter2.broadcastMessage(wrappedMessage);
    }

    public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
    {
        this.initialize();
        DeDupedMithraNotificationMessageHandler deDupeHandler = new DeDupedMithraNotificationMessageHandler(messageHandler);
        adapter1.setMessageProcessor(deDupeHandler);
        adapter2.setMessageProcessor(deDupeHandler);
    }

    public void shutdown()
    {
        adapter1.shutdown();
        adapter2.shutdown();
    }

    private long getMithraVmId()
    {
        return MithraProcessInfo.getVmId();
    }

    class DeDupedMithraNotificationMessageHandler implements MithraNotificationMessageHandler
    {
        private final MithraNotificationMessageHandler delegate;
        private final GenerationalExpiringSet<MessageKey> messagesWeHaveSeenBefore = new GenerationalExpiringSet(300);

        public DeDupedMithraNotificationMessageHandler(MithraNotificationMessageHandler delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void processNotificationMessage(String subject, byte[] wrappedMessage)
        {
            long wrapperVmId = readLong(wrappedMessage, 0);
            int wrappedSenderMessageId = readInt(wrappedMessage, 8);

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Received wrapped from " + wrapperVmId + ", messageId " + wrappedSenderMessageId);
            }

            if (this.isNewUnseenMessage(subject, wrapperVmId, wrappedSenderMessageId))
            {
                int size = wrappedMessage.length - WRAPPED_HEADER_SIZE;
                byte[] message = new byte[size];
                System.arraycopy(wrappedMessage, WRAPPED_HEADER_SIZE, message, 0, size);
                delegate.processNotificationMessage(subject, message);
            }
        }

        private boolean isNewUnseenMessage(String subject, long vmId, int messageId)
        {
            MessageKey key = new MessageKey(subject, vmId, messageId);
            synchronized (this.messagesWeHaveSeenBefore)
            {
                if (this.messagesWeHaveSeenBefore.remove(key))
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Duplicate/redundant message received: " + key);
                    }
                    return false;
                }
                else
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("New/unseen message received: " + key);
                    }
                    this.messagesWeHaveSeenBefore.add(key);
                    return true;
                }
            }
        }
    }

    public static void writeLong(long value, byte[] bytes, int offset)
    {
        bytes[offset] = (byte) ((value >> 56) & 0xFF);
        bytes[offset + 1] = (byte) ((value >> 48) & 0xFF);
        bytes[offset + 2] = (byte) ((value >> 40) & 0xFF);
        bytes[offset + 3] = (byte) ((value >> 32) & 0xFF);
        bytes[offset + 4] = (byte) ((value >> 24) & 0xFF);
        bytes[offset + 5] = (byte) ((value >> 16) & 0xFF);
        bytes[offset + 6] = (byte) ((value >> 8) & 0xFF);
        bytes[offset + 7] = (byte) (value & 0xFF);
    }

    public static void writeInt(int i, byte[] bytes, int offset)
    {
        bytes[offset] = (byte) ((i >> 24) & 0xFF);
        bytes[offset + 1] = (byte) ((i >> 16) & 0xFF);
        bytes[offset + 2] = (byte) ((i >> 8) & 0xFF);
        bytes[offset + 3] = (byte) (i & 0xFF);
    }

    private long readLong(byte[] bytes, int offset)
    {
        return (((long) bytes[offset]) & 0xFF) << 56
                | (((long) bytes[offset + 1]) & 0xFF) << 48
                | (((long) bytes[offset + 2]) & 0xFF) << 40
                | (((long) bytes[offset + 3]) & 0xFF) << 32
                | (((long) bytes[offset + 4]) & 0xFF) << 24
                | (((long) bytes[offset + 5]) & 0xFF) << 16
                | (((long) bytes[offset + 6]) & 0xFF) << 8
                | (((long) bytes[offset + 7]) & 0xFF);
    }

    private int readInt(byte[] bytes, int offset)
    {
        return (((int) bytes[offset]) & 0xFF) << 24
                | (((int) bytes[offset + 1]) & 0xFF) << 16
                | (((int) bytes[offset + 2]) & 0xFF) << 8
                | (((int) bytes[offset + 3]) & 0xFF);
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
