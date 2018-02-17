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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test.notification;

import com.gs.fw.common.mithra.notification.MithraMessagingAdapterFactory;
import com.gs.fw.common.mithra.notification.MithraNotificationMessageHandler;
import com.gs.fw.common.mithra.notification.MithraNotificationMessagingAdapter;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;
import java.util.Set;

import static com.gs.fw.common.mithra.notification.DeDupedDualMessagingAdapter.*;

/**
 * Created by stanle on 1/7/2016.
 */
public class InMemoryMessagingAdapterFactory implements MithraMessagingAdapterFactory
{
    private InMemoryMessagingAdapter adapter;

    @Override
    public MithraNotificationMessagingAdapter createMessagingAdapter(String subject)
    {
        adapter = new InMemoryMessagingAdapter(subject);
        return adapter;
    }

    @Override
    public void shutdown()
    {
    }

    public InMemoryMessagingAdapter getAdapter()
    {
        return adapter;
    }

    class InMemoryMessagingAdapter implements MithraNotificationMessagingAdapter
    {
        private List<String> messages = FastList.newList();
        private MithraNotificationMessageHandler messageHandler;
        private String subject;

        public InMemoryMessagingAdapter(String subject)
        {
            this.subject = subject;
        }

        @Override
        public void broadcastMessage(byte[] message)
        {
            //we get the wrapped message here. unwrap and accumulate in messages list for use in tests
            int realLength = message.length - WRAPPED_HEADER_SIZE;
            byte[] real = new byte[realLength];
            System.arraycopy(message, WRAPPED_HEADER_SIZE, real, 0, realLength);
            messages.add(new String(real));
        }

        @Override
        public void setMessageProcessor(MithraNotificationMessageHandler messageHandler)
        {
            this.messageHandler = messageHandler;
        }

        @Override
        public void shutdown()
        {
        }

        public List<String> getMessages()
        {
            return messages;
        }

        /*
            Simulate message consumption by injecting messages into the message handler
            The dedupe handler expects wrapped messages. So wrap the messages before handing over to the handler.
         */
        public void inject(Set<String> messages)
        {
            for (String message : messages)
            {
                byte[] messageBytes = message.getBytes();
                long wrapperVmId = this.hashCode();
                int wrapperMessageId = message.hashCode();
                byte[] wrappedMessage = new byte[WRAPPED_HEADER_SIZE + messageBytes.length];
                writeLong(wrapperVmId, wrappedMessage, 0);
                writeInt(wrapperMessageId, wrappedMessage, 8);
                System.arraycopy(messageBytes, 0, wrappedMessage, WRAPPED_HEADER_SIZE, messageBytes.length);
                messageHandler.processNotificationMessage(subject, wrappedMessage);
            }
        }
    }
}
