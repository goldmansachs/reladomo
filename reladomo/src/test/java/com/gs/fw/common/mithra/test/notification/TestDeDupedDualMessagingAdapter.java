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

package com.gs.fw.common.mithra.test.notification;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.notification.DeDupedDualMessagingAdapter;
import com.gs.fw.common.mithra.notification.MithraNotificationMessageHandler;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;

/**
 * Created by stanle on 1/7/2016.
 */
public class TestDeDupedDualMessagingAdapter extends TestCase
{
    public static final String TEST_SUBJECT = "some subject";
    private InMemoryMessagingAdapterFactory factory1 = new InMemoryMessagingAdapterFactory();
    private InMemoryMessagingAdapterFactory factory2 = new InMemoryMessagingAdapterFactory();
    private DeDupedDualMessagingAdapter deDupedDualAdapter;
    private RecordingHandler recordingHandler = new RecordingHandler();

    @Override
    public void setUp() throws Exception
    {
        deDupedDualAdapter = new DeDupedDualMessagingAdapter(TEST_SUBJECT, factory1, factory2);
        deDupedDualAdapter.setMessageProcessor(recordingHandler);
    }

    @Test
    public void testDualBroadcast()
    {
        UnifiedSet<String> messages = UnifiedSet.newSetWith("msg1", "msg2", "msg3");
        for (String message : messages)
        {
            deDupedDualAdapter.broadcastMessage(message.getBytes());
        }
        //both the underlying adapters get all the messages that were broadcast
        assertTrue(factory1.getAdapter().getMessages().containsAll(messages));
        assertTrue(factory2.getAdapter().getMessages().containsAll(messages));
    }

    @Test
    public void testDeDupe()
    {
        UnifiedSet<String> messageSet1 = UnifiedSet.newSetWith("msg1", "msg2", "msg3");
        UnifiedSet<String> messageSet2 = UnifiedSet.newSetWith("msg4");
        UnifiedSet<String> messageSet3 = UnifiedSet.newSetWith("msg5");

        //simulate message consumption by injecting messages into the adapter
        //set1 is consumed by both the the adapters
        factory1.getAdapter().inject(messageSet1);
        factory2.getAdapter().inject(messageSet1);

        //set2 is consumed just by adapter1
        factory1.getAdapter().inject(messageSet2);

        //set3 is consumed just by adapter2
        factory2.getAdapter().inject(messageSet3);

        //the actual event handler gets a unique set
        FastList<String> expectedMessages = FastList.newList();
        expectedMessages.addAll(messageSet1);
        expectedMessages.addAll(messageSet2);
        expectedMessages.addAll(messageSet3);

        List<MessageWithSubject> receivedMessages = recordingHandler.getMessages();
        UnifiedSet<String> receivedMessageSubjects = UnifiedSet.newSet();
        UnifiedSet<String> receivedMessageBodies = UnifiedSet.newSet();

        for (MessageWithSubject pair : receivedMessages)
        {
            receivedMessageSubjects.add(pair.getSubject());
            receivedMessageBodies.add(new String(pair.getMessage()));
        }

        assertTrue(receivedMessageBodies.containsAll(expectedMessages));
        assertEquals(1, receivedMessageSubjects.size());
        assertEquals(TEST_SUBJECT, receivedMessageSubjects.getFirst());
    }

    private static class RecordingHandler implements MithraNotificationMessageHandler
    {
        private List<MessageWithSubject> messages = FastList.newList();

        public List<MessageWithSubject> getMessages()
        {
            return messages;
        }

        @Override
        public void processNotificationMessage(String subject, byte[] message)
        {
            messages.add(new MessageWithSubject(subject, message));
        }
    }

    private static class MessageWithSubject
    {
        private final String subject;
        private final byte[] message;

        private MessageWithSubject(String subject, byte[] message)
        {
            this.subject = subject;
            this.message = message;
        }

        public byte[] getMessage()
        {
            return message;
        }

        public String getSubject()
        {
            return subject;
        }

        @Override
        public String toString()
        {
            return "Message[subject=" + this.subject + "; msgSize=" + this.message.length + ']';
        }
    }

}
