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

package com.gs.fw.common.mithra.test.util;


import com.gs.fw.common.mithra.notification.server.ClientNotificationHandler;
import com.gs.fw.common.mithra.notification.server.DualNotificationClient;
import com.gs.fw.common.mithra.notification.server.NotificationServer;
import com.gs.fw.common.mithra.notification.server.ServerSocketHandler;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import junit.framework.TestCase;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TestDualNotificationServers extends TestCase
{
    private static final String TEST_SUBJECT = "testSubject";

    private NotificationServer server1;
    private NotificationServer server2;

    private NotificationServer setupNormalServer()
    {
        NotificationServer server = new NotificationServer(0);
        server.start();
        return server;
    }

    private NotificationServer setupUnreliableServer(AtomicReference<Double> successRate)
    {
        NotificationServer server = new UnreliableNotificationServer(successRate, 0);
        server.start();
        return server;
    }

    public void testTwoClientsTwoServers()
    {
        this.server1 = this.setupNormalServer();
        this.server2 = this.setupNormalServer();

        RecordingClientHandler clientHandlerA = new RecordingClientHandler();
        DualNotificationClient clientA = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server2.getPort(), clientHandlerA);
        clientA.subscribe(TEST_SUBJECT);
        clientA.start();

        RecordingClientHandler clientHandlerB = new RecordingClientHandler();
        DualNotificationClient clientB = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server2.getPort(), clientHandlerB);
        clientB.subscribe(TEST_SUBJECT);
        clientB.start();

        server1.waitForMessagesReceived(6);
        server2.waitForMessagesReceived(6);

        byte[] firstMessage = new byte[] { 100 };
        clientA.broadcastNotification(TEST_SUBJECT, firstMessage);

        byte[] secondMessage = new byte[] { 50 };
        clientB.broadcastNotification(TEST_SUBJECT, secondMessage);

        server1.waitForMessagesReceived(10);
        server2.waitForMessagesReceived(10);
        clientA.waitForAllAcks();
        clientB.waitForAllAcks();
        server1.waitForAllAcks();
        server2.waitForAllAcks();

        clientA.waitForAllAcks();
        clientB.waitForAllAcks();
        server1.waitForAllAcks();
        server2.waitForAllAcks();

        clientA.shutdown();
        clientB.shutdown();

        assertEquals(1, clientHandlerA.messages.size());
        assertEquals(1, clientHandlerB.messages.size());

        MessageWithSubject firstReceived = clientHandlerA.messages.get(0);
        assertEquals(TEST_SUBJECT, firstReceived.getSubject());
        assertEquals(1, firstReceived.getMessage().length);
        assertEquals(50, firstReceived.getMessage()[0]);

        MessageWithSubject secondReceived = clientHandlerB.messages.get(0);
        assertEquals(TEST_SUBJECT, secondReceived.getSubject());
        assertEquals(1, secondReceived.getMessage().length);
        assertEquals(100, secondReceived.getMessage()[0]);
    }

    public void testTwoClientsOneServerUpOneServerDown()
    {
        this.server1 = this.setupNormalServer();
        this.server2 = null;

        RecordingClientHandler clientHandlerA = new RecordingClientHandler();
        DualNotificationClient clientA = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server1.getPort()+1, clientHandlerA);
        clientA.subscribe(TEST_SUBJECT);
        clientA.start();

        RecordingClientHandler clientHandlerB = new RecordingClientHandler();
        DualNotificationClient clientB = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server1.getPort()+1, clientHandlerB);
        clientB.subscribe(TEST_SUBJECT);
        clientB.start();

        server1.waitForMessagesReceived(6);

        byte[] firstMessage = new byte[] { 100 };
        clientA.broadcastNotification(TEST_SUBJECT, firstMessage);

        byte[] secondMessage = new byte[] { 50 };
        clientB.broadcastNotification(TEST_SUBJECT, secondMessage);

        server1.waitForMessagesReceived(10);

        MithraTestAbstract.sleep(100);

        server1.waitForAllAcks();

        MithraTestAbstract.sleep(100);

        server1.waitForAllAcks();

        clientA.shutdown();
        clientB.shutdown();

        assertEquals(1, clientHandlerA.messages.size());
        assertEquals(1, clientHandlerB.messages.size());

        MessageWithSubject firstReceived = clientHandlerA.messages.get(0);
        assertEquals(TEST_SUBJECT, firstReceived.getSubject());
        assertEquals(1, firstReceived.getMessage().length);
        assertEquals(50, firstReceived.getMessage()[0]);

        MessageWithSubject secondReceived = clientHandlerB.messages.get(0);
        assertEquals(TEST_SUBJECT, secondReceived.getSubject());
        assertEquals(1, secondReceived.getMessage().length);
        assertEquals(100, secondReceived.getMessage()[0]);
    }

    public void testTwoClientsOneReliableServerOneUnreliableService()
    {
        AtomicReference<Double> successRate = new AtomicReference(1.00d);

        this.server1 = this.setupUnreliableServer(successRate);
        this.server2 = this.setupNormalServer();

        RecordingClientHandler clientHandlerA = new RecordingClientHandler();
        DualNotificationClient clientA = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server2.getPort(), clientHandlerA);
        clientA.subscribe(TEST_SUBJECT);
        clientA.start();

        RecordingClientHandler clientHandlerB = new RecordingClientHandler();
        DualNotificationClient clientB = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server2.getPort(), clientHandlerB);
        clientB.subscribe(TEST_SUBJECT);
        clientB.start();

        server1.waitForMessagesReceived(6);
        server2.waitForMessagesReceived(6);

        successRate.set(0.90d);

        byte[] firstMessage = new byte[] { 100, 0 };
        byte[] secondMessage = new byte[] { 50, 0 };

        int sendCount = 50;
        for (int i = 0; i < sendCount; i++)
        {
            firstMessage[1] = (byte) i;
            clientA.broadcastNotification(TEST_SUBJECT, firstMessage);
            secondMessage[1] = (byte) (255 - i);
            clientB.broadcastNotification(TEST_SUBJECT, secondMessage);
        }

        server1.waitForMessagesReceived(6 + 4 * sendCount);
        server2.waitForMessagesReceived(6 + 4 * sendCount);
        clientA.waitForAllAcks();
        clientB.waitForAllAcks();
        server1.waitForAllAcks();
        server2.waitForAllAcks();

        clientA.waitForAllAcks();
        clientB.waitForAllAcks();
        server1.waitForAllAcks();
        server2.waitForAllAcks();

        clientA.shutdown();
        clientB.shutdown();

        assertEquals(sendCount, clientHandlerA.messages.size());

        for (int i = 0; i < clientHandlerA.messages.size(); i++)
        {
            MessageWithSubject messageReceived = clientHandlerA.messages.get(i);
            assertEquals(TEST_SUBJECT, messageReceived.getSubject());
            assertEquals(2, messageReceived.getMessage().length);
            assertEquals(50, messageReceived.getMessage()[0]);
        }

        assertEquals(sendCount, clientHandlerB.messages.size());

        MessageWithSubject firstReceived = clientHandlerA.messages.get(0);
        assertEquals(TEST_SUBJECT, firstReceived.getSubject());
        assertEquals(2, firstReceived.getMessage().length);
        assertEquals(50, firstReceived.getMessage()[0]);

        MessageWithSubject secondReceived = clientHandlerB.messages.get(0);
        assertEquals(TEST_SUBJECT, secondReceived.getSubject());
        assertEquals(2, secondReceived.getMessage().length);
        assertEquals(100, secondReceived.getMessage()[0]);

        successRate.set(1.00d);  // no failures during shutdown
    }

    public void testTwoClientsTwoServersButOneDies()
    {
        this.server1 = this.setupNormalServer();
        NotificationServer server = new NotificationServer(0);
        server.setSocketTimeout(100);
        server.start();
        this.server2 = server;

        RecordingClientHandler clientHandlerA = new RecordingClientHandler();
        DualNotificationClient clientA = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server2.getPort(), clientHandlerA);
        clientA.subscribe(TEST_SUBJECT);
        clientA.start();

        RecordingClientHandler clientHandlerB = new RecordingClientHandler();
        DualNotificationClient clientB = new DualNotificationClient("localhost", this.server1.getPort(), "localhost", this.server2.getPort(), clientHandlerB);
        clientB.subscribe(TEST_SUBJECT);
        clientB.start();

        server1.waitForMessagesReceived(6);
        server2.waitForMessagesReceived(6);

        byte[] firstMessage = new byte[] { 100 };
        clientA.broadcastNotification(TEST_SUBJECT, firstMessage);
        server1.waitForMessagesReceived(5);
        server2.waitForMessagesReceived(5);

        server2.shutdown();  // simulate server dying

        MithraTestAbstract.sleep(200);

        int count = 0;
        while (server2.getState() != Thread.State.TERMINATED && count++ < 120)
        {
            MithraTestAbstract.sleep(1000);
        }
        if (count >= 120)
        {
            fail("Notification Server #2 failed to shutdown");
        }

        server1.waitForMessagesSent(6);

        byte[] secondMessage = new byte[] { 50 };
        clientB.broadcastNotification(TEST_SUBJECT, secondMessage);
        server1.waitForMessagesReceived(5);

        clientA.waitForClientOneAcks();
        clientB.waitForClientOneAcks();
        server1.waitForAllAcks();

        clientA.waitForClientOneAcks();
        clientB.waitForClientOneAcks();
        server1.waitForAllAcks();
        server1.waitForMessagesSent(8);
        server1.waitForAllAcks();

        clientA.shutdown();
        clientB.shutdown();

        assertEquals(1, clientHandlerA.messages.size());
        assertEquals(1, clientHandlerB.messages.size());

        MessageWithSubject firstReceived = clientHandlerA.messages.get(0);
        assertEquals(TEST_SUBJECT, firstReceived.getSubject());
        assertEquals(1, firstReceived.getMessage().length);
        assertEquals(50, firstReceived.getMessage()[0]);

        MessageWithSubject secondReceived = clientHandlerB.messages.get(0);
        assertEquals(TEST_SUBJECT, secondReceived.getSubject());
        assertEquals(1, secondReceived.getMessage().length);
        assertEquals(100, secondReceived.getMessage()[0]);
    }

    @Override
    protected void tearDown() throws Exception
    {
        if (this.server1 != null)
        {
            this.server1.shutdown();
            this.server1 = null;
        }
        if (this.server2 != null)
        {
            this.server2.shutdown();
            this.server2 = null;
        }
        super.tearDown();
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

    private static class RecordingClientHandler implements ClientNotificationHandler
    {
        private List<MessageWithSubject> messages = FastList.newList();

        @Override
        public void handleMessage(String subject, byte[] message)
        {
            messages.add(new MessageWithSubject(subject, message));
        }
    }

    private static class UnreliableNotificationServer extends NotificationServer
    {
        private final AtomicReference<Double> successRate;

        private UnreliableNotificationServer(AtomicReference<Double> successRate, int port)
        {
            super(port);
            this.successRate = successRate;
        }

        @Override
        protected ServerSocketHandler createServerSocketHandler(Socket incoming)
        {
            return new UnreliableServerSocketHandler(successRate, incoming, this);
        }
    }

    private static class UnreliableServerSocketHandler extends ServerSocketHandler
    {
        private final AtomicReference<Double> successRate;

        private UnreliableServerSocketHandler(AtomicReference<Double> successRate, Socket socket, NotificationServer server)
        {
            super(socket, server);
            this.successRate = successRate;
        }

        @Override
        protected InputStream getInputStreamFromSocket() throws IOException
        {
            return new UnreliableInputStream(this.successRate, super.getInputStreamFromSocket());
        }

        @Override
        protected OutputStream getOutputStreamFromSocket() throws IOException
        {
            return new UnreliableOutputStream(this.successRate, super.getOutputStreamFromSocket());
        }
    }

    private static class UnreliableOutputStream extends OutputStream
    {
        private final AtomicReference<Double> successRate;
        private final OutputStream out;

        private UnreliableOutputStream(AtomicReference<Double> successRate, OutputStream out)
        {
            this.successRate = successRate;
            this.out = out;
        }

        @Override
        public void write(int i) throws IOException
        {
            if (Math.random() > this.successRate.get())
            {
                throw new IOException("Random error, for testing only!");
            }
            out.write(i);
        }

        @Override
        public void write(byte[] bytes) throws IOException
        {
            if (Math.random() > this.successRate.get())
            {
                throw new IOException("Random error, for testing only!");
            }
            out.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int off, int len) throws IOException
        {
            if (Math.random() > this.successRate.get())
            {
                throw new IOException("Random error, for testing only!");
            }
            out.write(bytes, off, len);
        }
    }

    private static class UnreliableInputStream extends InputStream
    {
        private final AtomicReference<Double> successRate;
        private final InputStream in;

        private UnreliableInputStream(AtomicReference<Double> successRate, InputStream in)
        {
            this.successRate = successRate;
            this.in = in;
        }

        @Override
        public int read() throws IOException
        {
            if (Math.random() > this.successRate.get())
            {
                throw new IOException("Random error, for testing only!");
            }
            return in.read();
        }

        @Override
        public int read(byte[] bytes) throws IOException
        {
            if (Math.random() > this.successRate.get())
            {
                throw new IOException("Random error, for testing only!");
            }
            return in.read(bytes);
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException
        {
            if (Math.random() > this.successRate.get())
            {
                throw new IOException("Random error, for testing only!");
            }
            return in.read(bytes, off, len);
        }
    }
}
