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

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.notification.server.ClientNotificationHandler;
import com.gs.fw.common.mithra.notification.server.NotificationClient;
import com.gs.fw.common.mithra.notification.server.NotificationServer;
import com.gs.fw.common.mithra.notification.server.ServerSocketHandler;
import junit.framework.TestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class TestNotificationServer extends TestCase
{

    private NotificationServer server;
    private int port;
    private static final String TEST_SUBJECT = "localhost:A";
    private static double ERROR_RATE = 0.98;

    protected void setUp() throws Exception
    {
    }

    private void setupNormalServer()
    {
        server = new NotificationServer(0);
        server.start();
        port = server.getPort();
    }

    private void setupUnreliableServer()
    {
        server = new UnreliableNotificationServer(0);
        server.start();
        port = server.getPort();
    }

    public void testTwoClients()
    {
        setupNormalServer();
        RecordingClientHandler handlerOne = new RecordingClientHandler();
        NotificationClient first = new NotificationClient("localhost", this.port, handlerOne);
        first.subscribe(TEST_SUBJECT);
        first.start();

        RecordingClientHandler handlerTwo = new RecordingClientHandler();
        NotificationClient second = new NotificationClient("localhost", this.port, handlerTwo);
        second.subscribe(TEST_SUBJECT);
        second.start();

        server.waitForMessagesReceived(6);

        byte[] firstMessage = new byte[1];
        firstMessage[0] = 100;
        first.broadcastNotification(TEST_SUBJECT, firstMessage);

        byte[] secondMessage = new byte[1];
        secondMessage[0] = 50;
        second.broadcastNotification(TEST_SUBJECT, secondMessage);

        server.waitForMessagesReceived(10);
        first.waitForAllAcks();
        second.waitForAllAcks();
        server.waitForAllAcks();

        first.waitForAllAcks();
        second.waitForAllAcks();
        server.waitForAllAcks();

        first.shutdown();
        second.shutdown();

        assertEquals(1, handlerOne.messages.size());
        assertEquals(1, handlerTwo.messages.size());

        MessageWithSubject firstReceived = handlerOne.messages.get(0);
        assertEquals(TEST_SUBJECT, firstReceived.getSubject());
        assertEquals(1, firstReceived.getMessage().length);
        assertEquals(50, firstReceived.getMessage()[0]);

        MessageWithSubject secondReceived = handlerTwo.messages.get(0);
        assertEquals(TEST_SUBJECT, secondReceived.getSubject());
        assertEquals(1, secondReceived.getMessage().length);
        assertEquals(100, secondReceived.getMessage()[0]);

    }

    public void testTwoClientsLargeMessage()
    {
        setupNormalServer();
        RecordingClientHandler handlerOne = new RecordingClientHandler();
        NotificationClient first = new NotificationClient("localhost", this.port, handlerOne);
        first.subscribe(TEST_SUBJECT);
        first.start();

        RecordingClientHandler handlerTwo = new RecordingClientHandler();
        NotificationClient second = new NotificationClient("localhost", this.port, handlerTwo);
        second.subscribe(TEST_SUBJECT);
        second.start();

        server.waitForMessagesReceived(6);

        byte[] firstMessage = new byte[5000];
        firstMessage[0] = 100;
        firstMessage[4999] = 50;
        first.broadcastNotification(TEST_SUBJECT, firstMessage);

        byte[] secondMessage = new byte[5000];
        secondMessage[0] = 50;
        secondMessage[4999] = 100;
        second.broadcastNotification(TEST_SUBJECT, secondMessage);

        server.waitForMessagesReceived(10);
        first.waitForAllAcks();
        second.waitForAllAcks();
        server.waitForAllAcks();

        first.waitForAllAcks();
        second.waitForAllAcks();
        server.waitForAllAcks();

        first.shutdown();
        second.shutdown();

        if (handlerTwo.messages.size() < 1)
        {
            System.currentTimeMillis();
        }
        assertEquals(1, handlerOne.messages.size());
        assertEquals(1, handlerTwo.messages.size());

        MessageWithSubject firstReceived = handlerOne.messages.get(0);
        assertEquals(TEST_SUBJECT, firstReceived.getSubject());
        assertEquals(5000, firstReceived.getMessage().length);
        assertEquals(50, firstReceived.getMessage()[0]);
        assertEquals(100, firstReceived.getMessage()[4999]);

        MessageWithSubject secondReceived = handlerTwo.messages.get(0);
        assertEquals(TEST_SUBJECT, secondReceived.getSubject());
        assertEquals(5000, secondReceived.getMessage().length);
        assertEquals(100, secondReceived.getMessage()[0]);
        assertEquals(50, secondReceived.getMessage()[4999]);

    }

    public void testTwoClientsHundredMessages()
    {
        runManyClients(2, 406);
    }

    public void testDozenClientsHundredMessages()
    {
        runManyClients(12, 14436);
    }

    public void testServerThreadFailureHandling() throws InterruptedException
    {
        final int failingPortNumber = -6;
        final CountDownLatch trigger = new CountDownLatch(1);

        NotificationServer.ExceptionHandler mockHandler = new  NotificationServer.ExceptionHandler () {
            @Override
            public void handleException(Throwable exception)
            {
                trigger.countDown();
            }
        };
        server = new NotificationServer(failingPortNumber, mockHandler);
        server.setDaemon(true);
        server.start();

        assertTrue("Exception handler was not called", trigger.await(1L, TimeUnit.MINUTES));
    }

    public void runManyClients(int clientCount, int expectedServerMessages)
    {
        setupNormalServer();
        RecordingClientHandler[] handlers = new RecordingClientHandler[clientCount];
        NotificationClient clients[] = new NotificationClient[clientCount];
        for(int i=0;i< clientCount;i++)
        {
            RecordingClientHandler handler = new RecordingClientHandler();
            handlers[i] = handler;
            NotificationClient client = new NotificationClient("localhost", this.port, handler);
            client.start();
            clients[i] = client;
        }

        for(int i=0;i< clientCount;i++)
        {
            clients[i].subscribe(TEST_SUBJECT);
        }

        server.waitForMessagesReceived(clientCount*3);

        for(int k=0;k<100;k++)
        {
            for(int i=0;i< clientCount;i++)
            {
                byte[] message = new byte[k*100];
                for(int j=0;j<k*100;j++)
                {
                    message[j] = (byte) j;
                }
                clients[i].broadcastNotification(TEST_SUBJECT, message);
            }
        }

        server.waitForMessagesReceived(expectedServerMessages);
        for(int i=0;i< clientCount;i++)
        {
            clients[i].waitForAllAcks();
        }
        server.waitForAllAcks();

        for(int i=0;i< clientCount;i++)
        {
            clients[i].waitForAllAcks();
        }
        server.waitForAllAcks();

        for(int i=0;i< clientCount;i++)
        {
            clients[i].shutdown();
        }

        for(int i=0;i< clientCount;i++)
        {
            assertEquals((clientCount - 1)*100, handlers[i].messages.size());
            for(MessageWithSubject message: handlers[i].messages)
            {
                assertEquals(TEST_SUBJECT, message.getSubject());
                assertTrue(message.getMessage().length % 100 == 0);
                for(int j=0;j<message.getMessage().length;j++)
                {
                    assertEquals((byte) j, message.getMessage()[j]);
                }
            }
        }
    }

    public void xtestTwoUnreliableClientsHundredMessages()
    {
        runManyClientsUnreliableNetwork(2);
    }

    public void xtestDozenUnreliableClientsHundredMessages()
    {
        runManyClientsUnreliableNetwork(12);
    }

    public void runManyClientsUnreliableNetwork(int clientCount)
    {
        setupUnreliableServer();
        ERROR_RATE = 1.1; // no errors
        RecordingClientHandler[] handlers = new RecordingClientHandler[clientCount];
        NotificationClient clients[] = new NotificationClient[clientCount];
        for(int i=0;i< clientCount;i++)
        {
            RecordingClientHandler handler = new RecordingClientHandler();
            handlers[i] = handler;
            NotificationClient client = new UnreliableNotificationClient("localhost", this.port, handler);
            client.start();
            clients[i] = client;
        }

        for(int i=0;i< clientCount;i++)
        {
            clients[i].subscribe(TEST_SUBJECT);
        }

        server.waitForMessagesReceived(clientCount);

        ERROR_RATE = 0.99; // 1% chance of error.

        for(int k=0;k<100;k++)
        {
            for(int i=0;i< clientCount;i++)
            {
                byte[] message = new byte[k*100];
                for(int j=0;j<k*100;j++)
                {
                    message[j] = (byte) j;
                }
                clients[i].broadcastNotification(TEST_SUBJECT, message);
            }
        }
        long now = System.currentTimeMillis();
        int done = 0;
        while(done < clientCount)
        {
            done = 0;
            for(int i=0;i< clientCount;i++)
            {
                if ((clientCount - 1)*100 > handlers[i].messages.size())
                {
                    try
                    {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
                else
                {
                    done++;
                }
                if (System.currentTimeMillis() > now + 5*60*1000)
                {
                    fail("did not complete after 5 minutes");
                }
            }
        }

        for(int i=0;i< clientCount;i++)
        {
            for(int k = 0;k<handlers[i].messages.size();k++)
            {
                MessageWithSubject message = handlers[i].messages.get(k);
                assertEquals(TEST_SUBJECT, message.getSubject());
                assertTrue(message.getMessage().length % 100 == 0);
                for(int j=0;j<message.getMessage().length;j++)
                {
                    assertEquals((byte) j, message.getMessage()[j]);
                }
            }
        }
    }

    protected void tearDown() throws Exception
    {
        server.shutdown();
    }

    private static class MessageWithSubject
    {
        private String subject;
        private byte[] message;

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
    }

    private static class RecordingClientHandler implements ClientNotificationHandler
    {
        private List<MessageWithSubject> messages = new ArrayList<MessageWithSubject>();

        public void handleMessage(String subject, byte[] message)
        {
            messages.add(new MessageWithSubject(subject, message));
        }
    }

    private static class UnreliableNotificationServer extends NotificationServer
    {
        private UnreliableNotificationServer(int port)
        {
            super(port);
        }

        protected ServerSocketHandler createServerSocketHandler(Socket incoming)
        {
            return new UnreliableServerSocketHandler(incoming, this);
        }
    }

    private static class UnreliableServerSocketHandler extends ServerSocketHandler
    {
        private UnreliableServerSocketHandler(Socket socket, NotificationServer server)
        {
            super(socket, server);
        }

        protected InputStream getInputStreamFromSocket() throws IOException
        {
            return new UnreliableInputStream(super.getInputStreamFromSocket());
        }

        protected OutputStream getOutputStreamFromSocket() throws IOException
        {
            return new UnreliableOutputStream(super.getOutputStreamFromSocket());
        }
    }

    private static class UnreliableNotificationClient extends NotificationClient
    {
        private UnreliableNotificationClient(String serverHostname, int serverPort, ClientNotificationHandler handler)
        {
            super(serverHostname, serverPort, handler);
        }

        protected InputStream getInputStreamFromSocket() throws IOException
        {
            return new UnreliableInputStream(super.getInputStreamFromSocket());
        }

        protected OutputStream getOutputStreamFromSocket() throws IOException
        {
            return new UnreliableOutputStream(super.getOutputStreamFromSocket());
        }
    }

    private static class UnreliableOutputStream extends OutputStream
    {
        private OutputStream out;

        private UnreliableOutputStream(OutputStream out)
        {
            this.out = out;
        }

        public void write(int b) throws IOException
        {
            if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            out.write(b);
        }

        public void write(byte b[]) throws IOException
        {
            if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            out.write(b);
        }

        public void write(byte b[], int off, int len) throws IOException
        {
            if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            out.write(b, off, len);
        }
    }

    private static class UnreliableInputStream extends InputStream
    {
        private InputStream in;

        private UnreliableInputStream(InputStream in)
        {
            this.in = in;
        }

        public int read() throws IOException
        {
            if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            return in.read();
        }

        public int read(byte b[]) throws IOException
        {
            if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            return in.read(b);
        }

        public int read(byte b[], int off, int len) throws IOException
        {
            if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            return in.read(b, off, len);
        }
    }
}
