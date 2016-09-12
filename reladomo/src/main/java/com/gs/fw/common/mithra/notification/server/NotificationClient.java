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

import com.gs.fw.common.mithra.util.ConcurrentIntObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.net.SocketException;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;


public class NotificationClient extends SocketHandler implements Runnable
{
    protected Logger logger = LoggerFactory.getLogger(NotificationClient.class);

    private static final AtomicInteger threadId = new AtomicInteger(10);

    private String serverHostname;
    private int serverPort;
    private volatile boolean shutdown = false;

    public static final int WAIT_BEFORE_RECONNECT = 60000;

    private static final int STATE_NOT_ESTABLISHED = 1;
    private static final int STATE_ESTABLISHED = 2;
    private static final int STATE_NOT_REESTABLISHED = 3;

    private volatile boolean isConnected = false;

    private int protocolState = STATE_NOT_ESTABLISHED;

    private Socket socket;
    private int clientId;
    private int serverId;
    private OutputStream socketOutputStream;
    private String diagnosticMessage;
    private ClientNotificationHandler handler;
    private ClientReaderThread readerThread;
    private long lastMessageSendTime;
    private Set<String> subscribed = new UnifiedSet<String>();
    private Thread runnerThread;
    private ConcurrentIntObjectHashMap<List<Message>> incompleteMessages = new ConcurrentIntObjectHashMap<List<Message>>();

    private AtomicInteger messageId = new AtomicInteger((int) (1000 * Math.random()));  // Random start point to avoid accidental reliance or expectations on this value
    private static final int MAX_MESSAGES_TO_KEEP_WHILE_DISCONNECTED = 100;
    private volatile long lastWarnTime;
    private static final long DISCONNECTED_WARN_PERIOD = 10*60000;

    public NotificationClient(String serverHostname, int serverPort, ClientNotificationHandler handler)
    {
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;
        this.handler = handler;
        this.diagnosticMessage = "Notification Server: " + serverHostname + ':' + serverPort;
    }

    public void start()
    {
        Thread t = new Thread(this);
        t.setDaemon(true);
        t.setName("Notification Client Writer (Unestablished) - "+threadId.incrementAndGet());
        t.start();
        this.runnerThread = t;
    }

    public void run()
    {
        while (!shutdown)
        {
            try
            {
                if (!isConnected)
                {
                    connect();
                }
                if (isConnected)
                {
                    switch (protocolState)
                    {
                        case STATE_NOT_ESTABLISHED:
                            establish();
                            break;
                        case STATE_NOT_REESTABLISHED:
                            reestablish();
                            break;
                        case STATE_ESTABLISHED:
                            writeMessages();
                            break;
                        default:
                            throw new RuntimeException("bad protocolState " + protocolState);
                    }
                }
                else
                {
                    try
                    {
                        Thread.sleep(WAIT_BEFORE_RECONNECT);
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
            }
            catch (Throwable e)
            {
                disconnect("Unexpected error: ", e);
            }
        }
    }

    protected int getSenderId()
    {
        return this.clientId;
    }

    public void debugSendMessage(Message m)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Sending message "+m.getPrintableHeader()+diagnosticMessage);
        }
    }

    private void writeMessages()
    {
        try
        {
            List<Message> messages = this.getNextMessagesToWrite();
            if (messages != null)
            {
                for(int i=0;i<messages.size();i++)
                {
                    Message m = messages.get(i);
                    m.setSenderId(this.clientId);
                    this.debugSendMessage(m);
                    m.writeMessage(this.socketOutputStream);
                }
                this.socketOutputStream.flush();
                lastMessageSendTime = System.currentTimeMillis();
            }
            else if (System.currentTimeMillis() - lastMessageSendTime > ServerSocketHandler.CLIENT_PING_PERIOD)
            {
                this.queuePingMessage();
            }
        }
        catch (IOException e)
        {
            disconnect("Could not send messages to server", e);
        }
    }

    protected void disconnect(String msg, Throwable e)
    {
        logger.error(msg, e);
        if (this.readerThread != null)
        {
            this.readerThread.abort();
            this.readerThread = null;
        }
        closeSocket();
    }

    private void closeSocket()
    {
        if (this.socket != null)
        {
            try
            {
                this.socket.close();
            }
            catch (IOException e1)
            {
                // ignore
            }
            this.socket =  null;
        }
        if (this.protocolState == STATE_ESTABLISHED)
        {
            this.protocolState = STATE_NOT_REESTABLISHED;
        }
        this.isConnected = false;
    }

    private void establish()
    {
        Message m = new Message(Message.TYPE_ESTABLISH,  0);
        m.setMessageId(getNextMessageId());
        m.setPacketNumber(0);
        m.setPacketStatus(Message.PACKET_STATUS_LAST);
        m.setPayloadSize(0);
        try
        {
            this.debugSendMessage(m);
            m.writeMessage(this.socketOutputStream);
            this.socketOutputStream.flush();
            InputStream in = getInputStreamFromSocket();
            Message response = Message.read(in);
            if (response.getType() != Message.TYPE_ESTABLISH_RESPONSE)
            {
                throw new IOException("Could not establish connection to server. Expected "+Message.TYPE_ESTABLISH_RESPONSE+
                        " but got "+response.getType());
            }
            this.clientId = response.readIntFromPayload(0);
            this.serverId = response.getSenderId();
            this.protocolState = STATE_ESTABLISHED;
            runnerThread.setName("Notification Client Writer - "+clientId);
            this.readerThread = new ClientReaderThread(this, getInputStreamFromSocket());
            if (logger.isInfoEnabled())
            {
                logger.info("Established link to server id: "+this.serverId+" with local id: "+this.clientId);
            }
            this.readerThread.start();
            sendAck(response);
            sendSubscribeAll();
        }
        catch(SocketException sce)
        {
            if (!shutdown)
            {
                disconnect("Could not establish connection to server", sce);
            }
            else
            {
                closeSocket();
            }
        }
        catch (IOException e)
        {
            disconnect("Could not establish connection to server", e);
        }
    }

    private void reestablish()
    {
        Message m = new Message(Message.TYPE_REESTABLISH,  this.clientId);
        m.setMessageId(getNextMessageId());
        m.setPacketNumber(0);
        m.setPacketStatus(Message.PACKET_STATUS_LAST);
        m.setPayloadSize(4);
        byte[] payload = new byte[4];
        m.setPayload(payload);
        m.writeIntInPayload(0, this.serverId);
        try
        {
            this.debugSendMessage(m);
            m.writeMessage(this.socketOutputStream);
            this.socketOutputStream.flush();
            InputStream in = getInputStreamFromSocket();
            Message response = Message.read(in);
            if (response.getType() == Message.TYPE_SERVER_RECYCLED)
            {
                this.clientId = response.readIntFromPayload(0);
                this.serverId = response.getSenderId();
                runnerThread.setName("Notification Client Writer - "+clientId);
                sendAck(response);
            }
            else if (response.getType() != Message.TYPE_ACK)
            {
                throw new IOException("Could not establish connection to server. Expected "+Message.TYPE_ESTABLISH_RESPONSE+
                        " but got "+response.getType());
            }
            this.protocolState = STATE_ESTABLISHED;
            this.readerThread = new ClientReaderThread(this, getInputStreamFromSocket());
            this.readerThread.start();
            if (logger.isInfoEnabled())
            {
                logger.info("Re-established link to server id: "+this.serverId+" with local id: "+this.clientId);
            }
            writerQueue.addAllFirst(unAcknowledgedMessages);
            unAcknowledgedMessages.clear();
            sendSubscribeAll();
        }
        catch(SocketException sce)
        {
            if (!shutdown)
            {
                disconnect("Could not establish connection to server", sce);
            }
            else
            {
                closeSocket();
            }
        }
        catch (IOException e)
        {
            disconnect("Could not establish connection to server", e);
        }
    }

    private synchronized void sendSubscribeAll()
    {
        if (this.subscribed.size() > 0)
        {
            Message m = new Message(Message.TYPE_SUBSCRIBE, this.getSenderId());
            m.setMessageId(this.getNextMessageId());
            m.setPacketNumber(0);
            m.setPacketStatus(Message.PACKET_STATUS_LAST);
            Object[] allSubjects = new Object[this.subscribed.size()];
            int count = 0;
            int totalSize = 0;
            for(Iterator<String> it = this.subscribed.iterator();it.hasNext();)
            {
                String subject = it.next();
                try
                {
                    byte[] subjectBytes = subject.getBytes(Message.STRING_CHARSET);
                    allSubjects[count] = subjectBytes;
                    totalSize += 4 + subjectBytes.length;
                }
                catch (UnsupportedEncodingException e)
                {
                    // should never get here
                    throw new RuntimeException("could not create bytes from string "+subject);
                }
                count++;
            }
            m.setPayloadSize(totalSize + 4);
            byte[] payload = new byte[totalSize + 4];

            m.setPayload(payload);
            m.writeIntInPayload(0, count);
            int offset = 4;
            for(int i=0;i<allSubjects.length;i++)
            {
                byte[] subjectBytes = (byte[]) allSubjects[i];
                m.writeIntInPayload(offset, subjectBytes.length);
                System.arraycopy(subjectBytes, 0, payload, offset+4, subjectBytes.length);
                offset += 4 + subjectBytes.length;
            }
            writerQueue.addFirst(m);
        }
    }

    protected int getNextMessageId()
    {
        return messageId.incrementAndGet();
    }

    private void connect()
    {
        try
        {
            this.socket = new Socket(serverHostname, serverPort);
            this.socket.setSoTimeout(ServerSocketHandler.SERVER_PING_PERIOD * 2);
            this.socket.setKeepAlive(true);
            this.socket.setTcpNoDelay(true);
            this.socketOutputStream = new BlockOutputStream(getOutputStreamFromSocket(), Message.TCP_PACKET_SIZE);
            isConnected = true;
            if (protocolState == STATE_ESTABLISHED)
            {
                protocolState = STATE_NOT_REESTABLISHED;
            }
            if (logger.isDebugEnabled())
            {
                logger.debug("Connected to " + this.diagnosticMessage);
            }
        }
        catch (IOException e)
        {
            logger.warn("Could not connect to " + this.diagnosticMessage + " Exception: "+e.getClass().getName()+": "+e.getMessage());
            if (logger.isDebugEnabled())
            {
                logger.debug("Could not connect to " + this.diagnosticMessage, e);
            }
            isConnected = false;
        }

    }

    public synchronized void subscribe(String subject)
    {
        this.subscribed.add(subject);
        if (this.clientId != 0)
        {
            sendSubscribtion(subject);
        }
    }

    private void sendSubscribtion(String subject)
    {
        Message m = new Message(Message.TYPE_SUBSCRIBE, this.getSenderId());
        m.setMessageId(this.getNextMessageId());
        m.setPacketNumber(0);
        m.setPacketStatus(Message.PACKET_STATUS_LAST);
        byte[] subjectBytes = new byte[0];
        try
        {
            subjectBytes = subject.getBytes(Message.STRING_CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            // should never get here
            throw new RuntimeException("could not create bytes from string "+subject);
        }
        m.setPayloadSize(subjectBytes.length + 8);
        byte[] payload = new byte[subjectBytes.length + 8];
        System.arraycopy(subjectBytes, 0, payload, 8, subjectBytes.length);
        m.setPayload(payload);
        m.writeIntInPayload(0, 1); // only one subscription
        m.writeIntInPayload(4, subjectBytes.length);
        writerQueue.add(m);
    }

    public synchronized void broadcastNotification(String subject, byte[] message)
    {
        byte[] subjectBytes;
        try
        {
            subjectBytes = subject.getBytes(Message.STRING_CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Could not convert subject to bytes "+subject);
        }
        if (subjectBytes.length > Message.PAYLOAD_MAX_SIZE - 4)
        {
            throw new RuntimeException("subject is too long");
        }
        int len = 4 + subjectBytes.length + message.length;
        Message first = new Message(Message.TYPE_NOTIFY, this.getSenderId());
        int messageId = this.getNextMessageId();
        first.setMessageId(messageId);
        first.setPacketNumber(0);
        first.setPacketStatus(Message.PACKET_STATUS_LAST);
        first.setPayloadSize(len);
        byte[] payload = new byte[len];
        first.setPayload(payload);
        first.writeIntInPayload(0, subjectBytes.length);
        System.arraycopy(subjectBytes, 0, payload, 4, subjectBytes.length);
        System.arraycopy(message, 0, payload, 4 + subjectBytes.length, message.length);
        writerQueue.add(first);
        if (!isConnected && writerQueue.size() > MAX_MESSAGES_TO_KEEP_WHILE_DISCONNECTED)
        {
            writerQueue.removeFirst();
            long now = System.currentTimeMillis();
            if (lastWarnTime < now - DISCONNECTED_WARN_PERIOD)
            {
                logger.warn("Losing messages, not connected to notification server");
                lastWarnTime = now;
            }
        }
    }

    public synchronized void broadcastNotificationWithPacketization(String subject, byte[] message)
    {
        byte[] subjectBytes;
        try
        {
            subjectBytes = subject.getBytes(Message.STRING_CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Could not convert subject to bytes "+subject);
        }
        if (subjectBytes.length > Message.PAYLOAD_MAX_SIZE - 4)
        {
            throw new RuntimeException("subject is too long");
        }
        int len = 4 + subjectBytes.length + message.length;
        int packetsToSend = len / Message.PAYLOAD_MAX_SIZE;
        if ((len % Message.PAYLOAD_MAX_SIZE)*Message.PAYLOAD_MAX_SIZE != len)
        {
            packetsToSend++;
        }
        List<Message> messages = new ArrayList<Message>(packetsToSend);
        Message first = new Message(Message.TYPE_NOTIFY, this.getSenderId());
        int messageId = this.getNextMessageId();
        first.setMessageId(messageId);
        first.setPacketNumber(0);
        first.setPacketStatus(packetsToSend == 1 ? Message.PACKET_STATUS_LAST : Message.PACKET_STATUS_OK);
        int payloadSize = packetsToSend == 1 ? len : Message.PAYLOAD_MAX_SIZE;
        first.setPayloadSize(payloadSize);
        byte[] payload = new byte[payloadSize];
        first.setPayload(payload);
        first.writeIntInPayload(0, subjectBytes.length);
        System.arraycopy(subjectBytes, 0, payload, 4, subjectBytes.length);
        int sentSoFar = payloadSize - 4 - subjectBytes.length;
        System.arraycopy(message, 0, payload, 4 + subjectBytes.length, sentSoFar);
        messages.add(first);
        for(int i=1;i<packetsToSend;i++)
        {
            Message m = new Message(Message.TYPE_NOTIFY, this.getSenderId());
            m.setMessageId(messageId);
            m.setPacketNumber(i);
            m.setPacketStatus(packetsToSend == (i+1) ? Message.PACKET_STATUS_LAST : Message.PACKET_STATUS_OK);
            payloadSize = packetsToSend == (i+1) ? message.length - sentSoFar : Message.PAYLOAD_MAX_SIZE;
            m.setPayloadSize(payloadSize);
            payload = new byte[payloadSize];
            m.setPayload(payload);
            System.arraycopy(message, sentSoFar, payload, 0, payloadSize);
            sentSoFar += payloadSize;
            messages.add(m);
        }
        writerQueue.addAll(messages);
    }

    public void shutdown()
    {
        this.shutdown = true;
        if (this.readerThread != null)
        {
            this.readerThread.abort();
        }
        if (isConnected)
        {
            Message m = new Message(Message.TYPE_SHUTDOWN, this.getSenderId());
            m.setMessageId(getNextMessageId());
            m.setPacketNumber(0);
            m.setPacketStatus(Message.PACKET_STATUS_LAST);
            m.setPayloadSize(0);
            this.writerQueue.add(m);
            try
            {
                this.runnerThread.join(10000);
            }
            catch (InterruptedException e)
            {
                //ignore
            }
            if (!writerQueue.isEmpty())
            {
                writeMessages();
            }
            try
            {
                if (this.socket != null)
                {
                    InputStream inputStream = this.socket.getInputStream();
                    while (inputStream.available() > 0 && inputStream.read() != -1)
                    {
                        //ignore anything coming over
                    }
                }
            }
            catch (IOException e)
            {
                //not a real exception. waiting for the socket to close
            }
        }
        closeSocket();
    }

    public void debugReceivedMessage(Message m)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(m.getPrintableHeader()+diagnosticMessage);
        }
    }

    public void respondToPing(Message m)
    {
        sendAck(m);
    }

    public void respondToNotify(Message m) throws IOException
    {
        if (m.getPacketNumber() == 0 && m.getPacketStatus() == Message.PACKET_STATUS_LAST)
        {
            // single complete message, no need to do reconstruct.
            int subjectSize = 4 + m.readIntFromPayload(0);
            String subject = m.readStringFromPayload(0);
            byte[] notification = new byte[m.getPayloadSize() - subjectSize];
            System.arraycopy(m.getPayload(), subjectSize, notification, 0, notification.length);
            this.handler.handleMessage(subject, notification);
        }
        else
        {
            appendMessage(m);
        }
        sendAck(m);
    }

    private void appendMessage(Message m) throws IOException
    {
        int messageId = m.getMessageId();
        if (m.getPacketNumber() == 0)
        {
            ArrayList<Message> messageList = new ArrayList<Message>();
            messageList.add(m);
            incompleteMessages.put(messageId, messageList);
        }
        else if (m.getPacketStatus() == Message.PACKET_STATUS_ABORT)
        {
            incompleteMessages.removeKey(m.getMessageId());
        }
        else
        {
            List<Message> messageList = incompleteMessages.get(messageId);
            if (messageList == null)
            {
                //ignore this condition, it's quite possible that we subscribed to a new subject and an inprogress message
                //is being picked up in the middle
                return;
            }
            if (m.getPacketNumber() > messageList.get(messageList.size() - 1).getPacketNumber() + 1)
            {
                throw new IOException("Missing packets for message "+m.getPrintableHeader());
            }
            if (m.getPacketNumber() ==  messageList.get(messageList.size() - 1).getPacketNumber() + 1)
            {
                //ignore duplicate packets
                messageList.add(m);
                if (m.getPacketStatus() == Message.PACKET_STATUS_LAST)
                {
                    reconstructMessage(messageList);
                    incompleteMessages.removeKey(messageId);
                }
            }
        }
    }

    private void reconstructMessage(List<Message> messageList) throws IOException
    {
        Message first = messageList.get(0);
        int subjectSize = 4 + first.readIntFromPayload(0);
        String subject = first.readStringFromPayload(0);
        int totalLength = first.getPayloadSize() - subjectSize;
        for(int i=1;i<messageList.size();i++)
        {
            totalLength += messageList.get(i).getPayloadSize();
        }
        byte[] notification = new byte[totalLength];
        int soFar = first.getPayloadSize() - subjectSize;
        System.arraycopy(first.getPayload(), subjectSize, notification, 0, soFar);
        for(int i=1;i<messageList.size();i++)
        {
            Message m = messageList.get(i);
            System.arraycopy(m.getPayload(), 0, notification, soFar, m.getPayloadSize());
            soFar += m.getPayloadSize();
        }
        this.handler.handleMessage(subject, notification);
    }

    public void waitForAllAcks()
    {
        while(protocolState != STATE_ESTABLISHED)
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
        super.waitForAllAcks();
    }

    public int getClientId()
    {
        return clientId;
    }

    protected InputStream getInputStreamFromSocket()
            throws IOException
    {
        return socket.getInputStream();
    }

    protected OutputStream getOutputStreamFromSocket()
            throws IOException
    {
        return socket.getOutputStream();
    }
}
