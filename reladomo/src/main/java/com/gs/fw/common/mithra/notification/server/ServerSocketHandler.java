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

package com.gs.fw.common.mithra.notification.server;

import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;


public class ServerSocketHandler extends SocketHandler
{
    protected Logger logger = LoggerFactory.getLogger(ServerSocketHandler.class);

    public static final int SERVER_PING_PERIOD = 60000;
    public static final int CLIENT_PING_PERIOD = 2*SERVER_PING_PERIOD;

    private static AtomicInteger clientIdPool = new AtomicInteger(0);
    private int clientId;

    private Socket socket;
    private OutputStream socketOutputStream;
    private NotificationServer server;
    private ServerReaderThread readerThread;
    private ServerWriterThread writerThread;
    private boolean expectingNewMessage = true;
    private int lastIncompleteMessageId;
    private int lastPacketNumber = -1;
    private String lastSubject;
    private int lastClonedMessageId;
    private volatile long abortTime;
    private volatile long startTime;
    private final UnifiedSet<String> subscribedSubjects = new UnifiedSet<String>();

    public ServerSocketHandler(Socket socket, NotificationServer server)
    {
        this.socket = socket;
        this.server = server;
    }

    public void start()
    {
        this.startTime = System.currentTimeMillis();
        InetSocketAddress socketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        InetAddress address = socketAddress.getAddress();
        diagnosticMessage  = " Host: "+address.getHostAddress()+" ("+socketAddress.getHostName()+"): "+socketAddress.getPort();

        InputStream inputStream = null;
        try
        {
            this.socket.setTcpNoDelay(true);
            this.socket.setKeepAlive(true);
            this.socket.setSoTimeout(CLIENT_PING_PERIOD * 2);
            inputStream = getInputStreamFromSocket();
            socketOutputStream = getOutputStreamFromSocket();
            readerThread = new ServerReaderThread(inputStream, this);
            readerThread.start();
        }
        catch (IOException e)
        {
            logger.error("could not get stream from socket", e);
            closeSocket();
            this.server.removeUnestablished(this);
        }

    }

    public long getStartTime()
    {
        return startTime;
    }

    protected OutputStream getOutputStreamFromSocket()
            throws IOException
    {
        return socket.getOutputStream();
    }

    protected InputStream getInputStreamFromSocket()
            throws IOException
    {
        return socket.getInputStream();
    }

    public int getClientId()
    {
        return clientId;
    }

    public long getAbortTime()
    {
        return abortTime;
    }

    public synchronized void abort(String message, Throwable t)
    {
        this.abortTime = System.currentTimeMillis();
        if (this.readerThread != null) readerThread.abort();
        if (this.writerThread != null) writerThread.abort();
        logger.warn(message+diagnosticMessage, t);
        closeSocket();
        this.readerThread = null;
        this.writerThread = null;
        server.abort(this);
    }

    public void closeSocket()
    {
        try
        {
            if (socket != null) socket.close();
        }
        catch (IOException e)
        {
            logger.warn("could not close socket "+diagnosticMessage, e);
        }
        socket = null;
    }

    private void createWriterThread()
    {
        writerThread = new ServerWriterThread(socketOutputStream, this);
        writerThread.start();
    }

    private void assignNewClientId()
    {
        int clientId = clientIdPool.incrementAndGet();
        if (clientId == getSenderId())
        {
            clientId = clientIdPool.incrementAndGet();
        }
        this.clientId = clientId;
    }

    public void respondToEstablish(Message m)
    {
        respondToEstablishWithNewClentId(Message.TYPE_ESTABLISH_RESPONSE);
    }

    private void respondToEstablishWithNewClentId(byte messageType)
    {
        Message response = new Message(messageType, getSenderId());
        assignNewClientId();

        response.setMessageId(getNextMessageId());
        response.setPacketNumber(0);
        response.setPacketStatus(Message.PACKET_STATUS_LAST);
        byte[] payload = new byte[4];
        response.setPayloadSize(4);
        response.setPayload(payload);
        response.writeIntInPayload(0, clientId);

        writerQueue.addFirst(response);

        server.markEstablished(this);
        createWriterThread();
    }

    public void respondToReestablish(Message m)
    {
        int serverId = m.readIntFromPayload(0);
        if (serverId != getSenderId())
        {
            respondToReestablishWithServerRecycle(m);
            return;
        }
        int existingClientId = m.getSenderId();
        ServerSocketHandler oldHandler = server.getExistingHandler(existingClientId);
        if (oldHandler == null)
        {
            respondToReestablishWithServerRecycle(m);
            return;
        }
        sendAck(m);
        this.writerQueue.addAll(oldHandler.unAcknowledgedMessages);
        this.writerQueue.addAll(oldHandler.writerQueue);
        this.subscribedSubjects.addAll(oldHandler.subscribedSubjects);
        this.lastClonedMessageId = oldHandler.lastClonedMessageId;
        this.lastIncompleteMessageId = oldHandler.lastIncompleteMessageId;
        this.lastPacketNumber = oldHandler.lastPacketNumber;
        this.lastSubject = oldHandler.lastSubject;
        this.clientId = existingClientId;
        server.markEstablished(this);
        server.removeAborted(oldHandler);
        createWriterThread();
    }

    private void respondToReestablishWithServerRecycle(Message m)
    {
        respondToEstablishWithNewClentId(Message.TYPE_SERVER_RECYCLED);
    }

    public void debugReceivedMessage(Message m)
    {
        server.gatherReceivedStats(m);
        if (logger.isDebugEnabled())
        {
            logger.debug("Received "+m.getPrintableHeader()+diagnosticMessage);
        }
    }

    public void debugSendMessage(Message m)
    {
        server.gatherSendStats(m);
        if (logger.isDebugEnabled())
        {
            logger.debug("Sending "+m.getPrintableHeader()+diagnosticMessage);
        }
    }

    protected int getSenderId()
    {
        return server.getServerId();
    }

    protected int getNextMessageId()
    {
        return server.getNextMessageId();
    }

    public void respondToNotify(Message m) throws IOException
    {
        if (expectingNewMessage)
        {
            if (m.getPacketNumber() == 0)
            {
                lastIncompleteMessageId = m.getMessageId();
                lastSubject = m.readStringFromPayload(0);
                lastClonedMessageId = getNextMessageId();
                lastPacketNumber = 0;
                expectingNewMessage = false;
            }
            else if (m.getMessageId() > lastIncompleteMessageId)
            {
                server.broadcastAbort(m, lastClonedMessageId, lastSubject);
                abort("message out of sequence", new IOException("Unexpected packet number "
                    +m.getPrintableHeader()+" "+diagnosticMessage));
                return;
            }
            else
            {
                logger.warn("Ignoring possible retransmit. Current message id: "+lastIncompleteMessageId+" ignored: "+m.getPrintableHeader()+" "+diagnosticMessage);
                sendAck(m);
                return;
            }
        }
        else
        {
            if (lastIncompleteMessageId == m.getMessageId())
            {
                if (lastPacketNumber + 1 == m.getPacketNumber())
                {
                    lastPacketNumber++; // all good
                }
                else if (m.getPacketNumber() <= lastPacketNumber)
                {
                    //ignore duplicate
                    sendAck(m);
                    return;
                }
                else
                {
                    server.broadcastAbort(m, lastClonedMessageId, lastSubject);
                    abort("packet out of sequence", new IOException("expecting packet "+(lastPacketNumber + 1)+" for message "+lastIncompleteMessageId+" but got "+
                            m.getPrintableHeader()+" "+diagnosticMessage));
                    return;
                }
            }
            else if (lastIncompleteMessageId > m.getMessageId())
            {
                logger.warn("Ignoring possible retransmit. Current message id: "+lastIncompleteMessageId+" ignored: "+m.getPrintableHeader()+" "+diagnosticMessage);
                sendAck(m);
                return;
            }
            else
            {
                server.broadcastAbort(m, lastClonedMessageId, lastSubject);
                abort("packet out of sequence", new IOException("expecting packet "+(lastPacketNumber + 1)+" for message "+lastIncompleteMessageId+" but got "+
                        m.getPrintableHeader()+" "+diagnosticMessage));
                return;
            }
        }
        if (m.getPacketStatus() == Message.PACKET_STATUS_LAST)
        {
            expectingNewMessage = true;
        }
        server.broadcastNotify(m, lastClonedMessageId, lastSubject);
        sendAck(m);
    }

    public void sendNotifyMessage(Message notifyMessage, String subject) throws IOException
    {
        queueIfSubscribed(notifyMessage, subject);
    }

    public void sendAbortMessage(Message abort, String subject)
    {
        queueIfSubscribed(abort, subject);
    }

    private void queueIfSubscribed(Message notifyMessage, String subject)
    {
        boolean haveToSend = false;
        synchronized (subscribedSubjects)
        {
            haveToSend = subscribedSubjects.contains(subject);
        }
        if (haveToSend)
        {
            writerQueue.add(notifyMessage);
        }
    }

    public void respondToSubscribe(Message m) throws IOException
    {
        int numberOfSubscriptions = m.readIntFromPayload(0);
        int offset = 4;
        for(int i=0;i<numberOfSubscriptions;i++)
        {
            int size = m.readIntFromPayload(offset);
            String subject = m.readStringFromPayload(offset);
            synchronized (subscribedSubjects)
            {
                subscribedSubjects.add(subject);
            }
            offset += size +4;
        }
        sendAck(m);
    }

    public void respondToPing(Message m)
    {
        sendAck(m);
    }

    public void respondToShutdown(Message m)
    {
        if (this.readerThread != null) readerThread.abort();
        if (this.writerThread != null) writerThread.abort();
        closeSocket();
        this.readerThread = null;
        this.writerThread = null;
        server.removeHandler(this);
    }
}
