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

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.fw.common.mithra.util.ConcurrentIntObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.lang.management.ManagementFactory;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Iterator;


public class NotificationServer extends Thread
{
    protected Logger logger = LoggerFactory.getLogger(NotificationServer.class);

    private static final WaitForAckProcedure WAIT_FOR_ACK_PROC = new WaitForAckProcedure();

    private int port;
    private int serverId;
    private int socketTimeout;
    private volatile boolean shutdown = false;
    private boolean listening = false;
    private final ExceptionHandler exceptionHandler;

    private AtomicInteger serverMessageId = new AtomicInteger(0);
    private final ConcurrentIntObjectHashMap<ServerSocketHandler> abortedClients = new ConcurrentIntObjectHashMap<ServerSocketHandler>();
    private final ConcurrentIntObjectHashMap<ServerSocketHandler> establishedClients = new ConcurrentIntObjectHashMap<ServerSocketHandler>();
    private final ArrayList<ServerSocketHandler> unestablishedClients = new ArrayList<ServerSocketHandler>();

    // stats
    private AtomicInteger totalMessagesReceived = new AtomicInteger();
    private AtomicLong totalPayloadReceived = new AtomicLong();
    private AtomicInteger totalMessagesSent = new AtomicInteger();
    private AtomicLong totalPayloadSent = new AtomicLong();
    private AtomicInteger totalMessagesBroadCast = new AtomicInteger();
    private AtomicInteger totalMessagesAborted = new AtomicInteger();
    private static final String PORT = "port";

    public NotificationServer(int port)
    {
        this(port, null);
    }

    public NotificationServer(int port, ExceptionHandler exceptionHandler)
    {
        this.port = port;

        this.setName("Mithra Notification Server on " + port);

        this.exceptionHandler = exceptionHandler;
    }

    public void run()
    {
        try
        {
            ServerSocket socket = null;
            synchronized (this)
            {
                socket = new ServerSocket(port);
                if (socketTimeout != 0)
                {
                    socket.setSoTimeout(socketTimeout);
                }
                if (port == 0)
                {
                    port = socket.getLocalPort();
                }
                listening = true;
                this.notifyAll();
            }
            assignServerId();
            new HouseKeepThread().start();
            logger.info("Waiting for connections on port " + port + " server id is: " + serverId);
            while (!shutdown)
            {
                Socket incoming = null;
                try
                {
                    incoming = socket.accept();
                    ServerSocketHandler serverSocketHandler = createServerSocketHandler(incoming);
                    synchronized (unestablishedClients)
                    {
                        unestablishedClients.add(serverSocketHandler);
                    }
                    serverSocketHandler.start();
                }
                catch (SocketTimeoutException e)
                {
                    //ignore
                }
            }
            RuntimeException runtimeException = new RuntimeException("Notification Server shutting down");
            for(Iterator<ServerSocketHandler> it = establishedClients.iterator(); it.hasNext(); )
            {
                it.next().abort("Notification Server shutting down", runtimeException);
            }
        }
        catch (Exception e)
        {
            logger.error("Notification server thread failed. Existing.", e);
            if (this.exceptionHandler != null)
            {
                this.exceptionHandler.handleException(e);
            }
        }
    }

    protected ServerSocketHandler createServerSocketHandler(Socket incoming)
    {
        return new ServerSocketHandler(incoming, this);
    }

    public void setSocketTimeout(int socketTimeout)
    {
        this.socketTimeout = socketTimeout;
    }

    public int getServerId()
    {
        return serverId;
    }

    public void markEstablished(ServerSocketHandler handler)
    {
        synchronized (unestablishedClients)
        {
            unestablishedClients.remove(handler);
        }
        establishedClients.put(handler.getClientId(), handler);
    }

    public void abort(ServerSocketHandler handler)
    {
        abortedClients.put(handler.getClientId(), handler);
        establishedClients.removeKey(handler.getClientId());
    }

    public ServerSocketHandler getExistingHandler(int clientId)
    {
        ServerSocketHandler result;
        result = abortedClients.get(clientId);
        if (result == null)
        {
            result = establishedClients.get(clientId);
            if (result != null)
            {
                result = asyncAbort(clientId, result);
            }
            else
            {
                synchronized (unestablishedClients)
                {
                    for(int i = 0;i<unestablishedClients.size();i++)
                    {
                        if (unestablishedClients.get(i).getClientId() == clientId)
                        {
                            result = unestablishedClients.get(i);
                        }
                    }
                }
                if (result != null)
                {
                    result = asyncAbort(clientId, result);
                }
            }
        }
        return result;
    }

    private ServerSocketHandler asyncAbort(int clientId, ServerSocketHandler result)
    {
        result.abort("Async abort after re-establish", new RuntimeException("async abort"));
        result = abortedClients.get(clientId);
        return result;
    }

    private void assignServerId()
    {
        short pid = getPid();
        if (pid == 0)
        {
            pid = (short)(Math.random()*Integer.MAX_VALUE);
        }
        long now = System.currentTimeMillis() & 0x00FFFFFFFFFF0000L;
        this.serverId = ((int)(now)) | (pid  ^ this.port);
    }

    private static short getPid()
    {
        String pidstr = ManagementFactory.getRuntimeMXBean().getName();
        if (pidstr != null)
        {
            int atIndex = pidstr.indexOf('@');
            if (atIndex > 0)
            {
                pidstr = pidstr.substring(0, atIndex);
            }
        }
        else pidstr = "0";
        short pid = 0;
        try
        {
            pid = (short) Integer.parseInt(pidstr);
        }
        catch (NumberFormatException e)
        {
            // ignore
        }
        return pid;
    }

    public synchronized int getPort()
    {
        while (port == 0)
        {
            try
            {
                this.wait();
            }
            catch (InterruptedException e)
            {
                //ignore
            }
        }
        return port;
    }

    public static void main(String[] args)
    {
        String portStr = System.getProperty(PORT, "0");
        int port = 0;
        try
        {
            port = Integer.parseInt(portStr);
        }
        catch (NumberFormatException e)
        {
            System.err.println("Could not parse port "+portStr+" The port must be an integer.");
            System.exit(-1);
        }
        new NotificationServer(port).start();
    }

    public int getNextMessageId()
    {
        return serverMessageId.incrementAndGet();
    }

    public void gatherReceivedStats(Message m)
    {
        totalMessagesReceived.incrementAndGet();
        totalPayloadReceived.addAndGet(m.getPayloadSize());
    }

    public void gatherSendStats(Message m)
    {
        totalMessagesSent.incrementAndGet();
        totalPayloadSent.addAndGet(m.getPayloadSize());
    }

    public void broadcastNotify(final Message m, int lastClonedMessageId, String subject) throws IOException
    {
        final Message cloned = m.cloneForServer(this.serverId, lastClonedMessageId);
        synchronized (unestablishedClients)
        {
            for(int i=0;i<unestablishedClients.size();i++)
            {
                ServerSocketHandler serverSocketHandler = unestablishedClients.get(i);
                if (serverSocketHandler.getClientId() != m.getSenderId())
                {
                    serverSocketHandler.sendNotifyMessage(cloned, subject);
                    totalMessagesBroadCast.incrementAndGet();
                }
            }
        }
        SendNotifyProcedure notifyProcedure = new SendNotifyProcedure(m.getSenderId(), subject, cloned);
        establishedClients.forEachValue(notifyProcedure);
        abortedClients.forEachValue(notifyProcedure);
    }

    public void broadcastAbort(final Message badMessage, int messageId, final String subject) throws IOException
    {
        final Message abort = new Message(Message.TYPE_NOTIFY, this.serverId);
        abort.setMessageId(messageId);
        abort.setPacketNumber(0);
        abort.setPacketStatus(Message.PACKET_STATUS_ABORT);
        abort.setPayloadSize(0);
        synchronized (unestablishedClients)
        {
            for(int i=0;i<unestablishedClients.size();i++)
            {
                ServerSocketHandler serverSocketHandler = unestablishedClients.get(i);
                if (serverSocketHandler.getClientId() != badMessage.getSenderId())
                {
                    serverSocketHandler.sendAbortMessage(abort, subject);
                    totalMessagesAborted.incrementAndGet();
                }
            }
        }
        SendAbortProcedure proc = new SendAbortProcedure(subject, abort, badMessage);
        establishedClients.forEachValue(proc);
        abortedClients.forEachValue(proc);
    }

    public void removeUnestablished(ServerSocketHandler serverSocketHandler)
    {
        synchronized (unestablishedClients)
        {
            unestablishedClients.remove(serverSocketHandler);
        }
    }

    public void removeAborted(ServerSocketHandler oldHandler)
    {
        abortedClients.removeKey(oldHandler.getClientId());
    }

    public void shutdown()
    {
        this.shutdown = true;
    }

    /* used for testing */
    public void waitForMessagesReceived(int received)
    {
        while(totalMessagesReceived.get() < received)
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
    }

    public void waitForMessagesSent(int sent)
    {
        while(totalMessagesSent.get() < sent)
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
    }

    public void waitForAllAcks()
    {
        synchronized (unestablishedClients)
        {
            for(int i=0;i<unestablishedClients.size();i++)
            {
                ServerSocketHandler serverSocketHandler = unestablishedClients.get(i);
                serverSocketHandler.waitForAllAcks();
            }
        }
        establishedClients.forEachValue(WAIT_FOR_ACK_PROC);
        abortedClients.forEachValue(WAIT_FOR_ACK_PROC);
    }

    public synchronized void waitForStartup()
    {
        while(!listening)
        {
            try
            {
                this.wait();
            }
            catch (InterruptedException e)
            {
                //ignore
            }
        }
    }

    public void removeHandler(ServerSocketHandler serverSocketHandler)
    {
        establishedClients.removeKey(serverSocketHandler.getClientId());
    }

    private class SendNotifyProcedure implements Procedure<ServerSocketHandler>
    {
        private int senderId;
        private String subject;
        private Message message;

        private SendNotifyProcedure(int senderId, String subject, Message message)
        {
            this.senderId = senderId;
            this.subject = subject;
            this.message = message;
        }

        public void value(ServerSocketHandler serverSocketHandler)
        {
            if (serverSocketHandler.getClientId() != senderId)
            {
                try
                {
                    serverSocketHandler.sendNotifyMessage(message, subject);
                    totalMessagesBroadCast.incrementAndGet();
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Should never get here", e);
                }
            }
        }
    }

    private class SendAbortProcedure implements Procedure<ServerSocketHandler>
    {
        private String subject;
        private Message abortMessage;
        private Message badMessage;

        private SendAbortProcedure(String subject, Message abortMessage, Message badMessage)
        {
            this.subject = subject;
            this.abortMessage = abortMessage;
            this.badMessage = badMessage;
        }

        public void value(ServerSocketHandler serverSocketHandler)
        {
            if (serverSocketHandler.getClientId() != badMessage.getSenderId())
            {
                serverSocketHandler.sendAbortMessage(abortMessage, subject);
                totalMessagesAborted.incrementAndGet();
            }
        }
    }

    private static class WaitForAckProcedure implements Procedure<ServerSocketHandler>
    {
        public void value(ServerSocketHandler serverSocketHandler)
        {
            serverSocketHandler.waitForAllAcks();
        }
    }

    private class HouseKeepThread extends Thread
    {
        private long lastStatTime = System.currentTimeMillis();

        private HouseKeepThread()
        {
            this.setDaemon(true);
            this.setName("Notification Server Cleanup Thread ("+serverId+")");
        }

        public void run()
        {
            while(!shutdown)
            {
                try
                {
                    cleanAborted();
                    cleanUnestablished();
                    if (System.currentTimeMillis() > lastStatTime + 10*60000)
                    {
                        reportStats();
                    }
                    Thread.sleep(10000);
                }
                catch(Throwable t)
                {
                    logger.error("error during cleuanup", t);
                }
            }
        }

        private void reportStats()
        {
            logger.info("Connected clients: "+establishedClients.size());
            logger.info("Total Messages Broadcast: "+totalMessagesBroadCast.get()+" Received: "+totalMessagesReceived.get()+" Sent: "+totalMessagesSent.get());
            logger.info("Total Payload Received: "+totalPayloadReceived.get()/1024+"K Sent: "+totalPayloadSent.get()/1024+"K");
        }

        private void cleanUnestablished()
        {
            long now = System.currentTimeMillis();
            synchronized (unestablishedClients)
            {
                Iterator<ServerSocketHandler> it = unestablishedClients.iterator();
                while(it.hasNext())
                {
                    ServerSocketHandler socketHandler = it.next();
                    if (socketHandler.getStartTime() < now - 3 * NotificationClient.WAIT_BEFORE_RECONNECT)
                    {
                        logger.info("Removing unestablished client "+socketHandler.getDiagnosticMessage());
                        it.remove();
                        socketHandler.closeSocket();
                    }
                }
            }
        }

        private void cleanAborted()
        {
            long now = System.currentTimeMillis();
            Iterator<ServerSocketHandler> it = abortedClients.iterator();
            while(it.hasNext())
            {
                ServerSocketHandler handler = it.next();
                if (handler.getAbortTime() < now - 3 * NotificationClient.WAIT_BEFORE_RECONNECT)
                {
                    logger.info("Removing disconnected client "+handler.getClientId()+ " "+handler.getDiagnosticMessage());
                    it.remove();
                }
            }
        }
    }


    public static interface ExceptionHandler
    {
        public void handleException(Throwable exception);
    }
}
