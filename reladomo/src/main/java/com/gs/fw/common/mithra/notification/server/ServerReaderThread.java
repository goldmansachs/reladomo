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

import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.SocketException;


public class ServerReaderThread extends Thread
{

    private static final int NOT_ESTABLISHED = 10;
    private static final int ESTABLISHED = 20;
    private static final AtomicInteger threadId = new AtomicInteger(10);

    private InputStream input;
    private ServerSocketHandler socketHandler;
    private volatile boolean aborted = false;

    private int currentMode = NOT_ESTABLISHED;

    public ServerReaderThread(InputStream input, ServerSocketHandler socketHandler)
    {
        this.input = new BufferedInputStream(input, 8192);
        this.socketHandler = socketHandler;
        this.setName("Reader Thread (Unestablished) - "+threadId.incrementAndGet());
    }

    public void run()
    {
        try
        {
            while (!aborted)
            {
                switch (currentMode)
                {
                    case NOT_ESTABLISHED:
                        readNotEstablished();
                        break;
                    case ESTABLISHED:
                        readEstablished();
                        break;
                    default:
                        throw new RuntimeException("not implemented");
                }
            }
        }
        catch(SocketException sce)
        {
            if (!aborted)
            {
                socketHandler.abort("unxpected exception while reading from client", sce);
            }
        }
        catch (IOException e)
        {
            socketHandler.abort("unxpected exception while reading from client", e);
        }
    }

    private void readEstablished() throws IOException
    {
        Message m = Message.read(input);
        socketHandler.debugReceivedMessage(m);
        switch(m.getType())
        {
            case Message.TYPE_ACK:
                socketHandler.processAck(m);
                break;
            case Message.TYPE_NOTIFY:
                socketHandler.respondToNotify(m);
                break;
            case Message.TYPE_PING:
                socketHandler.respondToPing(m);
                break;
            case Message.TYPE_SUBSCRIBE:
                socketHandler.respondToSubscribe(m);
                break;
            case Message.TYPE_SHUTDOWN:
                socketHandler.respondToShutdown(m);
                break;
            default:
                socketHandler.abort("got unexpected message type "+m.getType(), new IOException("got unexpected message type "+m.getType()));
        }
    }

    private void readNotEstablished() throws IOException
    {
        Message m = Message.read(input);
        socketHandler.debugReceivedMessage(m);
        switch(m.getType())
        {
            case Message.TYPE_ESTABLISH:
                socketHandler.respondToEstablish(m);
                this.currentMode = ESTABLISHED;
                break;
            case Message.TYPE_REESTABLISH:
                socketHandler.respondToReestablish(m);
                this.currentMode = ESTABLISHED;
                break;
            default:
                socketHandler.abort("got unexpected message type "+m.getType(), new IOException("got unexpected message type "+m.getType()));
        }
        this.setName("Reader Thread - "+this.socketHandler.getClientId());
    }

    public void abort()
    {
        this.aborted = true;
    }
}
