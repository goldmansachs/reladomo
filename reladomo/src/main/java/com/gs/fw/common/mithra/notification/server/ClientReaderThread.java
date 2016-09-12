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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;


public class ClientReaderThread extends Thread
{

    private InputStream input;
    private NotificationClient notificationClient;
    private volatile boolean aborted = false;

    public ClientReaderThread(NotificationClient notificationClient, InputStream input)
    {
        this.input = new BufferedInputStream(input, 8192);
        this.notificationClient = notificationClient;
        this.setName("Notification Reader - "+notificationClient.getClientId());
    }

    public void run()
    {
        try
        {
            while (!aborted)
            {
                readMessages();
            }
        }
        catch(SocketException closed)
        {
            if (!aborted)
            {
                notificationClient.disconnect("unxpected exception while reading from client", closed);
            }
        }
        catch (IOException e)
        {
            notificationClient.disconnect("unxpected exception while reading from client", e);
        }
    }

    private void readMessages() throws IOException
    {
        Message m = Message.read(input);
        notificationClient.debugReceivedMessage(m);
        switch(m.getType())
        {
            case Message.TYPE_ACK:
                notificationClient.processAck(m);
                break;
            case Message.TYPE_NOTIFY:
                notificationClient.respondToNotify(m);
                break;
            case Message.TYPE_PING:
                notificationClient.respondToPing(m);
                break;
            default:
                notificationClient.disconnect("got unexpected message type "+m.getType(), new IOException("got unexpected message type "+m.getType()));
        }
    }

    public void abort()
    {
        this.aborted = true;
    }
}
