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

import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class ServerWriterThread extends Thread
{

    private OutputStream output;
    private ServerSocketHandler socketHandler;
    private volatile boolean aborted = false;
    private long lastMessageSendTime;

    public ServerWriterThread(OutputStream output, ServerSocketHandler socketHandler)
    {
        this.output = new BlockOutputStream(output, Message.TCP_PACKET_SIZE);
        this.socketHandler = socketHandler;
        this.setName("Writer Thread - "+this.socketHandler.getClientId());
    }

    public void run()
    {
        while(!aborted)
        {
            try
            {
                List<Message> messages = socketHandler.getNextMessagesToWrite();
                if (!aborted && messages != null)
                {
                    for(int i=0;i<messages.size();i++)
                    {
                        Message m = messages.get(i);
                        this.socketHandler.debugSendMessage(m);
                        m.writeMessage(this.output);
                    }
                    this.output.flush();
                    lastMessageSendTime = System.currentTimeMillis();
                }
                else if (System.currentTimeMillis() - lastMessageSendTime > ServerSocketHandler.SERVER_PING_PERIOD)
                {
                    socketHandler.queuePingMessage();
                }
            }
            catch (Throwable e)
            {
                socketHandler.abort("Unexpected exception while sending", e);
            }
        }
    }

    public void abort()
    {
        this.aborted = true;
    }
}
