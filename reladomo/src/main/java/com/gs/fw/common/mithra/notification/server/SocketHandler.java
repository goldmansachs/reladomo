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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.io.IOException;


public abstract class SocketHandler
{
    protected String diagnosticMessage;
    protected final LinkedBlockingDeque<Message> writerQueue = new LinkedBlockingDeque<Message>();
    protected final LinkedBlockingDeque<Message> unAcknowledgedMessages = new LinkedBlockingDeque<Message>();

    public List<Message> getNextMessagesToWrite()
    {
        List<Message> result = null;
        try
        {
            Message m = writerQueue.pollFirst(10, TimeUnit.SECONDS);

            if (m != null)
            {
                result = new ArrayList<Message>(writerQueue.size() + 1);
                result.add(m);
                writerQueue.drainTo(result);
                for(int i=0; i<result.size();i++)
                {
                    Message toAck = result.get(i);
                    if (toAck.requiresAck()) unAcknowledgedMessages.add(toAck);
                }
            }
        }
        catch (InterruptedException e)
        {
            //ignore
        }
        return result;
    }

    public String getDiagnosticMessage()
    {
        return diagnosticMessage;
    }

    public void processAck(Message m) throws IOException
    {
        Iterator<Message> it = unAcknowledgedMessages.iterator();
        while(it.hasNext())
        {
            Message unacked = it.next();
            if (unacked.getMessageId() == m.getMessageId()
                    && unacked.getPacketNumber() == m.getPacketNumber())
            {
                it.remove();
                break;
            }
        }
    }

    private void throwBadAckException(Message m, Message responseFor)
            throws IOException
    {
        unAcknowledgedMessages.addFirst(responseFor);
        throw new IOException("Received an ack for the wrong message. Expecting an ack for "+
                responseFor.getPrintableHeader()+" but got an ack for "+m.getPrintableHeader());
    }

    protected void sendAck(Message m)
    {
        Message ack = Message.createAckMessage(m, this.getSenderId());
        writerQueue.add(ack);
    }

    protected abstract int getNextMessageId();

    protected abstract int getSenderId();

    public void queuePingMessage()
    {
        Message m = new Message(Message.TYPE_PING, getSenderId());
        m.setPacketNumber(0);
        m.setMessageId(getNextMessageId());
        m.setPayloadSize(0);
        m.setPacketStatus(Message.PACKET_STATUS_LAST);
        writerQueue.add(m);
    }

    public void waitForAllAcks()
    {
        long start = System.currentTimeMillis();
        while(writerQueue.size() != 0 && unAcknowledgedMessages.size() != 0 && (System.currentTimeMillis() - start) < 60000)
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
}
