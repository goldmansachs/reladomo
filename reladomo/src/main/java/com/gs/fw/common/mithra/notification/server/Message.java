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

import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;


public class Message
{

    private static final int MAGIC = 0xDEC0C0DE;
    private static final byte PROTOCOL_VERSION = 0x01;

    public static final byte TYPE_ESTABLISH = 0x01;
    public static final byte TYPE_ESTABLISH_RESPONSE = 0x02;
    public static final byte TYPE_REESTABLISH = 0x03;
    public static final byte TYPE_SERVER_RECYCLED = 0x04;
    public static final byte TYPE_SUBSCRIBE = 0x05;
    public static final byte TYPE_NOTIFY = 0x06;
    public static final byte TYPE_PING = 0x08;
    public static final byte TYPE_SHUTDOWN = 0x09;
    public static final byte TYPE_ACK = 0x10;

    public static final byte PACKET_STATUS_OK = 0x01;
    public static final byte PACKET_STATUS_LAST = 0x02;
    public static final byte PACKET_STATUS_ABORT = 0x03;

    public static final int MESSAGE_HEADER_SIZE = 23;
    public static final int TCP_PACKET_SIZE = 1500 /* Ethernet MTU */ - 24 /* tcp header size */;
    public static final int PAYLOAD_MAX_SIZE = TCP_PACKET_SIZE - MESSAGE_HEADER_SIZE;

    private byte type;
    private int senderId;
    private int messageId;
    private int packetNumber;
    private byte packetStatus;
    private int payloadSize;
    private byte[] payload;
    public static final String STRING_CHARSET = "ISO-8859-1";

    private static final IntObjectHashMap<String> readableType = new IntObjectHashMap<String>(20);

    static
    {
        readableType.put(TYPE_ESTABLISH, "Establish");
        readableType.put(TYPE_ESTABLISH_RESPONSE, "Establish Response");
        readableType.put(TYPE_REESTABLISH, "Reestablish");
        readableType.put(TYPE_SERVER_RECYCLED, "Server Recycled");
        readableType.put(TYPE_SUBSCRIBE, "Subscribe");
        readableType.put(TYPE_NOTIFY, "Notify");
        readableType.put(TYPE_PING, "Ping");
        readableType.put(TYPE_ACK, "Ack");
        readableType.put(TYPE_SHUTDOWN, "Shutdown");
    }

    public Message(byte type, int senderId)
    {
        this.type = type;
        this.senderId = senderId;
    }

    private Message()
    {
    }

    public void setSenderId(int senderId)
    {
        this.senderId = senderId;
    }

    public void setMessageId(int messageId)
    {
        this.messageId = messageId;
    }

    public void setPacketStatus(byte packetStatus)
    {
        this.packetStatus = packetStatus;
    }

    public void setPacketNumber(int packetNumber)
    {
        this.packetNumber = packetNumber;
    }

    public void setPayloadSize(int payloadSize)
    {
        this.payloadSize = payloadSize;
    }

    public void setPayload(byte[] payload)
    {
        this.payload = payload;
    }

    public int getMessageId()
    {
        return messageId;
    }

    public int getPacketNumber()
    {
        return packetNumber;
    }

    public byte getPacketStatus()
    {
        return packetStatus;
    }

    public byte[] getPayload()
    {
        return payload;
    }

    public int getPayloadSize()
    {
        return payloadSize;
    }

    public int getSenderId()
    {
        return senderId;
    }

    public byte getType()
    {
        return type;
    }

    public static Message read(InputStream in) throws IOException
    {
        Message result = new Message();
        result.readMessage(in);
        return result;
    }

    private void readMessage(InputStream in)
            throws IOException
    {
        this.readMagic(in);
        this.readProtocolVersion(in);
        this.type = readByte(in);
        this.senderId = readInt(in);
        this.messageId = readInt(in);
        this.packetNumber = readInt(in);
        this.packetStatus = readByte(in);
        this.payloadSize = readInt(in);
        this.readPayload(in);
    }

    public void writeMessage(OutputStream out) throws IOException
    {
        writeInt(out, MAGIC);
        writeByte(out, PROTOCOL_VERSION);
        writeByte(out, this.type);
        writeInt(out, senderId);
        writeInt(out, messageId);
        writeInt(out, packetNumber);
        writeByte(out, packetStatus);
        writeInt(out, payloadSize);
        writePayload(out);
    }

    private void writePayload(OutputStream out) throws IOException
    {
        if (payload != null && payloadSize > 0)
        {
            out.write(payload, 0, payloadSize);
        }
    }

    private static void writeByte(OutputStream out, byte b) throws IOException
    {
        out.write(b);
    }

    private static void writeInt(OutputStream out, int i) throws IOException
    {
        out.write((i >> 24) & 0xFF);
        out.write((i >> 16) & 0xFF);
        out.write((i >> 8) & 0xFF);
        out.write(i & 0xFF);
    }

    private void readPayload(InputStream in) throws IOException
    {
        this.payload = new byte[this.payloadSize];
        int left = payloadSize;
        while (left > 0)
        {
            int read = in.read(this.payload, payloadSize - left, left);
            if (read < 0)
            {
                throw new SocketException("could not read message's paylaod. Expecting "+this.payloadSize+" but got "+(payloadSize - left));
            }
            left -= read;
        }
    }

    private void readProtocolVersion(InputStream in) throws IOException
    {
        int protocol = safeRead(in);
        if (protocol != PROTOCOL_VERSION)
        {
            throw new IOException("Unexpected protocol version. Expected "+PROTOCOL_VERSION+" but got "+protocol);
        }
    }

    private byte readByte(InputStream in)
            throws IOException
    {
        return (byte) safeRead(in);
    }

    private void readMagic(InputStream in) throws IOException
    {
        int magic = readInt(in);
        if (magic != MAGIC)
        {
            throw new IOException("Unexpected start of message. Expected "+MAGIC+" but got "+magic);
        }
    }

    private int readInt(InputStream in) throws IOException
    {
        return safeRead(in) << 24 | safeRead(in) << 16 | safeRead(in) << 8 | safeRead(in);
    }

    private int safeRead(InputStream in) throws IOException
    {
        int i = in.read();
        if (i == -1) throw new SocketException("unexpected end of stream");
        return i;
    }

    public void writeIntInPayload(int offset, int i)
    {
        payload[offset] = (byte)((i >> 24) & 0xFF);
        payload[offset + 1] = (byte)((i >> 16) & 0xFF);
        payload[offset + 2] = (byte)((i >> 8) & 0xFF);
        payload[offset + 3] = (byte)(i & 0xFF);
    }

    public int readIntFromPayload(int offset)
    {
        return (((int)payload[offset]) & 0xFF) << 24 | (((int)payload[offset+1]) & 0xFF) << 16 | (((int)payload[offset+2]) & 0xFF) << 8 | (((int)payload[offset+3]) & 0xFF);
    }

    public String getPrintableHeader()
    {
        return " Message Type: "+type+" ("+readableType.get(type)+") Sender ID: "+senderId+" Message ID: "+messageId+" Packet Number: "+packetNumber+
                " Packet Status: "+packetStatus+" payload size: "+payloadSize;
    }

    public boolean requiresAck()
    {
        return type == TYPE_NOTIFY || type == TYPE_SUBSCRIBE || type == TYPE_PING || type == TYPE_REESTABLISH || type == TYPE_ESTABLISH_RESPONSE;
    }

    public Message cloneForServer(int serverId, int lastClonedMessageId)
    {
        Message result = new Message(this.type, serverId);
        result.setMessageId(lastClonedMessageId);
        result.setPacketNumber(this.packetNumber);
        result.setPacketStatus(this.packetStatus);
        result.setPayloadSize(this.payloadSize);
        result.setPayload(this.payload);
        return result;
    }

    public String readStringFromPayload(int offset) throws IOException
    {
        int size = this.readIntFromPayload(offset);
        String result = null;
        try
        {
            result = new String(this.payload, offset + 4, size, STRING_CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            IOException ioException = new IOException("Could not get string from payload");
            ioException.initCause(e);
            throw ioException;
        }
        return result;
    }

    public int getTotalMessageSize()
    {
        return MESSAGE_HEADER_SIZE + this.payloadSize;
    }

    public static Message createAckMessage(Message original, int senderId)
    {
        Message ack = new Message(Message.TYPE_ACK, senderId);
        ack.setPacketNumber(original.getPacketNumber());
        ack.setMessageId(original.getMessageId());
        ack.setPayloadSize(0);
        ack.setPacketStatus(Message.PACKET_STATUS_LAST);
        return ack;
    }
}
