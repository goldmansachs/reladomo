/*
  Copyright 2018 Goldman Sachs.
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

package com.gs.reladomo.jms;

import java.util.*;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;

public class InMemoryBytesMessage implements BytesMessage
{
    private byte[] payload;
    private Map<String, String> properties = new HashMap<String, String>();

    @Override
    public long getBodyLength() throws JMSException
    {
        return payload.length;
    }

    @Override
    public boolean readBoolean() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public byte readByte() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int readUnsignedByte() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public short readShort() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int readUnsignedShort() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public char readChar() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int readInt() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long readLong() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public float readFloat() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public double readDouble() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String readUTF() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int readBytes(byte[] bytes) throws JMSException
    {
        int len = Math.min(this.payload.length, bytes.length);
        System.arraycopy(this.payload, 0, bytes, 0, len);
        return len;
    }

    @Override
    public int readBytes(byte[] bytes, int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeBoolean(boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeByte(byte b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeShort(short i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeChar(char c) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeInt(int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeLong(long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeFloat(float v) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeDouble(double v) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeUTF(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeBytes(byte[] bytes) throws JMSException
    {
        this.payload = Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public void writeBytes(byte[] bytes, int i, int i2) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeObject(Object o) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void reset() throws JMSException
    {
        //nothing to do until other read methods are implemented
    }

    @Override
    public String getJMSMessageID() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSMessageID(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long getJMSTimestamp() throws JMSException
    {
        return System.currentTimeMillis();
    }

    @Override
    public void setJMSTimestamp(long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] bytes) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSCorrelationID(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getJMSCorrelationID() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Destination getJMSDestination() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSDeliveryMode(int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSRedelivered(boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getJMSType() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSType(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long getJMSExpiration() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSExpiration(long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getJMSPriority() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJMSPriority(int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clearProperties() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean propertyExists(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean getBooleanProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public byte getByteProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public short getShortProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getIntProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long getLongProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public float getFloatProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public double getDoubleProperty(String s) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getStringProperty(String s) throws JMSException
    {
        return properties.get(s);
    }

    @Override
    public Object getObjectProperty(String s) throws JMSException
    {
        return properties.get(s);
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException
    {
        Vector<Object> names = new Vector<Object>();
        names.addAll(properties.keySet());
        return names.elements();
    }

    @Override
    public void setBooleanProperty(String s, boolean b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setByteProperty(String s, byte b) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setShortProperty(String s, short i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setIntProperty(String s, int i) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setLongProperty(String s, long l) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setFloatProperty(String s, float v) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setDoubleProperty(String s, double v) throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setStringProperty(String s, String s2) throws JMSException
    {
        //we are not throwing an exception here as the OutgoingAsyncTopic send sets this property
        properties.put(s, s2);
    }

    @Override
    public void setObjectProperty(String s, Object o) throws JMSException
    {
        properties.put(s, o.toString());
    }

    @Override
    public void acknowledge() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clearBody() throws JMSException
    {
        throw new RuntimeException("not implemented");
    }
}
