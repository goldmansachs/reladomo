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

import javax.jms.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;

public class JmsUtil
{
    public static byte[] getMessageBodyAsBytes(Message message) throws JMSException
    {
        if (message instanceof BytesMessage)
        {
            BytesMessage bytesMessage = (BytesMessage) message;
            int length = (int) bytesMessage.getBodyLength();
            byte[] messageBytes = new byte[length];
            bytesMessage.readBytes(messageBytes);
            return messageBytes;
        }
        else if (message instanceof TextMessage)
        {
            TextMessage textMessage = (TextMessage) message;
            return textMessage.getText().getBytes();
        }
        throw new RuntimeException("Received message of unsupported type " + message.getClass());
    }

    public static String getMessageBodyAsString(Message message) throws JMSException
    {
        return new String(getMessageBodyAsBytes(message));
    }

    public static StringBuilder prettyPrintByteMessage(String prefix, Message message, byte[] bytes)
    {
        StringBuilder builder = new StringBuilder(prefix);
        try
        {
            builder.append(" id='").append(message.getJMSMessageID()).append("'");
            builder.append(" type=" ).append(getMessageType(message)).append("'");
            builder.append(" size=").append(bytes.length);
            builder.append(" properties=").append(printProperties(message).toString());
            builder.append(" body='").append(new String(bytes)).append("'");
        }
        catch (JMSException e)
        {
            builder.append("ERROR printing message ").append(e.getClass().getName()).append(": ").append(e.getMessage());
        }
        return builder;
    }

    private static StringBuffer printProperties(Message message) throws JMSException
    {
        Enumeration<String> propertyNames = message.getPropertyNames();
        StringBuffer buf = new StringBuffer("{");
        if (propertyNames != null)
        {
            while (propertyNames.hasMoreElements())
            {
                String propName = propertyNames.nextElement();
                Object objectProperty = message.getObjectProperty(propName);
                buf.append(" '").append(propName).append("'='").append(objectProperty.toString()).append("'");
            }
        }
        buf.append(" }");
        return buf;
    }

    public static JmsUtil.MessageHolder cloneMessage(Session session, Message original) throws JMSException
    {
        if (original instanceof BytesMessage)
        {
            BytesMessage bytesMessage = session.createBytesMessage();
            BytesMessage originalBytesMessage = (BytesMessage) original;
            originalBytesMessage.reset();
            int length = (int) originalBytesMessage.getBodyLength();
            byte[] messageBytes = new byte[length];
            originalBytesMessage.readBytes(messageBytes);

            bytesMessage.writeBytes(messageBytes);

            copyProperties(original, bytesMessage);
            return new JmsUtil.MessageHolder(bytesMessage, messageBytes, original);
        }
        else if (original instanceof TextMessage)
        {
            TextMessage textMessage = session.createTextMessage();
            TextMessage originalTextMessage = (TextMessage) original;
            String originalText = originalTextMessage.getText();
            textMessage.setText(originalText);

            copyProperties(original, textMessage);
            return new JmsUtil.MessageHolder(textMessage, originalText.getBytes(), original);
        }
        throw new RuntimeException("Unknown message type "+original.getClass().getName());
    }

    private static void copyProperties(Message original, Message cloneMessage) throws JMSException
    {
        Enumeration<String> propertyNames = original.getPropertyNames();
        if (propertyNames != null)
        {
            while (propertyNames.hasMoreElements())
            {
                String propName = propertyNames.nextElement();
                Object objectProperty = original.getObjectProperty(propName);
                cloneMessage.setObjectProperty(propName, objectProperty);
            }
        }
    }

    public static class MessageHolder
    {
        Message message;
        Message original;
        byte[] body;

        public MessageHolder(Message message, byte[] body, Message original)
        {
            this.message = message;
            this.body = body;
            this.original = original;
        }
    }

    private static String getMessageType(Message message)
    {
        return message.getClass().getSimpleName();
    }
}
