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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Message;

public class LoggingOutgoingTopicListener implements OutgoingTopicListener
{
    private static final Logger logger = LoggerFactory.getLogger(LoggingOutgoingTopicListener.class);

    @Override
    public void startBatchSend(boolean async)
    {
        logger.debug("start batch send");
    }

    @Override
    public void logByteMessage(String topicName, BytesMessage message, byte[] msgBody)
    {
        logger.debug("sending message on {}", topicName);
    }

    @Override
    public void endBatchSend()
    {
        logger.debug("end batch send");
    }

    @Override
    public void logClonedMessageSend(String topicName, Message message, byte[] body, Message original)
    {
        logger.debug("cloned message send on {}", topicName);
    }

    @Override
    public void logStringMessage(String topicName, BytesMessage message, String msgBody)
    {
        logger.debug("sending message on {}", topicName);
    }
}
