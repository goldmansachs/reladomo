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

import javax.jms.Message;
import java.util.List;
import java.util.concurrent.Future;

public interface InFlightBatch
{
    /**
     * Add a message to this loop.
     * @param message messages to be added. Parsing the message, so long as it doesn't incur IO, is fine to do here.
     * @param topicName the topic name of the loop.
     */
    void addMessage(Message message, String topicName);

    /**
     * Add an invalid message to this loop. This messages has caused an unexpected exception previously.
     * @param message message to be added. As the message is invalid, care should be taken not to do much with it.
     *                For example, if addMessage normally parses, this method should not without heavily guarding
     *                against possible throwables.
     * @param topicName the topic name of the loop.
     * @param lastMessageError error from the last time this message was tried
     */
    void addInvalidMessage(Message message, String topicName, Throwable lastMessageError);

    /**
     *
     * @return a list of futures to wait for before committing. May be empty.
     */
    List<Future> getOutgoingMessageFutures();

    /**
     *
     * @return the total number of messages added to this batch, valid or not.
     */
    int size();

    /**
     *
     * @return the number of valid messages expected to be processed in this batch (not discarded or combined)
     */
    int validAndNotSubsumedInFlightRecordsSize();
}
