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

public class LoopTimingStatistics
{
    private long flushAndCommitTime;
    private long receiveAndParseTime;
    private long firstMessageWaitTime;
    private long fullTransactionTime;

    public long getFlushAndCommitTime()
    {
        return flushAndCommitTime;
    }

    public void setFlushAndCommitTime(long flushAndCommitTime)
    {
        this.flushAndCommitTime = flushAndCommitTime;
    }

    public long getReceiveAndParseTime()
    {
        return receiveAndParseTime;
    }

    public void setReceiveAndParseTime(long receiveAndParseTime)
    {
        this.receiveAndParseTime = receiveAndParseTime;
    }

    public long getFirstMessageWaitTime()
    {
        return firstMessageWaitTime;
    }

    public void setFirstMessageWaitTime(long firstMessageWaitTime)
    {
        this.firstMessageWaitTime = firstMessageWaitTime;
    }

    public void reset()
    {
        this.flushAndCommitTime = 0;
        this.receiveAndParseTime = 0;
        this.firstMessageWaitTime = 0;
        this.fullTransactionTime = 0;
    }

    public void setFullTransactionTime(long fullTransactionTime)
    {
        this.fullTransactionTime = fullTransactionTime;
    }

    public long getFullTransactionTime()
    {
        return fullTransactionTime;
    }
}
