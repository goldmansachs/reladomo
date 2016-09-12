
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

package com.gs.fw.common.mithra.transaction;

public class TransactionStyle
{
    private int retries = 10;
    private int timeout;
    private boolean isRetriableAfterTimeout;

    /**
     * The transaction will have a timout of timeoutInSeconds and 10 retries. It will not retry on a timeout.
     * @param timeoutInSeconds the timeout in seconds
     */
    public TransactionStyle(int timeoutInSeconds)
    {
        this.timeout = timeoutInSeconds;
    }

    /**
     * The transaction will have a timeout of timeoutInSeconds with the given retires.
     * @param timeoutInSeconds the timeout in seconds
     * @param retries number of times to retry. applies to retries due to deadlock exceptions. Timeouts will not be retried
     */
    public TransactionStyle(int timeoutInSeconds, int retries)
    {
        this.retries = retries;
        this.timeout = timeoutInSeconds;
    }

    /**
     * The transaction will have a timeout of timeoutInSeconds with the given retires.
     * @param timeoutInSeconds the timeout in seconds
     * @param retries number of times to retry. applies to retries due to deadlock exceptions and possibly timeouts
     * @param isRetriableAfterTimeout if true, timeouts will be retried.
     */
    public TransactionStyle(int timeoutInSeconds, int retries, boolean isRetriableAfterTimeout)
    {
        this.retries = retries;
        this.timeout = timeoutInSeconds;
        this.isRetriableAfterTimeout = isRetriableAfterTimeout;
    }

    public int getRetries()
    {
        return this.retries;
    }

    public void setRetries(int retries)
    {
        this.retries = retries;
    }

    public int getTimeout()
    {
        return this.timeout;
    }

    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    public boolean isRetriableAfterTimeout()
    {
        return this.isRetriableAfterTimeout;
    }

    public void setRetriableAfterTimeout(boolean retriable)
    {
        this.isRetriableAfterTimeout = retriable;
    }
}
