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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestId implements Serializable
{
    private static final long serialVersionUID = -7684909902962589187L;

    private static final int CLIENT_IP = RequestId.createClientIp();
    private static final AtomicInteger TRANSACTION_ID = new AtomicInteger(0);

    private final int clientIp;
    private final long proxyId;
    private final int transactionId;

    private transient long finishedTime;

    public RequestId(long proxyId)
    {
        this.proxyId = proxyId;
        this.transactionId = TRANSACTION_ID.incrementAndGet();
        // static fields are not serialzied, so this is the copy
        this.clientIp = CLIENT_IP;
    }

    public RequestId(int clientIp, long proxyId, int transactionId)
    {
        this.clientIp = clientIp;
        this.proxyId = proxyId;
        this.transactionId = transactionId;
    }

    private static int createClientIp()
    {
        long time = System.currentTimeMillis();
        time &= 0xffffffffL;

        int clientIp;
        try
        {
            InetAddress inetadr = InetAddress.getLocalHost();
            byte[] tmp = inetadr.getAddress();
            clientIp = ((tmp[3] * 255 + tmp[2]) * 255 + tmp[1]) * 255 + tmp[0];
        }
        catch (Exception e)
        {
            // let's just fill it with something random:
            clientIp = (int) (Math.random() * Integer.MAX_VALUE);
        }
        clientIp ^= (int) time;
        return clientIp;
    }

    public long getProxyId()
    {
        return this.proxyId;
    }

    public int getClientIp()
    {
        return this.clientIp;
    }

    public int getTransactionId()
    {
        return this.transactionId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof RequestId)
        {
            RequestId other = (RequestId) obj;
            return this.clientIp == other.clientIp
                    && this.proxyId == other.proxyId
                    && this.transactionId == other.transactionId;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.clientIp ^ (int) (this.proxyId ^ this.proxyId >>> 32) ^ this.transactionId;
    }

    public void setFinishedTime(long finishedTime)
    {
        this.finishedTime = finishedTime;
    }

    public boolean isExpired()
    {
        return System.currentTimeMillis() - this.finishedTime > Context.MAX_LIFE_TIME_FROM_FINISHED;
    }
}
