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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.remote.RemoteTransactionId;
import com.gs.fw.common.mithra.remote.RemoteMithraService;



public class MithraTransactionException extends MithraDatabaseException
{

    private transient MithraTransaction transactionToWaitFor;
    private RemoteTransactionId remoteTransactionId;
    private transient RemoteMithraService remoteMithraService;
    private static long maxTransactionWaitTime = 30000; // default of 30 seconds

    public MithraTransactionException(String message, boolean retriable)
    {
        super(message);
        this.setRetriable(retriable);
    }

    public MithraTransactionException(String message)
    {
        super(message);
    }

    public MithraTransactionException(String message, Throwable nestedException)
    {
        super(message, nestedException);
    }

    public MithraTransactionException(String message, MithraTransaction transactionToWaitFor)
    {
        super(message);
        this.setRetriable(true);
        this.transactionToWaitFor = transactionToWaitFor;
    }

    public MithraTransaction getTransactionToWaitFor()
    {
        return transactionToWaitFor;
    }

    public void setRemoteTransactionId(RemoteTransactionId remoteTransactionId)
    {
        this.remoteTransactionId = remoteTransactionId;
    }

    public void setRemoteMithraService(RemoteMithraService remoteMithraService)
    {
        this.remoteMithraService = remoteMithraService;
    }

    public boolean mustWaitForRemoteTransaction()
    {
        return this.remoteTransactionId != null;
    }

    public void waitBeforeRetrying()
    {
        if (this.transactionToWaitFor != null)
        {
            this.transactionToWaitFor.waitUntilFinished(maxTransactionWaitTime);
        }
        else if (this.remoteTransactionId != null && this.remoteMithraService != null)
        {
            this.remoteMithraService.waitForRemoteTransaction(remoteTransactionId);
        }
        else
        {
            super.waitBeforeRetrying();
        }
    }

    public RemoteTransactionId getRemoteTransactionId()
    {
        return remoteTransactionId;
    }
}
