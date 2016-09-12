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

package com.gs.fw.common.mithra.remote;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;



public class RemoteTransactionId implements Externalizable
{

    private int serverVmId;
    private int transactionId;
    private long requestorVmId;

    public RemoteTransactionId()
    {
        // for externalizable
    }

    public RemoteTransactionId(int serverVmId, int transactionId)
    {
        this.serverVmId = serverVmId;
        this.transactionId = transactionId;
    }

    public int getServerVmId()
    {
        return serverVmId;
    }

    public void setServerVmId(int serverVmId)
    {
        this.serverVmId = serverVmId;
    }

    public int getTransactionId()
    {
        return transactionId;
    }

    public void setTransactionId(int transactionId)
    {
        this.transactionId = transactionId;
    }

    public long getRequestorVmId()
    {
        return requestorVmId;
    }

    public void setRequestorVmId(long requestorVmId)
    {
        this.requestorVmId = requestorVmId;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(serverVmId);
        out.writeInt(transactionId);
        out.writeLong(requestorVmId);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.serverVmId = in.readInt();
        this.transactionId = in.readInt();
        this.requestorVmId = in.readLong();
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof RemoteTransactionId))
        {
            return false;
        }

        final RemoteTransactionId remoteTransactionId = (RemoteTransactionId) o;

        if (serverVmId != remoteTransactionId.serverVmId)
        {
            return false;
        }
        if (transactionId != remoteTransactionId.transactionId)
        {
            return false;
        }

        return true;
    }

    public int hashCode()
    {
        return transactionId;
    }
}
