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

import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.finder.Operation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class RemoteContinuedCursorResult extends MithraRemoteResult
{
    private transient ServerContext serverContext;
    private List result;
    private int remoteQueueSize;
    private boolean finished;
    private Operation op;
    private int order;

    public RemoteContinuedCursorResult()
    {
        // for Externalizable
    }

    public RemoteContinuedCursorResult(List result, int order, boolean finished, int remoteQueueSize, Operation op, ServerContext serverContext)
    {
        this.result = result;
        this.order = order;
        this.finished = finished;
        this.remoteQueueSize = remoteQueueSize;
        this.op = op;
        this.serverContext = serverContext;
    }

    public void run()
    {
        //nothing to do
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(op);
        out.writeInt(order);
        //todo: we seem to ignore reachedMaxRetrieveCount in forEachWithCursor
        //out.writeBoolean(this.reachedMaxRetrieveCount);
        out.writeInt(result.size());
        for(int i=0;i<result.size();i++)
        {
            MithraObject mithraObject = (MithraObject) result.get(i);
            serverContext.serializeFullData(mithraObject, out);
        }
        out.writeBoolean(finished);
        out.writeInt(remoteQueueSize);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.op = (Operation) in.readObject();
        this.order = in.readInt();
        //this.reachedMaxRetrieveCount = in.readBoolean();
        MithraObjectPortal mithraObjectPortal = op.getResultObjectPortal();
        this.result = mithraObjectPortal.getMithraObjectDeserializer().deserializeList(op, in, true);
        this.finished= in.readBoolean();
        this.remoteQueueSize = in.readInt();
    }

    public List getResult()
    {
        return result;
    }

    public int getRemoteQueueSize()
    {
        return remoteQueueSize;
    }

    public boolean isFinished()
    {
        return finished;
    }

    public int getOrder()
    {
        return order;
    }
}
