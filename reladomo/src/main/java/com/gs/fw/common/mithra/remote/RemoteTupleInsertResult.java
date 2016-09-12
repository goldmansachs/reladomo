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

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;



public class RemoteTupleInsertResult extends MithraRemoteResult
{

    private transient TupleTempContext tempContext;
    private transient List tupleList;
    private transient MithraObjectPortal destination;
    private transient int bulkInsertThreshold;

    public RemoteTupleInsertResult(MithraObjectPortal destination, TupleTempContext tempContext, List tupleList, int bulkInsertThreshold)
    {
        this.bulkInsertThreshold = bulkInsertThreshold;
        this.destination = destination;
        this.tempContext = tempContext;
        this.tupleList = tupleList;
    }

    public RemoteTupleInsertResult()
    {
        // for externalizable
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        out.writeObject(tempContext);

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        this.tempContext = (TupleTempContext) in.readObject();
    }

    public TupleTempContext getTempContext()
    {
        return tempContext;
    }

    public void run()
    {
        destination.getMithraTuplePersister().insertTuples(tempContext, tupleList, bulkInsertThreshold);
    }
}
