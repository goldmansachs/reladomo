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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.behavior.TemporalContainer;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.transaction.MithraDatedObjectPersister;
import com.gs.fw.common.mithra.util.ListFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.sql.Timestamp;



public class RemoteGetForDateRangeResult extends MithraRemoteResult
{
    static private Logger logger = LoggerFactory.getLogger(RemoteGetForDateRangeResult.class.getName());

    private transient MithraDataObject mithraDataObject;
    private transient Timestamp start;
    private transient Timestamp end;
    private transient List results;

    public RemoteGetForDateRangeResult()
    {
        // for externalizable
    }

    public RemoteGetForDateRangeResult(MithraDataObject mithraDataObject, Timestamp start, Timestamp end)
    {
        this.mithraDataObject = mithraDataObject;
        this.start = start;
        this.end = end;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        this.writeRemoteTransactionId(out);
        out.writeInt(results.size());
        if (results.size() > 0)
        {
            out.writeObject(MithraSerialUtil.getDataClassNameToSerialize((MithraDataObject)results.get(0)));
            for(int i=0;i<results.size();i++)
            {
                MithraDataObject mithraDataObject = (MithraDataObject) results.get(i);
                mithraDataObject.zSerializeFullData(out);
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.readRemoteTransactionId(in);
        int count = in.readInt();
        if (count > 0)
        {
            Class dataClass = MithraSerialUtil.getDataClassToInstantiate((String) in.readObject());
            this.results = new ArrayList(count);
            for(int i=0;i<count;i++)
            {
                MithraDataObject mithraDataObject = MithraSerialUtil.instantiateData(dataClass);
                mithraDataObject.zDeserializeFullData(in);
                Cache cache = mithraDataObject.zGetMithraObjectPortal().getCache();
                this.results.add(cache.getTransactionalDataFromData(mithraDataObject));
            }
        }
        else
        {
            this.results = ListFactory.EMPTY_LIST;
        }
    }

    public void run()
    {
        MithraObjectPortal mithraObjectPortal = mithraDataObject.zGetMithraObjectPortal();
        Cache cache = mithraObjectPortal.getCache();
        TemporalContainer container = cache.getOrCreateContainer(mithraDataObject);
        this.results = container.getForDateRange(mithraDataObject, start, end);
    }

    public List getResultList()
    {
        return results;
    }
}
