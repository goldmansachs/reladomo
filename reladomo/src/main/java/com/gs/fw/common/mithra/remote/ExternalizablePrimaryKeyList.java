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
import com.gs.fw.common.mithra.MithraTransactionalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.List;
import java.util.ArrayList;



public class ExternalizablePrimaryKeyList implements Externalizable
{

    static private Logger logger = LoggerFactory.getLogger(ExternalizablePrimaryKeyList.class.getName());
    private List mithraDataObjects;

    public ExternalizablePrimaryKeyList(List mithraObjects)
    {
        mithraDataObjects = new ArrayList(mithraObjects.size());
        for(int i=0;i<mithraObjects.size();i++)
        {
            mithraDataObjects.add(((MithraTransactionalObject)mithraObjects.get(i)).zGetTxDataForRead());
        }

    }

    public ExternalizablePrimaryKeyList()
    {
        // for externalizble
    }

    public void setMithraDataObjects(List mithraDataObjects)
    {
        this.mithraDataObjects = mithraDataObjects;
    }

    public List getMithraDataObjects()
    {
        return mithraDataObjects;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(MithraSerialUtil.getDataClassNameToSerialize((MithraDataObject) mithraDataObjects.get(0)));
        out.writeInt(mithraDataObjects.size());
        for(int i=0;i<mithraDataObjects.size();i++)
        {
            MithraDataObject mithraDataObject = (MithraDataObject) mithraDataObjects.get(i);
            mithraDataObject.zSerializePrimaryKey(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        Class dataClass = MithraSerialUtil.getDataClassToInstantiate((String) in.readObject());
        int count = in.readInt();
        this.mithraDataObjects = new ArrayList(count);
        for(int i=0;i<count;i++)
        {
            MithraDataObject mithraDataObject = MithraSerialUtil.instantiateData(dataClass);
            mithraDataObject.zDeserializePrimaryKey(in);
            this.mithraDataObjects.add(mithraDataObject);
        }
    }
}
