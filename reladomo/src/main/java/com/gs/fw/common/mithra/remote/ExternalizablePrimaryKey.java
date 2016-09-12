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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;



public class ExternalizablePrimaryKey implements Externalizable
{

    static private Logger logger = LoggerFactory.getLogger(ExternalizablePrimaryKey.class.getName());
    private MithraDataObject mithraDataObject;

    public ExternalizablePrimaryKey(MithraDataObject mithraDataObject)
    {
        this.mithraDataObject = mithraDataObject;
    }

    public ExternalizablePrimaryKey()
    {
        // for externalizble
    }

    public MithraDataObject getMithraDataObject()
    {
        return mithraDataObject;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(MithraSerialUtil.getDataClassNameToSerialize(mithraDataObject));
        mithraDataObject.zSerializePrimaryKey(out);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        Class dataClassName = MithraSerialUtil.getDataClassToInstantiate((String) in.readObject());
        this.mithraDataObject = MithraSerialUtil.instantiateData(dataClassName);
        this.mithraDataObject.zDeserializePrimaryKey(in);
    }
}
