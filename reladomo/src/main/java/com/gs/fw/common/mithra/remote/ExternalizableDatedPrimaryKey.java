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

import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;



public class ExternalizableDatedPrimaryKey implements Externalizable
{
    static private Logger logger = LoggerFactory.getLogger(ExternalizableDatedPrimaryKey.class.getName());

    private MithraDatedObject mithraDatedObject;

    public ExternalizableDatedPrimaryKey()
    {
        // for externalizable
    }

    public ExternalizableDatedPrimaryKey(MithraDatedObject mithraDatedObject)
    {
        this.mithraDatedObject = mithraDatedObject;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(MithraSerialUtil.getDataClassNameToSerialize(this.mithraDatedObject.zGetCurrentOrTransactionalData()));
        this.mithraDatedObject.zSerializePrimaryKey(out);
    }

    public MithraDatedObject getMithraDatedObject()
    {
        return mithraDatedObject;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        Class dataClassName = MithraSerialUtil.getDataClassToInstantiate(in.readUTF());
        MithraDataObject data = MithraSerialUtil.instantiateData(dataClassName);
        this.mithraDatedObject = (MithraDatedObject) data.zGetMithraObjectPortal().getMithraObjectDeserializer().deserializeForRefresh(in);
    }

    
}
