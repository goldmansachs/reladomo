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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.portal.UpdateDataChooser;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;


public class ExternalizableMultiUpdateOperation implements Externalizable
{

    static private Logger logger = LoggerFactory.getLogger(ExternalizableMultiUpdateOperation.class.getName());
    private transient MultiUpdateOperation multiUpdateOperation;
    private transient UpdateDataChooser updateDataChooser;
    private transient MithraDataObject[] mithraDataObjects;
    private transient List updateWrappers;

    public ExternalizableMultiUpdateOperation(MultiUpdateOperation multiUpdateOperation, UpdateDataChooser updateDataChooser)
    {
        this.multiUpdateOperation = multiUpdateOperation;
        this.updateDataChooser = updateDataChooser;
    }

    public ExternalizableMultiUpdateOperation()
    {
        // for externalizble
    }

    public MithraDataObject[] getMithraDataObjects()
    {
        return mithraDataObjects;
    }

    public List getUpdateWrappers()
    {
        return updateWrappers;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        List updateWrappers = this.multiUpdateOperation.getUpdates();
        out.writeInt(updateWrappers.size());
        for(int i=0;i<updateWrappers.size();i++)
        {
            AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) updateWrappers.get(i);
            out.writeObject(updateWrapper.getClass().getName());
            out.writeObject(updateWrapper.getAttribute());
            updateWrapper.externalizeParameter(out);
        }

        List mithraObjects = this.multiUpdateOperation.getMithraObjects();
        out.writeInt(mithraObjects.size());
        MithraDataObject firstData = updateDataChooser.chooseDataForMultiUpdate((MithraTransactionalObject) mithraObjects.get(0));
        out.writeObject(MithraSerialUtil.getDataClassNameToSerialize(firstData));

        for(int i=0;i<mithraObjects.size();i++)
        {
            MithraDataObject dataObject = updateDataChooser.chooseDataForMultiUpdate((MithraTransactionalObject) mithraObjects.get(i));
            dataObject.zSerializePrimaryKey(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int updateCount = in.readInt();
        this.updateWrappers = new ArrayList(updateCount);
        for(int i=0;i<updateCount;i++)
        {
            String wrapperClassName = (String) in.readObject();
            Attribute attribute = (Attribute) in.readObject();
            AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) MithraSerialUtil.safeInstantiate(wrapperClassName);
            updateWrapper.setAttribute(attribute);
            updateWrapper.readParameter(in);
            updateWrappers.add(updateWrapper);
        }

        int objectCount = in.readInt();
        Class dataClass = MithraSerialUtil.getDataClassToInstantiate((String) in.readObject());
        this.mithraDataObjects = new MithraDataObject[objectCount];
        for(int i=0;i<objectCount;i++)
        {
            MithraDataObject mithraDataObject = MithraSerialUtil.instantiateData(dataClass);
            mithraDataObject.zDeserializePrimaryKey(in);
            this.mithraDataObjects[i] = mithraDataObject;
        }
    }
}
