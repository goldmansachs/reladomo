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
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.portal.UpdateDataChooser;
import com.gs.fw.common.mithra.transaction.BatchUpdateOperation;
import com.gs.fw.common.mithra.transaction.UpdateOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.List;
import java.util.ArrayList;



public class ExternalizableBatchUpdateOperation implements Externalizable
{

    static private Logger logger = LoggerFactory.getLogger(ExternalizableBatchUpdateOperation.class.getName());
    private transient BatchUpdateOperation batchUpdateOperation;
    private transient UpdateDataChooser updateDataChooser;
    private transient MithraDataObject[] mithraDataObjects;
    private transient List[] updateWrappers;

    public ExternalizableBatchUpdateOperation(BatchUpdateOperation batchUpdateOperation, UpdateDataChooser updateDataChooser)
    {
        this.batchUpdateOperation = batchUpdateOperation;
        this.updateDataChooser = updateDataChooser;
    }

    public ExternalizableBatchUpdateOperation()
    {
        // for externalizble
    }

    public MithraDataObject[] getMithraDataObjects()
    {
        return mithraDataObjects;
    }

    public List[] getUpdateWrappers()
    {
        return updateWrappers;
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        List updateOperations = this.batchUpdateOperation.getUpdateOperations();
        UpdateOperation firstOperaton = (UpdateOperation) updateOperations.get(0);
        MithraDataObject firstData = updateDataChooser.chooseDataForMultiUpdate(firstOperaton.getMithraObject());
        out.writeInt(updateOperations.size());
        List firstUpdates = firstOperaton.getUpdates();
        out.writeInt(firstUpdates.size());
        for(int i=0;i<firstUpdates.size();i++)
        {
            AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) firstUpdates.get(i);
            out.writeObject(updateWrapper.getClass().getName());
        }
        for(int i=0;i<firstUpdates.size();i++)
        {
            AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) firstUpdates.get(i);
            out.writeObject(updateWrapper.getAttribute());
        }

        out.writeObject(MithraSerialUtil.getDataClassNameToSerialize(firstData));

        for(int i=0;i<updateOperations.size();i++)
        {
            UpdateOperation updateOperation = (UpdateOperation) updateOperations.get(i);
            MithraDataObject dataObject = updateDataChooser.chooseDataForMultiUpdate(updateOperation.getMithraObject());
            dataObject.zSerializePrimaryKey(out);
        }
        for(int i=0;i<updateOperations.size();i++)
        {
            UpdateOperation updateOperation = (UpdateOperation) updateOperations.get(i);
            List updates = updateOperation.getUpdates();
            for(int j=0;j<updates.size();j++)
            {
                AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) updates.get(j);
                updateWrapper.externalizeParameter(out);
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int objectCount = in.readInt();
        int updateCount = in.readInt();
        String[] updateClassNames = new String[updateCount];
        for(int i=0;i<updateCount;i++)
        {
            updateClassNames[i] = (String) in.readObject();
        }
        Attribute[] updatedAttributes = new Attribute[updateCount];
        for(int i=0;i<updateCount;i++)
        {
            updatedAttributes[i] = (Attribute) in.readObject();
        }

        Class dataClass = MithraSerialUtil.getDataClassToInstantiate((String) in.readObject());
        this.mithraDataObjects = new MithraDataObject[objectCount];
        for(int i=0;i<objectCount;i++)
        {
            MithraDataObject mithraDataObject = MithraSerialUtil.instantiateData(dataClass);
            mithraDataObject.zDeserializePrimaryKey(in);
            this.mithraDataObjects[i] = mithraDataObject;
        }
        this.updateWrappers = new List[objectCount];
        for(int i=0;i<objectCount;i++)
        {
            updateWrappers[i] = new ArrayList(updateCount);
            for(int j=0;j<updateCount;j++)
            {
                AttributeUpdateWrapper updateWrapper = (AttributeUpdateWrapper) MithraSerialUtil.safeInstantiate(updateClassNames[j]);
                updateWrapper.setAttribute(updatedAttributes[j]);
                updateWrapper.readParameter(in);
                updateWrappers[i].add(updateWrapper);
            }
        }

    }
}
