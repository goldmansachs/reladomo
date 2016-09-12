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

package com.gs.fw.common.mithra.notification;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.remote.MithraRemoteResult;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.remote.MithraSerialUtil;

import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;



public class MithraNotificationEvent implements java.io.Serializable
{

    //todo: type-safe constant??
    public static final byte INSERT = 10;
    public static final byte UPDATE = 20;
    public static final byte DELETE = 30;
    public static final byte MASS_DELETE = 40;

    private String classname;
    private byte databaseOperation;
    private Operation operationForMassDelete;
    private Object sourceAttribute;

    //New
    private MithraDataObject[] dataObjects;
    private Attribute[] updatedAttributes;

    public MithraNotificationEvent()
    {

    }

    public MithraNotificationEvent(String classname, byte databaseOperation,
                                     MithraDataObject[] dataObjects, Attribute[] updatedAttributes,
                                     Operation operationForMassDelete,Object sourceAttribute)
    {
        this.classname = classname;
        this.databaseOperation = databaseOperation;
        this.dataObjects = dataObjects;
        this.operationForMassDelete = operationForMassDelete;
        this.updatedAttributes = updatedAttributes;
        this.sourceAttribute = sourceAttribute;
    }

    public String getClassname()
    {
        return classname;
    }

    public byte getDatabaseOperation()
    {
        return databaseOperation;
    }

    public MithraDataObject[] getDataObjects()
    {
        return dataObjects;
    }

    public Operation getOperationForMassDelete()
    {
        return operationForMassDelete;
    }

    public Attribute[] getUpdatedAttributes()
    {
        return updatedAttributes;
    }

    public Object getSourceAttribute()
    {
        return sourceAttribute;
    }

    public void readObject(ObjectInput in) throws IOException, ClassNotFoundException
    {
        readHeader(in);
        this.operationForMassDelete = (Operation) in.readObject();
        int updatedAttributesSize = in.readInt();
        if(updatedAttributesSize > 0)
        {
            this.updatedAttributes = new Attribute[updatedAttributesSize];
            for(int i = 0; i < updatedAttributesSize; i++)
            {
                this.updatedAttributes[i] = (Attribute)in.readObject();
            }
        }

        int dataObjectsSize = in.readInt();
        if(dataObjectsSize > 0)
        {
            this.dataObjects = new MithraDataObject[dataObjectsSize];
            MithraDataObject dataObject;
            Class dataClass = MithraSerialUtil.getDataClassForFinder(classname);
            for(int i = 0; i < dataObjectsSize; i++)
            {
                dataObject = MithraSerialUtil.instantiateData(dataClass);
                dataObject.zDeserializePrimaryKey(in);
                this.dataObjects[i] = dataObject;
            }
        }
    }

    public void readHeader(ObjectInput in)
            throws ClassNotFoundException, IOException
    {
        this.classname = (String) in.readObject();
        this.databaseOperation = in.readByte();
        this.sourceAttribute = in.readObject();
    }

    public void writeObject(ObjectOutput out) throws IOException
    {
        writeHeader(out);
        out.writeObject(operationForMassDelete);

        int updatedAttributesSize = 0;
        if(updatedAttributes != null)
        {
            updatedAttributesSize = updatedAttributes.length;
        }
        out.writeInt(updatedAttributesSize);
        for(int i = 0; i < updatedAttributesSize; i++)
        {
            out.writeObject(updatedAttributes[i]);
        }

        int dataObjectsSize = 0;
        if(dataObjects != null)
        {
            dataObjectsSize = dataObjects.length;
        }

        out.writeInt(dataObjectsSize);
        for(int i = 0; i < dataObjectsSize; i++)
        {
            (dataObjects[i]).zSerializePrimaryKey(out);
        }
    }

    public void writeHeader(ObjectOutput out)
            throws IOException
    {
        out.writeObject(classname);
        out.writeByte(databaseOperation);
        out.writeObject(sourceAttribute);
    }
}
