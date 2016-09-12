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

package com.gs.fw.common.mithra.attribute.update;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.util.HashUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;



public abstract class ObjectUpdateWrapper extends AttributeUpdateWrapper
{

    private Object newValue;

    protected ObjectUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, Object newValue)
    {
        super(attribute, dataToUpdate);
        this.newValue = newValue;
    }

    public ObjectUpdateWrapper()
    {
    }

    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        ObjectUpdateWrapper objectUpdateWrapper = (ObjectUpdateWrapper) other;
        if (this.newValue == null)
        {
            return objectUpdateWrapper.newValue == null;
        }
        return this.newValue.equals(objectUpdateWrapper.newValue);
    }

    public Object getValue()
    {
        return newValue;
    }

    protected void setNewValue(Object newValue)
    {
        this.newValue = newValue;
    }

    public int getNewValueHashCode()
    {
        if (newValue == null) return HashUtil.NULL_HASH;
        return newValue.hashCode();
    }

    public int valueHashCode(Object o)
    {
        return this.getNewValueHashCode();
    }

    public boolean isAttributeNull(Object o)
    {
        return this.newValue == null;
    }

    public Object valueOf(Object anObject)
    {
        return this.newValue;
    }

    public void updateData(MithraDataObject data)
    {
        this.getAttribute().setValue(data, this.newValue);
    }

    public void externalizeParameter(ObjectOutput out) throws IOException
    {
        out.writeObject(newValue);
    }

    public void readParameter(ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.newValue = in.readObject();
    }
}
