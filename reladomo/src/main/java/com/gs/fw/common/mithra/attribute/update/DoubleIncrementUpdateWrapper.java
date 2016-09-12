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
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;



public class DoubleIncrementUpdateWrapper extends DoubleUpdateWrapper
{

    public DoubleIncrementUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, double newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public DoubleIncrementUpdateWrapper()
    {
    }

    public Object valueOf(Object anObject)
    {
        DoubleAttribute doubleAttribute = (DoubleAttribute)this.getAttribute();
        return new Double(this.getNewValue() + doubleAttribute.doubleValueOf(this.getDataToUpdate())); 
    }

    public double doubleValueOf(Object o)
    {
        DoubleAttribute doubleAttribute = (DoubleAttribute)this.getAttribute();
        return this.getNewValue() + doubleAttribute.doubleValueOf(this.getDataToUpdate());
    }

    public String getSetAttributeSql()
    {
        String columnName = ((SingleColumnAttribute)this.getAttribute()).getColumnName();
        return columnName + " = "+columnName + " + ? ";
    }

    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof DoubleIncrementUpdateWrapper && this.getNewValue() == ((DoubleIncrementUpdateWrapper)other).getNewValue();
    }

    public void updateData(MithraDataObject data)
    {
        DoubleAttribute doubleAttribute = (DoubleAttribute)this.getAttribute();
        doubleAttribute.setDoubleValue(data, this.getNewValue() + doubleAttribute.doubleValueOf(data));
    }

    @Override
    public boolean canBeMultiUpdated(MultiUpdateOperation multiUpdateOperation, MithraTransactionalObject mithraObject)
    {
        return !multiUpdateOperation.getIndexedObjects().contains(mithraObject);
    }

    public AttributeUpdateWrapper combineForSameAttribute(AttributeUpdateWrapper oldUpdate)
    {
        if (this.getAttribute().equals(oldUpdate.getAttribute()))
        {
            if (oldUpdate instanceof DoubleIncrementUpdateWrapper)
            {
                return new DoubleIncrementUpdateWrapper(
                        this.getAttribute(),
                        this.getDataToUpdate(),
                        this.getNewValue()+((DoubleIncrementUpdateWrapper)oldUpdate).getNewValue()
                );
            }
            // oldUpdate is a plain DoubleUpdateWrapper
            return new DoubleUpdateWrapper(
                    this.getAttribute(),
                    this.getDataToUpdate(),
                    ((DoubleUpdateWrapper) oldUpdate).getNewValue() + this.getNewValue());
        }
        return null;
    }
}
