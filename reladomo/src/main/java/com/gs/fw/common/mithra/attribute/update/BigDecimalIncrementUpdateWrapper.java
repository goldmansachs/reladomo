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
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;

import java.math.BigDecimal;



public class BigDecimalIncrementUpdateWrapper extends BigDecimalUpdateWrapper
{

    public BigDecimalIncrementUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate, BigDecimal newValue)
    {
        super(attribute, dataToUpdate, newValue);
    }

    public BigDecimalIncrementUpdateWrapper()
    {
    }

    public Object valueOf(Object anObject)
    {
        BigDecimalAttribute bigDecimalAttribute = (BigDecimalAttribute)this.getAttribute();
        return ((BigDecimal)this.getValue()).add(bigDecimalAttribute.bigDecimalValueOf(this.getDataToUpdate()));
    }

    public BigDecimal bigDecimalValueOf(Object o)
    {
        BigDecimalAttribute bigDecimalAttribute = (BigDecimalAttribute)this.getAttribute();
        return ((BigDecimal)this.getValue()).add(bigDecimalAttribute.bigDecimalValueOf(this.getDataToUpdate()));
    }

    public String getSetAttributeSql()
    {
        String columnName = this.getAttribute().getColumnName();
        return columnName + " = "+columnName + " + ? ";
    }

    public boolean hasSameParameter(AttributeUpdateWrapper other)
    {
        return other instanceof BigDecimalIncrementUpdateWrapper && this.getValue() == ((BigDecimalIncrementUpdateWrapper)other).getValue();
    }

    public void updateData(MithraDataObject data)
    {
        BigDecimalAttribute bigDecimalAttribute = (BigDecimalAttribute)this.getAttribute();
        bigDecimalAttribute.setBigDecimalValue(data,((BigDecimal)this.getValue()).add(bigDecimalAttribute.bigDecimalValueOf(data)));
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
            if (oldUpdate instanceof BigDecimalIncrementUpdateWrapper)
            {
                return new BigDecimalIncrementUpdateWrapper(
                        this.getAttribute(),
                        this.getDataToUpdate(),
                        ((BigDecimal) this.getValue()).add((BigDecimal) ((BigDecimalIncrementUpdateWrapper) oldUpdate).getValue())
                );
            }
            // oldUpdate is a plain BigDecimalUpdateWrapper
            return new BigDecimalUpdateWrapper(
                    this.getAttribute(),
                    this.getDataToUpdate(),
                    ((BigDecimal)((BigDecimalUpdateWrapper) oldUpdate).getValue()).add((BigDecimal)this.getValue())
            );
        }
        return null;
    }
}
