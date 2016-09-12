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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.UpdateInfo;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimeZone;



public abstract class AttributeUpdateWrapper implements Extractor, Serializable, UpdateInfo
{

    private Attribute attribute;
    private transient MithraDataObject dataToUpdate;

    protected AttributeUpdateWrapper(Attribute attribute, MithraDataObject dataToUpdate)
    {
        this.attribute = attribute;
        this.dataToUpdate = dataToUpdate;
    }

    public AttributeUpdateWrapper()
    {
    }

    public void setAttribute(Attribute attribute)
    {
        this.attribute = attribute;
    }

    public Attribute getAttribute()
    {
        return attribute;
    }

    // this is called after the data is already changed
    public Object getUpdatedValue()
    {
        return attribute.valueOf(dataToUpdate);
    }

    public MithraDataObject getDataToUpdate()
    {
        return dataToUpdate;
    }

    public void setDataToUpdate(MithraDataObject dataToUpdate)
    {
        this.dataToUpdate = dataToUpdate;
    }

    public void incrementUpdateCount()
    {
        attribute.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        attribute.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        attribute.rollbackUpdateCount();
    }

    public void updateData()
    {
        this.updateData(this.dataToUpdate);
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return this.attribute.getOwnerPortal();
    }

    public String getSetAttributeSql()
    {
        return ((SingleColumnAttribute) this.attribute).getColumnName() + " = ? ";
    }

    public abstract boolean hasSameParameter(AttributeUpdateWrapper other);

    public abstract int setSqlParameters(PreparedStatement ps, int index, TimeZone timeZone, DatabaseType databaseType) throws SQLException;

    public abstract int getNewValueHashCode();

    public abstract void updateData(MithraDataObject data);

    public boolean valueEquals(Object first, Object second)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public void setValue(Object o, Object newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(Object o, Object newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNullUntil(Object o, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public AttributeUpdateWrapper combineForSameAttribute(AttributeUpdateWrapper oldUpdate)
    {
        if (this.getAttribute().equals(oldUpdate.getAttribute()))
        {
            return this;
        }
        return null;
    }

    public abstract void externalizeParameter(ObjectOutput out) throws IOException;

    public abstract void readParameter(ObjectInput in) throws IOException, ClassNotFoundException;

    public boolean canBeMultiUpdated(MultiUpdateOperation multiUpdateOperation, MithraTransactionalObject mithraObject)
    {
        return true;
    }
}
