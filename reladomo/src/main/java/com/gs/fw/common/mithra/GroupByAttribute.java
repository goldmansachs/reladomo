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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.util.*;

import java.sql.ResultSet;
import java.io.*;
import java.sql.SQLException;
import java.util.TimeZone;


public class GroupByAttribute implements Externalizable, MithraGroupByAttribute
{

    private static final long serialVersionUID = -3055754728807419870L;
    private Attribute attribute;
    public static final Nullable NULL_GROUPBY_VALUE = new MutableComparableReference()
    {
        public boolean isNull()
        {
            return true;
        }

        public boolean isInitialized()
        {
            return true;
        }

        public void checkForNull()
        {
            //do nothing;
        }

        public Object getAsObject()
        {
            return null;
        }

        public void setValueNull()
        {
            //do nothing
        }
    };

    public static final Nullable NULL_PRIMITIVE_GROUPBY_VALUE = new MutableNumber()
    {
        public boolean isNull()
        {
            return true;
        }

        public boolean isInitialized()
        {
            return true;
        }

        public void checkForNull()
        {
            throw new RuntimeException("groupBy attribute value is NULL. Must call isNull() first.");
        }

        public Object getAsObject()
        {
            return null;
        }

        public void setValueNull()
        {
            //do nothing
        }
    };
    public GroupByAttribute(Attribute attribute)
    {
        this.attribute = attribute;
    }

    public GroupByAttribute()
    {
    }

    public Attribute getAttribute()
    {
        return attribute;
    }

    public Object valueOf(Object o)
    {
        return this.getAttribute().valueOf(o);
    }

    public void populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object data, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException
    {
        this.getAttribute().zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, (AggregateData) data, databaseTimezone, dt);
    }

    public void populateAggregateDataValue(int pos, Object value, Object data)
    {
        this.getAttribute().zPopulateAggregateDataValue(pos, value, (AggregateData) data);
    }

    public boolean findDeepRelationshipInMemory(Operation op)
    {
        return this.getAttribute().zFindDeepRelationshipInMemory(op);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final GroupByAttribute that = (GroupByAttribute) o;

        return attribute.equals(that.attribute);
    }

    public int hashCode()
    {
        return attribute.hashCode();
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(attribute);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        attribute  = (Attribute) in.readObject();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return attribute.getTopLevelPortal();
    }

    public Nullable getNullGroupByAttribute()
    {
        if(this.attribute instanceof NonPrimitiveAttribute)
        {
            return NULL_GROUPBY_VALUE;
        }
        else
        {
            return NULL_PRIMITIVE_GROUPBY_VALUE;
        }
    }

    public void setValue(Object object, Object[] valueArray)
    {
        throw new RuntimeException("Not Implemented");
    }
}