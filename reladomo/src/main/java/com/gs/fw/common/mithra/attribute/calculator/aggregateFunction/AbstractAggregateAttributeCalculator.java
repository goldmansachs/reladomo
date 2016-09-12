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

package com.gs.fw.common.mithra.attribute.calculator.aggregateFunction;

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.util.Nullable;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;

import java.io.*;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;


public abstract class AbstractAggregateAttributeCalculator implements AggregateAttributeCalculator
{
    private String sqlFunction;

    protected AbstractAggregateAttributeCalculator(String sqlFunction)
    {
        this.sqlFunction = sqlFunction;
    }

    public AbstractAggregateAttributeCalculator()
    {
    }

    public abstract Attribute getAttribute();

    public MithraObjectPortal getOwnerPortal()
    {
        return this.getAttribute().getOwnerPortal();
    }

    public MithraObjectPortal zGetTopLevelPortal()
    {
        return getAttribute().getTopLevelPortal();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.getAttribute().generateMapperSql(query);
    }

    public Operation createMappedOperation()
    {
        return this.getAttribute().zCreateMappedOperation();
    }

    public boolean findDeepRelationshipInMemory(Operation op)
    {
        return this.getAttribute().zFindDeepRelationshipInMemory(op);
    }

    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt) throws SQLException
    {
        this.getAttribute().zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, data, databaseTimezone, dt);
        return 1;
    }

    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException
    {
        this.getAttribute().zPopulateValueFromResultSet(resultSetPosition, dataPosition, rs, object, method, databaseTimezone, dt, scratchArray);
        return 1;
    }

    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        this.getAttribute().serializeNonNullAggregateDataValue(valueWrappedInNullable, out);
    }

    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return this.getAttribute().deserializeNonNullAggregateDataValue(in);
    }

    public boolean executeForNull(Object context)
    {
        ((Nullable)context).setValueNull();
        return false;
    }

    public String getFullyQualifiedCalculatedExpression(SqlQuery query)
    {
        return this.sqlFunction+"("+this.getAttribute().getFullyQualifiedLeftHandExpression(query)+")";
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(sqlFunction);
    }

    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException
    {
        sqlFunction = in.readUTF();
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode();
    }

    public abstract Class valueType();

}