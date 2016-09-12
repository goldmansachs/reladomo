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

import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.*;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;


public abstract class AggregateAttribute<Owner> implements MithraAggregateAttribute, Externalizable
{
    private static final long serialVersionUID = -2010805891475659853L;
    private AggregateAttributeCalculator calculator;

    public AggregateAttribute(AggregateAttributeCalculator calculator)
    {
        this.calculator = calculator;
    }

    public AggregateAttribute()
    {
        //for externalizable
    }

    public AggregateAttributeCalculator getCalculator()
    {
        return this.calculator;
    }

    public Object aggregate(Object resultSoFar, Object newValue)
    {
        return this.getCalculator().aggregate(resultSoFar, newValue );
    }

    public String getFullyQualifiedAggregateExpresion(SqlQuery query)
    {
        return this.getCalculator().getFullyQualifiedCalculatedExpression(query);
    }

    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object data, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException
    {
        return this.getCalculator().populateValueFromResultSet(resultSetPosition, dataPosition, rs, (AggregateData)data, databaseTimezone, dt);
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return this.getCalculator().zGetTopLevelPortal();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.getCalculator().getOwnerPortal();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.getCalculator().generateMapperSql(query);
    }

    public Operation createMappedOperation()
    {
        return this.getCalculator().createMappedOperation();
    }

    public boolean findDeepRelationshipInMemory(Operation op)
    {
        return this.getCalculator().findDeepRelationshipInMemory(op);
    }

    public Nullable getDefaultValueForEmptyGroup()
    {
        return this.getCalculator().getDefaultValueForEmptyGroup();
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(calculator);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        calculator = (AggregateAttributeCalculator) in.readObject();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof AggregateAttribute)) return false;

        AggregateAttribute that = (AggregateAttribute) o;
        return calculator.equals(that.calculator);

    }

    public int hashCode()
    {
        return calculator.hashCode();
    }

    public Class valueType()
    {
        return this.getCalculator().valueType();
    }

     public void setValue(Object instance, Object[] valueArray)
    {
        throw new RuntimeException("Not Implemented");
    }

}
