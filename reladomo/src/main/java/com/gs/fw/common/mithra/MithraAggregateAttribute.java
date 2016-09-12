
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

public interface MithraAggregateAttribute extends com.gs.fw.finder.AggregateAttribute
{

    public String getFullyQualifiedAggregateExpresion(SqlQuery query);

    public int populateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object data, TimeZone databaseTimezone, DatabaseType dt, Object[] scratchArray) throws SQLException;

    public MithraObjectPortal getOwnerPortal();

    public AggregateAttributeCalculator getCalculator();

    public Object aggregate(Object resultSoFar, Object newValue);

    public void generateMapperSql(AggregateSqlQuery query);

    public boolean findDeepRelationshipInMemory(Operation op);

    public Nullable getDefaultValueForEmptyGroup();

    public Operation createMappedOperation();

    public void setSqlParameter(PreparedStatement pstmt, int startIndex, Object value) throws SQLException;

    public MithraObjectPortal getTopLevelPortal();

    public Class valueType();

    public void setValue(Object instance, Object[] valueArray);

}
