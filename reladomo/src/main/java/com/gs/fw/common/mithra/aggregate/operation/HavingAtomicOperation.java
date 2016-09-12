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

package com.gs.fw.common.mithra.aggregate.operation;

import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.Map;
import java.util.Set;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class HavingAtomicOperation<T extends Comparable> extends AbstractHavingOperation implements SqlParameterSetter
{
    
    private MithraAggregateAttribute aggregateAttribute;
    private HavingFilter filter;
    private T value;

    private static int attributeCount = 0;

    public HavingAtomicOperation(MithraAggregateAttribute aggregateAttribute, T value, HavingFilter filter)
    {
        this.value = value;
        this.aggregateAttribute = aggregateAttribute;
        this.filter = filter;
    }

    public void zGenerateSql(AggregateSqlQuery query)
    {
        query.appendHavingClause(aggregateAttribute.getFullyQualifiedAggregateExpresion(query));
        query.appendHavingClause(filter.getFilterExpression());
        query.addSqlParameterSetter(this);
    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return aggregateAttribute.getTopLevelPortal();
    }

    public void zAddMissingAggregateAttributes(Set<MithraAggregateAttribute> aggregateAttributes, Map<String, MithraAggregateAttribute> nameToAggregateMap )
    {
        if(!aggregateAttributes.contains(aggregateAttribute))
        {
            nameToAggregateMap.put(createMissingAttributeName(),aggregateAttribute);
        }
    }

    public boolean zMatches(AggregateData aggregateData, Map<MithraAggregateAttribute, String> attributeToNameMap)
    {
        return filter.matches((Comparable) aggregateData.getAttributeAsObject(attributeToNameMap.get(aggregateAttribute)), this.value);
    }

    public void zGenerateMapperSql(AggregateSqlQuery aggregateSqlQuery)
    {
        this.aggregateAttribute.generateMapperSql(aggregateSqlQuery);
    }

    public Operation zCreateMappedOperation()
    {
        return this.aggregateAttribute.createMappedOperation();  
    }

    private String createMissingAttributeName()
    {
        int next = attributeCount++;
        return "A"+next+"$_$";
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        aggregateAttribute.setSqlParameter(pstmt, startIndex, value);
        return 1;
    }
}
