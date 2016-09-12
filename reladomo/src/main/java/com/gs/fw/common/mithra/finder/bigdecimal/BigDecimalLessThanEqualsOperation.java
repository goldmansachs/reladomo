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

package com.gs.fw.common.mithra.finder.bigdecimal;


import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.NonPrimitiveGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.NonPrimitiveLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.SqlQuery;

public class BigDecimalLessThanEqualsOperation extends NonPrimitiveLessThanEqualsOperation
{
    public BigDecimalLessThanEqualsOperation()
    {
        super();
    }

    public BigDecimalLessThanEqualsOperation(Attribute attribute, Comparable parameter)
    {
        super(attribute, parameter);
    }

    @Override
    public void generateSql(SqlQuery query)
    {
        if (query.getDatabaseType().useBigDecimalValuesInRangeOperations())
        {
            query.appendWhereClause(this.getAttribute().getFullyQualifiedLeftHandExpression(query));
            query.appendWhereClause("<=");
            query.appendWhereClause(this.getParameter().toString());
        }
        else
        {
            super.generateSql(query);
        }
    }
}
