
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

package com.gs.fw.common.mithra.finder.asofop;

import com.gs.fw.common.mithra.attribute.TemporalAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.finder.timestamp.TimestampEqOperation;
import com.gs.fw.common.mithra.util.NullDataTimestamp;

import java.sql.Timestamp;

public class AsOfEqInfiniteNullOperation extends AsOfEqOperation
{

    public AsOfEqInfiniteNullOperation(AsOfAttribute attribute, Timestamp parameter)
    {
        super(attribute, parameter);
    }
    
    public AsOfEqInfiniteNullOperation()
    {
        // for externalizable
    }

    public AtomicOperation createAsOfOperationCopy(TemporalAttribute rightAttribute, Operation op)
    {
        if (rightAttribute.isAsOfAttribute())
        {
            return new AsOfEqInfiniteNullOperation((AsOfAttribute) rightAttribute, this.getParameter());
        }
        return new TimestampEqOperation((TimestampAttribute) rightAttribute, this.getParameter());
    }

    @Override
    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        if (this.getParameter().equals(NullDataTimestamp.getInstance()))
        {
            toStringContext.append("is null");
        }
        else
        {
            toStringContext.append("=").append(this.getParameter().toString());
        }
    }

    public void generateSql(SqlQuery query, ObjectWithMapperStack attributeWithStack, ObjectWithMapperStack asOfOperationWithStack)
    {
        query.restoreMapperStack(attributeWithStack);
        query.beginAnd();
        TemporalAttribute temporalAttribute = (TemporalAttribute) attributeWithStack.getObject();
        if (temporalAttribute.isAsOfAttribute())
        {
            AsOfAttribute attribute = (AsOfAttribute) temporalAttribute;
            if (this.getParameter().equals(NullDataTimestamp.getInstance()))
            {
                query.appendWhereClause(attribute.getFullyQualifiedToColumnName(query) + " is null");
            }
            else
            {
                query.addSqlParameterSetter(this);
                query.addSqlParameterSetter(this); // since we have two clauses
                if (attribute.isToIsInclusive())
                {
                    query.appendWhereClause(attribute.getFullyQualifiedFromColumnName(query) + " < ? and ");
                    query.appendWhereClause("("+attribute.getFullyQualifiedToColumnName(query));
                    query.appendWhereClause(" >= ? or "+attribute.getFullyQualifiedToColumnName(query)+" is null)");
                }
                else
                {
                    query.appendWhereClause(attribute.getFullyQualifiedFromColumnName(query) + " <= ? and ");
                    query.appendWhereClause("("+attribute.getFullyQualifiedToColumnName(query));
                    query.appendWhereClause(" > ? or "+attribute.getFullyQualifiedToColumnName(query)+" is null)");
                }
            }
        }
//        else
//        {
//            TimestampAttribute attribute = (TimestampAttribute) temporalAttribute;
//            query.addSqlParameterSetter(this);
//            query.appendWhereClause(attribute.getFullyQualifiedLeftHandExpression(query) + " = ?");
//        }
        query.endAnd();
    }

}
