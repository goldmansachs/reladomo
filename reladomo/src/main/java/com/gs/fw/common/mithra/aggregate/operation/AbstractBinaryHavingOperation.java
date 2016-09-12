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

import com.gs.fw.common.mithra.finder.AggregateSqlQuery;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.NoOperation;
import com.gs.fw.common.mithra.util.InternalList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.HavingOperation;

import java.util.Map;
import java.util.Set;



public abstract class AbstractBinaryHavingOperation extends AbstractHavingOperation
{
    protected InternalList operands;

    public AbstractBinaryHavingOperation(HavingOperation left, HavingOperation right)
    {
        this.operands = new InternalList(4);
        this.addOperand(left);
        this.addOperand(right);
    }

    protected abstract String getOperationName();

    protected abstract void addOperand(HavingOperation operand);

    protected InternalList getOperands()
    {
        return operands;
    }

    public void zGenerateSql(AggregateSqlQuery query)
    {
        boolean wroteSomething = false;
        for (int i = 0; i < operands.size(); i++)
        {
            com.gs.fw.common.mithra.HavingOperation op = ((HavingOperation) operands.get(i));
            if (!wroteSomething)
            {
                query.appendHavingClause("(");
                wroteSomething = true;
            }
            else
            {
                query.appendHavingClause(getOperationName());
            }

            op.zGenerateSql(query);
        }
        if (wroteSomething) query.appendHavingClause(")");
    }

    public void zGenerateMapperSql(AggregateSqlQuery query)
    {
        for (int i = 0; i < operands.size(); i++)
        {
            HavingOperation op = ((HavingOperation) operands.get(i));
            op.zGenerateMapperSql(query);
        }

    }

    public MithraObjectPortal getResultObjectPortal()
    {
        return ((HavingOperation) operands.get(0)).getResultObjectPortal();
    }

    public void zAddMissingAggregateAttributes(Set<MithraAggregateAttribute> aggregateAttributes, Map<String, MithraAggregateAttribute> nameToAttributeMap)
    {
        for(int i = 0; i < operands.size(); i++)
        {
            HavingOperation op = (HavingOperation) operands.get(i);
            op.zAddMissingAggregateAttributes(aggregateAttributes, nameToAttributeMap);
        }
    }


    public Operation zCreateMappedOperation()
    {
        Operation mappedOperation = NoOperation.instance();
        for(int i = 0; i < operands.size(); i++)
        {
            HavingOperation op = (HavingOperation) operands.get(i);
            mappedOperation.and(op.zCreateMappedOperation());
        }
        return mappedOperation;
    }

}
