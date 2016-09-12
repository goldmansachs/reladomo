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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.HavingOperation;

import java.util.Map;



public class HavingAndOperation extends AbstractBinaryHavingOperation
{
    public HavingAndOperation(HavingOperation left, com.gs.fw.common.mithra.HavingOperation right)
    {
        super(left, right);

    }

    public String getOperationName()
    {
        return " and ";
    }

    protected void addOperand(HavingOperation operand)
    {
        if (operand instanceof HavingAndOperation)
        {
            operands.addAll(((AbstractBinaryHavingOperation) operand).getOperands());
        }
        else
        {
            operands.add(operand);
        }
    }

    public boolean zMatches(AggregateData aggregateData, Map<MithraAggregateAttribute, String> attributeToNameMap)
    {
        for (int i = 0; i < operands.size(); i++)
        {
            HavingOperation op = ((HavingOperation) operands.get(i));
            if (!op.zMatches(aggregateData, attributeToNameMap)) return false;
        }
        return true;
    }




}
