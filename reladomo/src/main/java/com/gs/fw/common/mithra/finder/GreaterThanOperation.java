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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.attribute.Attribute;



public abstract class GreaterThanOperation extends RangeOperation
{

    protected GreaterThanOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected GreaterThanOperation()
    {
        // for externalizable
    }

    public void generateSql(SqlQuery query)
    {
        query.appendWhereClause(this.getAttribute().getFullyQualifiedLeftHandExpression(query)+" > ?");
        query.addSqlParameterSetter(this);
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        return op.zCombinedAndWithAtomicGreaterThan(this);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return op.zCombinedAndWithAtomicGreaterThan(this);
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return op.zCombinedAndWithAtomicGreaterThan(this);
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return op.zCombinedAndWithAtomicGreaterThan(this);
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return op.zCombinedAndWithAtomicGreaterThan(this);
    }
}
