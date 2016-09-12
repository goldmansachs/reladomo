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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.PersisterId;

import java.util.List;



public abstract class NotInOperation extends AtomicSetBasedOperation
{

    public NotInOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected NotInOperation()
    {
        // for Externalizable
    }

    @Override
    protected boolean isNot()
    {
        return true;
    }

    public boolean usesUniqueIndex()
    {
        return false;
    }

    public boolean usesImmutableUniqueIndex()
    {
        return false;
    }

    public boolean usesNonUniqueIndex()
    {
        return false;
    }

    @Override
    public int zEstimateReturnSize()
    {
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    @Override
    public int zEstimateMaxReturnSize()
    {
        return this.getResultObjectPortal().getCache().estimateQuerySize();
    }

    public int getIndexRef()
    {
        return -1;
    }

    protected List getByIndex()
    {
        throw new RuntimeException("should never get here");
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        // todo: rezaem: implement zCombineWithNotInOperation
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        // todo: combine in with equality; need to implement clone first.
        return null;
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return null; // not worth combining
    }

    public List applyOperationToPartialCache()
    {
        if (this.usesUniqueIndex())
        {
            List result = this.getByIndex();
            if (result.size() == this.getSetSize()) return result;
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return null;
    }

    public int getClauseCount(SqlQuery query)
    {
        return this.getSetSize();
    }

    public boolean maySplit()
    {
        return false;
    }

    public void generateTupleTempContextJoinSql(SqlQuery sqlQuery, TupleTempContext tempContext, Object source, PersisterId persisterId, int position, boolean inOrClause)
    {
        String fullyQualifiedColumnName = this.getAttribute().getFullyQualifiedLeftHandExpression(sqlQuery);
        String tempTableName = tempContext.getFullyQualifiedTableName(source, persisterId);
        sqlQuery.addTupleTempContextJoin(tempTableName, false,
                fullyQualifiedColumnName + " = "+ tempTableName +".c0",
                fullyQualifiedColumnName + " not in ( )", tempTableName +".c0 is null", position);
    }
}
