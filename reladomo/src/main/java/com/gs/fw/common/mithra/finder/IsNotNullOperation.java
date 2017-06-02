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
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.sqcache.ExactMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.NoMatchSmr;
import com.gs.fw.common.mithra.finder.sqcache.ShapeMatchResult;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;



public class IsNotNullOperation  extends AbstractAtomicOperation
{

    public IsNotNullOperation(Attribute attribute)
    {
        super(attribute);
    }

    protected boolean isIndexed()
    {
        return false;
    }

    protected List getByIndex()
    {
        throw new RuntimeException("should not get here");
    }

    public List applyOperationToPartialCache()
    {
        // a nullable attribute is not likely to be uniquely indexable.
        return null;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        return !extractor.isAttributeNull(o);
    }

    public SingleColumnAttribute getSingleColumnAttribute()
    {
        return (SingleColumnAttribute) this.getAttribute();
    }

    public void generateSql(SqlQuery query)
    {
        query.appendWhereClause(this.getAttribute().getFullyQualifiedLeftHandExpression(query)+" IS NOT NULL");
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        // nothing to set
        return 0;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof IsNotNullOperation)
        {
            IsNotNullOperation other = (IsNotNullOperation) obj;
            return other.getAttribute() == this.getAttribute();
        }
        return false;
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        if (op instanceof IsNotNullOperation)
        {
            return zCombineWithNotNullOperation((IsNotNullOperation) op);
        }
        if (op instanceof IsNullOperation)
        {
            return zCombineWithNullOperation((IsNullOperation) op);
        }
        return op.zCombinedAnd(this);
    }

    private Operation zCombineWithNullOperation(IsNullOperation isNullOperation)
    {
        if (isNullOperation.getAttribute().equals(this.getAttribute()))
        {
            return new None(this.getAttribute());
        }
        return null;
    }

    private Operation zCombineWithNotNullOperation(IsNotNullOperation other)
    {
        if (other.getAttribute().equals(this.getAttribute()))
        {
            return this;
        }
        return null;
    }

    protected Operation zCombinedAndWithAtomic(AtomicOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            return op;
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return this.zCombinedAndWithAtomic(op);
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        if (op.operatesOnAttribute(this.getAttribute()))
        {
            return new None(this.getAttribute());
        }
        return null;
    }

    @Override
    public Operation zCombinedAndWithRange(RangeOperation op)
    {
        return this.zCombinedAndWithAtomic(op);
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        return this.zCombinedAndWithAtomic(op);
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("is not null");
    }

    @Override
    public ShapeMatchResult zShapeMatch(Operation existingOperation)
    {
        if (existingOperation.equals(this))
        {
            return ExactMatchSmr.INSTANCE;
        }
        return NoMatchSmr.INSTANCE;
    }

    @Override
    public int zShapeHash()
    {
        return this.hashCode();
    }
}
