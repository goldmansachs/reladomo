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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;



public class AtomicSelfEqualityOperation extends AbstractAtomicOperation
{

    private Attribute rightAttribute;

    public AtomicSelfEqualityOperation(Attribute left, Attribute right)
    {
        super(left);
        this.rightAttribute = right;
    }

    public int getIndexRef()
    {
        return -1; // can't use index
    }

    @Override
    public void zRegisterEqualitiesAndAtomicOperations(TransitivePropagator transitivePropagator)
    {
        //nothing to do
    }

    protected List getByIndex()
    {
        throw new RuntimeException("should never be called");
    }

    public Operation zCombinedAnd(Operation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithMapped(MappedOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        // todo: rezaem: implement combine
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        // todo: rezaem: implement combine
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        // todo: rezaem: implement combine
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThanEquals(LessThanEqualsOperation op)
    {
        // todo: rezaem: implement combine
        return null;
    }

    public Operation zCombinedAndWithIn(InOperation op)
    {
        // todo: rezaem: implement combine
        return null;
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        super.addDepenedentAttributesToSet(set);
        set.add(this.getRightAttribute());
    }

    public Attribute getRightAttribute()
    {
        return rightAttribute;
    }

    public void generateSql(SqlQuery query)
    {
        query.appendWhereClause( this.getAttribute().getFullyQualifiedLeftHandExpression(query)
                + " = " +  this.getRightAttribute().getFullyQualifiedLeftHandExpression(query));
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        throw new RuntimeException("should never get called");
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        return this.getAttribute().valueEquals(o, o, this.getRightAttribute());
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.getRightAttribute().hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof AtomicSelfEqualityOperation)
        {
            AtomicSelfEqualityOperation other = (AtomicSelfEqualityOperation) obj;
            return this.getAttribute().equals(other.getAttribute()) && this.getRightAttribute().equals(other.getRightAttribute());
        }
        return false;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("=");
        this.rightAttribute.zAppendToString(toStringContext);
    }
}
