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

package com.gs.fw.common.mithra.finder.string;

import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.WildcardParser;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;



public abstract class StringNotLikeOperation extends AbstractAtomicOperation
{

    private String parameter;

    protected StringNotLikeOperation(StringAttribute attribute, String parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public int getIndexRef()
    {
        return -1; // these operations can't be resolved in our simple indices
    }

    protected List getByIndex()
    {
        throw new RuntimeException("should never get here");
    }

    protected String getParameter()
    {
        return parameter;
    }

    public List applyOperationToPartialCache()
    {
        return null;
    }

    public int hashCode()
    {
        return ~(this.getClass().hashCode() ^ this.getAttribute().hashCode() ^ this.parameter.hashCode());
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof StringNotLikeOperation && obj.getClass() == this.getClass())
        {
            StringNotLikeOperation other = (StringNotLikeOperation) obj;
            return this.parameter.equals(other.parameter) && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    /*
    returns the combined and operation. Many operations must be combined to correctly resolve a query.
    Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    public Operation zCombinedAnd(Operation op)
    {
        // todo: rezaem: implement zCombinedAnd
//        if (!(op instanceof StringLikeOperation)) return op.zCombinedAnd(this);
        return null;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        // todo: rezaem: implements combine with atomic
        return null;
    }

    public Operation zCombinedAndWithMultiEquality(MultiEqualityOperation op)
    {
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

    public void generateSql(SqlQuery query)
    {
        String escape = "";
        if (!query.getDatabaseType().escapeLikeMetaChars(this.getParameter()).equals(this.getParameter()))
        {
            escape = " escape '='";
        }
        query.appendWhereClause( this.getAttribute().getFullyQualifiedLeftHandExpression(query)+" not like ?"+escape);
        query.addSqlParameterSetter(this);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setString(startIndex, this.getLikeParameter(query));
        return 1;
    }

    protected abstract String getLikeParameter(SqlQuery sqlQuery);
}
