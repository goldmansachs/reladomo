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
import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class NonPrimitiveGreaterThanEqualsOperation extends GreaterThanEqualsOperation
{

    private Comparable parameter;

    public NonPrimitiveGreaterThanEqualsOperation(Attribute attribute, Comparable parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public NonPrimitiveGreaterThanEqualsOperation()
    {
        // for externalizable
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append(">=").append("\"" + this.parameter.toString() + "\"");
    }

    public Comparable getParameter()
    {
        return parameter;
    }

    protected void setParameter(Comparable parameter)
    {
        this.parameter = parameter;
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        Object incoming = ((NonPrimitiveAttribute)this.getAttribute()).valueOf(o);
        if (incoming == null) return false;
        return parameter.compareTo(incoming) <= 0;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        ((NonPrimitiveAttribute)this.getAttribute()).setSqlParameter(startIndex, pstmt, parameter, query.getTimeZone(), query.getDatabaseType());
        return 1;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            NonPrimitiveEqOperation ieo = (NonPrimitiveEqOperation) op;
            if (!ieo.zIsNullOperation() && parameter.compareTo(ieo.getParameterAsObject()) <= 0 )
            {
                return op;
            }
            return new None(this.getAttribute());
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThan(GreaterThanOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            Object target = ((NonPrimitiveGreaterThanOperation) op).getParameter();
            if (this.parameter.compareTo(target) <= 0) return op;
            return this;
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            Object target = ((NonPrimitiveGreaterThanEqualsOperation) op).getParameter();
            if (this.parameter.compareTo(target) <= 0) return op;
            return this;
        }
        return null;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter.hashCode() ^ 0xF0F0F0;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof NonPrimitiveGreaterThanEqualsOperation)
        {
            NonPrimitiveGreaterThanEqualsOperation other = (NonPrimitiveGreaterThanEqualsOperation) obj;
            return this.parameter.equals(other.parameter) && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

}
