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

package com.gs.fw.common.mithra.finder.longop;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class LongGreaterThanOperation extends GreaterThanOperation
{

    private long parameter;

    public LongGreaterThanOperation(Attribute attribute, long parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public long getParameter()
    {
        return parameter;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append(">").append(this.parameter);
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        LongAttribute LongAttribute = (LongAttribute)this.getAttribute();
        if (LongAttribute.isAttributeNull(o)) return false;
        return LongAttribute.longValueOf(o) > parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setLong(startIndex, parameter);
        return 1;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            if (!op.zIsNullOperation() && ((LongEqOperation) op).getParameter() > this.parameter)
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
            long target = ((LongGreaterThanOperation) op).getParameter();
            if (target > this.parameter) return op;
            return this;
        }
        return null;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter) ^ 0xF0F0F0;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof LongGreaterThanOperation)
        {
            LongGreaterThanOperation other = (LongGreaterThanOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

}
