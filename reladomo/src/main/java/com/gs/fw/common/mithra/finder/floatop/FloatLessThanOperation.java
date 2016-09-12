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

package com.gs.fw.common.mithra.finder.floatop;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.FloatAttribute;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class FloatLessThanOperation extends LessThanOperation
{

    private float parameter;

    public FloatLessThanOperation(Attribute attribute, float parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public float getParameter()
    {
        return parameter;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("<").append(this.parameter);
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        FloatAttribute integerAttribute = (FloatAttribute)this.getAttribute();
        if (integerAttribute.isAttributeNull(o)) return false;
        return integerAttribute.floatValueOf(o) < parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setFloat(startIndex, parameter);
        return 1;
    }

    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            if (!op.zIsNullOperation() && ((FloatEqOperation) op).getParameter() < this.parameter)
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
            float target = ((FloatGreaterThanOperation) op).getParameter();
            if (target >= this.parameter) return new None(this.getAttribute());
            return null;
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicGreaterThanEquals(GreaterThanEqualsOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            float target = ((FloatGreaterThanEqualsOperation) op).getParameter();
            if (target >= this.parameter) return new None(this.getAttribute());
            return null;
        }
        return null;
    }

    public Operation zCombinedAndWithAtomicLessThan(LessThanOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            float target = ((FloatLessThanOperation) op).getParameter();
            if (target < this.parameter) return op;
            return this;
        }
        return null;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter) ^ 0xA0A0A0;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof FloatLessThanOperation)
        {
            FloatLessThanOperation other = (FloatLessThanOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

}
