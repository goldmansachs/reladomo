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

package com.gs.fw.common.mithra.finder.integer;

import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.finder.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class IntegerNotEqOperation extends AtomicNotEqualityOperation implements SqlParameterSetter, NegatableOperation
{

    private int parameter;

    public IntegerNotEqOperation(IntegerAttribute attribute, int parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    public Operation zNegate()
    {
        return new IntegerEqOperation((IntegerAttribute) this.getAttribute(), this.parameter);
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        IntegerAttribute integerAttribute = (IntegerAttribute)this.getAttribute();
        if (integerAttribute.isAttributeNull(o)) return false;
        return integerAttribute.intValueOf(o) != parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setInt(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ this.parameter);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof IntegerNotEqOperation)
        {
            IntegerNotEqOperation other = (IntegerNotEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    public Object getParameterAsObject()
    {
        return Integer.valueOf(this.parameter);
    }

    public int getParameter()
    {
        return parameter;
    }
}
