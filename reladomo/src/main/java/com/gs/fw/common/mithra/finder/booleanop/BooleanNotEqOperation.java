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

package com.gs.fw.common.mithra.finder.booleanop;

import com.gs.fw.common.mithra.attribute.BooleanAttribute;
import com.gs.fw.common.mithra.finder.AtomicNotEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class BooleanNotEqOperation extends AtomicNotEqualityOperation implements SqlParameterSetter
{

    private boolean parameter;

    public BooleanNotEqOperation(BooleanAttribute attribute, boolean parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        BooleanAttribute attribute = (BooleanAttribute)this.getAttribute();
        if (attribute.isAttributeNull(o)) return false;
        return attribute.booleanValueOf(o) != parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setBoolean(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter));
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof BooleanNotEqOperation)
        {
            BooleanNotEqOperation other = (BooleanNotEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    public boolean getParameter()
    {
        return parameter;
    }

    public Object getParameterAsObject()
    {
        return Boolean.valueOf(this.parameter);
    }
}
