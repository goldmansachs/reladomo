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
import com.gs.fw.common.mithra.finder.AtomicNotEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class LongNotEqOperation extends AtomicNotEqualityOperation implements SqlParameterSetter
{

    private long parameter;

    public LongNotEqOperation(Attribute attribute, long parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        LongAttribute longAttribute = (LongAttribute)this.getAttribute();
        if (longAttribute.isAttributeNull(o)) return false;
        return longAttribute.longValueOf(o) != parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setLong(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter));
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof LongNotEqOperation)
        {
            LongNotEqOperation other = (LongNotEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    public Object getParameterAsObject()
    {
        return Long.valueOf(this.parameter);
    }

    public long getParameter()
    {
        return parameter;
    }
}
