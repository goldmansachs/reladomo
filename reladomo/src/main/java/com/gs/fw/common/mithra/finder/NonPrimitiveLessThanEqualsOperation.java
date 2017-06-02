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
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.paramop.OpWithBigDecimalParamExtractor;
import com.gs.fw.common.mithra.finder.paramop.OpWithObjectParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithStringParamExtractor;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class NonPrimitiveLessThanEqualsOperation extends LessThanEqualsOperation implements OpWithObjectParam
{

    private Comparable parameter;

    public NonPrimitiveLessThanEqualsOperation(Attribute attribute, Comparable parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public NonPrimitiveLessThanEqualsOperation()
    {
        // for externalizable
    }

    @Override
    public Extractor getStaticExtractor()
    {
        if (this.parameter instanceof String)
        {
            return OpWithStringParamExtractor.INSTANCE;
        }
        return OpWithBigDecimalParamExtractor.INSTANCE;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("<=").append("\"" + this.parameter.toString() + "\"");
    }

    public Comparable getParameter()
    {
        return parameter;
    }

    protected void setParameter(Comparable parameter)
    {
        this.parameter = parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        ((NonPrimitiveAttribute)this.getAttribute()).setSqlParameter(startIndex, pstmt, parameter, query.getTimeZone(), query.getDatabaseType());
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter.hashCode() ^ 0x0A0A0A;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof NonPrimitiveLessThanEqualsOperation)
        {
            NonPrimitiveLessThanEqualsOperation other = (NonPrimitiveLessThanEqualsOperation) obj;
            return this.parameter.equals(other.parameter) && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object holder, Extractor extractor)
    {
        Object incoming = extractor.valueOf(holder);
        if (incoming == null) return false;
        return parameter.compareTo(incoming) >= 0;
    }
}
