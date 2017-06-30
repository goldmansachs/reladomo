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

package com.gs.fw.common.mithra.finder.doubleop;

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.paramop.OpWithDoubleParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithDoubleParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;



public class DoubleLessThanEqualsOperation extends LessThanEqualsOperation implements OpWithDoubleParam
{

    private double parameter;

    public DoubleLessThanEqualsOperation(Attribute attribute, double parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public double getParameter()
    {
        return parameter;
    }

    @Override
    public Extractor getStaticExtractor()
    {
        return OpWithDoubleParamExtractor.INSTANCE;
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("<=").append(this.parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setDouble(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter) ^ 0x0A0A0A;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof DoubleLessThanEqualsOperation)
        {
            DoubleLessThanEqualsOperation other = (DoubleLessThanEqualsOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object holder, Extractor extractor)
    {
        return !extractor.isAttributeNull(holder) && ((DoubleExtractor)extractor).doubleValueOf(holder) <= this.getParameter();
    }
}
