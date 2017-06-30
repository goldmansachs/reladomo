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

import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.AtomicNotEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.paramop.OpWithDoubleParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithDoubleParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DoubleNotEqOperation  extends AtomicNotEqualityOperation implements SqlParameterSetter, OpWithDoubleParam
{
    private double parameter;

    public DoubleNotEqOperation(DoubleAttribute attribute, double parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithDoubleParamExtractor.INSTANCE;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        DoubleExtractor doubleAttribute = (DoubleExtractor) extractor;
        if (doubleAttribute.isAttributeNull(o)) return false;
        return doubleAttribute.doubleValueOf(o) != parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setDouble(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter));
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof DoubleNotEqOperation)
        {
            DoubleNotEqOperation other = (DoubleNotEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    public Object getParameterAsObject()
    {
        return new Double(this.parameter);
    }

    public double getParameter()
    {
        return parameter;
    }
}
