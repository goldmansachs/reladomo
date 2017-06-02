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

import com.gs.fw.common.mithra.attribute.FloatAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.FloatExtractor;
import com.gs.fw.common.mithra.finder.AtomicNotEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.paramop.OpWithFloatParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithFloatParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FloatNotEqOperation  extends AtomicNotEqualityOperation implements SqlParameterSetter, OpWithFloatParam
{
    private float parameter;

    public FloatNotEqOperation(FloatAttribute attribute, float parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithFloatParamExtractor.INSTANCE;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        FloatExtractor floatAttribute = (FloatExtractor) extractor;
        if (floatAttribute.isAttributeNull(o)) return false;
        return floatAttribute.floatValueOf(o) != parameter;
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setFloat(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return ~(this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter));
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof FloatNotEqOperation)
        {
            FloatNotEqOperation other = (FloatNotEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    public Object getParameterAsObject()
    {
        return new Float(this.parameter);
    }

    public float getParameter()
    {
        return parameter;
    }
}
