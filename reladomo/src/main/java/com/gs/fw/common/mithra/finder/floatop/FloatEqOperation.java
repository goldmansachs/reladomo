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
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public class FloatEqOperation  extends AtomicEqualityOperation implements SqlParameterSetter
{
    private float parameter;

    public FloatEqOperation(FloatAttribute attribute, float parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        FloatAttribute floatAttribute = (FloatAttribute)this.getAttribute();
        if (floatAttribute.isAttributeNull(o)) return false;
        return floatAttribute.floatValueOf(o) == parameter;
    }

    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setFloat(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof FloatEqOperation)
        {
            FloatEqOperation other = (FloatEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    public int getParameterHashCode()
    {
        return HashUtil.hash(this.parameter);
    }

    public Object getParameterAsObject()
    {
        return new Float(this.parameter);
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        if (extractor.isAttributeNull(other)) return false;
        return ((FloatExtractor) extractor).floatValueOf(other) == this.parameter;
    }

    public float getParameter()
    {
        return parameter;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements FloatExtractor
    {
        public float floatValueOf(Object o)
        {
            return getParameter();
        }

        public Object valueOf(Object o)
        {
            return new Float(this.floatValueOf(o));
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.floatValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.floatValueOf(first) == this.floatValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((FloatExtractor) secondExtractor).floatValueOf(second) == this.floatValueOf(first);
        }

    }
}
