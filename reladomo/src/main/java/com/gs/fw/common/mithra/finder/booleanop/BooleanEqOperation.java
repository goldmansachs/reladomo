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
import com.gs.fw.common.mithra.extractor.BooleanExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.paramop.OpWithBooleanParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithBooleanParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;



public class BooleanEqOperation extends AtomicEqualityOperation implements SqlParameterSetter, OpWithBooleanParam
{

    private boolean parameter;

    public BooleanEqOperation(BooleanAttribute attribute, boolean parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithBooleanParamExtractor.INSTANCE;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        BooleanExtractor attribute = (BooleanExtractor) extractor;
        if (attribute.isAttributeNull(o)) return false;
        return attribute.booleanValueOf(o) == parameter;
    }

    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setBoolean(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof BooleanEqOperation)
        {
            BooleanEqOperation other = (BooleanEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    public boolean getParameter()
    {
        return parameter;
    }

    @Override
    public int getParameterHashCode()
    {
        return HashUtil.hash(this.parameter);
    }

    public Object getParameterAsObject()
    {
        return Boolean.valueOf(this.parameter);
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        if (extractor.isAttributeNull(other)) return false;
        return ((BooleanExtractor) extractor).booleanValueOf(other) == this.parameter;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements BooleanExtractor
    {
        public boolean booleanValueOf(Object o)
        {
            return getParameter();
        }

        public Object valueOf(Object o)
        {
            return Boolean.valueOf(this.booleanValueOf(o));
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.booleanValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.booleanValueOf(first) == this.booleanValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((BooleanExtractor) secondExtractor).booleanValueOf(second) == this.booleanValueOf(first);
        }
    }

}
