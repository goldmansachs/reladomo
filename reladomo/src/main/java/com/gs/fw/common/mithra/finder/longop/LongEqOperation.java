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
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.LongExtractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.paramop.OpWithLongParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithLongParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;



public class LongEqOperation extends AtomicEqualityOperation implements SqlParameterSetter, OpWithLongParam
{

    private long parameter;

    public LongEqOperation(Attribute attribute, long parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithLongParamExtractor.INSTANCE;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        LongExtractor longAttribute = (LongExtractor) extractor;
        return !longAttribute.isAttributeNull(o) && longAttribute.longValueOf(o) == parameter;
    }

    @Override
    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setLong(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash(this.parameter);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof LongEqOperation)
        {
            LongEqOperation other = (LongEqOperation) obj;
            return this.parameter == other.parameter && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    public int getParameterHashCode()
    {
        return HashUtil.hash(this.parameter);
    }

    @Override
    public Object getParameterAsObject()
    {
        return Long.valueOf(this.parameter);
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        if (extractor.isAttributeNull(other)) return false;
        return ((LongExtractor) extractor).longValueOf(other) == this.parameter;
    }

    public long getParameter()
    {
        return parameter;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements LongExtractor
    {
        public long longValueOf(Object o)
        {
            return getParameter();
        }

        public Object valueOf(Object o)
        {
            return Long.valueOf(this.longValueOf(o));
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.longValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.longValueOf(first) == this.longValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((LongExtractor) secondExtractor).longValueOf(second) == this.longValueOf(first);
        }
    }
}
