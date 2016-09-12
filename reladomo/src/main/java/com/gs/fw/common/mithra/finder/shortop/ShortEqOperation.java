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

package com.gs.fw.common.mithra.finder.shortop;

import com.gs.fw.common.mithra.attribute.ShortAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.ShortExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;



public class ShortEqOperation extends AtomicEqualityOperation implements SqlParameterSetter
{

    private short parameter;

    public ShortEqOperation(ShortAttribute attribute, short parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        ShortAttribute shortAttribute = (ShortAttribute)this.getAttribute();
        if (shortAttribute.isAttributeNull(o)) return false;
        return shortAttribute.shortValueOf(o) == parameter;
    }

    @Override
    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setShort(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof ShortEqOperation)
        {
            ShortEqOperation other = (ShortEqOperation) obj;
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
        return Short.valueOf(this.parameter);
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        if (extractor.isAttributeNull(other)) return false;
        return ((ShortExtractor) extractor).shortValueOf(other) == this.parameter;
    }

    public short getParameter()
    {
        return parameter;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements ShortExtractor
    {
        public int intValueOf(Object o)
        {
            return (int) this.shortValueOf(o);
        }

        public short shortValueOf(Object o)
        {
            return getParameter();
        }

        @Override
        public boolean isAttributeNull(Object o)
        {
            return false;
        }

        public Object valueOf(Object o)
        {
            return Short.valueOf(this.shortValueOf(o));
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.shortValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.shortValueOf(first) == this.shortValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((ShortExtractor) secondExtractor).shortValueOf(second) == this.shortValueOf(first);
        }
    }
}
