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

package com.gs.fw.common.mithra.finder.byteop;

import com.gs.fw.common.mithra.attribute.ByteAttribute;
import com.gs.fw.common.mithra.extractor.ByteExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.paramop.OpWithByteParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithByteParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public class ByteEqOperation  extends AtomicEqualityOperation implements SqlParameterSetter, OpWithByteParam
{
    private byte parameter;

    public ByteEqOperation(ByteAttribute attribute, byte parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithByteParamExtractor.INSTANCE;
    }

    @Override
    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        ByteExtractor byteAttribute = (ByteExtractor) extractor;
        if (byteAttribute.isAttributeNull(o)) return false;
        return byteAttribute.byteValueOf(o) == parameter;
    }

    @Override
    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        pstmt.setByte(startIndex, parameter);
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter;
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof ByteEqOperation)
        {
            ByteEqOperation other = (ByteEqOperation) obj;
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
        return Byte.valueOf(this.parameter);
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        if (extractor.isAttributeNull(other)) return false;
        return ((ByteExtractor) extractor).byteValueOf(other) == this.parameter;
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    public byte getParameter()
    {
        return parameter;
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements ByteExtractor
    {
        public int intValueOf(Object o)
        {
            return (int) this.byteValueOf(o);
        }

        public byte byteValueOf(Object o)
        {
            return getParameter();
        }

        public Object valueOf(Object o)
        {
            return Byte.valueOf(this.byteValueOf(o));
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash(this.byteValueOf(o));
        }

        public boolean valueEquals(Object first, Object second)
        {
            return this.byteValueOf(first) == this.byteValueOf(second);
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            if (secondExtractor.isAttributeNull(second)) return false;
            return ((ByteExtractor) secondExtractor).byteValueOf(second) == this.byteValueOf(first);
        }
    }
}
