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

package com.gs.fw.common.mithra.finder.bytearray;

import com.gs.fw.common.mithra.attribute.ByteArrayAttribute;
import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.SqlParameterSetter;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.ToStringContext;
import com.gs.fw.common.mithra.finder.paramop.OpWithByteArrayParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithByteArrayParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


public class ByteArrayEqOperation extends AtomicEqualityOperation implements SqlParameterSetter, OpWithByteArrayParam
{

    private byte[] parameter;

    public ByteArrayEqOperation(ByteArrayAttribute attribute, byte[] parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    protected ByteArrayEqOperation()
    {
        // for externalizable
        super();
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        return OpWithByteArrayParamExtractor.INSTANCE;
    }

    protected void setParameter(byte[] parameter)
    {
        this.parameter = parameter;
    }

    @Override
    public byte[] getParameter()
    {
        return this.parameter;
    }

    public static boolean byteArrayEquals(Object a, Object b)
    {
        if (a instanceof byte[] && b instanceof byte[])
        {
            byte[] b1 = (byte[]) a;
            byte[] b2 = (byte[]) b;
            if (b1.length == b2.length)
            {
                for(int i=0;i<b1.length;i++)
                {
                    if (b1[i] != b2[i]) return false;
                }
                return true;
            }
        }
        return false;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        Object incoming = extractor.valueOf(o);
        if (incoming == null) return parameter == null;
        return byteArrayEquals(incoming, parameter);
    }

    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), this, new Extractor[] { this.getParameterExtractor() }, true);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        ((NonPrimitiveAttribute)this.getAttribute()).setSqlParameter(startIndex, pstmt, parameter, query.getTimeZone(), query.getDatabaseType());
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ HashUtil.hash((byte[]) this.parameter);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof ByteArrayEqOperation)
        {
            ByteArrayEqOperation other = (ByteArrayEqOperation) obj;
            return this.byteArrayEquals(this.parameter, other.parameter) && this.getAttribute().equals(other.getAttribute());
        }
        return false;
    }

    @Override
    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("=");
        toStringContext.append(Arrays.toString((byte[])this.parameter));
    }

    @Override
    public int getParameterHashCode()
    {
        return HashUtil.hash((byte[]) this.parameter);
    }

    public Object getParameterAsObject()
    {
        return this.parameter;
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        Object second = extractor.valueOf(other);
        return second != null && byteArrayEquals(second, this.parameter);
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements Extractor
    {
        public Object valueOf(Object o)
        {
            return getParameterAsObject();
        }

        public boolean isAttributeNull(Object o)
        {
            return this.valueOf(o) == null;
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.hash((byte[])getParameterAsObject());
        }

        public boolean valueEquals(Object first, Object second)
        {
            if (first == second) return true;
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = this.isAttributeNull(second);
            if (firstNull) return secondNull;
            return byteArrayEquals(this.valueOf(first), this.valueOf(second));
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = secondExtractor.isAttributeNull(second);
            if (firstNull != secondNull) return false;
            if (!firstNull) return byteArrayEquals(this.valueOf(first), secondExtractor.valueOf(second));
            return true;
        }
    }
}
