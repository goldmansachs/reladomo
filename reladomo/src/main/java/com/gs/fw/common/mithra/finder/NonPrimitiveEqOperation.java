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

import com.gs.fw.common.mithra.attribute.NonPrimitiveAttribute;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.OperationParameterExtractor;
import com.gs.fw.common.mithra.extractor.StringExtractor;
import com.gs.fw.common.mithra.finder.paramop.OpWithBigDecimalParamExtractor;
import com.gs.fw.common.mithra.finder.paramop.OpWithObjectParam;
import com.gs.fw.common.mithra.finder.paramop.OpWithStringParamExtractor;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.StringPool;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;



public class NonPrimitiveEqOperation extends AtomicEqualityOperation implements SqlParameterSetter, OpWithObjectParam
{

    private Object parameter;

    public NonPrimitiveEqOperation(NonPrimitiveAttribute attribute, Object parameter)
    {
        super(attribute);
        this.parameter = parameter;
    }

    public NonPrimitiveEqOperation()
    {
        // for externalizable
        super();
    }

    @Override
    protected Extractor getStaticExtractor()
    {
        if (this.parameter instanceof String)
        {
            return OpWithStringParamExtractor.INSTANCE;
        }
        return OpWithBigDecimalParamExtractor.INSTANCE;
    }

    @Override
    public Object getParameter()
    {
        return this.parameter;
    }

    protected void setParameter(Object parameter)
    {
        this.parameter = parameter;
    }

    protected boolean matchesWithoutDeleteCheck(Object o, Extractor extractor)
    {
        Object incoming = extractor.valueOf(o);
        return incoming == null ? parameter == null : incoming.equals(parameter);
    }

    protected List getByIndex()
    {
        return this.getCache().get(this.getIndexRef(), parameter);
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        ((NonPrimitiveAttribute)this.getAttribute()).setSqlParameter(startIndex, pstmt, parameter, query.getTimeZone(), query.getDatabaseType());
        return 1;
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode() ^ this.parameter.hashCode();
    }

    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("=");
        NonPrimitiveAttribute nonPrimitiveAttribute = (NonPrimitiveAttribute) this.getAttribute();
        String formattedValue = nonPrimitiveAttribute.formattedValue(this.getParameterAsObject());
        toStringContext.append(formattedValue);
    }

    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj instanceof NonPrimitiveEqOperation)
        {
            NonPrimitiveEqOperation other = (NonPrimitiveEqOperation) obj;
            return this.parameter.equals(other.parameter) && this.getAttribute().equals(other.getAttribute());
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
        return this.parameter;
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        Object secondValue = extractor.valueOf(other);
        if (this.parameter == secondValue) return true; // takes care of both null

        return (this.parameter != null) && this.parameter.equals(secondValue);
    }

    public Extractor getParameterExtractor()
    {
        return new ParameterExtractor();
    }

    protected class ParameterExtractor extends OperationParameterExtractor implements Extractor, StringExtractor
    {
        public Object valueOf(Object o)
        {
            return getParameterAsObject();
        }

        @Override
        public int offHeapValueOf(Object o)
        {
            return StringPool.getInstance().getOffHeapAddressWithoutAdding((String)getParameterAsObject());
        }

        @Override
        public String stringValueOf(Object o)
        {
            return (String) getParameterAsObject();
        }

        @Override
        public void setStringValue(Object o, String newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public boolean isAttributeNull(Object o)
        {
            return this.valueOf(o) == null;
        }

        public int valueHashCode(Object o)
        {
            return valueOf(o).hashCode();
        }

       public boolean valueEquals(Object first, Object second)
        {
            if (first == second) return true;
            boolean firstNull = this.isAttributeNull(first);
            boolean secondNull = this.isAttributeNull(second);
            if (firstNull) return secondNull;
            return this.valueOf(first).equals(this.valueOf(second));
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            Object firstValue = this.valueOf(first);
            Object secondValue = secondExtractor.valueOf(second);
            if (firstValue == secondValue) return true; // takes care of both null

            return (firstValue != null) && firstValue.equals(secondValue);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof ParameterExtractor)
            {
                return nullSafeEquals(getParameterAsObject(), ((ParameterExtractor) obj).valueOf(null));
            }
            return false;
        }

        private boolean nullSafeEquals(Object value, Object other)
        {
            if (value == null)
            {
                if (other == null)
                {
                    return true;
                }
            }
            else if (other == value || value.equals(other))
            {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            Object p = getParameterAsObject();
            if (p == null)
            {
                return HashUtil.NULL_HASH;
            }
            return p.hashCode();
        }
    }

}
