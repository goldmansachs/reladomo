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

import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.SingleColumnAttribute;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.StringPool;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.math.BigDecimal;



public class IsNullOperation  extends AtomicEqualityOperation
{
    private static final NullExtractor extractor = new NullExtractor();

    public IsNullOperation(Attribute attribute)
    {
        super(attribute);
    }

    @Override
    public int getParameterHashCode()
    {
        return HashUtil.NULL_HASH;
    }

    public Object getParameterAsObject()
    {
        return null;
    }

    @Override
    public boolean parameterValueEquals(Object other, Extractor extractor)
    {
        return extractor.isAttributeNull(other);
    }

    public Extractor getParameterExtractor()
    {
        return extractor;
    }

    protected List getByIndex()
    {
        return this.getCache().getNulls(this.getIndexRef());
    }

    @Override
    public List applyOperationToPartialCache()
    {
        // a nullable attribute is not likely to be uniquely indexable.
        return null;
    }

    protected Boolean matchesWithoutDeleteCheck(Object o)
    {
        return this.getAttribute().isAttributeNull(o);
    }

    public SingleColumnAttribute getSingleColumnAttribute()
    {
        return (SingleColumnAttribute) this.getAttribute();
    }

    @Override
    public void generateSql(SqlQuery query)
    {
        query.appendWhereClause(this.getAttribute().getFullyQualifiedLeftHandExpression(query)+" IS NULL");
    }

    public int setSqlParameters(PreparedStatement pstmt, int startIndex, SqlQuery query) throws SQLException
    {
        // nothing to set
        return 0;
    }

    @Override
    public void zToString(ToStringContext toStringContext)
    {
        this.getAttribute().zAppendToString(toStringContext);
        toStringContext.append("is null");
    }

    public int hashCode()
    {
        return this.getAttribute().hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof IsNullOperation)
        {
            IsNullOperation other = (IsNullOperation) obj;
            return other.getAttribute() == this.getAttribute();
        }
        return false;
    }

    /*
    returns the combined and operation. Many operations are more efficient when combined.
    This method is internal to Mithra's operation processing.
    */
    @Override
    public Operation zCombinedAnd(Operation op)
    {
        if (op instanceof IsNullOperation)
        {
            return zCombineWithNullOperation((IsNullOperation) op);
        }
        return op.zCombinedAnd(this);
    }

    private Operation zCombineWithNullOperation(IsNullOperation other)
    {
        if (other.getAttribute().equals(this.getAttribute()))
        {
            return this;
        }
        return null;
    }

    @Override
    public Operation zCombinedAndWithAtomicEquality(AtomicEqualityOperation op)
    {
        if (op.getAttribute().equals(this.getAttribute()))
        {
            if (op.zIsNullOperation())
            {
                return this;
            }
            return new None(this.getAttribute());
        }
        // == ok is fine in this case, as the strings are ultimately coming from a static reference in the finder
        else if (op.zGetResultClassName() == this.zGetResultClassName())
        {
            return new MultiEqualityOperation(this, op);
        }
        return null;
    }

    private static class NullExtractor extends OperationParameterExtractor implements ByteExtractor, ShortExtractor,
            IntExtractor, LongExtractor, FloatExtractor, DoubleExtractor, StringExtractor,
            TimestampExtractor, DateExtractor, BigDecimalExtractor
    {
        public boolean isAttributeNull(Object o)
        {
            return true;
        }

        public byte byteValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public int intValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public boolean valueEquals(Object first, Object second, Extractor secondExtractor)
        {
            return secondExtractor.isAttributeNull(second);
        }

        public int valueHashCode(Object o)
        {
            return HashUtil.NULL_HASH;
        }

        public boolean valueEquals(Object first, Object second)
        {
            return second instanceof IsNullOperation;
        }

        @Override
        public int offHeapValueOf(Object o)
        {
            return 0;
        }

        public Object valueOf(Object anObject)
        {
            return null;
        }

        @Override
        public long dateValueOfAsLong(Object valueHolder)
        {
            throw new RuntimeException("not implemented");
        }

        public short shortValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public long longValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public float floatValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public double doubleValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public String stringValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public void setStringValue(Object o, String newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public Timestamp timestampValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public long timestampValueOfAsLong(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public Date dateValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }

        public void setDateValue(Object o, Date newValue)
        {
            throw new RuntimeException("not implemented");
        }

        public BigDecimal bigDecimalValueOf(Object o)
        {
            throw new RuntimeException("not implemented");
        }
    }

}
