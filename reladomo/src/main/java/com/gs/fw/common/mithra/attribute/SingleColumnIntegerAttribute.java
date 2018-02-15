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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.attribute;

import com.gs.collections.api.set.primitive.IntSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.cache.offheap.OffHeapIntExtractorWithOffset;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.integer.*;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.math.BigDecimal;


public abstract class SingleColumnIntegerAttribute<T> extends IntegerAttribute<T> implements VersionAttribute<T>, SequenceAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.intExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 3146117102692991067L;

    protected SingleColumnIntegerAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, boolean isIdentity)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.isIdentity = isIdentity;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnIntegerAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.INTEGER);
        }
        else
        {
            pps.setInt(pos, this.intValueOf((T) mithraDataObject));
        }
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public boolean isSourceAttribute()
    {
        return this.columnName == null;
    }

    public String getColumnName()
    {
        return this.columnName;
    }

    public Operation eq(int other)
    {
        return new IntegerEqOperation(this, other);
    }

    public Operation eq(long other)
    {
        if (other > Integer.MAX_VALUE || other < Integer.MIN_VALUE)
        {
            return new None(this);
        }
        return new IntegerEqOperation(this, (int) other);
    }

    public Operation notEq(int other)
    {
        return new IntegerNotEqOperation(this, other);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(IntSet intSet)
    {
        Operation op;
        switch (intSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(intSet.intIterator().next());
                break;
            default:
                op = new IntegerInOperation(this, intSet);
                break;
        }

        return op;
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.IntSet intSet)
    {
        Operation op;
        switch (intSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(intSet.intIterator().next());
                break;
            default:
                op = new IntegerInOperation(this, intSet);
                break;
        }

        return op;
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(IntSet intSet)
    {
        Operation op;
        switch (intSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(intSet.intIterator().next());
                break;
            default:
                op = new IntegerNotInOperation(this, intSet);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.IntSet intSet)
    {
        Operation op;
        switch (intSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(intSet.intIterator().next());
                break;
            default:
                op = new IntegerNotInOperation(this, intSet);
                break;
        }

        return op;
    }

    public Operation greaterThan(int target)
    {
        return new IntegerGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(int target)
    {
        return new IntegerGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(int target)
    {
        return new IntegerLessThanOperation(this, target);
    }

    public Operation lessThanEquals(int target)
    {
        return new IntegerLessThanEqualsOperation(this, target);
    }

    public Operation eq(IntegerAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(IntegerAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(IntegerAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(IntegerAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("Non-equality join is not yet supported");
    }


    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        String result = this.getColumnName();
        String databaseAlias = query.getDatabaseAlias(this.getOwnerPortal());
        if (databaseAlias != null)
        {
            if (this.uniqueAlias != null)
            {
                result = databaseAlias + this.uniqueAlias + "." + result;
            }
            else
            {
                result = databaseAlias + "." + result;
            }
        }
        return result;
    }

    public void setVersionAttributeSqlParameters(PreparedStatement pps, MithraDataObject mdo, int pos, TimeZone databaseTimeZone) throws SQLException
    {
        pps.setInt(pos, this.intValueOf((T) mdo) - 1);
    }

    public void forEach(final IntegerProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.intValueOf(o), context);
        }
    }

    public void forEach(final DoubleProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.intValueOf(o), context);
        }
    }

    public void forEach(final LongProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.intValueOf(o), context);
        }
    }

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.intValueOf(o), context);
        }
    }

    public void forEach(final BigDecimalProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(BigDecimal.valueOf(this.intValueOf(o)), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append("integer");
        if (this.isIdentity)
        {
            sb.append(' ').append(dt.getIdentityTableCreationStatement());
        }
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.INTEGER;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleIntegerAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnIntegerAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic,
            boolean isIdentity, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasSequence, boolean hasShadowAttribute)
    {
        SingleColumnIntegerAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic,
                isIdentity, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, hasSequence, false);
        if (hasShadowAttribute)
        {
            SingleColumnIntegerAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic,
                    isIdentity, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, hasSequence, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnIntegerAttribute generateInternal(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive, boolean hasBusDate,
            RelatedFinder relatedFinder, Map<String, Object> properties, boolean isTransactional, boolean isOptimistic,
            boolean isIdentity, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasSequence, boolean hasShadowAttribute)
    {
        SingleColumnIntegerAttribute e;
        try
        {
            e = (SingleColumnIntegerAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnIntegerAttribute", hasSequence, hasShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.isIdentity = isIdentity;
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        int result = rs.getInt(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.intValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapIntExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        IntegerAttribute intAttr = (IntegerAttribute) this;
        for(int p = 0; p < 32; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    intAttr.setIntValue(data.get(i), intAttr.intValueOf(data.get(i)) | ((in.readWithException() << p)));
                }
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        IntegerAttribute intAttr = (IntegerAttribute) this;
        for(int p = 0; p < 32; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    int v = intAttr.intValueOf(data.get(i));
                    out.write((byte) ((v >>> p) & 0xFF));
                }
            }
        }
    }

    @Override
    public Object zDecodeColumnarData(ColumnarInStream in, int count) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zWritePlainTextFromColumnar(Object columnData, int row, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }
}
