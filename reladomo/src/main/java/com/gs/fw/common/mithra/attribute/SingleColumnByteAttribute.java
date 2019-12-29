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

import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapByteExtractorWithOffset;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.AtomicSelfNotEqualityOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.byteop.ByteEqOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteGreaterThanOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteInOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteLessThanOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteNotEqOperation;
import com.gs.fw.common.mithra.finder.byteop.ByteNotInOperation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.eclipse.collections.api.set.primitive.ByteSet;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public abstract class SingleColumnByteAttribute<T> extends ByteAttribute<T> implements SingleColumnAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.byteExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = -1267026867995523366L;

    protected SingleColumnByteAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnByteAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.SMALLINT);
        }
        else
        {
            pps.setByte(pos, this.byteValueOf((T) mithraDataObject));
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

    public Operation eq(byte other)
    {
        return new ByteEqOperation(this, other);
    }

    public Operation notEq(byte other)
    {
        return new ByteNotEqOperation(this, other);
    }

    @Override
    public Operation in(ByteSet byteSet)
    {
        Operation op;
        switch (byteSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(byteSet.byteIterator().next());
                break;
            default:
                op = new ByteInOperation(this, byteSet);
        }

        return op;
    }

    @Override
    public Operation notIn(ByteSet byteSet)
    {
        Operation op;
        switch (byteSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(byteSet.byteIterator().next());
                break;
            default:
                op = new ByteNotInOperation(this, byteSet);
                break;
        }

        return op;
    }

    public Operation greaterThan(byte target)
    {
        return new ByteGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(byte target)
    {
        return new ByteGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(byte target)
    {
        return new ByteLessThanOperation(this, target);
    }

    public Operation lessThanEquals(byte target)
    {
        return new ByteLessThanEqualsOperation(this, target);
    }

    // join operation:
    public Operation eq(ByteAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(ByteAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(ByteAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(ByteAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("non-equality join is not yet supported");
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

    public void forEach(final IntegerProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.byteValueOf(o), context);
        }
    }

    public void forEach(final DoubleProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.byteValueOf(o), context);
        }
    }

    public void forEach(final LongProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.byteValueOf(o), context);
        }
    }

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.byteValueOf(o), context);
        }
    }

    public void forEach(final BigDecimalProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(BigDecimal.valueOf(this.byteValueOf(o)), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForByte());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.TINYINT || info.getType() == Types.SMALLINT;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleByteAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnByteAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnByteAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional,
                isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, false);
        if (hasShadowAttribute)
        {
            SingleColumnByteAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                    busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional,
                    isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnByteAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnByteAttribute e;
        try
        {
            e = (SingleColumnByteAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnByteAttribute", false, hasShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        byte result = rs.getByte(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.byteValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapByteExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        ByteAttribute byteAttr = (ByteAttribute) this;

        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                out.write(byteAttr.byteValueOf(data.get(i)));
            }
        }
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        ByteAttribute byteAttr = (ByteAttribute) this;
        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                byteAttr.setByteValue(data.get(i), in.readWithExceptionAsByte());
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
