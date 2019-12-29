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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.cache.offheap.OffHeapLongExtractorWithOffset;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.AtomicSelfNotEqualityOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.longop.LongEqOperation;
import com.gs.fw.common.mithra.finder.longop.LongGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.longop.LongGreaterThanOperation;
import com.gs.fw.common.mithra.finder.longop.LongInOperation;
import com.gs.fw.common.mithra.finder.longop.LongLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.longop.LongLessThanOperation;
import com.gs.fw.common.mithra.finder.longop.LongNotEqOperation;
import com.gs.fw.common.mithra.finder.longop.LongNotInOperation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.eclipse.collections.api.set.primitive.LongSet;
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


public abstract class SingleColumnLongAttribute<T> extends LongAttribute<T> implements VersionAttribute<T>, SequenceAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.longExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 6368131979044263763L;

    protected SingleColumnLongAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, boolean isIdentity)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.isIdentity = isIdentity;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnLongAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.BIGINT);
        }
        else
        {
            pps.setLong(pos, this.longValueOf((T) mithraDataObject));
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

    public Operation eq(long other)
    {
        return new LongEqOperation(this, other);
    }

    public Operation notEq(long other)
    {
        return new LongNotEqOperation(this, other);
    }

    @Override
    public Operation in(LongSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(set.longIterator().next());
                break;
            default:
                op = new LongInOperation(this, set);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(LongSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.longIterator().next());
                break;
            default:
                op = new LongNotInOperation(this, set);
                break;
        }

        return op;
    }

    public Operation greaterThan(long target)
    {
        return new LongGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(long target)
    {
        return new LongGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(long target)
    {
        return new LongLessThanOperation(this, target);
    }

    public Operation lessThanEquals(long target)
    {
        return new LongLessThanEqualsOperation(this, target);
    }

    // join operation:
    public Operation eq(LongAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(LongAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(LongAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(LongAttribute other)
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
        pps.setLong(pos, this.longValueOf((T) mdo) - 1);
    }

    public boolean hasSameVersion(MithraDataObject first, MithraDataObject second)
    {
        throw new RuntimeException("not implemented");
    }

    public void forEach(final LongProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.longValueOf(o), context);
        }
    }

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.longValueOf(o), context);
        }
    }

    public void forEach(final DoubleProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.longValueOf(o), context);
        }
    }

    public void forEach(final BigDecimalProcedure proc, T o, Object context)
    {
        if(!checkForNull(proc, o, context))
        {
            proc.execute(BigDecimal.valueOf(this.longValueOf(o)), context);
        }
    }

    public void forEach(final IntegerProcedure proc, T o, Object context)
    {
        throw new RuntimeException("should not get here");
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForLong());
        if (this.isIdentity)
        {
            sb.append(' ').append(dt.getIdentityTableCreationStatement());
        }
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.BIGINT || info.getType() == Types.NUMERIC;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleLongAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnLongAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic,
            boolean isIdentity, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasSequence, boolean hasShadowAttribute)
    {
        SingleColumnLongAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic,
                isIdentity, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, hasSequence, false);
        if (hasShadowAttribute)
        {
            SingleColumnLongAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic,
                    isIdentity, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, hasSequence, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnLongAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic,
            boolean isIdentity, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasSequence, boolean hasShadowAttribute)
    {
        SingleColumnLongAttribute e;
        try
        {
            e = (SingleColumnLongAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnLongAttribute", hasSequence, hasShadowAttribute).newInstance();
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
        long result = rs.getLong(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.longValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapLongExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        LongAttribute attr = (LongAttribute) this;
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    attr.setLongValue(data.get(i), attr.longValueOf(data.get(i)) | ((((long)in.read()) << p)));
                }
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        LongAttribute attr = (LongAttribute) this;
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    long v = attr.longValueOf(data.get(i));
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
