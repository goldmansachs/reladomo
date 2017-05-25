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

package com.gs.fw.common.mithra.attribute;

import com.gs.collections.api.set.primitive.ShortSet;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.cache.offheap.OffHeapShortExtractorWithOffset;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.shortop.*;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.math.BigDecimal;


public abstract class SingleColumnShortAttribute<T> extends ShortAttribute<T> implements SingleColumnAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.shortExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 8865112612537628103L;

    protected SingleColumnShortAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnShortAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.SMALLINT);
        }
        else
        {
            pps.setShort(pos, this.shortValueOf((T) mithraDataObject));
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

    public Operation eq(short other)
    {
        return new ShortEqOperation(this, other);
    }

    public Operation notEq(short other)
    {
        return new ShortNotEqOperation(this, other);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(ShortSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(set.shortIterator().next());
                break;
            default:
                op = new ShortInOperation(this, set);
                break;
        }

        return op;
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.ShortSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(set.shortIterator().next());
                break;
            default:
                op = new ShortInOperation(this, set);
                break;
        }

        return op;
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(ShortSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.shortIterator().next());
                break;
            default:
                op = new ShortNotInOperation(this, set);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.ShortSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.shortIterator().next());
                break;
            default:
                op = new ShortNotInOperation(this, set);
                break;
        }

        return op;
    }

    public Operation greaterThan(short target)
    {
        return new ShortGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(short target)
    {
        return new ShortGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(short target)
    {
        return new ShortLessThanOperation(this, target);
    }

    public Operation lessThanEquals(short target)
    {
        return new ShortLessThanEqualsOperation(this, target);
    }

    // join operation:
    public Operation eq(ShortAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(ShortAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(ShortAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(ShortAttribute other)
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
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.shortValueOf(o), context);
        }
    }

    public void forEach(final DoubleProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.shortValueOf(o), context);
        }
    }

    public void forEach(final LongProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.shortValueOf(o), context);
        }
    }

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.shortValueOf(o), context);
        }
    }

    public void forEach(final BigDecimalProcedure proc, T o, Object context)
    {
        if(!checkForNull(proc, o, context))
        {
            proc.execute(BigDecimal.valueOf(this.shortValueOf(o)), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForShortJava());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.INTEGER || info.getType() == Types.SMALLINT;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleShortAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnShortAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnShortAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, false);
        if (hasShadowAttribute)
        {
            SingleColumnShortAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnShortAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnShortAttribute e;
        try
        {
            e = (SingleColumnShortAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnShortAttribute", false, hasShadowAttribute).newInstance();
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
        short result = rs.getShort(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.shortValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapShortExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }


    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        ShortAttribute attr = (ShortAttribute) this;
        for(int p = 0; p < 16; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    attr.setShortValue(data.get(i), (short) (attr.shortValueOf(data.get(i)) | ((in.readWithException() << p))));
                }
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        ShortAttribute attr = (ShortAttribute) this;
        for(int p = 0; p < 16; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    short v = attr.shortValueOf(data.get(i));
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
