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

import com.gs.collections.api.set.primitive.DoubleSet;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDoubleExtractorWithOffset;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.doubleop.*;
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


public abstract class SingleColumnDoubleAttribute<T> extends DoubleAttribute<T> implements SingleColumnAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.doubleExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias = "";

    protected SingleColumnDoubleAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    private static final long serialVersionUID = -988455817082024623L;

    protected SingleColumnDoubleAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.DOUBLE);
        }
        else
        {
            pps.setDouble(pos, this.doubleValueOf((T) mithraDataObject));
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

    public Operation eq(double other)
    {
        return new DoubleEqOperation(this, other);
    }

    public Operation notEq(double other)
    {
        return new DoubleNotEqOperation(this, other);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(DoubleSet doubleSet)
    {
        Operation op;
        switch (doubleSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(doubleSet.doubleIterator().next());
                break;
            default:
                op = new DoubleInOperation(this, doubleSet);
                break;
        }

        return op;
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.DoubleSet doubleSet)
    {
        Operation op;
        switch (doubleSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(doubleSet.doubleIterator().next());
                break;
            default:
                op = new DoubleInOperation(this, doubleSet);
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
    public Operation notIn(DoubleSet doubleSet)
    {
        Operation op;
        switch (doubleSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(doubleSet.doubleIterator().next());
                break;
            default:
                op = new DoubleNotInOperation(this, doubleSet);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.DoubleSet doubleSet)
    {
        Operation op;
        switch (doubleSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(doubleSet.doubleIterator().next());
                break;
            default:
                op = new DoubleNotInOperation(this, doubleSet);
                break;
        }

        return op;
    }

    public Operation greaterThan(double target)
    {
        return new DoubleGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(double target)
    {
        return new DoubleGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(double target)
    {
        return new DoubleLessThanOperation(this, target);
    }

    public Operation lessThanEquals(double target)
    {
        return new DoubleLessThanEqualsOperation(this, target);
    }

    public Operation eq(DoubleAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(DoubleAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(DoubleAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(DoubleAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            if (other instanceof MappedAttribute)
            {
                other = (DoubleAttribute) ((MappedAttribute)other).getWrappedAttribute();
            }
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
            if (this.uniqueAlias.length() > 0)
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

    public void forEach(final DoubleProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.doubleValueOf(o), context);
        }
    }

    public void forEach(final BigDecimalProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(new BigDecimal(this.doubleValueOf(o)), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForDouble());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        if (info.getType() == Types.FLOAT) return info.getSize() == 8;
        return info.getType() == Types.DECIMAL || info.getType() == Types.NUMERIC || info.getType() == Types.DOUBLE;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleDoubleAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnDoubleAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnDoubleAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional,
                isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, false);
        if (hasShadowAttribute)
        {
            SingleColumnDoubleAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                    busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional,
                    isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnDoubleAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnDoubleAttribute e;
        try
        {
            e = (SingleColumnDoubleAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnDoubleAttribute", false, hasShadowAttribute).newInstance();
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
        double result = rs.getDouble(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.doubleValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapDoubleExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        DoubleAttribute attr = (DoubleAttribute) this;
        long[] values = new long[data.size()];
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    values[i] |= (((long)in.readWithException()) << p);
                }
            }
        }
        for (int i = 0; i < data.size(); i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                attr.setDoubleValue(data.get(i), Double.longBitsToDouble(values[i]));
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        DoubleAttribute attr = (DoubleAttribute) this;
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    long v = Double.doubleToRawLongBits(attr.doubleValueOf(data.get(i)));
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
