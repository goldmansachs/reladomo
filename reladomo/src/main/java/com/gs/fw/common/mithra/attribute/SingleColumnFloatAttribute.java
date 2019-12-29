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
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.cache.offheap.OffHeapFloatExtractorWithOffset;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.AtomicSelfNotEqualityOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.floatop.FloatEqOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatGreaterThanOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatInOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatLessThanOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatNotEqOperation;
import com.gs.fw.common.mithra.finder.floatop.FloatNotInOperation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.eclipse.collections.api.set.primitive.FloatSet;
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


public abstract class SingleColumnFloatAttribute<T> extends FloatAttribute<T> implements SingleColumnAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.floatExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 4540088964825528233L;

    protected SingleColumnFloatAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnFloatAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.FLOAT);
        }
        else
        {
            pps.setFloat(pos, this.floatValueOf((T) mithraDataObject));
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

    public Operation eq(float other)
    {
        return new FloatEqOperation(this, other);
    }

    public Operation notEq(float other)
    {
        return new FloatNotEqOperation(this, other);
    }

    @Override
    public Operation in(FloatSet floatSet)
    {
        Operation op;
        switch (floatSet.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(floatSet.floatIterator().next());
                break;
            default:
                op = new FloatInOperation(this, floatSet);
                break;
        }
        return op;
    }

    @Override
    public Operation notIn(FloatSet floatSet)
    {
        Operation op;
        switch (floatSet.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(floatSet.floatIterator().next());
                break;
            default:
                op = new FloatNotInOperation(this, floatSet);
                break;
        }
        return op;
    }

    public Operation greaterThan(float target)
    {
        return new FloatGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(float target)
    {
        return new FloatGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(float target)
    {
        return new FloatLessThanOperation(this, target);
    }

    public Operation lessThanEquals(float target)
    {
        return new FloatLessThanEqualsOperation(this, target);
    }

    // join operation:
    public Operation eq(FloatAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(FloatAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(FloatAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(FloatAttribute other)
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

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.floatValueOf(o), context);
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
            proc.execute(this.floatValueOf(o), context);
        }
    }

    public void forEach(final BigDecimalProcedure proc, T o, Object context)
    {
        if(!checkForNull(proc, o, context))
        {
            proc.execute(BigDecimal.valueOf(this.floatValueOf(o)), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForFloat());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.FLOAT || info.getType() == Types.DOUBLE || info.getType() == Types.DECIMAL || info.getType() == Types.NUMERIC;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleFloatAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnFloatAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnFloatAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, false);
        if (hasShadowAttribute)
        {
            SingleColumnFloatAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnFloatAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnFloatAttribute e;
        try
        {
            e = (SingleColumnFloatAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnFloatAttribute", false, hasShadowAttribute).newInstance();
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
        float result = rs.getFloat(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.floatValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapFloatExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        FloatAttribute attr = (FloatAttribute) this;
        int[] values = new int[data.size()];
        for(int p = 0; p < 32; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    values[i] |= ((in.readWithException() << p));
                }
            }
        }
        for (int i = 0; i < data.size(); i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                attr.setFloatValue(data.get(i), Float.intBitsToFloat(values[i]));
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        FloatAttribute attr = (FloatAttribute) this;
        for(int p = 0; p < 32; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    int v = Float.floatToIntBits(attr.floatValueOf(data.get(i)));
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
