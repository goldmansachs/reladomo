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

import com.gs.collections.api.block.procedure.primitive.ObjectIntProcedure;
import com.gs.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.cache.offheap.OffHeapTimestampExtractorWithOffset;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.attribute.calculator.procedure.TimestampProcedure;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.timestamp.*;
import com.gs.fw.common.mithra.util.ImmutableTimestamp;
import com.gs.fw.common.mithra.util.MithraTimestamp;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;
import java.util.Date;


public abstract class SingleColumnTimestampAttribute<Owner> extends TimestampAttribute<Owner> implements SingleColumnAttribute<Owner>, VersionAttribute<Owner>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.timestampExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 7227697285345172793L;

    protected SingleColumnTimestampAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, byte conversionType,
            boolean setAsString, boolean isAsOfAttributeTo, Timestamp infinity, byte precision)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setTimestampProperties(conversionType, setAsString, isAsOfAttributeTo, infinity, precision);
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnTimestampAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        Timestamp obj = this.timestampValueOf((Owner) mithraDataObject);
        this.setSqlParameter(pos, pps, obj, databaseTimeZone, databaseType);
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

    public Operation eq(Timestamp other)
    {
        return (other == null) ? this.isNull() : new TimestampEqOperation(this, other);
    }

    public Operation notEq(Timestamp other)
    {
        return (other == null) ? this.isNotNull() : new TimestampNotEqOperation(this, other);
    }

    protected Operation getNotInOperation(NonPrimitiveAttribute attribute, Set<Timestamp> set)
    {
        return new TimestampNotInOperation(attribute, set);
    }

    protected Operation getInOperation(NonPrimitiveAttribute attribute, Set<Timestamp> set)
    {
        return new TimestampInOperation(attribute, set);
    }

    public Operation eq(AsOfAttribute other)
    {
        return new MappedOperation(new TimestampAsOfEqualityMapper(this, other, true), new All(other));
    }

    // join operation:
    public Operation eq(TimestampAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(TimestampAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(TimestampAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(TimestampAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("non-equality join is not yet supported");
    }

    public Operation greaterThan(Timestamp target)
    {
        return new TimestampGreaterThanOperation(this, target);
    }

    public Operation greaterThanEquals(Timestamp target)
    {
        return new TimestampGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(Timestamp target)
    {
        return new TimestampLessThanOperation(this, target);
    }

    public Operation lessThanEquals(Timestamp target)
    {
        return new TimestampLessThanEqualsOperation(this, target);
    }

    public Operation eq(Date other)
    {
        return (other == null) ? this.isNull() : new TimestampEqOperation(this, new Timestamp(other.getTime()));
    }

    public Operation notEq(Date other)
    {
        return new TimestampNotEqOperation(this, new Timestamp(other.getTime()));
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

    public void forEach(final TimestampProcedure proc, Owner o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.timestampValueOf(o), context);
        }
    }

    public void setVersionAttributeSqlParameters(PreparedStatement pps, MithraDataObject mdo, int pos, TimeZone databaseTimeZone) throws SQLException
    {
        throw new RuntimeException("should not get here");
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForTimestamp());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.TIMESTAMP;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleTimestampAttribute(pos, tupleTempContext, this);
    }

    public static SingleColumnTimestampAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, byte conversionType,
            boolean setAsString, boolean isAsOfAttributeTo, Timestamp infinity, byte precision, boolean hasShadowAttribute)
    {
        SingleColumnTimestampAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                conversionType, setAsString, isAsOfAttributeTo, infinity, false, precision);
        if (hasShadowAttribute)
        {
            SingleColumnTimestampAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    conversionType, setAsString, isAsOfAttributeTo, infinity, true, precision);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnTimestampAttribute generateInternal(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder,
            Map<String, Object> properties, boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, byte conversionType, boolean setAsString,
            boolean isAsOfAttributeTo, Timestamp infinity, boolean hasShadowAttribute, byte precision)
    {
        SingleColumnTimestampAttribute e;
        try
        {
            e = (SingleColumnTimestampAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnTimestampAttribute", false, hasShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.setTimestampProperties(conversionType, setAsString, isAsOfAttributeTo, infinity, precision);
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone dbTimeZone) throws SQLException
    {
        Timestamp result = databaseType.getTimestampFromResultSet(rs, pos, this.getConversionTimeZone(dbTimeZone));
        if (result != null) result = this.zFixPrecisionAndInfinityIfNecessary(result, dbTimeZone);
        return result;
    }

    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.timestampValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapTimestampExtractorWithOffset(this.offHeapFieldOffset);
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        if (hasRealNanos(data))
        {
            encodeColumnarDataWithNanos(data, out, nulls);
        }
        else
        {
            encodeColumnarDataNoNanos(data, out, nulls);
        }
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        byte marker = in.readWithExceptionAsByte();
        if ((marker & 1) == 1)
        {
            decodeColumnarDataWithNanos(data, in, nulls, (marker & 2) != 0);
        }
        else
        {
            decodeColumnarDataNoNanos(data, in, nulls, (marker & 2) != 0);
        }
    }


    private void decodeColumnarDataNoNanos(List data, ColumnarInStream in, BitsInBytes nulls, boolean hasCommon) throws IOException
    {
        TimestampAttribute attr = (TimestampAttribute) this;
        BitsInBytes commonBits = null;
        if (hasCommon)
        {
            long time = in.readLong();
            time -= MithraTimestamp.getDefaultOffsetForTime(time);
            Timestamp common = new ImmutableTimestamp(time);
            commonBits = decodeColumnarCommon(data, in, attr, common);
        }
        long[] times = decodeColumnarTime(data, in, nulls, commonBits);
        for(int i=0;i<data.size();i++)
        {
            if ((nulls == null || !nulls.get(i)) && (commonBits == null || !commonBits.get(i)))
            {
                attr.setTimestampValue(data.get(i), new ImmutableTimestamp(times[i]));
            }
        }
    }

    private long[] decodeColumnarTime(List data, ColumnarInStream in, BitsInBytes nulls, BitsInBytes commonBits) throws IOException
    {
        long[] result = new long[data.size()];
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if ((nulls == null || !nulls.get(i)) && (commonBits == null || !commonBits.get(i)))
                {
                    result[i] |= ((((long)in.read()) << p));
                }
            }
        }
        for(int i=0;i<result.length;i++)
        {
            result[i] -= MithraTimestamp.getDefaultOffsetForTime(result[i]);
        }
        return result;
    }

    private int[] decodeColumnarNanos(List data, ColumnarInStream in, BitsInBytes nulls, BitsInBytes commonBits) throws IOException
    {
        int[] result = new int[data.size()];
        for(int p = 0; p < 32; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if ((nulls == null || !nulls.get(i)) && (commonBits == null || !commonBits.get(i)))
                {
                    result[i] |= (in.read() << p);
                }
            }
        }
        return result;
    }

    private BitsInBytes decodeColumnarCommon(List data, ColumnarInStream in, TimestampAttribute attr, Timestamp common) throws IOException
    {
        BitsInBytes commonBits;
        commonBits = BitsInBytes.readFromInputStream(in.getInputStream(), data.size());
        for(int i=0;i<data.size();i++)
        {
            if (commonBits.get(i))
            {
                attr.setTimestampValue(data.get(i), common);
            }
        }
        return commonBits;
    }

    private void encodeColumnarDataNoNanos(List data, ColumnarOutStream out, BitsInBytes nulls) throws IOException
    {
        byte marker = 0;
        Timestamp t = findCommonValue(data);
        if (t != null)
        {
            marker |= 2;
        }
        out.write(marker);
        if (t != null)
        {
            out.writeLong(MithraTimestamp.getSerializableTime(false, t.getTime()));
        }
        BitsInBytes common = writeCommonValueBits(data, out, t);
        TimestampAttribute attr = (TimestampAttribute) this;
        encodeColumnarTime(data, out, nulls, common, attr);

    }

    private BitsInBytes writeCommonValueBits(List data, ColumnarOutStream out, Timestamp t) throws IOException
    {
        TimestampAttribute attr = (TimestampAttribute) this;
        BitsInBytes commonBits = null;
        if (t != null)
        {
            commonBits = new BitsInBytes(data.size());
            for(int i=0;i<data.size();i++)
            {
                if (t.equals(attr.timestampValueOf(data.get(i))))
                {
                    commonBits.set(i);
                }
            }
            commonBits.writeToOutputStream(out.getOutputStream());
        }
        return commonBits;
    }

    private Timestamp findCommonValue(List data)
    {
        ObjectIntHashMap<Timestamp> counts = new ObjectIntHashMap(data.size());
        TimestampAttribute attr = (TimestampAttribute) this;

        for(int i=0;i<data.size();i++)
        {
            Timestamp t = attr.timestampValueOf(data.get(i));
            if (t != null)
            {
                counts.addToValue(t, 1);
            }
        }

        MaxProc procedure = new MaxProc();
        counts.forEachKeyValue(procedure);
        if (procedure.maxCount > data.size() >> 4)
        {
            return procedure.max;
        }
        return null;
    }

    private static class MaxProc implements ObjectIntProcedure<Timestamp>
    {
        private int maxCount;
        private Timestamp max;

        @Override
        public void value(Timestamp each, int parameter)
        {
            if (max == null || parameter > maxCount)
            {
                max = each;
                maxCount = parameter;
            }
        }
    }

    private void encodeColumnarTime(List data, ColumnarOutStream out, BitsInBytes nulls, BitsInBytes common, TimestampAttribute attr) throws IOException
    {
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if ((nulls == null || !nulls.get(i)) && (common == null || !common.get(i)))
                {
                    long v = MithraTimestamp.getSerializableTime(false, attr.timestampValueOfAsLong(data.get(i)));
                    out.write((byte) ((v >>> p) & 0xFF));
                }
            }
        }
    }

    private void decodeColumnarDataWithNanos(List data, ColumnarInStream in, BitsInBytes nulls, boolean hasCommon) throws IOException
    {
        TimestampAttribute attr = (TimestampAttribute) this;
        BitsInBytes commonBits = null;
        if (hasCommon)
        {
            long time = in.readLong();
            time -= MithraTimestamp.getDefaultOffsetForTime(time);
            int nanos = in.readInt();
            Timestamp common = new ImmutableTimestamp(time, nanos);
            commonBits = decodeColumnarCommon(data, in, attr, common);
        }
        long[] times = decodeColumnarTime(data, in, nulls, commonBits);
        int[] nanos = decodeColumnarNanos(data, in, nulls, commonBits);

        for(int i=0;i<data.size();i++)
        {
            if ((nulls == null || !nulls.get(i)) && (commonBits == null || !commonBits.get(i)))
            {
                attr.setTimestampValue(data.get(i), new ImmutableTimestamp(times[i], nanos[i]));
            }
        }
    }

    private void encodeColumnarDataWithNanos(List data, ColumnarOutStream out, BitsInBytes nulls) throws IOException
    {
        byte marker = 1;
        Timestamp t = findCommonValue(data);
        if (t != null)
        {
            marker |= 2;
        }
        out.write(marker);
        if (t != null)
        {
            out.writeLong(MithraTimestamp.getSerializableTime(false, t.getTime()));
            out.writeInt(t.getNanos());
        }
        BitsInBytes common = writeCommonValueBits(data, out, t);
        TimestampAttribute attr = (TimestampAttribute) this;
        encodeColumnarTime(data, out, nulls, common, attr);
        for(int p = 0; p < 32; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if ((nulls == null || !nulls.get(i)) && (common == null || !common.get(i)))
                {
                    int v = attr.timestampValueOf(data.get(i)).getNanos();
                    out.write((byte) ((v >>> p) & 0xFF));
                }
            }
        }
    }

    private boolean hasRealNanos(List data)
    {
        TimestampAttribute attr = (TimestampAttribute) this;
        for(int i=0;i<data.size();i++)
        {
            Timestamp v = attr.timestampValueOf(data.get(i));
            if (v != null && (v.getTime() % 1000) * 1000000 - v.getNanos() != 0)
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void zWritePlainTextFromColumnar(Object columnData, int row, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Object zDecodeColumnarData(ColumnarInStream in, int count) throws IOException
    {
        throw new RuntimeException("not implemented");
    }
}
