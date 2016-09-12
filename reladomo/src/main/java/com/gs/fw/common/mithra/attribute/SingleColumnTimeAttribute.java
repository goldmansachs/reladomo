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

import com.gs.fw.common.mithra.attribute.calculator.procedure.TimeProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapTimeExtractorWithOffset;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.AtomicSelfNotEqualityOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.time.*;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.Time;
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
import java.util.Set;
import java.util.TimeZone;

public abstract class SingleColumnTimeAttribute<Owner> extends TimeAttribute<Owner> implements SingleColumnAttribute<Owner>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.timeExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 0;

    protected SingleColumnTimeAttribute(String columnName, String uniqueAlias, String attributeName,
                                        String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
                                        boolean hasBusTime, RelatedFinder relatedFinder, Map<String, Object> properties,
                                        boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnTimeAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        Object obj = this.valueOf((Owner) mithraDataObject);

        if (obj != null)
        {
            databaseType.setTime(pps, pos, (Time) obj);
        }
        else
        {
            databaseType.setTimeNull(pps, pos);
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

    public Operation eq(Time other)
    {
        return (other == null) ? this.isNull() : new TimeEqOperation(this, other);
    }

    public Operation notEq(Time other)
    {
        return (other == null) ? this.isNotNull() : new TimeNotEqOperation(this, other);
    }


    protected Operation getNotInOperation(NonPrimitiveAttribute attribute, Set<Time> set)
    {
        return new TimeNotInOperation(attribute, set);
    }

    protected Operation getInOperation(NonPrimitiveAttribute attribute, Set<Time> set)
    {
        return new TimeInOperation(attribute, set);
    }

    // join operation:
    public Operation eq(TimeAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(TimeAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(TimeAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(TimeAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("Non-equality join is not yet supported");
    }

    public Operation greaterThan(Time target)
    {
        return new TimeGreaterThanOperation(this,  target);
    }

    public Operation greaterThanEquals(Time target)
    {
        return new TimeGreaterThanEqualsOperation(this, target);
    }

    public Operation lessThan(Time target)
    {
        return new TimeLessThanOperation(this, target);
    }

    public Operation lessThanEquals(Time target)
    {
        return new TimeLessThanEqualsOperation(this, target);
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

    public void forEach(final TimeProcedure proc, Owner o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.timeValueOf(o), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForTime());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.TIME;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleTimeAttribute(pos, tupleTempContext);
    }

    public static SingleColumnTimeAttribute generate(String columnName, String uniqueAlias, String attributeName,
                                                     String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
                                                     boolean hasBusTime, RelatedFinder relatedFinder, Map<String, Object> properties,
                                                     boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition,boolean hasShadowAttribute)
    {
        SingleColumnTimeAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusTime, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                false);
        if (hasShadowAttribute)
        {
            SingleColumnTimeAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusTime, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnTimeAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
                                                              String busClassName, boolean isNullablePrimitive, boolean hasBusTime, RelatedFinder relatedFinder, Map<String, Object> properties,
                                                              boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnTimeAttribute e;
        try
        {
            e = (SingleColumnTimeAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusTime, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnTimeAttribute", false, hasShadowAttribute).newInstance();
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
        return databaseType.getTime(rs, pos);
    }

    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.timeValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapTimeExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        TimeAttribute timeAttr = (TimeAttribute) this;
        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                timeAttr.setTimeValue(data.get(i), Time.withNanos(in.readWithExceptionAsByte(), in.readWithExceptionAsByte(), in.readWithException(), in.readInt()));
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        TimeAttribute timeAttr = (TimeAttribute) this;

        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                Time time = timeAttr.timeValueOf(data.get(i));
                out.write(time.getHour());
                out.write(time.getMinute());
                out.write(time.getSecond());
                out.writeInt(time.getNano());
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
