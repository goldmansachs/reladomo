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

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import com.gs.fw.common.mithra.attribute.calculator.procedure.DateProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapDateExtractorWithOffset;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.AtomicSelfNotEqualityOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.date.DateEqOperation;
import com.gs.fw.common.mithra.finder.date.DateGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.date.DateGreaterThanOperation;
import com.gs.fw.common.mithra.finder.date.DateInOperation;
import com.gs.fw.common.mithra.finder.date.DateLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.date.DateLessThanOperation;
import com.gs.fw.common.mithra.finder.date.DateNotEqOperation;
import com.gs.fw.common.mithra.finder.date.DateNotInOperation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;


public abstract class SingleColumnDateAttribute<Owner> extends DateAttribute<Owner> implements SingleColumnAttribute<Owner>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.dateExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 911195690161459903L;

    protected SingleColumnDateAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, boolean setAsString)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        this.setDateProperties(setAsString);
    }

    protected SingleColumnDateAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        Object obj = this.valueOf((Owner) mithraDataObject);

        if (obj != null)
        {
            databaseType.setDate(pps, pos, (Date) obj, this.isSetAsString());
        }
        else
        {
            pps.setNull(pos, java.sql.Types.DATE);
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

    public Operation eq(Date other)
    {
        return (other == null) ? this.isNull() : new DateEqOperation(this, this.normalizeDate(other));
    }

    public Operation notEq(Date other)
    {
        return (other == null) ? this.isNotNull() : new DateNotEqOperation(this, this.normalizeDate(other));
    }


    protected Operation getNotInOperation(NonPrimitiveAttribute attribute, Set<Date> set)
    {
        return new DateNotInOperation(attribute, set);
    }

    protected Operation getInOperation(NonPrimitiveAttribute attribute, Set<Date> set)
    {
        return new DateInOperation(attribute, set);
    }

    // join operation:
    public Operation eq(DateAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(DateAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(DateAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(DateAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("Non-equality join is not yet supported");
    }

    public Operation greaterThan(Date target)
    {
        return new DateGreaterThanOperation(this,  this.normalizeDate(target));
    }

    public Operation greaterThanEquals(Date target)
    {
        return new DateGreaterThanEqualsOperation(this, this.normalizeDate(target));
    }

    public Operation lessThan(Date target)
    {
        return new DateLessThanOperation(this, this.normalizeDate(target));
    }

    public Operation lessThanEquals(Date target)
    {
        return new DateLessThanEqualsOperation(this, this.normalizeDate(target));
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

    public void forEach(final DateProcedure proc, Owner o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.dateValueOf(o), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append("date");
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.DATE;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleDateAttribute(pos, this.isSetAsString(), tupleTempContext);
    }

    public static SingleColumnDateAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean setAsString, boolean hasShadowAttribute)
    {
        SingleColumnDateAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                setAsString, false);
        if (hasShadowAttribute)
        {
            SingleColumnDateAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    setAsString, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnDateAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean setAsString, boolean hasShadowAttribute)
    {
        SingleColumnDateAttribute e;
        try
        {
            e = (SingleColumnDateAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnDateAttribute", false, hasShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setDateProperties(setAsString);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        return rs.getDate(pos);
    }

    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.dateValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapDateExtractorWithOffset(this.offHeapFieldOffset, this.offHeapNullBitsOffset, this.offHeapNullBitsPosition);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        DateAttribute attr = (DateAttribute) this;
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
                attr.setDateValue(data.get(i), new Date(values[i]));
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        DateAttribute attr = (DateAttribute) this;
        for(int p = 0; p < 64; p+=8)
        {
            for (int i = 0; i < data.size(); i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    long v = attr.dateValueOfAsLong(data.get(i));
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
