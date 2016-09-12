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

import com.gs.fw.common.mithra.attribute.calculator.procedure.StringProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.cache.offheap.OffHeapStringExtractorWithOffset;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.*;
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
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public abstract class SingleColumnStringAttribute<Owner> extends StringAttribute<Owner> implements SingleColumnAttribute<Owner>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.stringExtractorWriter();
    private static final Charset COLUMNAR_CHAR_SET = Charset.forName("UTF-8");

    protected transient String columnName;
    protected transient String uniqueAlias;

    private static final long serialVersionUID = -2531503578551321292L;

    protected SingleColumnStringAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int maxLength, boolean stringTrim)
    {
        super(maxLength, stringTrim);
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnStringAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        Object obj = this.valueOf((Owner) mithraDataObject);

        if (obj != null)
        {
            String value = (String) obj;
            pps.setString(pos, value);
        }
        else
        {
            pps.setNull(pos, java.sql.Types.VARCHAR);
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

    public Operation eq(StringAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(StringAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(StringAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(StringAttribute other)
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

    public void forEach(final StringProcedure proc, Owner o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.stringValueOf(o), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        int maxLen = this.getMaxLength();
        if (maxLen == Integer.MAX_VALUE)
        {
            maxLen = 255;
            sqlLogger.warn("MaxLength must be set for "+this.getAttributeName());
        }
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForString()).append('(').append(maxLen).append(')');
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        if (this.getMaxLength() != Integer.MAX_VALUE)
        {
            return info.getSize() == this.getMaxLength();
        }
        // the negative numbers are from JDK 1.6 N* types, like NCHAR
        return info.getType() == Types.CHAR || info.getType() == Types.VARCHAR || info.getType() == -15 || info.getType() == Types.LONGVARCHAR || info.getType() == -16 || info.getType() == -9;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleStringAttribute(pos, Math.max(this.getMaxLength(), tupleTempContext.getMaxLength(pos)), tupleTempContext, this.mustTrim());
    }

    public static SingleColumnStringAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, int maxLength, boolean mustTrim, boolean hasShadowAttribute)
    {
        SingleColumnStringAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                maxLength, mustTrim, false);
        if (hasShadowAttribute)
        {
            SingleColumnStringAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName,
                    isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    maxLength, mustTrim, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnStringAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, int maxLength, boolean mustTrim, boolean hasShadowAttribute)
    {
        SingleColumnStringAttribute e;
        try
        {
            e = (SingleColumnStringAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnStringAttribute", false, hasShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.setMaxLength(maxLength);
        e.setMustTrim(mustTrim);
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        return rs.getString(pos);
    }

    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.stringValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapStringExtractorWithOffset(this.offHeapFieldOffset);
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        StringAttribute strAttr = (StringAttribute) this;
        for (int i = 0; i < data.size(); i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                int length = in.readInt();
                byte[] bytes = new byte[length];
                in.fullyRead(bytes);
                strAttr.setStringValue(data.get(i), new String(bytes, COLUMNAR_CHAR_SET));
            }
        }
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        StringAttribute strAttr = (StringAttribute) this;
        for (int i = 0; i < data.size(); i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                String v = strAttr.stringValueOf(data.get(i));
                byte[] bytes = v.getBytes(COLUMNAR_CHAR_SET);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        }
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
