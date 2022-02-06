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

import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.All;
import com.gs.fw.common.mithra.finder.NonPrimitiveNotInOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.bytearray.ByteArrayEqOperation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArrayInOperation;
import com.gs.fw.common.mithra.finder.bytearray.ByteArraySet;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
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


public abstract class SingleColumnByteArrayAttribute<Owner> extends ByteArrayAttribute<Owner> implements SingleColumnAttribute<Owner>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.byteArrayExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 403628467890246635L;

    protected SingleColumnByteArrayAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int maxLength)
    {
        super(maxLength);
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnByteArrayAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        Object obj = this.valueOf((Owner) mithraDataObject);

        if (obj != null)
        {
            byte[] value = (byte[]) obj;
            pps.setBytes(pos, value);
        }
        else
        {
            pps.setNull(pos, java.sql.Types.VARBINARY);
        }

    }

    public void setUniqueAlias(String uniqueAlias)
    {
        this.uniqueAlias = uniqueAlias;
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

    public Operation eq(byte[] other)
    {
        if (other == null)
        {
            return this.isNull();
        }
        return new ByteArrayEqOperation(this, other);
    }

    public Operation eq(ByteArrayAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(ByteArrayAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(ByteArrayAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation in(ByteArraySet set)
    {
        if (set.size() == 0)
        {
            return new None(this);
        }
        else if (set.size() == 1)
        {
            return this.eq((byte[])set.iterator().next());
        }
        return new ByteArrayInOperation(this, set);
    }

    public Operation notIn(ByteArraySet set)
    {
        if (set.size() == 0)
        {
            return new All(this);
        }
        else if (set.size() == 1)
        {
            return this.notEq((byte[])set.iterator().next());
        }
        return new NonPrimitiveNotInOperation(this, set);
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

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        int maxLen = 0;
        if (dt.varBinaryHasLength())
        {
            maxLen = this.getMaxLength();
            if (maxLen == Integer.MAX_VALUE)
            {
                maxLen = 255;
                sqlLogger.warn("MaxLength must be set for "+this.getAttributeName());
            }
        }
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForVarBinary());
        if (dt.varBinaryHasLength())
        {
            sb.append('(').append(maxLen).append(')');
        }
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.VARBINARY || info.getType() == Types.LONGVARBINARY || info.getType() == Types.BLOB;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleByteArrayAttribute(pos, this.getMaxLength(), tupleTempContext);
    }

    public static SingleColumnByteArrayAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, int maxLength, boolean hasShadowAttribute)
    {
        SingleColumnByteArrayAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                maxLength, false);
        if (hasShadowAttribute)
        {
            SingleColumnByteArrayAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                    busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    maxLength, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnByteArrayAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, int maxLength, boolean hasShadowAttribute)
    {
        SingleColumnByteArrayAttribute e;
        try
        {
            e = (SingleColumnByteArrayAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, "com/gs/fw/common/mithra/attribute/SingleColumnByteArrayAttribute", false, hasShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.setMaxLength(maxLength);
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        byte[] result = rs.getBytes(pos);
        if (rs.wasNull()) result = null;
        return result;
    }

    public void writeValueToStream(Owner object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.byteArrayValueOf(object), os);
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        ByteArrayAttribute attr = (ByteArrayAttribute) this;

        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                byte[] bytes = attr.byteArrayValueOf(data.get(i));
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        }
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        ByteArrayAttribute attr = (ByteArrayAttribute) this;
        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                int len = in.readInt();
                byte[] v = new byte[len];
                in.fullyRead(v);
                attr.setByteArrayValue(data.get(i), v);
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
