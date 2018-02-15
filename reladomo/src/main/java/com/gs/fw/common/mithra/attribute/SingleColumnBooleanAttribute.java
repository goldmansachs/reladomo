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

import com.gs.collections.api.set.primitive.BooleanSet;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BooleanProcedure;
import com.gs.fw.common.mithra.cache.offheap.OffHeapBooleanExtractorWithOffset;
import com.gs.fw.common.mithra.cache.offheap.OffHeapExtractor;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.booleanop.BooleanEqOperation;
import com.gs.fw.common.mithra.finder.booleanop.BooleanNotEqOperation;
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


public abstract class SingleColumnBooleanAttribute<T> extends BooleanAttribute<T> implements SingleColumnAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.booleanExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = 10228773948960016L;

    protected SingleColumnBooleanAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnBooleanAttribute() {}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, databaseType.getNullableBooleanJavaSqlType());
        }
        else
        {
            pps.setBoolean(pos, this.booleanValueOf((T) mithraDataObject));
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

    public Operation eq(boolean other)
    {
        return new BooleanEqOperation(this, other);
    }

    public Operation notEq(boolean other)
    {
        return new BooleanNotEqOperation(this, other);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(BooleanSet booleanSet)
    {
        if (booleanSet.isEmpty())
        {
            return new None(this);
        }
        if (booleanSet.size() == 1)
        {
            return this.eq(booleanSet.booleanIterator().next());
        }
        if (this.isNullable())
        {
            return new IsNotNullOperation(this);
        }
        return new All(this);
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.BooleanSet booleanSet)
    {
        if (booleanSet.isEmpty())
        {
            return new None(this);
        }
        if (booleanSet.size() == 1)
        {
            return this.eq(booleanSet.booleanIterator().next());
        }
        if (this.isNullable())
        {
            return new IsNotNullOperation(this);
        }
        return new All(this);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(BooleanSet booleanSet)
    {
       if (booleanSet.isEmpty())
        {
            return new All(this);
        }
        if (booleanSet.size() == 1)
        {
            return this.notEq(booleanSet.booleanIterator().next());
        }
        return new None(this); // notIn implies notNull, so notIn(true, false) means no match at all.
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.BooleanSet booleanSet)
    {
       if (booleanSet.isEmpty())
        {
            return new All(this);
        }
        if (booleanSet.size() == 1)
        {
            return this.notEq(booleanSet.booleanIterator().next());
        }
        return new None(this); // notIn implies notNull, so notIn(true, false) means no match at all.
    }

    // join operation:
    public Operation eq(BooleanAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(BooleanAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(BooleanAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(BooleanAttribute other)
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

    public void forEach(final BooleanProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.booleanValueOf(o), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append((mustBeIndexable || this.isNullable()) ? dt.getIndexableSqlDataTypeForBoolean() : dt.getSqlDataTypeForBoolean());
        appendNullable(sb, dt);
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        return info.getType() == Types.BIT || info.getType() == Types.BOOLEAN;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleBooleanAttribute(pos, this.isNullable(), tupleTempContext);
    }

    public static SingleColumnBooleanAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnBooleanAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, false);
        if (hasShadowAttribute)
        {
            SingleColumnBooleanAttribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots,
                busClassName, isNullablePrimitive, hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnBooleanAttribute generateInternal(String columnName, String uniqueAlias, String attributeName, String busClassNameWithDots,
            String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, boolean hasShadowAttribute)
    {
        SingleColumnBooleanAttribute e;
        try
        {
            e = (SingleColumnBooleanAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, "com/gs/fw/common/mithra/attribute/SingleColumnBooleanAttribute", false, hasShadowAttribute).newInstance();
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
        boolean result = rs.getBoolean(pos);
        if (rs.wasNull()) return null;
        return result;
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.booleanValueOf(object), os);
    }

    @Override
    public OffHeapExtractor zCreateOffHeapExtractor()
    {
        return new OffHeapBooleanExtractorWithOffset(this.offHeapFieldOffset);
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        BooleanAttribute attr = (BooleanAttribute) this;

        BitsInBytes values = new BitsInBytes(data.size());
        for(int i=0;i<data.size();i++)
        {
            if ((nulls == null || !nulls.get(i)) && attr.booleanValueOf(data.get(i)))
            {
                values.set(i);
            }
        }
        values.writeToOutputStream(out.getOutputStream());
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        BooleanAttribute attr = (BooleanAttribute) this;

        BitsInBytes values = BitsInBytes.readFromInputStream(in.getInputStream(), data.size());
        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                attr.setBooleanValue(data.get(i), values.get(i));
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
