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
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.asm.ExtractorWriter;
import com.gs.fw.common.mithra.finder.AtomicSelfNotEqualityOperation;
import com.gs.fw.common.mithra.finder.NonPrimitiveEqOperation;
import com.gs.fw.common.mithra.finder.NonPrimitiveNotEqOperation;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.SqlQuery;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalGreaterThanEqualsOperation;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalGreaterThanOperation;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalLessThanEqualsOperation;
import com.gs.fw.common.mithra.finder.bigdecimal.BigDecimalLessThanOperation;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.fileparser.BitsInBytes;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.eclipse.collections.api.set.primitive.DoubleSet;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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


public abstract class SingleColumnBigDecimalAttribute<T> extends BigDecimalAttribute<T> implements SingleColumnAttribute<T>
{
    private static final ExtractorWriter extractorWriter = ExtractorWriter.bigDecimalExtractorWriter();

    private transient String columnName;
    private transient String uniqueAlias;

    private static final long serialVersionUID = -2531503578551321292L;

    protected SingleColumnBigDecimalAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnBigDecimalAttribute(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int precision, int scale)
    {
        this.columnName = columnName;
        this.uniqueAlias = uniqueAlias;
        setPrecision(precision);
        setScale(scale);
        this.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
    }

    protected SingleColumnBigDecimalAttribute(){}

    public void setSqlParameters(PreparedStatement pps, Object mithraDataObject, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        if (this.isAttributeNull((T) mithraDataObject))
        {
            pps.setNull(pos, java.sql.Types.DECIMAL);
        }
        else
        {
            pps.setBigDecimal(pos, this.bigDecimalValueOf((T) mithraDataObject));
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

    public Operation eq(BigDecimal parameter)
    {
        return (parameter == null) ? this.isNull() : new NonPrimitiveEqOperation(this, parameter);
    }

    public Operation notEq(BigDecimal parameter)
    {
        return (parameter == null) ? this.isNotNull() : new NonPrimitiveNotEqOperation(this, parameter);
    }

    public Operation greaterThan(BigDecimal parameter)
    {
        return new BigDecimalGreaterThanOperation(this, parameter);
    }

    public Operation greaterThanEquals(BigDecimal parameter)
    {
        return new BigDecimalGreaterThanEqualsOperation(this, parameter);
    }

    public Operation lessThan(BigDecimal target)
    {
        return new BigDecimalLessThanOperation(this, target);
    }

    public Operation lessThanEquals(BigDecimal parameter)
    {
        return new BigDecimalLessThanEqualsOperation(this, parameter);
    }

    public Operation eq(double other)
    {
        return eq(createBigDecimalFromDouble(other));
    }

    public Operation notEq(double other)
    {
        return notEq(createBigDecimalFromDouble(other));
    }

    @Override
    public Operation in(DoubleSet doubleSet)
    {
        return this.in(createBigDecimalSetFromDoubleSet(doubleSet));
    }

    @Override
    public Operation notIn(DoubleSet doubleSet)
    {
        return this.notIn(createBigDecimalSetFromDoubleSet(doubleSet));
    }

    public Operation greaterThan(double target)
    {
        return greaterThan(createBigDecimalFromDouble(target));
    }

    public Operation greaterThanEquals(double target)
    {
        return greaterThanEquals(createBigDecimalFromDouble(target));
    }

    public Operation lessThan(double target) 
    {
        return lessThan(createBigDecimalFromDouble(target));
    }

    public Operation lessThanEquals(double target)
    {
        return lessThanEquals(createBigDecimalFromDouble(target));
    }

    public Operation eq(BigDecimalAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation joinEq(BigDecimalAttribute other)
    {
        return this.joinEqWithSourceAndAsOfCheck(other);
    }

    public Operation filterEq(BigDecimalAttribute other)
    {
        return this.filterEqWithCheck(other);
    }

    public Operation notEq(BigDecimalAttribute other)
    {
        if (this.getOwnerPortal().equals(other.getOwnerPortal()))
        {
            return new AtomicSelfNotEqualityOperation(this, other);
        }
        throw new RuntimeException("Non-equality join is not yet supported");
    }

    public void forEach(BigDecimalProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
        }
        else
        {
            proc.execute(this.bigDecimalValueOf(o), context);
        }
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

    public static SingleColumnBigDecimalAttribute generate(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive,
            boolean hasBusDate, RelatedFinder relatedFinder, Map<String, Object> properties,
            boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, int precision, int scale, boolean hasShadowAttribute)
    {
        SingleColumnBigDecimalAttribute e = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName, isNullablePrimitive,
                hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, precision, scale, false);
        if (hasShadowAttribute)
        {
            Attribute shadow = generateInternal(columnName, uniqueAlias, attributeName, busClassNameWithDots, busClassName, isNullablePrimitive,
                hasBusDate, relatedFinder, properties, isTransactional, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition, precision, scale, true);
            e.setShadowAttribute(shadow);
        }
        return e;
    }

    private static SingleColumnBigDecimalAttribute generateInternal(String columnName, String uniqueAlias, String attributeName,
            String busClassNameWithDots, String busClassName, boolean isNullablePrimitive, boolean hasBusDate, RelatedFinder relatedFinder,
            Map<String, Object> properties, boolean isTransactional, boolean isOptimistic, int offHeapFieldOffset, int offHeapNullBitsOffset, int offHeapNullBitsPosition, int precision, int scale, boolean isShadowAttribute)
    {
        SingleColumnBigDecimalAttribute e;
        try
        {
            e = (SingleColumnBigDecimalAttribute) extractorWriter.createClass(attributeName, isNullablePrimitive, hasBusDate, busClassNameWithDots,
                    busClassName, isOptimistic, offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition,
                    "com/gs/fw/common/mithra/attribute/SingleColumnBigDecimalAttribute", false, isShadowAttribute).newInstance();
        }
        catch (Exception excp)
        {
            throw new RuntimeException("could not create class for "+attributeName+" in "+busClassName, excp);
        }
        e.columnName = columnName;
        e.uniqueAlias = uniqueAlias;
        e.setPrecision(precision);
        e.setScale(scale);
        e.setAll(attributeName, busClassNameWithDots, busClassName, isNullablePrimitive, relatedFinder, properties, isTransactional);
        e.setOffHeapOffsets(offHeapFieldOffset, offHeapNullBitsOffset, offHeapNullBitsPosition);
        return e;
    }

    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return new SingleColumnTupleBigDecimalAttribute(pos, this.isNullable(), tupleTempContext, this.getPrecision(), this.getScale());
    }

    public boolean verifyColumn(ColumnInfo info)
    {
        if (info.isNullable() != this.isNullable()) return false;
        
        return info.getType() == Types.DECIMAL && info.getPrecision() == this.getPrecision() && info.getScale() == this.getScale();
    }

    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        sb.append(this.columnName).append(' ').append(dt.getSqlDataTypeForBigDecimal()).append('(').append(getPrecision()).append(',').append(getScale()).append(')');
        appendNullable(sb, dt);
    }

    public void forEach(final IntegerProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.bigDecimalValueOf(o).intValue(), context);
        }
    }

    public void forEach(final DoubleProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.bigDecimalValueOf(o).doubleValue(), context);
        }
    }

    public void forEach(final LongProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.bigDecimalValueOf(o).longValue(), context);
        }
    }

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        if (!checkForNull(proc, o, context))
        {
            proc.execute(this.bigDecimalValueOf(o).floatValue(), context);
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this.getSerializationReplacement();
    }

    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        return rs.getBigDecimal(pos);
    }

    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        formatter.write(this.bigDecimalValueOf(object), os);
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        BitsInBytes nulls = out.encodeColumnarNull(data, this);
        BigDecimalAttribute attr = (BigDecimalAttribute) this;
        ObjectOutputStream oos = new ObjectOutputStream(out.getOutputStream());

        for(int i=0;i<data.size();i++)
        {
            if (nulls == null || !nulls.get(i))
            {
                BigDecimal v = attr.bigDecimalValueOf(data.get(i));
                oos.writeObject(v);
            }
        }
        oos.flush();
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        BitsInBytes nulls = in.decodeColumnarNull(this, data);
        BigDecimalAttribute attr = (BigDecimalAttribute) this;

        try
        {
            ObjectInputStream ois = new ObjectInputStream(in.getInputStream());
            for(int i=0;i<data.size();i++)
            {
                if (nulls == null || !nulls.get(i))
                {
                    attr.setBigDecimalValue(data.get(i), (BigDecimal) ois.readObject());
                }
            }
        }
        catch (ClassNotFoundException e)
        {
            throw new IOException("Bad stream", e);
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
