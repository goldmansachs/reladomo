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

import com.gs.collections.api.set.primitive.IntSet;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.finder.*;
import com.gs.fw.common.mithra.finder.integer.*;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.util.ColumnInfo;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.fileparser.ColumnarInStream;
import com.gs.fw.common.mithra.util.fileparser.ColumnarOutStream;
import org.slf4j.Logger;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;


public class CalculatedIntegerAttribute<T> extends IntegerAttribute<T> implements IntExtractor<T, Integer>, SingleColumnAttribute<T>
{
    private NumericAttributeCalculator calculator;

    public CalculatedIntegerAttribute(NumericAttributeCalculator calculator)
    {
        this.calculator = calculator;
    }

    public String zGetTopOwnerClassName()
    {
        return this.calculator.getTopOwnerClassName();
    }

    public MithraObjectPortal getTopLevelPortal()
    {
        return this.calculator.getTopLevelPortal();
    }

    public MithraObjectPortal getOwnerPortal()
    {
        return this.calculator.getOwnerPortal();
    }

    public Object readResolve()
    {
        return this;
    }

    public AsOfAttribute[] getAsOfAttributes()
    {
        return this.calculator.getAsOfAttributes();
    }

    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    public NumericAttributeCalculator getCalculator()
    {
        return this.calculator;
    }

    public boolean isAttributeNull(T o)
    {
        return this.calculator.isAttributeNull(o);
    }

    public int intValueOf(T o)
    {
        return this.calculator.intValueOf(o);
    }

    public void setIntValue(T o, int newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueNull(Object o)
    {
        throw new RuntimeException("not implemented");
    }

    public String getFullyQualifiedLeftHandExpression(SqlQuery query)
    {
        return this.calculator.getFullyQualifiedCalculatedExpression(query);
    }

    @Override
    public void zAppendToString(ToStringContext toStringContext)
    {
        this.calculator.appendToString(toStringContext);
    }

    public String getColumnName()
    {
        throw new RuntimeException("method getColumName() can not be called on a calculated attribute");
    }

    public Class valueType()
    {
        return Integer.class;
    }

    public EqualityMapper constructEqualityMapper(Attribute right)
    {
        throw new RuntimeException("not implemented");
    }

    public boolean isSourceAttribute()
    {
        return false;
    }

    public void setValue(T o, Integer newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public void setValueUntil(T o, Integer newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    public void setUntil(T o, Integer newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o))
        {
            return HashUtil.NULL_HASH;
        }
        return HashUtil.hash(this.intValueOf(o));
    }

    public boolean valueEquals(T first, T second)
    {
        if (first == second)
        {
            return true;
        }
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = this.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.intValueOf(first) == this.intValueOf(second);
        }
        return true;
    }

    public <O> boolean valueEquals(T first, O second, Extractor<O, Integer> secondExtractor)
    {
        boolean firstNull = this.isAttributeNull(first);
        boolean secondNull = secondExtractor.isAttributeNull(second);
        if (firstNull != secondNull)
        {
            return false;
        }
        if (!firstNull)
        {
            return this.intValueOf(first) == ((IntExtractor) secondExtractor).intValueOf(second);
        }
        return true;
    }

    public OrderBy ascendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public OrderBy descendingOrderBy()
    {
        throw new RuntimeException("not implemented");
    }

    public Integer valueOf(T anObject)
    {
        return isAttributeNull(anObject) ? null : Integer.valueOf(this.intValueOf(anObject));
    }

    public void addDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    public Operation eq(int parameter)
    {
        return this.calculator.optimizedIntegerEq(parameter, this);
    }

    public Operation eq(long other)
    {
        if (other > Integer.MAX_VALUE || other < Integer.MIN_VALUE)
        {
            return new None(this);
        }
        return new IntegerEqOperation(this, (int) other);
    }

    public Operation notEq(int other)
    {
         return new IntegerNotEqOperation(this, other);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation in(IntSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(set.intIterator().next());
                break;
            default:
                op = new IntegerInOperation(this, set);
                break;
        }

        return op;
    }

    @Override
    public Operation in(org.eclipse.collections.api.set.primitive.IntSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new None(this);
                break;
            case 1:
                op = this.eq(set.intIterator().next());
                break;
            default:
                op = new IntegerInOperation(this, set);
                break;
        }

        return op;
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public Operation notIn(IntSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.intIterator().next());
                break;
            default:
                op = new IntegerNotInOperation(this, set);
                break;
        }

        return op;
    }

    @Override
    public Operation notIn(org.eclipse.collections.api.set.primitive.IntSet set)
    {
        Operation op;
        switch (set.size())
        {
            case 0:
                op = new All(this);
                break;
            case 1:
                op = this.notEq(set.intIterator().next());
                break;
            default:
                op = new IntegerNotInOperation(this, set);
                break;
        }

        return op;
    }

    public Operation greaterThan(int parameter)
    {
        return new IntegerGreaterThanOperation(this, parameter);
    }

    public Operation greaterThanEquals(int parameter)
    {
        return new IntegerGreaterThanEqualsOperation(this, parameter);
    }

    public Operation lessThan(int parameter)
    {
        return new IntegerLessThanOperation(this, parameter);
    }

    public Operation lessThanEquals(int parameter)
    {
        return new IntegerLessThanEqualsOperation(this, parameter);
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        throw new RuntimeException("should never get here");
    }

    // join operation:
    public Operation eq(IntegerAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation joinEq(IntegerAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation filterEq(IntegerAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    public Operation notEq(IntegerAttribute other)
    {
        throw new RuntimeException("not implemented");
    }

    // todo: rezaem: create an in operation that takes an extractor
// this is tricky, because it requires a new set that not only takes a hashcode strategy
// but has a new method: contains(object, hashStrategy)
/*
    public Operation in(List dataHolders, Extractor extractor)
    {
        return new InOperationWithExtractor(this, dataHolders, extractor);
    }
*/

    public int getUpdateCount()
    {
        return this.calculator.getUpdateCount();
    }

    public int getNonTxUpdateCount()
    {
        return this.calculator.getNonTxUpdateCount();
    }

    public void incrementUpdateCount()
    {
        this.calculator.incrementUpdateCount();
    }

    public void commitUpdateCount()
    {
        this.calculator.commitUpdateCount();
    }

    public void rollbackUpdateCount()
    {
        this.calculator.rollbackUpdateCount();
    }

    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        return other instanceof CalculatedIntegerAttribute && ((CalculatedIntegerAttribute)other).calculator.equals(this.calculator);
    }

    public int hashCode()
    {
        return super.hashCode() ^ this.calculator.hashCode();
    }

    public void generateMapperSql(AggregateSqlQuery query)
    {
        this.calculator.generateMapperSql(query);
    }

    public Operation zCreateMappedOperation()
    {
        return this.calculator.createMappedOperation();
    }

    public void forEach(DoubleProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(FloatProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(BigDecimalProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(LongProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void forEach(IntegerProcedure proc, Object o, Object context)
    {
        this.calculator.forEach(proc, o, context);
    }

    public void zAddDependentPortalsToSet(Set set)
    {
        this.calculator.addDependentPortalsToSet(set);
    }

    public void zAddDepenedentAttributesToSet(Set set)
    {
        this.calculator.addDepenedentAttributesToSet(set);
    }

    //SingleColumnAttribute - todo - try to remove the unused methods

    @Override
    public void appendColumnDefinition(StringBuilder sb, DatabaseType dt, Logger sqlLogger, boolean mustBeIndexable)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setColumnName(String columnName)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeValueToStream(T object, OutputStreamFormatter formatter, OutputStream os) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean verifyColumn(ColumnInfo info)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setSqlParameters(PreparedStatement ps, Object dataObject, int position, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public SingleColumnAttribute createTupleAttribute(int pos, TupleTempContext tupleTempContext)
    {
        return this.calculator.createTupleAttribute(pos, tupleTempContext);
    }

    @Override
    public Object readResultSet(ResultSet rs, int pos, DatabaseType databaseType, TimeZone timeZone) throws SQLException
    {
        throw new RuntimeException("not implemented");
    }

    public Operation defaultEq(int value)
    {
        return new IntegerEqOperation(this, value);
    }

    @Override
    public Attribute getSourceAttribute()
    {
        return this.getOwnerPortal().getFinder().getSourceAttribute();
    }

    @Override
    public void zDecodeColumnarData(List data, ColumnarInStream in) throws IOException
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void zEncodeColumnarData(List data, ColumnarOutStream out) throws IOException
    {
        throw new RuntimeException("not implemented");
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
