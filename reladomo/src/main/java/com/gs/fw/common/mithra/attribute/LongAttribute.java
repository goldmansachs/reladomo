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

import com.gs.collections.api.set.primitive.LongSet;
import com.gs.collections.api.set.primitive.MutableLongSet;
import com.gs.collections.impl.set.mutable.primitive.LongHashSet;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.aggregate.attribute.LongAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbsoluteValueCalculatorLong;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.StandardDeviationCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.numericType.BigDecimalNumericType;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.FloatNumericType;
import com.gs.fw.common.mithra.attribute.numericType.LongNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.LongNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.LongExtractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.LongOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public abstract class LongAttribute<T> extends PrimitiveNumericAttribute<T, Long> implements com.gs.fw.finder.attribute.LongAttribute<T>, LongExtractor<T, Long>
{
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = -302964650737340928L;

    @Override
    public Class valueType()
    {
        return Long.class;
    }

    @Override
    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeLong(this.longValueOf(o));
    }

    @Override
    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        this.setLongValue(o, in.readLong());
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Number) other).longValue());
    }

    public abstract Operation eq(long other);

    public abstract Operation notEq(long other);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public abstract Operation in(LongSet longSet);

    @Override
    public abstract Operation in(org.eclipse.collections.api.set.primitive.LongSet longSet);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public abstract Operation notIn(LongSet longSet);

    @Override
    public abstract Operation notIn(org.eclipse.collections.api.set.primitive.LongSet longSet);

    public abstract Operation greaterThan(long target);

    public abstract Operation greaterThanEquals(long target);

    public abstract Operation lessThan(long target);

    public abstract Operation lessThanEquals(long target);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    @Deprecated
    public abstract Operation eq(LongAttribute other);

    public abstract Operation joinEq(LongAttribute other);

    public abstract Operation filterEq(LongAttribute other);

    public abstract Operation notEq(LongAttribute other);

    public Long valueOf(T o)
    {
        return isAttributeNull(o) ? null : Long.valueOf(this.longValueOf(o));
    }

    public void setValue(T o, Long newValue)
    {
        this.setLongValue(o, newValue.longValue());
    }

    public int valueHashCode(T o)
    {
        return (this.isAttributeNull(o)) ? HashUtil.NULL_HASH : HashUtil.hash(this.longValueOf(o));
    }

    @Override
    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.longValueOf(first) == this.longValueOf(second);
    }

    @Override
    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Long> secondExtractor)
    {
        return this.longValueOf(first) == ((LongExtractor) secondExtractor).longValueOf(second);
    }

    @Override
    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new LongOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    @Override
    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new LongOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final LongExtractor longExtractor = (LongExtractor) extractor;
        final MutableLongSet set = new LongHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!longExtractor.isAttributeNull(o))
            {
                set.add(longExtractor.longValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final LongExtractor longExtractor = (LongExtractor) extractor;
        final MutableLongSet set = new LongHashSet();
        for (Object o : objects)
        {
            if (!longExtractor.isAttributeNull(o))
            {
                set.add(longExtractor.longValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        LongExtractor longExtractor = (LongExtractor) extractor;
        MutableLongSet set = new LongHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!longExtractor.isAttributeNull(o))
            {
                set.add(longExtractor.longValueOf(o));
                if (set.size() > maxInClause)
                {
                    return new None(this);
                }
            }
        }
        return this.in(set);
    }

    @Override
    public void parseNumberAndSet(double value, T data, int lineNumber) throws ParseException
    {
        if (value > Long.MAX_VALUE || value < Long.MIN_VALUE || Math.floor(value) != value)
        {
            throw new ParseException("Incorrect long value " + value + " on line " +
                    lineNumber + " for attribute " + this.getClass().getName(), lineNumber);
        }
        this.setLongValue(data, (long) value);
    }

    public void populateValueFromResultSet(int position, ResultSet rs, Object[] values)
            throws SQLException
    {
        values[position] = Long.valueOf(rs.getLong(position));
    }

    @Override
    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setLongValue(data, Long.parseLong(value));
    }

    @Override
    public void setValueUntil(T o, Long newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.longValue(), exclusiveUntil);
    }

    protected void setUntil(Object o, long l, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.longValueOf(object));
    }

    @Override
    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        long firstValue = this.longValueOf((T) dataObjects[0]);
        MutableLongSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            long nextValue = this.longValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new LongHashSet();
                set.add(firstValue);
                set.add(nextValue);
            }
        }
        if (set != null)
        {
            return set.size();
        }
        return 1;
    }

    @Override
    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableLong((Long) value));
    }

    public NumericType getNumericType()
    {
        return LongNumericType.getInstance();
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        long l = rs.getLong(resultSetPosition);
        if (rs.wasNull())
        {
            tempArray[0] = null;
        }
        else
        {
            tempArray[0] = l;
        }
        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalArgumentException e)
        {
            if (tempArray[0] == null && method.getParameterTypes()[0].isPrimitive())
            {
                throw new MithraBusinessException("Aggregate result returned null for " + method.getName() + " of class " + object.getClass().getName() + " which cannot be set as primitive", e);
            }
            throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + "  of class " + object.getClass().getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
        }
        catch (InvocationTargetException e)
        {
            throw new MithraBusinessException("Error invoking method " + method.getName() + "of class " + object.getClass().getName(), e);
        }
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        MutableLong obj;
        long i = rs.getLong(resultSetPosition);
        if (rs.wasNull())
        {
            obj = new MutableLong();
        }
        else
        {
            obj = new MutableLong(i);
        }
        data.setValueAt(dataPosition, obj);
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeLong(((MutableNumber)valueWrappedInNullable).longValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableLong(in.readLong());
    }
    // ByteAttribute operands

    public LongAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createAdditionCalculator(this, (ByteAttribute) attribute));
    }

    public LongAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createSubtractionCalculator(this, (ByteAttribute) attribute));
    }

    public LongAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createMultiplicationCalculator(this, (ByteAttribute) attribute));
    }

    public LongAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createDivisionCalculator(this, (ByteAttribute) attribute));
    }

    // ShortAttribute operands

    public LongAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createAdditionCalculator(this, (ShortAttribute) attribute));
    }

    public LongAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createSubtractionCalculator(this, (ShortAttribute) attribute));
    }

    public LongAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createMultiplicationCalculator(this, (ShortAttribute) attribute));
    }

    public LongAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createDivisionCalculator(this, (ShortAttribute) attribute));
    }

    // IntegerAttribute operands

    public LongAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createAdditionCalculator(this, (IntegerAttribute) attribute));
    }

    public LongAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createSubtractionCalculator(this, (IntegerAttribute) attribute));
    }

    public LongAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createMultiplicationCalculator(this, (IntegerAttribute) attribute));
    }

    public LongAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createDivisionCalculator(this, (IntegerAttribute) attribute));
    }

    // LongAttribute operands

    public LongAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createAdditionCalculator(this, (LongAttribute) attribute));
    }

    public LongAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createSubtractionCalculator(this, (LongAttribute) attribute));
    }

    public LongAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createMultiplicationCalculator(this, (LongAttribute) attribute));
    }

    public LongAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return LongNumericType.getInstance().createCalculatedAttribute(LongNumericType.getInstance().createDivisionCalculator(this, (LongAttribute) attribute));
    }

    // FloatAttribute operands

    public FloatAttribute plus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createAdditionCalculator(this, (FloatAttribute) attribute));
    }

    public FloatAttribute minus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createSubtractionCalculator(this, (FloatAttribute) attribute));
    }

    public FloatAttribute times(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createMultiplicationCalculator(this, (FloatAttribute) attribute));
    }

    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createDivisionCalculator(this, (FloatAttribute) attribute));
    }

    // DoubleAttribute operands

    public DoubleAttribute plus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createAdditionCalculator(this, (DoubleAttribute) attribute));
    }

    public DoubleAttribute minus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createSubtractionCalculator(this, (DoubleAttribute) attribute));
    }

    public DoubleAttribute times(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createMultiplicationCalculator(this, (DoubleAttribute) attribute));
    }

    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createDivisionCalculator(this, (DoubleAttribute) attribute));
    }

    // BigDecimalAttribute operands

    public BigDecimalAttribute plus(BigDecimalAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, attribute));
    }

    public BigDecimalAttribute minus(BigDecimalAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, attribute));
    }

    public BigDecimalAttribute times(BigDecimalAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, attribute));
    }

    public BigDecimalAttribute dividedBy(BigDecimalAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, attribute, attribute.getScale()));
    }


    @Override
    public LongAttribute absoluteValue()
    {
        return new CalculatedLongAttribute(new AbsoluteValueCalculatorLong(this));
    }

    public NumericAttribute zDispatchAddTo(NumericAttribute firstAddend)
    {
        return firstAddend.plus(this);
    }

    public NumericAttribute zDispatchSubtractFrom(NumericAttribute minuend)
    {
        return minuend.minus(this);
    }

    public NumericAttribute zDispatchMultiplyBy(NumericAttribute firstMultiplicand)
    {
        return firstMultiplicand.times(this);
    }

    public NumericAttribute zDispatchDivideInto(NumericAttribute divdend)
    {
        return divdend.dividedBy(this);
    }

    @Override
    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForLong();
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new LongNullUpdateWrapper(this, data);
    }

    @Override
    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((LongAttribute)left).longValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(0);
    }

    @Override
    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.longValueOf(result));
    }

    @Override
    public LongAggregateAttribute min()
    {
        return new LongAggregateAttribute(new MinCalculatorNumeric(this));
    }

    @Override
    public LongAggregateAttribute max()
    {
        return new LongAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public LongAggregateAttribute sum()
    {
        return new LongAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public LongAggregateAttribute avg()
    {
        return new LongAggregateAttribute(new AverageCalculatorNumeric(this));
    }
}
