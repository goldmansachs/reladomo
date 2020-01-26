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

import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.aggregate.attribute.IntegerAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbsoluteValueCalculatorInteger;
import com.gs.fw.common.mithra.attribute.calculator.IntegerToStringCalculator;
import com.gs.fw.common.mithra.attribute.calculator.ModCalculatorInteger;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstAdditionCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstAdditionCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstAdditionCalculatorInteger;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstDivisionCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstDivisionCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstDivisionCalculatorInteger;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstMultiplicationCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstMultiplicationCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstMultiplicationCalculatorInteger;
import com.gs.fw.common.mithra.attribute.numericType.BigDecimalNumericType;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.FloatNumericType;
import com.gs.fw.common.mithra.attribute.numericType.IntegerNumericType;
import com.gs.fw.common.mithra.attribute.numericType.LongNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.IntegerNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.BigDecimalExtractor;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.IntExtractor;
import com.gs.fw.common.mithra.extractor.LongExtractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.IntegerOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MutableInteger;
import com.gs.fw.common.mithra.util.MutableNumber;
import com.gs.fw.common.mithra.util.Nullable;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public abstract class IntegerAttribute<T> extends PrimitiveNumericAttribute<T, Integer> implements com.gs.fw.finder.attribute.IntegerAttribute<T>,
        IntExtractor<T, Integer>, LongExtractor<T, Integer>, DoubleExtractor<T, Integer>, BigDecimalExtractor<T, Integer>
{
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = -4174808420300734279L;

    @Override
    public Class valueType()
    {
        return Integer.class;
    }

    @Override
    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeInt(this.intValueOf(o));
    }

    @Override
    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        this.setIntValue(o, in.readInt());
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Number) other).intValue());
    }

    /**
     * extracts the int values represented by this attribute from the list and puts them in a set.
     * If the int attribute is null, it's ignored.
     * @param list incoming list
     * @return the set
     */
    public MutableIntSet asGscSet(List<T> list)
    {
        return asSet(list, this, new IntHashSet());
    }

    /**
     * extracts the int values represented by this attribute from the list and adds them to the setToAppend.
     * If the int attribute is null, it's ignored.
     * @param list incoming list
     * @param setToAppend the set to append to
     */
    public MutableIntSet asSet(List<T> list, MutableIntSet setToAppend)
    {
        asSet(list, this, setToAppend);
        return setToAppend;
    }

    public abstract Operation eq(int other);

    public abstract Operation eq(long other);

    public abstract Operation notEq(int other);

    @Override
    public abstract Operation in(IntSet intSet);

    @Override
    public abstract Operation notIn(IntSet intSet);

    public abstract Operation greaterThan(int target);

    public abstract Operation greaterThanEquals(int target);

    public abstract Operation lessThan(int target);

    public abstract Operation lessThanEquals(int target);

    // join operation:
    /**
     * @deprecated use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    /**
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     * @deprecated use joinEq or filterEq instead
     */
    @Deprecated
    public abstract Operation eq(IntegerAttribute other);

    public abstract Operation joinEq(IntegerAttribute other);

    public abstract Operation filterEq(IntegerAttribute other);

    public abstract Operation notEq(IntegerAttribute other);

    @Override
    public void copyValueFrom(T dest, T src)
    {
        if (this.isAttributeNull(src))
        {
            this.setValueNull(dest);
        }
        else
        {
            this.setValue(dest, this.intValueOf(src));
        }
    }

    public Integer valueOf(T o)
    {
        return (this.isAttributeNull(o)) ? null : Integer.valueOf(this.intValueOf(o));
    }

    public void setValue(T o, Integer newValue)
    {
        this.setIntValue(o, newValue.intValue());
    }

    public int valueHashCode(T o)
    {
        return (this.isAttributeNull(o)) ? HashUtil.NULL_HASH : HashUtil.hash(this.intValueOf(o));
    }

    @Override
    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.intValueOf(first) == this.intValueOf(second);
    }

    @Override
    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Integer> secondExtractor)
    {
        return this.intValueOf(first) == ((IntExtractor) secondExtractor).intValueOf(second);
    }

    @Override
    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new IntegerOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    @Override
    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new IntegerOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final IntExtractor integerExtractor = (IntExtractor) extractor;
        final IntSet set = asSet(objects, integerExtractor);
        return this.in(set);
    }

    private IntSet asSet(List objects, IntExtractor integerExtractor)
    {
        return this.asSet(objects, integerExtractor, new IntHashSet());
    }

    private MutableIntSet asSet(List objects, IntExtractor integerExtractor, MutableIntSet intSet)
    {
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!integerExtractor.isAttributeNull(o))
            {
                intSet.add(integerExtractor.intValueOf(o));
            }
        }
        return intSet;
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final IntExtractor integerExtractor = (IntExtractor) extractor;
        final MutableIntSet set = new IntHashSet();
        for (Object o : objects)
        {
            if (!integerExtractor.isAttributeNull(o))
            {
                set.add(integerExtractor.intValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        IntExtractor integerExtractor = (IntExtractor) extractor;
        MutableIntSet set = new IntHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!integerExtractor.isAttributeNull(o))
            {
                set.add(integerExtractor.intValueOf(o));
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
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE || Math.floor(value) != value)
        {
            throw new ParseException("Incorrect int value " + value + " on line " +
                    lineNumber + " for attribute " + this.getClass().getName(), lineNumber);
        }
        this.setIntValue(data, (int) value);
    }

    @Override
    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setIntValue(data, Integer.parseInt(value));
    }

    @Override
    public void setValueUntil(T o, Integer newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.intValue(), exclusiveUntil);
    }

    protected void setUntil(Object o, int i, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public StringAttribute<T> convertToStringAttribute()
    {
        return new CalculatedStringAttribute(new IntegerToStringCalculator(this));
    }

    public long longValueOf(T o)
    {
        return this.intValueOf(o);
    }

    public void setLongValue(T o, long newValue)
    {
        this.setIntValue(o, (int) newValue);
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.intValueOf(object));
    }

    @Override
    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        int firstValue = this.intValueOf((T) dataObjects[0]);
        MutableIntSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            int nextValue = this.intValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new IntHashSet();
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
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        MutableInteger obj;
        int i = rs.getInt(resultSetPosition);
        if (rs.wasNull())
        {
            obj = new MutableInteger();
        }
        else
        {
            obj = new MutableInteger(i);
        }
        data.setValueAt(dataPosition, obj);
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        int i = rs.getInt(resultSetPosition);
        if (rs.wasNull())
        {
            tempArray[0] = null;
        }
        else
        {
            tempArray[0] = i;
        }
        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalArgumentException e)
        {
            if (tempArray[0] == null && method.getParameterTypes()[0].isPrimitive())
            {
                throw new MithraNullPrimitiveException("Aggregate result returned null for " + method.getName() + " of class " + object.getClass().getName() + " which cannot be set as primitive", e);
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
    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableInteger((Integer) value));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeInt(((MutableNumber)valueWrappedInNullable).intValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableInteger(in.readInt());
    }

    public NumericType getNumericType()
    {
        return IntegerNumericType.getInstance();
    }

    public void increment(Object o, double increment)
    {
        this.setDoubleValue((T) o, this.doubleValueOf((T) o) + increment);
    }

    public void incrementUntil(Object o, double increment, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public void increment(T o, BigDecimal increment)
    {
        this.setBigDecimalValue(o, this.bigDecimalValueOf(o).add(increment));
    }

    public void incrementUntil(T o, BigDecimal increment, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    // ByteAttribute operands

    public IntegerAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createAdditionCalculator(this, (ByteAttribute) attribute));
    }

    public IntegerAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createSubtractionCalculator(this, (ByteAttribute) attribute));
    }

    public IntegerAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createMultiplicationCalculator(this, (ByteAttribute) attribute));
    }

    public IntegerAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createDivisionCalculator(this, (ByteAttribute) attribute));
    }

    // ShortAttribute operands

    public IntegerAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createAdditionCalculator(this, (ShortAttribute) attribute));
    }

    public IntegerAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createSubtractionCalculator(this, (ShortAttribute) attribute));
    }

    public IntegerAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createMultiplicationCalculator(this, (ShortAttribute) attribute));
    }

    public IntegerAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createDivisionCalculator(this, (ShortAttribute) attribute));
    }

    // IntegerAttribute operands

    public IntegerAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createAdditionCalculator(this, (IntegerAttribute) attribute));
    }

    public IntegerAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createSubtractionCalculator(this, (IntegerAttribute) attribute));
    }

    public IntegerAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createMultiplicationCalculator(this, (IntegerAttribute) attribute));
    }

    public IntegerAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return IntegerNumericType.getInstance().createCalculatedAttribute(IntegerNumericType.getInstance().createDivisionCalculator(this, (IntegerAttribute) attribute));
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
    public IntegerAttribute absoluteValue()
    {
        return new CalculatedIntegerAttribute(new AbsoluteValueCalculatorInteger(this));
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
        return databaseType.getSqlDataTypeForInt();
    }

    public IntegerAttribute mod(int divisor)
    {
        return new CalculatedIntegerAttribute(new ModCalculatorInteger(this, divisor));
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new IntegerNullUpdateWrapper(this, data);
    }

    @Override
    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((IntegerAttribute) left).intValueOf(original));
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
        return this.eq(this.intValueOf(result));
    }

    public double doubleValueOf(T o)
    {
        return intValueOf(o);
    }

    public void setDoubleValue(T o, double newValue)
    {
        this.setIntValue(o, (int) newValue);
    }

    public BigDecimal bigDecimalValueOf(T o)
    {
        return BigDecimal.valueOf(intValueOf(o));
    }

    public void setBigDecimalValue(T o, BigDecimal newValue)
    {
        this.setIntValue(o, newValue.intValue());
    }

    public IntegerAttribute plus(int addend)
    {
        return new CalculatedIntegerAttribute(new ConstAdditionCalculatorInteger(this, addend));
    }

    public IntegerAttribute minus(int addend)
    {
        return this.plus(-addend);
    }

    public IntegerAttribute times(int multiplicand)
    {
        return new CalculatedIntegerAttribute(new ConstMultiplicationCalculatorInteger(this, multiplicand));
    }

    public IntegerAttribute dividedBy(int divisor)
    {
        return new CalculatedIntegerAttribute(new ConstDivisionCalculatorInteger(this, divisor));
    }

    public DoubleAttribute plus(double addend)
    {
        return new CalculatedDoubleAttribute(new ConstAdditionCalculatorDouble(this, addend));
    }

    public DoubleAttribute minus(double addend)
    {
        return this.plus(-addend);
    }

    public DoubleAttribute times(double multiplicand)
    {
        return new CalculatedDoubleAttribute(new ConstMultiplicationCalculatorDouble(this, multiplicand));
    }

    public DoubleAttribute dividedBy(double divisor)
    {
        return new CalculatedDoubleAttribute(new ConstDivisionCalculatorDouble(this, divisor));
    }

    public BigDecimalAttribute plus(BigDecimal addend)
    {
        return new CalculatedBigDecimalAttribute(new ConstAdditionCalculatorBigDecimal(this, addend));
    }

    public BigDecimalAttribute minus(BigDecimal addend)
    {
        return this.plus(addend.negate());
    }

    public BigDecimalAttribute times(BigDecimal multiplicand)
    {
        return new CalculatedBigDecimalAttribute(new ConstMultiplicationCalculatorBigDecimal(this, multiplicand));
    }

    public BigDecimalAttribute dividedBy(BigDecimal divisor)
    {
        return new CalculatedBigDecimalAttribute(new ConstDivisionCalculatorBigDecimal(this, divisor));
    }

    @Override
    public IntegerAggregateAttribute min()
    {
        return new IntegerAggregateAttribute(new MinCalculatorNumeric(this));
    }

    @Override
    public IntegerAggregateAttribute max()
    {
        return new IntegerAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public IntegerAggregateAttribute sum()
    {
        return new IntegerAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public IntegerAggregateAttribute avg()
    {
        return new IntegerAggregateAttribute(new AverageCalculatorNumeric(this));
    }

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, T reladomoObject) throws IOException
    {
        writer.writeInt(context, this.getAttributeName(), this.intValueOf(reladomoObject));
    }
}