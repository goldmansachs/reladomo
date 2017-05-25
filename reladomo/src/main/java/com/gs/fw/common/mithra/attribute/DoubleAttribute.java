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

import com.gs.collections.api.set.primitive.DoubleSet;
import com.gs.collections.api.set.primitive.MutableDoubleSet;
import com.gs.collections.impl.set.mutable.primitive.DoubleHashSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbsoluteValueCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstAdditionCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstAdditionCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstDivisionCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstDivisionCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstMultiplicationCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstMultiplicationCalculatorDouble;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.attribute.numericType.BigDecimalNumericType;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.DoubleNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.BigDecimalExtractor;
import com.gs.fw.common.mithra.extractor.DoubleExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.DoubleOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

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

public abstract class DoubleAttribute<T> extends PrimitiveNumericAttribute<T, Double>
        implements com.gs.fw.finder.attribute.DoubleAttribute<T>, DoubleExtractor<T, Double>, BigDecimalExtractor<T, Double>
{

    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = -8510176674287064350L;

    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Number) other).doubleValue());
    }

    public abstract Operation eq(double other);

    public abstract Operation notEq(double other);

    @Override
    public abstract Operation in(DoubleSet doubleSet);

    @Override
    public abstract Operation notIn(DoubleSet doubleSet);

    public abstract Operation greaterThan(double target);

    public abstract Operation greaterThanEquals(double target);

    public abstract Operation lessThan(double target);

    public abstract Operation lessThanEquals(double target);

    public abstract void forEach(DoubleProcedure proc, T o, Object context);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(DoubleAttribute other);

    public abstract Operation joinEq(DoubleAttribute other);

    public abstract Operation filterEq(DoubleAttribute other);

    public abstract Operation notEq(DoubleAttribute other);

    public Double valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return new Double(this.doubleValueOf(o));
    }

    public void setValue(T o, Double newValue)
    {
        this.setDoubleValue(o, newValue.doubleValue());
    }

    public int valueHashCode(T o)
    {
        if (this.isAttributeNull(o)) return HashUtil.NULL_HASH;
        return HashUtil.hash(this.doubleValueOf(o));
    }

    public BigDecimal bigDecimalValueOf(T o)
    {
        return BigDecimal.valueOf(doubleValueOf(o));
    }

    public void setBigDecimalValue(T o, BigDecimal newValue)
    {
        this.setDoubleValue(o, newValue.doubleValue());
    }

    public void increment(Object o, double increment)
    {
        this.setDoubleValue((T) o, this.doubleValueOf((T) o) + increment);
    }

    public void increment(Object o, BigDecimal increment)
    {
        this.setBigDecimalValue((T) o, this.bigDecimalValueOf((T) o).add(increment));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.doubleValueOf(first) == this.doubleValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Double> secondExtractor)
    {
        return this.doubleValueOf(first) == ((DoubleExtractor) secondExtractor).doubleValueOf(second);
    }

    public Class valueType()
    {
        return Double.class;
    }

    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeDouble(this.doubleValueOf(o));
    }

    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException, ClassNotFoundException
    {
        this.setDoubleValue(o, in.readDouble());
    }

    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new DoubleOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new DoubleOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final DoubleExtractor doubleExtractor = (DoubleExtractor) extractor;
        final MutableDoubleSet set = new DoubleHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!doubleExtractor.isAttributeNull(o))
            {
                set.add(doubleExtractor.doubleValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final DoubleExtractor doubleExtractor = (DoubleExtractor) extractor;
        final MutableDoubleSet set = new DoubleHashSet();
        for (Object o : objects)
        {
            if (!doubleExtractor.isAttributeNull(o))
            {
                set.add(doubleExtractor.doubleValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        DoubleExtractor doubleExtractor = (DoubleExtractor) extractor;
        MutableDoubleSet set = new DoubleHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!doubleExtractor.isAttributeNull(o))
            {
                set.add(doubleExtractor.doubleValueOf(o));
                if (set.size() > maxInClause)
                {
                    return new None(this);
                }
            }
        }
        return this.in(set);
    }

    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setDoubleValue(data, Double.parseDouble(value));
    }

    public void parseNumberAndSet(double value, T data, int lineNumber) throws ParseException
    {
        this.setDoubleValue(data, value);
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        double d = rs.getDouble(resultSetPosition);
        if (rs.wasNull())
        {
            tempArray[0] = null;
        }
        else
        {
            tempArray[0] = d;
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

    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, AggregateData data, TimeZone databaseTimezone, DatabaseType dt)
            throws SQLException
    {
        MutableDouble obj;
        double d = rs.getDouble(resultSetPosition);
        if (rs.wasNull())
        {
            obj = new MutableDouble();
        }
        else
        {
            obj = new MutableDouble(d);
        }
        data.setValueAt(dataPosition, obj);
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeDouble(((MutableNumber)valueWrappedInNullable).doubleValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableDouble(in.readDouble());
    }

    public void incrementUntil(Object o, double increment, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public void incrementUntil(Object o, BigDecimal increment, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public void setValueUntil(T o, Double newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.doubleValue(), exclusiveUntil);
    }

    public void setUntil(Object o, double v, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.doubleValueOf(object));
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        double firstValue = this.doubleValueOf((T) dataObjects[0]);
        MutableDoubleSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            double nextValue = this.doubleValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new DoubleHashSet();
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
        data.setValueAt((position), new MutableDouble((Double) value));
    }

    public NumericType getNumericType()
    {
        return DoubleNumericType.getInstance();
    }

    // ByteAttribute operands

    public DoubleAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createAdditionCalculator(this, (ByteAttribute) attribute));
    }

    public DoubleAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createSubtractionCalculator(this, (ByteAttribute) attribute));
    }

    public DoubleAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createMultiplicationCalculator(this, (ByteAttribute) attribute));
    }

    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createDivisionCalculator(this, (ByteAttribute) attribute));
    }

    // ShortAttribute operands

    public DoubleAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createAdditionCalculator(this, (ShortAttribute) attribute));
    }

    public DoubleAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createSubtractionCalculator(this, (ShortAttribute) attribute));
    }

    public DoubleAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createMultiplicationCalculator(this, (ShortAttribute) attribute));
    }

    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createDivisionCalculator(this, (ShortAttribute) attribute));
    }

    // IntegerAttribute operands

    public DoubleAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createAdditionCalculator(this, (IntegerAttribute) attribute));
    }

    public DoubleAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createSubtractionCalculator(this, (IntegerAttribute) attribute));
    }

    public DoubleAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createMultiplicationCalculator(this, (IntegerAttribute) attribute));
    }

    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createDivisionCalculator(this, (IntegerAttribute) attribute));
    }

    // LongAttribute operands

    public DoubleAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createAdditionCalculator(this, (LongAttribute) attribute));
    }

    public DoubleAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createSubtractionCalculator(this, (LongAttribute) attribute));
    }

    public DoubleAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createMultiplicationCalculator(this, (LongAttribute) attribute));
    }

    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createDivisionCalculator(this, (LongAttribute) attribute));
    }

    // FloatAttribute operands

    public DoubleAttribute plus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createAdditionCalculator(this, (FloatAttribute) attribute));
    }

    public DoubleAttribute minus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createSubtractionCalculator(this, (FloatAttribute) attribute));
    }

    public DoubleAttribute times(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createMultiplicationCalculator(this, (FloatAttribute) attribute));
    }

    public DoubleAttribute dividedBy(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return DoubleNumericType.getInstance().createCalculatedAttribute(DoubleNumericType.getInstance().createDivisionCalculator(this, (FloatAttribute) attribute));
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
    // absoluteValue

    public DoubleAttribute absoluteValue()
    {
        return new CalculatedDoubleAttribute(new AbsoluteValueCalculatorDouble(this));
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

    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForDouble();
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new DoubleNullUpdateWrapper(this, data);
    }

    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((DoubleAttribute)left).doubleValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(0.0);
    }

    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.doubleValueOf(result));
    }

    public DoubleAttribute plus(int addend)
    {
        return plus((double)addend);
    }

    public DoubleAttribute minus(int addend)
    {
        return this.plus(-(double)addend);
    }

    public DoubleAttribute times(int multiplicand)
    {
        return times((double) multiplicand);
    }

    public DoubleAttribute dividedBy(int divisor)
    {
        return dividedBy((double)divisor);
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


    public DoubleAggregateAttribute min()
    {
        return new DoubleAggregateAttribute(new MinCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute max()
    {
        return new DoubleAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute sum()
    {
        return new DoubleAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute avg()
    {
        return new DoubleAggregateAttribute(new AverageCalculatorNumeric(this));
    }

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, T reladomoObject) throws IOException
    {
        writer.writeDouble(context, this.getAttributeName(), this.doubleValueOf(reladomoObject));
    }
}
