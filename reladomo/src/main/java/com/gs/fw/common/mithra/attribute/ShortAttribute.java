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

import com.gs.collections.api.set.primitive.MutableShortSet;
import com.gs.collections.api.set.primitive.ShortSet;
import com.gs.collections.impl.set.mutable.primitive.ShortHashSet;
import com.gs.fw.common.mithra.AggregateData;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraNullPrimitiveException;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.aggregate.attribute.ShortAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.StandardDeviationCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.numericType.BigDecimalNumericType;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.FloatNumericType;
import com.gs.fw.common.mithra.attribute.numericType.IntegerNumericType;
import com.gs.fw.common.mithra.attribute.numericType.LongNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.ShortNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.ShortExtractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.finder.orderby.ShortOrderBy;
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


public abstract class ShortAttribute<T> extends PrimitiveNumericAttribute<T, Short> implements com.gs.fw.finder.attribute.ShortAttribute<T>, ShortExtractor<T, Short>
{
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = 5966315453618954393L;

    public int intValueOf(T o)
    {
        return (int) this.shortValueOf(o);
    }

    public void setIntValue(T o, int newValue)
    {
        this.setShortValue(o, (short) newValue);
    }

    @Override
    public Class valueType()
    {
        return Short.class;
    }

    @Override
    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeShort(this.shortValueOf(o));
    }

    @Override
    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        this.setShortValue(o, in.readShort());
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Number) other).shortValue());
    }

    public abstract Operation eq(short other);

    public abstract Operation notEq(short other);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public abstract Operation in(ShortSet shortSet);

    @Override
    public abstract Operation in(org.eclipse.collections.api.set.primitive.ShortSet shortSet);

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2018.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    @Override
    public abstract Operation notIn(ShortSet shortSet);

    @Override
    public abstract Operation notIn(org.eclipse.collections.api.set.primitive.ShortSet shortSet);

    public abstract Operation greaterThan(short target);

    public abstract Operation greaterThanEquals(short target);

    public abstract Operation lessThan(short target);

    public abstract Operation lessThanEquals(short target);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    @Deprecated
    public abstract Operation eq(ShortAttribute other);

    public abstract Operation joinEq(ShortAttribute other);

    public abstract Operation filterEq(ShortAttribute other);

    public abstract Operation notEq(ShortAttribute other);

    public Short valueOf(T o)
    {
        return this.isAttributeNull(o) ? null : Short.valueOf(this.shortValueOf(o));
    }

    public void setValue(T o, Short newValue)
    {
        this.setShortValue(o, newValue.shortValue());
    }

    public int valueHashCode(T o)
    {
        return this.isAttributeNull(o) ? HashUtil.NULL_HASH : HashUtil.hash(this.shortValueOf(o));
    }

    @Override
    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.shortValueOf(first) == this.shortValueOf(second);
    }

    @Override
    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Short> secondExtractor)
    {
        return this.shortValueOf(first) == ((ShortExtractor) secondExtractor).shortValueOf(second);

    }

    @Override
    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new ShortOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    @Override
    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new ShortOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final ShortExtractor shortExtractor = (ShortExtractor) extractor;
        final MutableShortSet set = new ShortHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!shortExtractor.isAttributeNull(o))
            {
                set.add(shortExtractor.shortValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final ShortExtractor shortExtractor = (ShortExtractor) extractor;
        final MutableShortSet set = new ShortHashSet();
        for (Object o : objects)
        {
            if (!shortExtractor.isAttributeNull(o))
            {
                set.add(shortExtractor.shortValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        ShortExtractor shortExtractor = (ShortExtractor) extractor;
        MutableShortSet set = new ShortHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!shortExtractor.isAttributeNull(o))
            {
                set.add(shortExtractor.shortValueOf(o));
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
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE || Math.floor(value) != value)
        {
            throw new ParseException("Incorrect short value " + value + " on line " +
                    lineNumber + " for attribute " + this.getClass().getName(), lineNumber);
        }
        this.setShortValue(data, (short) value);
    }

    @Override
    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setShortValue(data, Short.parseShort(value));
    }

      @Override
      public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
      {
          short s = rs.getShort(resultSetPosition);
          if (rs.wasNull())
          {
              tempArray[0] = null;
          }
          else
          {
              tempArray[0] = s;
          }
          try
          {
              method.invoke(object, tempArray);
          }
          catch (IllegalArgumentException e)
          {
              if (tempArray[0] == null && method.getParameterTypes()[0].isPrimitive())
              {
                  throw new MithraNullPrimitiveException("Aggregate result returned null for" + method.getName() + " of class " + object.getClass().getName() + " which cannot be set as primitive", e);
              }
              throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + "  of class " + object.getClass().getName(), e);
          }
          catch (IllegalAccessException e)
          {
              throw new MithraBusinessException("No valid access to invoke method " + method.getName() + " of class " + object.getClass().getName(), e);
          }
          catch (InvocationTargetException e)
          {
              throw new MithraBusinessException("Error invoking method " + method.getName() + " of class " + object.getClass().getName(), e);
          }
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
    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableInteger((Short) value));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeShort(((MutableNumber)valueWrappedInNullable).shortValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableInteger(in.readShort());
    }

    @Override
    public void setValueUntil(T o, Short newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.shortValue(), exclusiveUntil);
    }

    protected void setUntil(Object o, short i, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.shortValueOf(object));
    }

    @Override
    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        short firstValue = this.shortValueOf((T) dataObjects[0]);
        MutableShortSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            short nextValue = this.shortValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new ShortHashSet();
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

    public NumericType getNumericType()
    {
        return IntegerNumericType.getInstance();
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

    // absoluteValue() is not supported for ShortAttributes in Mithra

    @Override
    public ShortAttribute absoluteValue()
    {
        throw new UnsupportedOperationException("absoluteValue is not implemented for ShortAttribute");
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
        return databaseType.getSqlDataTypeForShortJava();
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new ShortNullUpdateWrapper(this, data);
    }

    @Override
    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((ShortAttribute)left).shortValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq((short)0);
    }

    @Override
    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.shortValueOf(result));
    }

    @Override
    public ShortAggregateAttribute min()
    {
        return new ShortAggregateAttribute(new MinCalculatorNumeric(this));
    }

    @Override
    public ShortAggregateAttribute max()
    {
        return new ShortAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public ShortAggregateAttribute sum()
    {
        return new ShortAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public ShortAggregateAttribute avg()
    {
        return new ShortAggregateAttribute(new AverageCalculatorNumeric(this));
    }
}
