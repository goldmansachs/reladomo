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

import com.gs.collections.api.set.primitive.ByteSet;
import com.gs.collections.api.set.primitive.MutableByteSet;
import com.gs.collections.impl.set.mutable.primitive.ByteHashSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.aggregate.attribute.ByteAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.numericType.BigDecimalNumericType;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.FloatNumericType;
import com.gs.fw.common.mithra.attribute.numericType.IntegerNumericType;
import com.gs.fw.common.mithra.attribute.numericType.LongNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.ByteNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.ByteExtractor;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.ByteOrderBy;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public abstract class ByteAttribute<T> extends PrimitiveNumericAttribute<T, Byte> implements com.gs.fw.finder.attribute.ByteAttribute<T>, ByteExtractor<T, Byte>
{
    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = 8657358290353527740L;

    public int intValueOf(T o)
    {
        return (int) this.byteValueOf(o);
    }

    public void setIntValue(T o, int newValue)
    {
        this.setByteValue(o, (byte) newValue);
    }

    @Override
    public Class valueType()
    {
        return Byte.class;
    }

    @Override
    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeByte(this.byteValueOf(o));
    }

    @Override
    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        this.setByteValue(o, in.readByte());
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Byte) other).byteValue());
    }

    public abstract Operation eq(byte other);

    public abstract Operation notEq(byte other);

    @Override
    public abstract Operation in(ByteSet byteSet);

    @Override
    public abstract Operation notIn(ByteSet byteSet);

    public abstract Operation greaterThan(byte target);

    public abstract Operation greaterThanEquals(byte target);

    public abstract Operation lessThan(byte target);

    public abstract Operation lessThanEquals(byte target);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    @Deprecated
    public abstract Operation eq(ByteAttribute other);

    public abstract Operation joinEq(ByteAttribute other);

    public abstract Operation filterEq(ByteAttribute other);

    public abstract Operation notEq(ByteAttribute other);

    public Byte valueOf(T o)
    {
        return this.isAttributeNull(o) ? null : Byte.valueOf(this.byteValueOf(o));
    }

    public void setValue(T o, Byte newValue)
    {
        this.setByteValue(o, newValue.byteValue());
    }

    public int valueHashCode(T o)
    {
        return this.isAttributeNull(o) ? HashUtil.NULL_HASH : HashUtil.hash(this.byteValueOf(o));
    }

    @Override
    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.byteValueOf(first) == this.byteValueOf(second);
    }

    @Override
    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Byte> secondExtractor)
    {
        return this.byteValueOf(first) == ((ByteExtractor) secondExtractor).byteValueOf(second);
    }

    @Override
    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new ByteOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    @Override
    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new ByteOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final ByteExtractor byteExtractor = (ByteExtractor) extractor;
        final MutableByteSet set = new ByteHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!byteExtractor.isAttributeNull(o))
            {
                set.add(byteExtractor.byteValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final ByteExtractor byteExtractor = (ByteExtractor) extractor;
        final MutableByteSet set = new ByteHashSet();
        for (Object o : objects)
        {
            if (!byteExtractor.isAttributeNull(o))
            {
                set.add(byteExtractor.byteValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        ByteExtractor byteExtractor = (ByteExtractor) extractor;
        MutableByteSet set = new ByteHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!byteExtractor.isAttributeNull(o))
            {
                set.add(byteExtractor.byteValueOf(o));
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
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE || Math.floor(value) != value)
        {
            throw new ParseException("Incorrect byte value " + value + " on line " +
                    lineNumber + " for attribute " + this.getClass().getName(), lineNumber);
        }
        this.setByteValue(data, (byte) value);
    }

    @Override
    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setByteValue(data, Byte.parseByte(value));
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.byteValueOf(object));
    }

    @Override
    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        byte firstValue = this.byteValueOf((T) dataObjects[0]);
        MutableByteSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            byte nextValue = this.byteValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new ByteHashSet();
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
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        byte b = rs.getByte(resultSetPosition);
        if (rs.wasNull())
        {
            tempArray[0] = null;
        }
        else
        {
            tempArray[0] = b;
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
        data.setValueAt((position), new MutableInteger((Byte) value));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeByte(((MutableInteger)valueWrappedInNullable).intValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableInteger(in.readByte());
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
    public ByteAttribute absoluteValue()
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
        return databaseType.getSqlDataTypeForByte();
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new ByteNullUpdateWrapper(this, data);
    }

    @Override
    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((ByteAttribute)left).byteValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq((byte)0);
    }

    @Override
    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.byteValueOf(result));
    }

    @Override
    public ByteAggregateAttribute min()
    {
        return new ByteAggregateAttribute(new MinCalculatorNumeric(this));
    }

    @Override
    public ByteAggregateAttribute max()
    {
        return new ByteAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public ByteAggregateAttribute sum()
    {
        return new ByteAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public ByteAggregateAttribute avg()
    {
        return new ByteAggregateAttribute(new AverageCalculatorNumeric(this));
    }


    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, T reladomoObject) throws IOException
    {
        writer.writeByte(context, this.getAttributeName(), this.byteValueOf(reladomoObject));
    }
}
