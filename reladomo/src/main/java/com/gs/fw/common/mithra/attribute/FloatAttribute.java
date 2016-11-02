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

import com.gs.collections.api.set.primitive.FloatSet;
import com.gs.collections.api.set.primitive.MutableFloatSet;
import com.gs.collections.impl.set.mutable.primitive.FloatHashSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.aggregate.attribute.FloatAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbsoluteValueCalculatorFloat;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.FloatNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.FloatNullUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.FloatExtractor;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.FloatOrderBy;
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
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public abstract class FloatAttribute<T> extends PrimitiveNumericAttribute<T, Float> implements com.gs.fw.finder.attribute.FloatAttribute<T>, FloatExtractor<T, Float>
{

    private transient OrderBy ascendingOrderBy;
    private transient OrderBy descendingOrderBy;

    private static final long serialVersionUID = 7440817907209877988L;

    public Class valueType()
    {
        return Float.class;
    }

    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
        out.writeFloat(this.floatValueOf(o));
    }

    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException
    {
        this.setFloatValue(o, in.readFloat());
    }

    public Operation nonPrimitiveEq(Object other)
    {
        if (other == null) return this.isNull();
        return this.eq(((Number) other).floatValue());
    }

    public abstract Operation eq(float other);

    public abstract Operation notEq(float other);

    @Override
    public abstract Operation in(FloatSet floatSet);

    @Override
    public abstract Operation notIn(FloatSet floatSet);

    public abstract Operation greaterThan(float target);

    public abstract Operation greaterThanEquals(float target);

    public abstract Operation lessThan(float target);

    public abstract Operation lessThanEquals(float target);

    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    public abstract Operation eq(FloatAttribute other);

    public abstract Operation joinEq(FloatAttribute other);

    public abstract Operation filterEq(FloatAttribute other);

    public abstract Operation notEq(FloatAttribute other);

    public Float valueOf(T o)
    {
        if (this.isAttributeNull(o)) return null;
        return new Float(this.floatValueOf(o));
    }

    public void setValue(T o, Float newValue)
    {
        this.setFloatValue(o, newValue.floatValue());
    }

    public int valueHashCode(T o)
    {
        return (this.isAttributeNull(o)) ? HashUtil.NULL_HASH : HashUtil.hash(this.floatValueOf(o));
    }

    protected boolean primitiveValueEquals(T first, T second)
    {
        return this.floatValueOf(first) == this.floatValueOf(second);
    }

    protected <O> boolean primitiveValueEquals(T first, O second, Extractor<O, Float> secondExtractor)
    {
        return this.floatValueOf(first) == ((FloatExtractor) secondExtractor).floatValueOf(second);
    }

    public OrderBy ascendingOrderBy()
    {
        if (this.ascendingOrderBy == null)
        {
            this.ascendingOrderBy = new FloatOrderBy(this, true);
        }
        return this.ascendingOrderBy;
    }

    public OrderBy descendingOrderBy()
    {
        if (this.descendingOrderBy == null)
        {
            this.descendingOrderBy = new FloatOrderBy(this, false);
        }
        return this.descendingOrderBy;
    }

    @Override
    public Operation in(final List objects, final Extractor extractor)
    {
        final FloatExtractor floatExtractor = (FloatExtractor) extractor;
        final MutableFloatSet set = new FloatHashSet();
        for (int i = 0, n = objects.size(); i < n; i++)
        {
            final Object o = objects.get(i);
            if (!floatExtractor.isAttributeNull(o))
            {
                set.add(floatExtractor.floatValueOf(o));
            }
        }
        return this.in(set);
    }

    @Override
    public Operation in(final Iterable objects, final Extractor extractor)
    {
        final FloatExtractor floatExtractor = (FloatExtractor) extractor;
        final MutableFloatSet set = new FloatHashSet();
        for (Object o : objects)
        {
            if (!floatExtractor.isAttributeNull(o))
            {
                set.add(floatExtractor.floatValueOf(o));
            }
        }
        return this.in(set);
    }

    public Operation zInWithMax(int maxInClause, List objects, Extractor extractor)
    {
        FloatExtractor floatExtractor = (FloatExtractor) extractor;
        MutableFloatSet set = new FloatHashSet();
        for (int i = 0; i < objects.size(); i++)
        {
            Object o = objects.get(i);
            if (!floatExtractor.isAttributeNull(o))
            {
                set.add(floatExtractor.floatValueOf(o));
                if (set.size() > maxInClause)
                {
                    return new None(this);
                }
            }
        }
        return this.in(set);
    }

    public void parseNumberAndSet(double value, T data, int lineNumber) throws ParseException
    {
        this.setFloatValue(data, (float) value);
    }

    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setFloatValue(data, Float.parseFloat(value));
    }

    public void setValueUntil(T o, Float newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue.floatValue(), exclusiveUntil);
    }

    protected void setUntil(T o, float v, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.floatValueOf(object));
    }

    public int zCountUniqueInstances(MithraDataObject[] dataObjects)
    {
        if (this.isAttributeNull((T) dataObjects[0]))
        {
            return 1;
        }
        float firstValue = this.floatValueOf((T) dataObjects[0]);
        MutableFloatSet set = null;
        for (int i = 1; i < dataObjects.length; i++)
        {
            float nextValue = this.floatValueOf((T) dataObjects[i]);
            if (set != null)
            {
                set.add(nextValue);
            }
            else if (nextValue != firstValue)
            {
                set = new FloatHashSet();
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

    public void populateValueFromResultSet(int position, ResultSet rs, Object[] values)
            throws SQLException
    {
        values[position] = new Float(rs.getFloat(position));
    }

    @Override
    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableFloat((Float) value));
    }

    public NumericType getNumericType()
    {
        return FloatNumericType.getInstance();
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        float f = rs.getFloat(resultSetPosition);
        if (rs.wasNull())
        {
            tempArray[0] = null;
        }
        else
        {
            tempArray[0] = f;
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
        MutableFloat obj;
        float i = rs.getFloat(resultSetPosition);
        if (rs.wasNull())
        {
            obj = new MutableFloat();
        }
        else
        {
            obj = new MutableFloat(i);
        }
        data.setValueAt(dataPosition, obj);
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        out.writeFloat(((MutableNumber)valueWrappedInNullable).floatValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableFloat(in.readFloat());
    }

    // ByteAttribute operands

    public FloatAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createAdditionCalculator(this, (ByteAttribute) attribute));
    }

    public FloatAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createSubtractionCalculator(this, (ByteAttribute) attribute));
    }

    public FloatAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createMultiplicationCalculator(this, (ByteAttribute) attribute));
    }

    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createDivisionCalculator(this, (ByteAttribute) attribute));
    }

    // ShortAttribute operands

    public FloatAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createAdditionCalculator(this, (ShortAttribute) attribute));
    }

    public FloatAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createSubtractionCalculator(this, (ShortAttribute) attribute));
    }

    public FloatAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createMultiplicationCalculator(this, (ShortAttribute) attribute));
    }

    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createDivisionCalculator(this, (ShortAttribute) attribute));
    }

    // IntegerAttribute operands

    public FloatAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createAdditionCalculator(this, (IntegerAttribute) attribute));
    }

    public FloatAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createSubtractionCalculator(this, (IntegerAttribute) attribute));
    }

    public FloatAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createMultiplicationCalculator(this, (IntegerAttribute) attribute));
    }

    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createDivisionCalculator(this, (IntegerAttribute) attribute));
    }

    // LongAttribute operands

    public FloatAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createAdditionCalculator(this, (LongAttribute) attribute));
    }

    public FloatAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createSubtractionCalculator(this, (LongAttribute) attribute));
    }

    public FloatAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createMultiplicationCalculator(this, (LongAttribute) attribute));
    }

    public FloatAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createDivisionCalculator(this, (LongAttribute) attribute));
    }

    //BigDecimal

    public FloatAttribute plus(BigDecimalAttribute attribute)
       {
           return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createAdditionCalculator(this, attribute));
       }

       public FloatAttribute minus(BigDecimalAttribute attribute)
       {
           return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createSubtractionCalculator(this, attribute));
       }

       public FloatAttribute times(BigDecimalAttribute attribute)
       {
           return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createMultiplicationCalculator(this,attribute));
       }

       public FloatAttribute dividedBy(BigDecimalAttribute attribute)
       {
           return FloatNumericType.getInstance().createCalculatedAttribute(FloatNumericType.getInstance().createDivisionCalculator(this, attribute));
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

    public FloatAttribute absoluteValue()
    {
        return new CalculatedFloatAttribute(new AbsoluteValueCalculatorFloat(this)); 
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
        return databaseType.getSqlDataTypeForFloat();
    }

    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new FloatNullUpdateWrapper(this, data);
    }

    public Operation zGetOperationFromOriginal(Object original, Attribute left, Map tempOperationPool)
    {
        if (left.isAttributeNull(original))
        {
            return this.isNull();
        }
        return this.eq(((FloatAttribute)left).floatValueOf(original));
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(0.0f);
    }

    public Operation zGetOperationFromResult(T result, Map<Attribute, Object> tempOperationPool)
    {
        if (this.isAttributeNull(result))
        {
            return this.isNull();
        }
        return this.eq(this.floatValueOf(result));
    }

    public FloatAggregateAttribute min()
    {
        return new FloatAggregateAttribute(new MinCalculatorNumeric(this));
    }

    public FloatAggregateAttribute max()
    {
        return new FloatAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public FloatAggregateAttribute sum()
    {
        return new FloatAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public FloatAggregateAttribute avg()
    {
        return new FloatAggregateAttribute(new AverageCalculatorNumeric(this));
    }

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, T reladomoObject) throws IOException
    {
        writer.writeFloat(context, this.getAttributeName(), this.floatValueOf(reladomoObject));
    }
}
