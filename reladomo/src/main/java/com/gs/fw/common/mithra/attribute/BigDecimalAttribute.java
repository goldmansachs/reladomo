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
import com.gs.fw.common.mithra.MithraAggregateAttribute;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.aggregate.attribute.BigDecimalAggregateAttribute;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.AbsoluteValueCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AverageCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MaxCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.MinCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.StandardDeviationCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.StandardDeviationPopCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.SumCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.VarianceCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.VariancePopCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstAdditionCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstDivisionCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.ConstMultiplicationCalculatorBigDecimal;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.NullHandlingProcedure;
import com.gs.fw.common.mithra.attribute.numericType.BigDecimalNumericType;
import com.gs.fw.common.mithra.attribute.numericType.DoubleNumericType;
import com.gs.fw.common.mithra.attribute.numericType.FloatNumericType;
import com.gs.fw.common.mithra.attribute.numericType.IntegerNumericType;
import com.gs.fw.common.mithra.attribute.numericType.LongNumericType;
import com.gs.fw.common.mithra.attribute.numericType.NumericType;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.BigDecimalUpdateWrapper;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.extractor.BigDecimalExtractor;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.BigDecimalUtil;
import com.gs.fw.common.mithra.util.HashUtil;
import com.gs.fw.common.mithra.util.MutableBigDecimal;
import com.gs.fw.common.mithra.util.Nullable;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;
import org.eclipse.collections.api.set.primitive.DoubleSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;


public abstract class BigDecimalAttribute<T> extends NonPrimitiveAttribute<T, BigDecimal>
        implements BigDecimalExtractor<T, BigDecimal>, NumericAttribute<T, BigDecimal>
{
    private static final long serialVersionUID = 4725357335985740152L;

    private int scale;
    private int precision;

    public BigDecimalAttribute()
    {
    }

    protected void setScale(int scale)
    {
        this.scale = scale;
    }

    protected void setPrecision(int precision)
    {
        this.precision = precision;
    }

    public int getScale()
    {
        return this.scale;
    }

    public int getPrecision()
    {
        return this.precision;
    }

    @Override
    protected void serializedNonNullValue(T o, ObjectOutput out) throws IOException
    {
       writeToStream(out, this.bigDecimalValueOf(o));
    }

    @Override
    protected void deserializedNonNullValue(T o, ObjectInput in) throws IOException, ClassNotFoundException
    {
       this.setBigDecimalValue(o, this.readFromStream(in));
    }

    @Override
    public void serializeNonNullAggregateDataValue(Nullable valueWrappedInNullable, ObjectOutput out) throws IOException
    {
        this.writeToStream(out, ((MutableBigDecimal)valueWrappedInNullable).bigDecimalValue());
    }

    @Override
    public Nullable deserializeNonNullAggregateDataValue(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return new MutableBigDecimal(this.readFromStream(in));
    }

    public BigDecimal readFromStream(ObjectInput in) throws IOException
    {
        int length = in.readInt();
        byte[] buf = new byte[length];
        int read = in.read(buf);
        while(read < length)
        {
            read += in.read(buf, read, length - read);
        }
        BigInteger unscaledValue = new BigInteger(buf);
        return  new BigDecimal(unscaledValue, in.readInt());
    }

    public void writeToStream(ObjectOutput out, BigDecimal bigDecimal) throws IOException
    {
        byte[] buf = bigDecimal.unscaledValue().toByteArray();
        out.writeInt(buf.length);
        out.write(buf);
        out.writeInt(bigDecimal.scale());
    }

    @Override
    public Operation nonPrimitiveEq(Object other)
    {
        return this.eq(((BigDecimal) other));
    }

    @Override
    public abstract Operation eq(BigDecimal other);

    @Override
    public abstract Operation notEq(BigDecimal other);

    public abstract Operation greaterThan(BigDecimal target);

    public abstract Operation greaterThanEquals(BigDecimal target);

    public abstract Operation lessThan(BigDecimal target);

    public abstract Operation lessThanEquals(BigDecimal target);

    public abstract Operation eq(double other);

    public abstract Operation notEq(double other);

    public abstract Operation in(DoubleSet doubleSet);

    public abstract Operation notIn(DoubleSet doubleSet);

    public abstract Operation greaterThan(double target);

    public abstract Operation greaterThanEquals(double target);

    public abstract Operation lessThan(double target);

    public abstract Operation lessThanEquals(double target);


    // join operation:
    /**
     * @deprecated  use joinEq or filterEq instead
     * @param other Attribute to join to
     * @return Operation corresponding to the join
     **/
    @Deprecated
    public abstract Operation eq(BigDecimalAttribute other);

    public abstract Operation joinEq(BigDecimalAttribute other);

    public abstract Operation filterEq(BigDecimalAttribute other);

    public abstract Operation notEq(BigDecimalAttribute other);

    public abstract void forEach(BigDecimalProcedure proc, T o, Object context);

    public NumericType getNumericType()
    {
        return BigDecimalNumericType.getInstance();
    }

    @Override
    public void setSqlParameter(int index, PreparedStatement ps, Object o, TimeZone databaseTimeZone, DatabaseType databaseType) throws SQLException
    {
        ps.setBigDecimal(index, (BigDecimal)o);
    }

    public BigDecimal valueOf(T o)
    {
        return this.bigDecimalValueOf(o);
    }

    public void setValue(T o, BigDecimal newValue)
    {
        this.setBigDecimalValue(o, newValue);
    }

    @Override
    public Class valueType()
    {
        return BigDecimal.class;
    }

    @Override
    public void parseStringAndSet(String value, T data, int lineNumber, Format format) throws ParseException
    {
        this.setBigDecimalValue(data, new BigDecimal(value));
    }

    @Override
    public void parseNumberAndSet(double value, T data, int lineNumber) throws ParseException
    {
        this.setBigDecimalValue(data, BigDecimal.valueOf(value));
    }

    public void setSqlParameters(PreparedStatement pps, MithraDataObject mdo, int pos, TimeZone databaseTimeZone)
            throws SQLException
    {
        Object obj = this.valueOf((T) mdo);

        if (obj != null)
        {
            BigDecimal value = (BigDecimal) obj;
            pps.setBigDecimal(pos, value);
        }
        else
        {
            pps.setNull(pos, java.sql.Types.DECIMAL);
        }
    }

    @Override
    public void setValueUntil(T o, BigDecimal newValue, Timestamp exclusiveUntil)
    {
        this.setUntil(o, newValue, exclusiveUntil);
    }

    protected void setUntil(Object o, BigDecimal s, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("not implemented");
    }

    public String valueOfAsString(T object, Formatter formatter)
    {
        return formatter.format(this.bigDecimalValueOf(object));
    }

    @Override
    public int valueHashCode(T o)
    {
        BigDecimal val = this.bigDecimalValueOf(o);
        if (val == null) return HashUtil.NULL_HASH;
        return HashUtil.hash(val);
    }

    public void increment(T o, BigDecimal increment)
    {
        this.setBigDecimalValue(o, this.bigDecimalValueOf(o).add(increment));
    }

    public void incrementUntil(T o, BigDecimal increment, Timestamp exclusiveUntil)
    {
         throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public void setSqlParameters(PreparedStatement pps, MithraDataObject mdo, int pos, TimeZone databaseTimeZone, DatabaseType databaseType)
            throws SQLException
    {
        BigDecimal obj = this.bigDecimalValueOf((T)mdo);
        this.setSqlParameter(pos, pps, obj, databaseTimeZone, databaseType);
    }

    @Override
    public void zPopulateValueFromResultSet(int resultSetPosition, int dataPosition, ResultSet rs, Object object, Method method, TimeZone databaseTimezone, DatabaseType dt, Object[] tempArray) throws SQLException
    {
        BigDecimal bd = rs.getBigDecimal(resultSetPosition);
        if (bd != null)
        {
            bd = BigDecimalUtil.validateBigDecimalValue(bd, dt.getMaxPrecision(), this.getScale());
        }
        tempArray[0] = bd;
        try
        {
            method.invoke(object, tempArray);
        }
        catch (IllegalArgumentException e)
        {
            throw new MithraBusinessException("Invalid argument " + tempArray[0] + " passed in invoking method " + method.getName() + " of class " + object.getClass().getName(), e);
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
        BigDecimal bd = rs.getBigDecimal(resultSetPosition);
        if(bd != null)
        {
           bd = BigDecimalUtil.validateBigDecimalValue(bd, dt.getMaxPrecision(), this.getScale());
        }
        data.setValueAt(dataPosition, new MutableBigDecimal(bd));
    }

    @Override
    public void zPopulateAggregateDataValue(int position, Object value, AggregateData data)
    {
        data.setValueAt((position), new MutableBigDecimal((BigDecimal) value));
    }

    @Override
    public String zGetSqlForDatabaseType(DatabaseType databaseType)
    {
        return databaseType.getSqlDataTypeForBigDecimal();
    }

    @Override
    public AttributeUpdateWrapper zConstructNullUpdateWrapper(MithraDataObject data)
    {
        return new BigDecimalUpdateWrapper(this, data, null);
    }

    protected BigDecimal createBigDecimalFromDouble(double doubleValue)
    {
        return BigDecimalUtil.createBigDecimalFromDouble(doubleValue, this.getPrecision(), this.getScale());
    }

    protected Set<BigDecimal> createBigDecimalSetFromDoubleSet(DoubleSet doubleSet)
    {
        return BigDecimalUtil.createBigDecimalSetFromDoubleSet(doubleSet, this.getPrecision(), this.getScale());
    }

// ByteAttribute operands

    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, (ByteAttribute) attribute));
    }

    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, (ByteAttribute) attribute));
    }

    public BigDecimalAttribute times(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, (ByteAttribute) attribute));
    }

    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.ByteAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, (ByteAttribute) attribute, this.getScale()));
    }

    // ShortAttribute operands

    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, (ShortAttribute) attribute));
    }

    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, (ShortAttribute) attribute));
    }

    public BigDecimalAttribute times(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, (ShortAttribute) attribute));
    }

    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.ShortAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, (ShortAttribute) attribute, this.getScale()));
    }

    // IntegerAttribute operands

    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, (IntegerAttribute) attribute));
    }

    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, (IntegerAttribute) attribute));
    }

    public BigDecimalAttribute times(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, (IntegerAttribute) attribute));
    }

    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.IntegerAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, (IntegerAttribute) attribute, this.getScale()));
    }

    // LongAttribute operands

    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, (LongAttribute) attribute));
    }

    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, (LongAttribute) attribute));
    }

    public BigDecimalAttribute times(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, (LongAttribute) attribute));
    }

    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.LongAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, (LongAttribute) attribute, this.getScale()));
    }

    // FloatAttribute operands

    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, (FloatAttribute) attribute));
    }

    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, (FloatAttribute) attribute));
    }

    public BigDecimalAttribute times(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, (FloatAttribute) attribute));
    }

    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.FloatAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, (FloatAttribute) attribute, this.getScale()));
    }

   // DoubleAttribute operands

    public BigDecimalAttribute plus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createAdditionCalculator(this, (DoubleAttribute) attribute));
    }

    public BigDecimalAttribute minus(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createSubtractionCalculator(this, (DoubleAttribute) attribute));
    }

    public BigDecimalAttribute times(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this, (DoubleAttribute) attribute));
    }

    public BigDecimalAttribute dividedBy(com.gs.fw.finder.attribute.DoubleAttribute attribute)
    {
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, (DoubleAttribute) attribute, this.getScale()));
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
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createMultiplicationCalculator(this,attribute));
    }

    public BigDecimalAttribute dividedBy(BigDecimalAttribute attribute)
    {
        int scale = BigDecimalUtil.calculateQuotientScale(this.getPrecision(), this.getScale(), attribute.getPrecision(), attribute.getScale());
        return BigDecimalNumericType.getInstance().createCalculatedAttribute(BigDecimalNumericType.getInstance().createDivisionCalculator(this, attribute, scale));
    }

    public BigDecimalAttribute absoluteValue()
    {
        return new CalculatedBigDecimalAttribute(new AbsoluteValueCalculatorBigDecimal(this));
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
    public MithraAggregateAttribute min()
    {
        return new BigDecimalAggregateAttribute(new MinCalculatorNumeric(this));
    }

    @Override
    public MithraAggregateAttribute max()
    {
        return new BigDecimalAggregateAttribute(new MaxCalculatorNumeric(this));
    }

    public MithraAggregateAttribute sum()
    {
        return new BigDecimalAggregateAttribute(new SumCalculatorNumeric(this));
    }

    public MithraAggregateAttribute avg()
    {                                                                                                        
        return new BigDecimalAggregateAttribute(new AverageCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute standardDeviationSample()
    {
        return new DoubleAggregateAttribute(new StandardDeviationCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute standardDeviationPopulation()
    {
        return new DoubleAggregateAttribute(new StandardDeviationPopCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute varianceSample()
    {
        return new DoubleAggregateAttribute(new VarianceCalculatorNumeric(this));
    }

    public DoubleAggregateAttribute variancePopulation()
    {
        return new DoubleAggregateAttribute(new VariancePopCalculatorNumeric(this));
    }

    public BigDecimalAttribute plus(double addend)
    {
        return new CalculatedBigDecimalAttribute(new ConstAdditionCalculatorBigDecimal(this, BigDecimal.valueOf(addend)));
    }

    public BigDecimalAttribute minus(double addend)
    {
        return this.plus(-addend);
    }

    public BigDecimalAttribute times(double multiplicand)
    {
        return new CalculatedBigDecimalAttribute(new ConstMultiplicationCalculatorBigDecimal(this, BigDecimal.valueOf(multiplicand)));
    }

    public BigDecimalAttribute dividedBy(double divisor)
    {
        return new CalculatedBigDecimalAttribute(new ConstDivisionCalculatorBigDecimal(this, BigDecimal.valueOf(divisor)));
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

    public boolean checkForNull(NullHandlingProcedure proc, T o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
            return true;
        }
        return false;
    }

    public NumericAttribute getMappedAttributeWithCommonMapper(NumericAttribute calculatedAttribute, Mapper commonMapper, Mapper mapperRemainder, Function parentSelector)
    {
        Function selector = mapperRemainder == null ? parentSelector : mapperRemainder.getTopParentSelector(((DeepRelationshipAttribute) parentSelector));
        return this.getCalculatedType(calculatedAttribute).createMappedCalculatedAttribute(calculatedAttribute, commonMapper, selector);
    }

    public NumericType getCalculatedType(NumericAttribute other)
    {
        return this.getCalculatedType(this.getNumericType().getTypeBitmap() & other.getNumericType().getTypeBitmap());
    }

    private NumericType getCalculatedType(int bitMask)
    {
        switch (bitMask)
        {
            case 0:
                return BigDecimalNumericType.getInstance();
            case 1:
                return DoubleNumericType.getInstance();
            case 3:
                return FloatNumericType.getInstance();
            case 7:
                return LongNumericType.getInstance();
            case 15:
            case 31:
            case 63:
                return IntegerNumericType.getInstance();

            default:
                throw new MithraBusinessException("Invalid Numeric Type");
        }
    }

    public void forEach(final IntegerProcedure proc, T o, Object context)
    {
        throw new RuntimeException("Should never get here");
    }

    public void forEach(final LongProcedure proc, T o, Object context)
    {
        throw new RuntimeException("Should never get here");
    }

    public void forEach(final FloatProcedure proc, T o, Object context)
    {
        throw new RuntimeException("Should never get here");
    }

    @Override
    public Operation zGetPrototypeOperation(Map<Attribute, Object> tempOperationPool)
    {
        return this.eq(BigDecimal.ZERO);
    }

    @Override
    protected void zWriteNonNullSerial(ReladomoSerializationContext context, SerialWriter writer, T reladomoObject) throws IOException
    {
        writer.writeBigDecimal(context, this.getAttributeName(), this.bigDecimalValueOf(reladomoObject));
    }
}
