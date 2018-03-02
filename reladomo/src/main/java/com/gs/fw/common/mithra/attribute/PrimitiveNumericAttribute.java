
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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.aggregate.attribute.DoubleAggregateAttribute;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.StandardDeviationCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.StandardDeviationPopCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.VarianceCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.VariancePopCalculatorNumeric;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.attribute.calculator.procedure.NullHandlingProcedure;
import com.gs.fw.common.mithra.attribute.numericType.*;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.finder.Mapper;

import java.sql.Timestamp;

public abstract class PrimitiveNumericAttribute<Owner, V> extends Attribute<Owner, V> implements NumericAttribute<Owner, V>
{

    private static final long serialVersionUID = -8848454605312848733L;

    public PrimitiveNumericAttribute()
    {
    }

    public int getScale()
    {
        return 0;
    }

    public int getPrecision()
    {
        return 0;
    }

    public boolean valueEquals(Owner first, Owner second)
    {
        if (this.isNullable())
        {
            boolean firstNull = this.isAttributeNull(first);
            return firstNull == this.isAttributeNull(second) && (firstNull || this.primitiveValueEquals(first, second));
        }
        else
        {
            return this.primitiveValueEquals(first, second);
        }
    }

    public <O> boolean valueEquals(Owner first, O second, Extractor<O, V> secondExtractor)
    {
        boolean firstNull = this.isNullable() && this.isAttributeNull(first);
        return firstNull == secondExtractor.isAttributeNull(second) && (firstNull || this.primitiveValueEquals(first, second, secondExtractor));
    }

    public void setValueUntil(Owner o, V newValue, Timestamp exclusiveUntil)
    {
        throw new RuntimeException("This method can only be called on objects with asof attributes");
    }

    public NumericAttribute absoluteValue()
    {
        throw new UnsupportedOperationException("absoluteValue() is not yet implemented for this attribute");
    }

    public NumericType getCalculatedType(NumericAttribute other)
    {
        return this.getCalculatedType(this.getNumericType().getTypeBitmap() & other.getNumericType().getTypeBitmap());
    }

    public boolean checkForNull(NullHandlingProcedure proc, Owner o, Object context)
    {
        if (this.isAttributeNull(o))
        {
            proc.executeForNull(context);
            return true;
        }
        return false;
    }

    private NumericType getCalculatedType(int bitMask)
    {
        switch (bitMask)
        {
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

    protected abstract boolean primitiveValueEquals(Owner first, Owner second);

    protected abstract <O> boolean primitiveValueEquals(Owner first, O second, Extractor<O, V> secondExtractor);

    public void forEach(final IntegerProcedure proc, Owner o, Object context)
    {
        throw new RuntimeException("Should never get here");
    }

    public void forEach(final LongProcedure proc, Owner o, Object context)
    {
        throw new RuntimeException("Should never get here");
    }

    public void forEach(final FloatProcedure proc, Owner o, Object context)
    {
        throw new RuntimeException("Should never get here");
    }

    public NumericAttribute getMappedAttributeWithCommonMapper(NumericAttribute calculatedAttribute, Mapper commonMapper, Mapper mapperRemainder, Function parentSelector)
    {
        Function selector = mapperRemainder == null ? parentSelector : mapperRemainder.getTopParentSelector(((DeepRelationshipAttribute) parentSelector));
        return this.getCalculatedType(calculatedAttribute).createMappedCalculatedAttribute(calculatedAttribute, commonMapper, selector);
    }

    protected NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder, NumericAttribute wrappedAttribute)
    {
        NumericAttribute attr = wrappedAttribute;
        if (mapperRemainder != null)
        {
            attr = createMappedAttributeWithMapperRemainder(mapperRemainder);
        }
        return attr;
    }

    protected NumericAttribute createMappedAttributeWithMapperRemainder(Mapper mapperRemainder)
    {
        return null;
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
}
