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

package com.gs.fw.common.mithra.attribute.numericType;

import com.gs.collections.api.block.function.Function;
import com.gs.fw.common.mithra.attribute.CalculatedDoubleAttribute;
import com.gs.fw.common.mithra.attribute.DoubleAttribute;
import com.gs.fw.common.mithra.attribute.MappedDoubleAttribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.DoubleProcedure;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.util.MutableAverage;
import com.gs.fw.common.mithra.util.MutableDouble;
import com.gs.fw.common.mithra.util.MutableNumber;


public class DoubleNumericType implements PrimitiveNumericType
{
    private static final DoubleNumericType instance = new DoubleNumericType();

    protected DoubleNumericType()
    {
    }

    public static DoubleNumericType getInstance()
    {
        return instance;
    }

    public DoubleAttribute createCalculatedAttribute(NumericAttributeCalculator calculator)
    {
        return new CalculatedDoubleAttribute(calculator);
    }

    public DoubleAttribute createMappedCalculatedAttribute(NumericAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        return new MappedDoubleAttribute((DoubleAttribute) wrappedAttribute, mapper, parentSelector);
    }

    public ArithmeticAttributeCalculator createSubtractionCalculator(NumericAttribute attr1, NumericAttribute attr2)
    {
        return new SubstractionCalculator(attr1, attr2);
    }

    public ArithmeticAttributeCalculator createAdditionCalculator(NumericAttribute attr1, NumericAttribute attr2)
    {
        return new AdditionCalculator(attr1, attr2);
    }

    public ArithmeticAttributeCalculator createMultiplicationCalculator(NumericAttribute attr1, NumericAttribute attr2)
    {
        return new MultiplicationCalculator(attr1, attr2);
    }

    public ArithmeticAttributeCalculator createDivisionCalculator(NumericAttribute attr1, NumericAttribute attr2)
    {
        return new DivisionCalculator(attr1, attr2);
    }

    public byte getTypeBitmap()
    {
        return 1;
    }

    public MutableNumber createMutableNumber()
    {
        return new MutableDouble();
    }

    public MutableNumber createMutableAverage()
    {
        return new MutableAverage();
    }

    public void executeForEach(AggregateAttributeCalculator calculator, NumericAttribute attribute, Object previousValue, Object newValue)
    {
        attribute.forEach((DoubleProcedure)calculator, newValue, previousValue);
    }

    public Class valueType()
    {
        return Double.class;
    }
}
