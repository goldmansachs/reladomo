
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

import com.gs.fw.common.mithra.attribute.CalculatedIntegerAttribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.attribute.MappedIntegerAttribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.IntegerProcedure;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.util.Function;
import com.gs.fw.common.mithra.util.MutableIntAverage;
import com.gs.fw.common.mithra.util.MutableInteger;
import com.gs.fw.common.mithra.util.MutableNumber;

public class IntegerNumericType implements PrimitiveNumericType
{
    private static final IntegerNumericType instance = new IntegerNumericType();

    protected IntegerNumericType()
    {
    }

    public static IntegerNumericType getInstance()
    {
        return instance;
    }

    public IntegerAttribute createMappedCalculatedAttribute(NumericAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        return new MappedIntegerAttribute((IntegerAttribute) wrappedAttribute, mapper, parentSelector);
    }

    public IntegerAttribute createCalculatedAttribute(NumericAttributeCalculator calculator)
    {
        return new CalculatedIntegerAttribute(calculator);
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
        return 15;
    }

    public MutableNumber createMutableNumber()
    {
        return new MutableInteger();
    }

    public MutableNumber createMutableAverage()
    {
        return new MutableIntAverage();
    }

    public void executeForEach(AggregateAttributeCalculator calculator, NumericAttribute attribute, Object previousValue, Object newValue)
    {
        attribute.forEach((IntegerProcedure) calculator, newValue, previousValue);
    }

    public Class valueType()
    {
        return Integer.class;

    }
}
