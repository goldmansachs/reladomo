
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

import com.gs.fw.common.mithra.attribute.CalculatedFloatAttribute;
import com.gs.fw.common.mithra.attribute.FloatAttribute;
import com.gs.fw.common.mithra.attribute.MappedFloatAttribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.FloatProcedure;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.util.Function;
import com.gs.fw.common.mithra.util.MutableAverage;
import com.gs.fw.common.mithra.util.MutableFloat;
import com.gs.fw.common.mithra.util.MutableNumber;

public class FloatNumericType implements PrimitiveNumericType
{

    private static final FloatNumericType instance = new FloatNumericType();

    protected FloatNumericType()
    {
    }

    public static FloatNumericType getInstance()
    {
        return instance;
    }

    public FloatAttribute createMappedCalculatedAttribute(NumericAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        return new MappedFloatAttribute((FloatAttribute) wrappedAttribute, mapper, parentSelector);
    }

    public FloatAttribute createCalculatedAttribute(NumericAttributeCalculator calculator)
    {
        return new CalculatedFloatAttribute(calculator);
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
        return 3;
    }

    public MutableNumber createMutableNumber()
    {
        return new MutableFloat();
    }

    public MutableNumber createMutableAverage()
    {
        return new MutableAverage();
    }

    public void executeForEach(AggregateAttributeCalculator calculator, NumericAttribute attribute, Object previousValue, Object newValue)
    {
        attribute.forEach((FloatProcedure)calculator, newValue, previousValue);
    }

    public Class valueType()
    {
        return Float.class;
    }
}
