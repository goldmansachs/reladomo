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

import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.CalculatedBigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.BigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.MappedBigDecimalAttribute;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.BigDecimalProcedure;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.*;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.util.*;

import java.math.BigDecimal;



public class BigDecimalNumericType implements NonPrimitiveNumericType
{
    private static final BigDecimalNumericType instance = new BigDecimalNumericType();

    protected BigDecimalNumericType()
    {
    }

    public static BigDecimalNumericType getInstance()
    {
        return instance;
    }

    public BigDecimalAttribute createCalculatedAttribute(NumericAttributeCalculator calculator)
    {
        return new CalculatedBigDecimalAttribute(calculator);
    }

    public BigDecimalAttribute createMappedCalculatedAttribute(NumericAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        return new MappedBigDecimalAttribute((BigDecimalAttribute) wrappedAttribute, mapper, parentSelector);
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

    public ArithmeticAttributeCalculator createDivisionCalculator(NumericAttribute attr1, NumericAttribute attr2, int scale)
    {
        return new DivisionCalculator(attr1, attr2, scale);
    }

    public byte getTypeBitmap()
    {
        return 0;
    }

    public MutableNumber createMutableNumber()
    {
        return new MutableBigDecimal();
    }

    public MutableNumber createMutableAverage()
    {
        return new MutableBigDecimalAvg();
    }

    public void executeForEach(AggregateAttributeCalculator calculator, NumericAttribute attribute, Object previousValue, Object newValue)
    {
        attribute.forEach((BigDecimalProcedure)calculator, newValue, previousValue);
    }

    public Class valueType()
    {
        return BigDecimal.class;
    }
}
