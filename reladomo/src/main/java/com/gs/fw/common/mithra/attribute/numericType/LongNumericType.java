
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

package com.gs.fw.common.mithra.attribute.numericType;

import com.gs.fw.common.mithra.attribute.CalculatedLongAttribute;
import com.gs.fw.common.mithra.attribute.LongAttribute;
import com.gs.fw.common.mithra.attribute.MappedLongAttribute;
import com.gs.fw.common.mithra.attribute.NumericAttribute;
import com.gs.fw.common.mithra.attribute.calculator.NumericAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.aggregateFunction.AggregateAttributeCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.*;
import com.gs.fw.common.mithra.attribute.calculator.procedure.LongProcedure;
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.util.MutableIntAverage;
import com.gs.fw.common.mithra.util.MutableLong;
import com.gs.fw.common.mithra.util.MutableNumber;

public class LongNumericType implements PrimitiveNumericType
{
    private static final LongNumericType instance = new LongNumericType();

    protected LongNumericType()
    {
    }

    public static LongNumericType getInstance()
    {
        return instance;
    }

    public LongAttribute createMappedCalculatedAttribute(NumericAttribute wrappedAttribute, Mapper mapper, Function parentSelector)
    {
        return new MappedLongAttribute((LongAttribute) wrappedAttribute, mapper, parentSelector);
    }

    public LongAttribute createCalculatedAttribute(NumericAttributeCalculator calculator)
    {
        return new CalculatedLongAttribute(calculator);
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
        return 7;
    }

    public MutableNumber createMutableNumber()
    {
        return new MutableLong();
    }

    public MutableNumber createMutableAverage()
    {
        return new MutableIntAverage();
    }

    public void executeForEach(AggregateAttributeCalculator calculator, NumericAttribute attribute, Object previousValue, Object newValue)
    {
        attribute.forEach((LongProcedure) calculator, newValue, previousValue);
    }

    public Class valueType()
    {
        return Long.class;
    }
}
