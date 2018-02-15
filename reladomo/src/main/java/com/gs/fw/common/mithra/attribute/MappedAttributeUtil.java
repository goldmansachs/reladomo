
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
import com.gs.fw.common.mithra.extractor.Function;
import com.gs.fw.common.mithra.finder.Mapper;
import com.gs.fw.common.mithra.finder.DeepRelationshipAttribute;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.AdditionCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.SubstractionCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.MultiplicationCalculator;
import com.gs.fw.common.mithra.attribute.calculator.arithmeticCalculator.DivisionCalculator;
import com.gs.fw.common.mithra.attribute.calculator.procedure.*;

import java.math.BigDecimal;

public class MappedAttributeUtil
{

    public static void forEach(final IntegerProcedure proc, Object o, Object context,
                               Function parentSelector, final NumericAttribute wrappedAttribute)
    {
        if (parentSelector == null) proc.execute(0, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)parentSelector).forEach(parentProc, o, context);
        }
    }

    public static void forEach(final LongProcedure proc, Object o, Object context,
            Function parentSelector, final NumericAttribute wrappedAttribute)
    {
        if (parentSelector == null) proc.execute(0, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)parentSelector).forEach(parentProc, o, context);
        }
    }

    public static void forEach(final FloatProcedure proc, Object o, Object context,
            Function parentSelector, final NumericAttribute wrappedAttribute)
    {
        if (parentSelector == null) proc.execute(0, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)parentSelector).forEach(parentProc, o, context);
        }
    }

    public static void forEach(final DoubleProcedure proc, Object o, Object context,
            Function parentSelector, final NumericAttribute wrappedAttribute)
    {
        if (parentSelector == null) proc.execute(0, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)parentSelector).forEach(parentProc, o, context);
        }
    }

    public static void forEach(final BigDecimalProcedure proc, Object o, Object context,
            Function parentSelector, final NumericAttribute wrappedAttribute)
    {
        if (parentSelector == null) proc.execute(BigDecimal.ZERO, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)parentSelector).forEach(parentProc, o, context);
        }
    }

    public static void forEach(final ObjectProcedure proc, Object o, Object context,
            Function parentSelector, final Attribute wrappedAttribute)
    {
        if (parentSelector == null) proc.execute(0, context);
        else
        {
            ObjectProcedure parentProc = new ObjectProcedure()
            {
                public boolean execute(Object object, Object innerContext)
                {
                    wrappedAttribute.forEach(proc, object, innerContext);
                    return true;
                }
            };
            ((DeepRelationshipAttribute)parentSelector).forEach(parentProc, o, context);
        }
    }

    public static void setValueNull(Object o, Function parentSelector, Attribute wrappedAttribute )
    {
        if (parentSelector == null) return;
        Object result = parentSelector.valueOf(o);
        if (result == null) return;
        wrappedAttribute.setValueNull(result);
    }

    public static boolean isAttributeNull(Object o, Function parentSelector, Attribute wrappedAttribute)
    {
        if (parentSelector == null) return true;
        Object result = parentSelector.valueOf(o);
        if (result == null) return true;
        return wrappedAttribute.isAttributeNull(result);
    }

    public static NumericAttribute plus(Attribute first, NumericAttribute second)
    {
        NumericAttribute numericFirst = (NumericAttribute) first;
        MappedAttribute mappedFirst = (MappedAttribute) first;
        if (second instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) second;
            Mapper commonMapper = mappedFirst.getMapper().getCommonMapper(mappedAttribute.getMapper());
            if (commonMapper != null)
            {
                Mapper mapperRemainder = mappedFirst.getMapper().getMapperRemainder(commonMapper);
                Mapper otherMapperRemainder = mappedAttribute.getMapper().getMapperRemainder(commonMapper);
                NumericAttribute attr = first.createMappedAttributeWithMapperRemainder(mapperRemainder, (NumericAttribute) mappedFirst.getWrappedAttribute());

                NumericAttribute otherAttr = first.createOtherMappedAttributeWithMapperRemainder(mappedAttribute, otherMapperRemainder);
                return numericFirst.getMappedAttributeWithCommonMapper(otherAttr.zDispatchAddTo(attr), commonMapper, mapperRemainder, mappedFirst.getParentSelector());
            }
        }
        return numericFirst.getCalculatedType(second).createCalculatedAttribute(new AdditionCalculator(numericFirst, second));
    }

    public static NumericAttribute minus(Attribute first, NumericAttribute second)
    {
        NumericAttribute numericFirst = (NumericAttribute) first;
        MappedAttribute mappedFirst = (MappedAttribute) first;
        if (second instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) second;
            Mapper commonMapper = mappedFirst.getMapper().getCommonMapper(mappedAttribute.getMapper());
            if (commonMapper != null)
            {
                Mapper mapperRemainder = mappedFirst.getMapper().getMapperRemainder(commonMapper);
                Mapper otherMapperRemainder = mappedAttribute.getMapper().getMapperRemainder(commonMapper);
                NumericAttribute attr = first.createMappedAttributeWithMapperRemainder(mapperRemainder, (NumericAttribute) mappedFirst.getWrappedAttribute());

                NumericAttribute otherAttr = first.createOtherMappedAttributeWithMapperRemainder(mappedAttribute, otherMapperRemainder);
                return numericFirst.getMappedAttributeWithCommonMapper(otherAttr.zDispatchSubtractFrom(attr), commonMapper, mapperRemainder, mappedFirst.getParentSelector());
            }
        }
        return numericFirst.getCalculatedType(second).createCalculatedAttribute(new SubstractionCalculator(numericFirst, second));
    }

    public static NumericAttribute times(Attribute first, NumericAttribute second)
    {
        NumericAttribute numericFirst = (NumericAttribute) first;
        MappedAttribute mappedFirst = (MappedAttribute) first;
        if (second instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) second;
            Mapper commonMapper = mappedFirst.getMapper().getCommonMapper(mappedAttribute.getMapper());
            if (commonMapper != null)
            {
                Mapper mapperRemainder = mappedFirst.getMapper().getMapperRemainder(commonMapper);
                Mapper otherMapperRemainder = mappedAttribute.getMapper().getMapperRemainder(commonMapper);
                NumericAttribute attr = first.createMappedAttributeWithMapperRemainder(mapperRemainder, (NumericAttribute) mappedFirst.getWrappedAttribute());

                NumericAttribute otherAttr = first.createOtherMappedAttributeWithMapperRemainder(mappedAttribute, otherMapperRemainder);
                return numericFirst.getMappedAttributeWithCommonMapper(otherAttr.zDispatchMultiplyBy(attr), commonMapper, mapperRemainder, mappedFirst.getParentSelector());
            }
        }
        return numericFirst.getCalculatedType(second).createCalculatedAttribute(new MultiplicationCalculator(numericFirst, second));
    }

    public static NumericAttribute dividedBy(Attribute first, NumericAttribute second)
    {
        NumericAttribute numericFirst = (NumericAttribute) first;
        MappedAttribute mappedFirst = (MappedAttribute) first;
        if (second instanceof MappedAttribute)
        {
            MappedAttribute mappedAttribute = (MappedAttribute) second;
            Mapper commonMapper = mappedFirst.getMapper().getCommonMapper(mappedAttribute.getMapper());
            if (commonMapper != null)
            {
                Mapper mapperRemainder = mappedFirst.getMapper().getMapperRemainder(commonMapper);
                Mapper otherMapperRemainder = mappedAttribute.getMapper().getMapperRemainder(commonMapper);
                NumericAttribute attr = first.createMappedAttributeWithMapperRemainder(mapperRemainder, (NumericAttribute) mappedFirst.getWrappedAttribute());

                NumericAttribute otherAttr = first.createOtherMappedAttributeWithMapperRemainder(mappedAttribute, otherMapperRemainder);
                return numericFirst.getMappedAttributeWithCommonMapper(otherAttr.zDispatchDivideInto(attr), commonMapper, mapperRemainder, mappedFirst.getParentSelector());
            }
        }
        return numericFirst.getCalculatedType(second).createCalculatedAttribute(new DivisionCalculator(numericFirst, second));
    }

    public static TupleAttribute tupleWith(MappedAttribute first, Attribute attr)
    {
        if (attr instanceof MappedAttribute)
        {
            MappedAttribute second = (MappedAttribute) attr;
            if (first.getMapper().equals(second.getMapper()))
            {
                return new MappedTupleAttribute(first, second);
            }
        }
        throw new MithraBusinessException("Cannot form tuples across relationships. The tuple must be created from attributes of the same object.");
    }

    public static TupleAttribute tupleWith(MappedAttribute first, Attribute... attrs)
    {
        for(Attribute a: attrs)
        {
            if (!(a instanceof MappedAttribute) || ((MappedAttribute)a).getMapper().equals(first.getMapper()))
            {
                throw new MithraBusinessException("Cannot form tuples across relationships. The tuple must be created from attributes of the same object.");
            }
        }
        return new MappedTupleAttribute(first, attrs);
    }

    public static TupleAttribute tupleWith(MappedAttribute first, TupleAttribute attr)
    {
        if (attr instanceof MappedTupleAttribute)
        {
            MappedTupleAttribute second = (MappedTupleAttribute) attr;
            if (first.getMapper().equals(second.getMapper()))
            {
                return new MappedTupleAttribute(first, second);
            }
        }
        throw new MithraBusinessException("Cannot form tuples across relationships. The tuple must be created from attributes of the same object.");
    }
}
