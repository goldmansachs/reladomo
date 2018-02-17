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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.MithraBusinessException;
import org.eclipse.collections.api.iterator.DoubleIterator;
import org.eclipse.collections.api.set.primitive.DoubleSet;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Set;


public class BigDecimalUtil
{
    public static final int MAX_PRECISION = 31;

    public static BigDecimal createBigDecimalFromDouble(double doubleValue, int expectedPrecision, int expectedScale)
    {
        return validateBigDecimalValue(new BigDecimal(doubleValue, new MathContext(expectedPrecision, RoundingMode.HALF_UP)), expectedPrecision, expectedScale);
    }

    /**
     * @deprecated  GS Collections variant of public APIs will be decommissioned in Mar 2019.
     * Use Eclipse Collections variant of the same API instead.
     **/
    @Deprecated
    public static Set<BigDecimal> createBigDecimalSetFromDoubleSet(com.gs.collections.api.set.primitive.DoubleSet doubleSet, int expectedScale, int expectedPrecision)
    {
        Set<BigDecimal> bigDecimalSet = UnifiedSet.newSet(doubleSet.size());
        for (com.gs.collections.api.iterator.DoubleIterator it = doubleSet.doubleIterator(); it.hasNext();)
        {
            bigDecimalSet.add(createBigDecimalFromDouble(it.next(), expectedScale, expectedPrecision));
        }
        return bigDecimalSet;
    }

    public static Set<BigDecimal> createBigDecimalSetFromDoubleSet(DoubleSet doubleSet, int expectedScale, int expectedPrecision)
    {
        Set<BigDecimal> bigDecimalSet = UnifiedSet.newSet(doubleSet.size());
        for (DoubleIterator it = doubleSet.doubleIterator(); it.hasNext();)
        {
            bigDecimalSet.add(createBigDecimalFromDouble(it.next(), expectedScale, expectedPrecision));
        }
        return bigDecimalSet;
    }

    public static Set<BigDecimal> validateBigDecimalSet(Set<BigDecimal> set, int expectedPrecision, int expectedScale)
    {
        Set<BigDecimal> scaledSet = new UnifiedSet<BigDecimal>(set.size());
        for (BigDecimal newValue : set)
        {
            scaledSet.add(validateBigDecimalValue(newValue, expectedPrecision, expectedScale));
        }
        return scaledSet;
    }

    private static boolean isAbsBetweenZeroAndOne(BigDecimal newValue)
    {
        BigDecimal newValueAbs = newValue.abs();
        if(newValueAbs.compareTo(BigDecimal.ZERO) >= 0)
        {
           if(newValueAbs.compareTo(BigDecimal.ONE) <= 0)
           {
               return true;
           }
        }
        return false;
    }

    //todo: moh - review this logic, not sure if the rounding is correct for all cases
    public static BigDecimal validateBigDecimalValue(BigDecimal newValue, int expectedPrecision,int expectedScale)
    {
        if(newValue != null)
        {
            int scale = newValue.scale();
            int precision = newValue.precision();

            if(isAbsBetweenZeroAndOne(newValue))
            {
                newValue = newValue.setScale(expectedScale, java.math.RoundingMode.HALF_UP);
            }
            else
            {
                int integerPart = precision - scale;
                int expectedIntegerPart = expectedPrecision - expectedScale;

                if (integerPart > expectedIntegerPart)
                {
                    throw new MithraBusinessException("Invalid BigDecimal: " + newValue.toPlainString() + ". Expected a BigDecimal with (precision, scale) of: (" + expectedPrecision + "," + expectedScale + ")");
                }
                if (scale != expectedScale || precision > expectedPrecision)
                {
                    scale = expectedScale;
                    newValue = newValue.setScale(scale, java.math.RoundingMode.HALF_UP);
                }
            }
        }
        return newValue;
    }

    public static BigDecimal divide(int scale, BigDecimal dividend, BigDecimal divisor)
    {
        return dividend.divide(divisor, scale, RoundingMode.HALF_UP);
    }
    // From the Sybase Transact-SQL user guide
    public static int calculateQuotientScale(int precision1, int scale1,int precision2,  int scale2)
    {
        return Math.max(scale1 + precision2 -scale2 + 1, 6);        
    }

    public static int calculateQuotientPrecision(int precision1,  int scale1, int precision2, int scale2)
    {
        return Math.max(scale1 + precision2 + 1, 6) + precision1 - scale1 + precision2;
    }

    public static int calculateAdditionScale(int scale1, int scale2)
    {
        return Math.max(scale1, scale2);
    }

    public static int calculateAdditionPrecision(int precision1, int scale1,  int precision2, int scale2)
    {
        int maxPrecision = 31;
        return Math.min(maxPrecision, Math.max(precision1 - scale1,precision2-scale2)+Math.max(scale1,scale2)+1);
    }

    public static int calculateProductScale(int scale1, int scale2)
    {
        return scale1 + scale2;
    }

    public static int calculateProductPrecision(int precision1,int precision2)
    {
        return precision1 + precision2;
    }


    public static int calculateSubstractionScale(int scale1, int scale2)
    {
        return Math.max(scale1,scale2);
    }

    public static int calculateSubstractionPrecision(int precision1,int scale1, int precision2, int scale2)
    {
        return Math.max(precision1 -scale1,precision2-scale2)+Math.max(scale1,scale2)+1;
    }



}
